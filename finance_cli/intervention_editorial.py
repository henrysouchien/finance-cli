"""LLM editorial cache for intervention-backed UI surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import threading
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

from .ai_client import default_model, send_request
from .ai_egress import normalize_ai_egress_mode
from .billing import has_active_engagement, resolve_request
from .cost_tracking import estimate_ai_cost_usd6, record_and_settle_cost

logger = logging.getLogger(__name__)

EDITORIAL_TTL_SECONDS = 4 * 60 * 60
_MAX_HEADLINE_CHARS = 140
_MAX_BULLET_CHARS = 180
_MAX_BULLETS = 2
_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class EditorialEntry:
    headline: str
    detail_bullets: tuple[str, ...]
    generated_at: datetime
    expires_at: datetime
    provider: str
    model: str

    def as_payload(self) -> dict[str, Any]:
        return {
            "headline": self.headline,
            "detail_bullets": list(self.detail_bullets),
            "generated_at": self.generated_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "provider": self.provider,
            "model": self.model,
        }


@dataclass(frozen=True)
class EditorialRequest:
    key: str
    surface: str
    intervention: dict[str, Any]
    profile: dict[str, Any]


_CACHE: dict[str, EditorialEntry] = {}
_IN_FLIGHT: set[str] = set()
_LOCK = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _user_hash(user_id: str | int) -> str:
    return hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()[:16]


def _clean_text(value: Any, *, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _fingerprint(surface: str, user_id: str | int, profile: Mapping[str, Any], intervention: Mapping[str, Any]) -> str:
    relevant = {
        "surface": surface,
        "user_hash": _user_hash(user_id),
        "profile_hash": _json_hash(profile),
        "intervention": {
            "pattern_id": intervention.get("pattern_id"),
            "move": intervention.get("move"),
            "headline": intervention.get("headline"),
            "detail_bullets": intervention.get("detail_bullets"),
            "tier4_ladder": intervention.get("tier4_ladder"),
            "action": intervention.get("action"),
            "dollar_impact_cents": intervention.get("dollar_impact_cents"),
            "goal_link": intervention.get("goal_link"),
        },
    }
    return _json_hash(relevant)


def _get_cache_hit(key: str, now: datetime) -> EditorialEntry | None:
    with _LOCK:
        entry = _CACHE.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            _CACHE.pop(key, None)
            return None
        return entry


def clear_editorial_cache() -> None:
    """Clear process-local editorial cache state. Intended for tests."""
    with _LOCK:
        _CACHE.clear()
        _IN_FLIGHT.clear()


def editorial_cache_enabled() -> bool:
    raw = str(os.getenv("CASHNERD_EDITORIAL_CACHE_ENABLED", "") or "").strip().lower()
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return str(os.getenv("APP_ENV", "") or "").strip().lower() == "production"


def build_financial_profile(summary_data: Mapping[str, Any]) -> dict[str, Any]:
    onboarding = summary_data.get("onboarding_state")
    onboarding_map = onboarding if isinstance(onboarding, Mapping) else {}
    goals = summary_data.get("goals")
    alerts = summary_data.get("budget_alerts")
    stat_annotations = summary_data.get("stat_annotations")
    stats = stat_annotations if isinstance(stat_annotations, Mapping) else {}
    return {
        "months_of_history": int(onboarding_map.get("months_of_history") or 0),
        "transaction_count": int(onboarding_map.get("transaction_count") or 0),
        "vendor_memory_count": int(onboarding_map.get("vendor_memory_count") or 0),
        "categorization_rate": round(float(onboarding_map.get("categorization_rate") or 0), 3),
        "active_goal_count": len(goals) if isinstance(goals, Sequence) and not isinstance(goals, (str, bytes)) else 0,
        "budget_alert_count": len(alerts) if isinstance(alerts, Sequence) and not isinstance(alerts, (str, bytes)) else 0,
        "stat_annotation_slots": sorted(key for key, value in stats.items() if value),
    }


def attach_cached_editorials(
    surfaces: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    user_id: str | int,
    profile: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], list[EditorialRequest]]:
    resolved_now = now or _now()
    profile_payload = dict(profile)
    enriched_surfaces: dict[str, list[dict[str, Any]]] = {}
    missing: list[EditorialRequest] = []

    for surface, interventions in surfaces.items():
        enriched: list[dict[str, Any]] = []
        for raw in interventions:
            item = dict(raw)
            key = _fingerprint(surface, user_id, profile_payload, item)
            cached = _get_cache_hit(key, resolved_now)
            if cached is not None:
                item["editorial"] = cached.as_payload()
            else:
                item.setdefault("editorial", None)
                missing.append(
                    EditorialRequest(
                        key=key,
                        surface=surface,
                        intervention=item,
                        profile=profile_payload,
                    )
                )
            enriched.append(item)
        enriched_surfaces[surface] = enriched

    return enriched_surfaces, missing


def _claim_requests(requests: Sequence[EditorialRequest], now: datetime) -> list[EditorialRequest]:
    claimed: list[EditorialRequest] = []
    with _LOCK:
        for request in requests:
            entry = _CACHE.get(request.key)
            if entry is not None and entry.expires_at > now:
                continue
            if request.key in _IN_FLIGHT:
                continue
            _IN_FLIGHT.add(request.key)
            claimed.append(request)
    return claimed


def _release_requests(requests: Sequence[EditorialRequest]) -> None:
    with _LOCK:
        for request in requests:
            _IN_FLIGHT.discard(request.key)


def _provider_key(provider: str) -> str | None:
    explicit = str(os.getenv("CASHNERD_EDITORIAL_API_KEY", "") or "").strip()
    if explicit:
        return explicit
    if provider == "claude":
        return (
            str(os.getenv("ANTHROPIC_API_KEY", "") or "").strip()
            or str(os.getenv("ANTHROPIC_AUTH_TOKEN", "") or "").strip()
            or None
        )
    if provider == "openai":
        return str(os.getenv("OPENAI_API_KEY", "") or "").strip() or None
    return None


def _resolve_provider_model(provider: str | None = None, model: str | None = None) -> tuple[str, str, str | None]:
    provider_name = str(provider or os.getenv("CASHNERD_EDITORIAL_AI_PROVIDER", "") or "claude").strip().lower()
    model_name = str(model or os.getenv("CASHNERD_EDITORIAL_AI_MODEL", "") or "").strip()
    if not model_name:
        model_name = default_model(provider_name)
    return provider_name, model_name, _provider_key(provider_name)


def _billing_settings(settings: Any | None) -> Any:
    return SimpleNamespace(
        stripe_price_lite=str(getattr(settings, "stripe_price_lite", "") or os.getenv("STRIPE_PRICE_LITE", ""))
    )


def _resolve_billable_model(
    *,
    user: Mapping[str, Any] | None,
    db_path: str | Path | None,
    settings: Any | None,
    provider: str,
    model: str,
) -> tuple[str, str, bool] | None:
    if user is None or db_path is None:
        return provider, model, False
    if normalize_ai_egress_mode(user.get("ai_egress_mode")) != "full":
        return None
    if not has_active_engagement(user):
        return None

    resolution = resolve_request(
        user,
        Path(db_path),
        _billing_settings(settings),
        explicit_model=model if provider == "claude" else None,
    )
    if resolution.action == "block":
        return None
    if resolution.mode == "byok":
        logger.info("editorial_cache_skip reason=byok user_id=%s", user.get("user_id"))
        return None
    if resolution.action == "downgrade":
        downgraded = resolution.effective_model
        if downgraded.startswith("claude-"):
            return "claude", downgraded, False
    return provider, resolution.effective_model if provider == "claude" else model, False


_SYSTEM_PROMPT = """You write short CashNerd dashboard editorial copy.
Return strict JSON only: {"items":[{"key":"...","headline":"...","detail_bullets":["..."]}]}.
Rules:
- Preserve the deterministic recommendation. Do not invent accounts, dates, amounts, categories, or actions.
- Lead with the specific next move, not a report of the user's data.
- Use dollar amounts already present in the input when they matter.
- Keep each headline under 120 characters and each bullet under 140 characters.
- No shame, panic, hype, or generic praise.
- If the deterministic copy is already best, return it lightly tightened."""


def _build_user_prompt(requests: Sequence[EditorialRequest]) -> str:
    items: list[dict[str, Any]] = []
    for request in requests:
        intervention = request.intervention
        action = intervention.get("action")
        action_map = action if isinstance(action, Mapping) else {}
        items.append(
            {
                "key": request.key,
                "surface": request.surface,
                "deterministic_headline": intervention.get("headline"),
                "detail_bullets": intervention.get("detail_bullets") or [],
                "tier4_ladder": intervention.get("tier4_ladder"),
                "dollar_impact_cents": intervention.get("dollar_impact_cents"),
                "move": intervention.get("move"),
                "action_label": action_map.get("label"),
                "profile": request.profile,
            }
        )
    return json.dumps({"items": items}, sort_keys=True, ensure_ascii=True)


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end <= start:
            raise ValueError("editorial response did not contain a JSON object")
        decoded = json.loads(text[start : end + 1])
    if not isinstance(decoded, dict):
        raise ValueError("editorial response was not a JSON object")
    return decoded


def _parse_response(raw: str) -> dict[str, tuple[str, tuple[str, ...]]]:
    decoded = _extract_json_object(raw)
    items = decoded.get("items")
    if not isinstance(items, list):
        raise ValueError("editorial response missing items list")

    parsed: dict[str, tuple[str, tuple[str, ...]]] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        key = str(item.get("key") or "").strip()
        headline = _clean_text(item.get("headline"), limit=_MAX_HEADLINE_CHARS)
        if not key or not headline:
            continue
        bullets_raw = item.get("detail_bullets") or []
        bullets = []
        if isinstance(bullets_raw, list):
            for bullet in bullets_raw:
                text = _clean_text(bullet, limit=_MAX_BULLET_CHARS)
                if text:
                    bullets.append(text)
                if len(bullets) >= _MAX_BULLETS:
                    break
        parsed[key] = (headline, tuple(bullets))
    return parsed


def warm_editorial_cache(
    requests: Sequence[EditorialRequest],
    *,
    user: Mapping[str, Any] | None = None,
    db_path: str | Path | None = None,
    settings: Any | None = None,
    provider: str | None = None,
    model: str | None = None,
    send_fn: Callable[..., tuple[str, dict[str, int]]] = send_request,
    now: datetime | None = None,
) -> dict[str, Any]:
    resolved_now = now or _now()
    claimed = _claim_requests(requests, resolved_now)
    if not claimed:
        return {"warmed": 0, "skipped": "no_misses"}

    try:
        provider_name, model_name, api_key = _resolve_provider_model(provider, model)
        billable = _resolve_billable_model(
            user=user,
            db_path=db_path,
            settings=settings,
            provider=provider_name,
            model=model_name,
        )
        if billable is None:
            return {"warmed": 0, "skipped": "policy_or_billing"}
        provider_name, model_name, is_byok = billable
        api_key = _provider_key(provider_name)
        if not api_key:
            return {"warmed": 0, "skipped": "api_key_missing"}

        raw, usage = send_fn(
            provider_name,
            system_prompt=_SYSTEM_PROMPT,
            user_prompt=_build_user_prompt(claimed),
            model=model_name,
            max_tokens=800 if provider_name == "claude" else None,
            temperature=0,
            timeout=60,
            api_key=api_key,
        )
        parsed = _parse_response(raw)
        expires_at = resolved_now + timedelta(seconds=EDITORIAL_TTL_SECONDS)
        warmed = 0
        with _LOCK:
            for request in claimed:
                values = parsed.get(request.key)
                if values is None:
                    continue
                headline, bullets = values
                _CACHE[request.key] = EditorialEntry(
                    headline=headline,
                    detail_bullets=bullets,
                    generated_at=resolved_now,
                    expires_at=expires_at,
                    provider=provider_name,
                    model=model_name,
                )
                warmed += 1

        input_tokens = int(usage.get("input_tokens") or 0)
        output_tokens = int(usage.get("output_tokens") or 0)
        cost_usd6 = estimate_ai_cost_usd6(
            provider_name,
            model=model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        if db_path is not None and (input_tokens or output_tokens or cost_usd6):
            record_and_settle_cost(
                db_path,
                provider_name,
                "editorial",
                cost_usd6,
                idempotency_key=(
                    "editorial_"
                    + _json_hash(
                        {
                            "generated_at": resolved_now.isoformat(),
                            "keys": [request.key for request in claimed],
                        }
                    )[:32]
                ),
                is_byok=is_byok,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                model=model_name,
                is_estimated=True,
            )
        return {"warmed": warmed, "skipped": None}
    except Exception:
        logger.exception("editorial_cache_warm_failed")
        return {"warmed": 0, "skipped": "error"}
    finally:
        _release_requests(claimed)


__all__ = [
    "EDITORIAL_TTL_SECONDS",
    "EditorialRequest",
    "attach_cached_editorials",
    "build_financial_profile",
    "clear_editorial_cache",
    "editorial_cache_enabled",
    "warm_editorial_cache",
]
