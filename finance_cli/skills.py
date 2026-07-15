"""Skill registry and loader."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SKILL_FILES: dict[str, str] = {
    "normalizer_builder": "NORMALIZER_BUILDER_SKILL.md",
    "onboarding": "ONBOARDING_SKILL.md",
    "coach_debt_payoff": "COACH_DEBT_PAYOFF_SKILL.md",
    "coach_emergency_fund": "COACH_EMERGENCY_FUND_SKILL.md",
    "coach_savings_goal": "COACH_SAVINGS_GOAL_SKILL.md",
    "coach_spending_plan": "COACH_SPENDING_PLAN_SKILL.md",
    "coach_homebuying_readiness": "COACH_HOMEBUYING_READINESS_SKILL.md",
    "coach_retirement_contribution_readiness": "COACH_RETIREMENT_CONTRIBUTION_READINESS_SKILL.md",
    "coach_retirement_income_readiness": "COACH_RETIREMENT_INCOME_READINESS_SKILL.md",
    "coach_investment_readiness": "COACH_INVESTMENT_READINESS_SKILL.md",
    "coach_financial_plan_intake": "COACH_FINANCIAL_PLAN_INTAKE_SKILL.md",
    "coach_estate_document_readiness": "COACH_ESTATE_DOCUMENT_READINESS_SKILL.md",
    "coach_risk_insurance_readiness": "COACH_RISK_INSURANCE_READINESS_SKILL.md",
    "coach_advisor_handoff_readiness": "COACH_ADVISOR_HANDOFF_READINESS_SKILL.md",
    "coach_tax_readiness": "COACH_TAX_READINESS_SKILL.md",
}

_SKILLS_DIR = Path(__file__).resolve().parent.parent / "docs" / "skills"
_FRONTMATTER_DELIMITER = "---"
_SKILL_STATE_CLASSES = frozenset(
    {"advisor-no-state", "advisor-with-decision-log", "deprecated", "producer"}
)
_PROFILE_EXPORT_FIELDS = (
    "version",
    "model",
    "max_turns",
    "timeout",
    "tool_packs",
    "tool_packs_enabled",
    "persist_state",
    "scope",
    "interactive",
    "metadata",
    "mcp_servers",
    "session_inject_servers",
    "timeout_overrides",
    "state_dir",
    "max_budget_usd",
    "max_retries",
    "initial_message",
    "delivery_label",
    "agent_callable",
    "agent_description",
    "mode",
    "extra_excluded_tools",
)


@dataclass
class SkillProfile:
    """Parsed markdown skill definition used by CashNerd skill loading."""

    name: str
    system_prompt: str
    version: str | None = None
    model: str | None = None
    max_turns: int | None = None
    timeout: float | None = None
    tool_packs: list[str] | None = None
    persist_state: bool = False
    scope: str | None = None
    interactive: bool = False
    metadata: dict[str, Any] | None = None
    mcp_servers: list[str] | None = None
    session_inject_servers: list[str] | None = None
    timeout_overrides: dict[str, int] | None = None
    state_dir: str | None = None
    max_budget_usd: float | None = None
    max_retries: int | None = None
    initial_message: str | None = None
    delivery_label: str | None = None
    agent_callable: bool = False
    agent_description: str | None = None
    resumable: bool = False
    resume_mcp_session_reset_ok: bool = False
    mode: str = "full"
    extra_excluded_tools: set[str] = field(default_factory=set)
    tool_packs_enabled: bool = True
    provider: str | None = None
    state_class: str | None = None


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_optional_bool(value: Any, *, field_name: str, path: Path) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1", "on"}:
            return True
        if normalized in {"false", "no", "0", "off"}:
            return False
    raise ValueError(f"{path}: '{field_name}' must be a boolean")


def _coerce_optional_int(value: Any, *, field_name: str, path: Path) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{path}: '{field_name}' must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text:
            try:
                return int(text)
            except ValueError as exc:
                raise ValueError(f"{path}: '{field_name}' must be an integer") from exc
    raise ValueError(f"{path}: '{field_name}' must be an integer")


def _coerce_optional_float(value: Any, *, field_name: str, path: Path) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{path}: '{field_name}' must be a number")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if text:
            try:
                return float(text)
            except ValueError as exc:
                raise ValueError(f"{path}: '{field_name}' must be a number") from exc
    raise ValueError(f"{path}: '{field_name}' must be a number")


def _coerce_optional_string_list(
    value: Any,
    *,
    field_name: str,
    path: Path,
) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        raw_items = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raise ValueError(f"{path}: '{field_name}' must be a list of strings")

    items = [_clean_string(item) for item in raw_items]
    result = [item for item in items if item]
    return result or None


def _coerce_optional_string_set(value: Any, *, field_name: str, path: Path) -> set[str]:
    items = _coerce_optional_string_list(value, field_name=field_name, path=path)
    return set(items or [])


def _coerce_optional_timeout_overrides(
    value: Any,
    *,
    field_name: str,
    path: Path,
) -> dict[str, int] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{path}: '{field_name}' must be a mapping")

    overrides: dict[str, int] = {}
    for raw_name, raw_timeout in value.items():
        server_name = _clean_string(raw_name)
        if server_name is None:
            continue
        timeout_value = _coerce_optional_int(
            raw_timeout,
            field_name=f"{field_name}.{server_name}",
            path=path,
        )
        if timeout_value is not None:
            overrides[server_name] = timeout_value
    return overrides or None


def _coerce_optional_scope(value: Any, *, field_name: str, path: Path) -> str | None:
    text = _clean_string(value)
    if text is None:
        return None
    if text not in {"ticker", "portfolio", "industry"}:
        raise ValueError(
            f"{path}: invalid '{field_name}' {text!r}: expected ticker, portfolio, or industry"
        )
    return text


def _coerce_mode(value: Any, *, field_name: str, path: Path) -> str:
    text = _clean_string(value)
    if text is None:
        return "full"
    if text not in {"full", "recommend"}:
        raise ValueError(f"{path}: '{field_name}' must be 'full' or 'recommend'")
    return text


def _coerce_optional_state_class(value: Any, *, field_name: str, path: Path) -> str | None:
    text = _clean_string(value)
    if text is None:
        return None
    if text not in _SKILL_STATE_CLASSES:
        allowed = ", ".join(sorted(_SKILL_STATE_CLASSES))
        raise ValueError(f"{path}: '{field_name}' must be one of: {allowed}")
    return text


def _split_frontmatter(text: str, *, path: Path) -> tuple[dict[str, Any], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != _FRONTMATTER_DELIMITER:
        return {}, text

    for index, line in enumerate(lines[1:], start=1):
        if line.strip() != _FRONTMATTER_DELIMITER:
            continue
        frontmatter_text = "\n".join(lines[1:index])
        body = "\n".join(lines[index + 1 :])
        payload = yaml.safe_load(frontmatter_text) or {}
        if not isinstance(payload, dict):
            raise ValueError(f"{path}: skill frontmatter must be a YAML mapping")
        return payload, body

    return {}, text


def parse_skill_file(path: Path) -> SkillProfile:
    """Parse a markdown skill file into a `SkillProfile`."""
    text = path.read_text(encoding="utf-8")
    frontmatter, body = _split_frontmatter(text, path=path)

    raw_name = frontmatter.pop("name", None)
    raw_version = frontmatter.pop("version", None)
    raw_model = frontmatter.pop("model", None)
    raw_provider = frontmatter.pop("provider", None)
    raw_max_turns = frontmatter.pop("max_turns", None)
    raw_timeout = frontmatter.pop("timeout", None)
    raw_tool_packs = frontmatter.pop("tool_packs", None)
    raw_persist_state = frontmatter.pop("persist_state", None)
    raw_scope = frontmatter.pop("scope", None)
    raw_interactive = frontmatter.pop("interactive", None)
    raw_agent_callable = frontmatter.pop("agent_callable", None)
    raw_agent_description = frontmatter.pop("agent_description", None)
    raw_resumable = frontmatter.pop("resumable", None)
    raw_resume_mcp_session_reset_ok = frontmatter.pop("resume_mcp_session_reset_ok", None)
    raw_mode = frontmatter.pop("mode", None)
    raw_state_class = frontmatter.pop("state_class", None)
    raw_extra_excluded_tools = frontmatter.pop("extra_excluded_tools", None)
    raw_tool_packs_enabled = frontmatter.pop("tool_packs_enabled", None)
    raw_metadata = frontmatter.pop("metadata", None)

    if raw_metadata is None:
        metadata: dict[str, Any] = {}
    elif isinstance(raw_metadata, dict):
        metadata = dict(raw_metadata)
    else:
        raise ValueError(f"{path}: 'metadata' must be a mapping when provided")

    metadata.update(frontmatter)
    original_metadata_keys = set(metadata.keys())

    raw_mcp_servers = metadata.pop("mcp_servers", None)
    raw_session_inject_servers = metadata.pop("session_inject_servers", None)
    raw_timeout_overrides = metadata.pop("timeout_overrides", None)
    raw_state_dir = metadata.pop("state_dir", None)
    raw_max_budget_usd = metadata.pop("max_budget_usd", None)
    raw_max_retries = metadata.pop("max_retries", None)
    raw_initial_message = metadata.pop("initial_message", None)
    raw_delivery_label = metadata.pop("delivery_label", None)

    coerced_mcp_servers = _coerce_optional_string_list(
        raw_mcp_servers,
        field_name="mcp_servers",
        path=path,
    )
    coerced_session_inject_servers = _coerce_optional_string_list(
        raw_session_inject_servers,
        field_name="session_inject_servers",
        path=path,
    )
    coerced_timeout_overrides = _coerce_optional_timeout_overrides(
        raw_timeout_overrides,
        field_name="timeout_overrides",
        path=path,
    )
    coerced_state_dir = _clean_string(raw_state_dir)
    coerced_max_budget_usd = _coerce_optional_float(
        raw_max_budget_usd,
        field_name="max_budget_usd",
        path=path,
    )
    coerced_max_retries = _coerce_optional_int(
        raw_max_retries,
        field_name="max_retries",
        path=path,
    )
    coerced_initial_message = _clean_string(raw_initial_message)
    coerced_delivery_label = _clean_string(raw_delivery_label)
    coerced_agent_callable = _coerce_optional_bool(
        raw_agent_callable,
        field_name="agent_callable",
        path=path,
    )
    coerced_agent_description = _clean_string(raw_agent_description)
    coerced_resumable = _coerce_optional_bool(
        raw_resumable,
        field_name="resumable",
        path=path,
    )
    coerced_resume_mcp_session_reset_ok = _coerce_optional_bool(
        raw_resume_mcp_session_reset_ok,
        field_name="resume_mcp_session_reset_ok",
        path=path,
    )
    coerced_mode = _coerce_mode(raw_mode, field_name="mode", path=path)
    coerced_state_class = _coerce_optional_state_class(
        raw_state_class,
        field_name="state_class",
        path=path,
    )
    coerced_extra_excluded_tools = _coerce_optional_string_set(
        raw_extra_excluded_tools,
        field_name="extra_excluded_tools",
        path=path,
    )
    coerced_tool_packs_enabled = (
        True
        if raw_tool_packs_enabled is None
        else _coerce_optional_bool(
            raw_tool_packs_enabled,
            field_name="tool_packs_enabled",
            path=path,
        )
    )

    for key, coerced in [
        ("mcp_servers", coerced_mcp_servers),
        ("session_inject_servers", coerced_session_inject_servers),
        ("timeout_overrides", coerced_timeout_overrides),
        ("state_dir", coerced_state_dir),
        ("max_budget_usd", coerced_max_budget_usd),
        ("max_retries", coerced_max_retries),
        ("initial_message", coerced_initial_message),
        ("delivery_label", coerced_delivery_label),
    ]:
        if key in original_metadata_keys:
            metadata[key] = coerced

    if coerced_resumable and coerced_session_inject_servers and not coerced_resume_mcp_session_reset_ok:
        raise ValueError(
            f"{path}: 'resumable: true' with 'session_inject_servers' requires "
            "'resume_mcp_session_reset_ok: true'"
        )

    return SkillProfile(
        name=_clean_string(raw_name) or path.stem,
        system_prompt=body.strip(),
        version=_clean_string(raw_version),
        model=_clean_string(raw_model),
        max_turns=_coerce_optional_int(raw_max_turns, field_name="max_turns", path=path),
        timeout=_coerce_optional_float(raw_timeout, field_name="timeout", path=path),
        tool_packs=_coerce_optional_string_list(raw_tool_packs, field_name="tool_packs", path=path),
        persist_state=_coerce_optional_bool(
            raw_persist_state,
            field_name="persist_state",
            path=path,
        ),
        scope=_coerce_optional_scope(raw_scope, field_name="scope", path=path),
        interactive=_coerce_optional_bool(raw_interactive, field_name="interactive", path=path),
        metadata=metadata or None,
        mcp_servers=coerced_mcp_servers,
        session_inject_servers=coerced_session_inject_servers,
        timeout_overrides=coerced_timeout_overrides,
        state_dir=coerced_state_dir,
        max_budget_usd=coerced_max_budget_usd,
        max_retries=coerced_max_retries,
        initial_message=coerced_initial_message,
        delivery_label=coerced_delivery_label,
        agent_callable=coerced_agent_callable,
        agent_description=coerced_agent_description,
        resumable=coerced_resumable,
        resume_mcp_session_reset_ok=coerced_resume_mcp_session_reset_ok,
        mode=coerced_mode,
        extra_excluded_tools=coerced_extra_excluded_tools,
        tool_packs_enabled=coerced_tool_packs_enabled,
        provider=_clean_string(raw_provider),
        state_class=coerced_state_class,
    )


def _serialize_profile(profile: SkillProfile) -> dict[str, Any]:
    payload = {name: getattr(profile, name) for name in _PROFILE_EXPORT_FIELDS}
    payload["extra_excluded_tools"] = sorted(profile.extra_excluded_tools)
    return payload


def _available_skills() -> list[str]:
    return list(SKILL_FILES.keys())


def _skill_path(name: str) -> tuple[str | None, Path | None]:
    filename = SKILL_FILES.get(name)
    if filename is None:
        return None, None
    return filename, _SKILLS_DIR / filename


def load_skill_profile(name: str) -> SkillProfile | None:
    """Load and parse a skill profile by registry name."""
    filename, skill_path = _skill_path(name)
    del filename
    if skill_path is None or not skill_path.exists():
        return None
    return parse_skill_file(skill_path)


def load_skill(name: str) -> dict:
    """Load a skill file by registry name."""
    filename, skill_path = _skill_path(name)
    if filename is None or skill_path is None:
        available = _available_skills()
        return {
            "data": {"available": available},
            "summary": {"error": "Unknown skill", "available": available},
        }

    if not skill_path.exists():
        return {
            "data": {"error": f"Skill file not found: {filename}"},
            "summary": {"error": f"Skill file not found: {filename}"},
        }

    profile = parse_skill_file(skill_path)
    content = profile.system_prompt.strip()
    return {
        "data": {
            "name": name,
            "content": content,
            "profile": _serialize_profile(profile),
        },
        "summary": {
            "skill": name,
            "file": filename,
            "lines": len(content.splitlines()),
        },
    }
