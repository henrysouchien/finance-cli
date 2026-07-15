"""Shared institution name canonicalization."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path

from . import normalizer_sidecars
from .user_context import get_user_context

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"[^a-z0-9 ]")
_WS_RE = re.compile(r"\s+")
_USER_HOME_ENV = "FINANCE_CLI_HOME"
_USER_REGISTRY_ENV = "FINANCE_CLI_INSTITUTION_NAMES_PATH"

# Normalized input key -> canonical institution label.
CANONICAL_NAMES: dict[str, str] = {
    # Bank of America
    "bank of america": "Bank of America",
    "bofa": "Bank of America",
    "boa": "Bank of America",
    "bofa checking": "Bank of America",
    "boa checking": "Bank of America",
    "bank of america checking": "Bank of America",
    "bofa credit": "Bank of America",
    "boa credit": "Bank of America",
    "bank of america credit": "Bank of America",
    "bank of america credit card": "Bank of America",
    # American Express
    "amex": "American Express",
    "american express": "American Express",
    "american express company": "American Express",
    # Chase
    "chase": "Chase",
    "chase credit": "Chase",
    "chase credit card": "Chase",
    "chase checking": "Chase",
    "jpmorgan chase": "Chase",
    # Barclays
    "barclays": "Barclays",
    "barclays cards": "Barclays",
    "barclays card": "Barclays",
    "barclays us": "Barclays",
    # Apple Card
    "apple": "Apple Card",
    "apple card": "Apple Card",
    "apple card inc": "Apple Card",
    "apple card goldman sachs bank usa": "Apple Card",
    "goldman sachs": "Apple Card",
    "goldman sachs bank usa": "Apple Card",
    # Citi
    "citi": "Citi",
    "citibank": "Citi",
    # Other
    "bloomingdales": "Bloomingdale's",
    "bloomingdale s": "Bloomingdale's",
    "bloomingdale s citibank n a": "Bloomingdale's",
    "schwab": "Schwab",
    "charles schwab": "Schwab",
    "venmo": "Venmo",
    "venmo personal": "Venmo",
    "paypal": "PayPal",
    "paypal credit syncb": "PayPal",
    "paypal credit": "PayPal",
    "merrill": "Merrill",
}


@dataclass
class _UserRegistryCache:
    signature: str | None = None
    names: dict[str, str] = field(default_factory=dict)


_USER_REGISTRY_CACHES: dict[Path, _UserRegistryCache] = {}
_USER_REGISTRY_CACHE_LOCK = threading.RLock()


def _default_user_data_dir() -> Path:
    override = os.getenv(_USER_HOME_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return (Path.home() / ".finance_cli").expanduser().resolve()


def user_registry_path() -> Path:
    override = os.getenv(_USER_REGISTRY_ENV)
    if override:
        return Path(override).expanduser().resolve()
    user_context = get_user_context()
    if (
        user_context is not None
        and not user_context.local_mode
        and user_context.expected_user_id is not None
    ):
        return (
            Path(user_context.db_path).expanduser().resolve().parent
            / "institution_names.json"
        )
    return _default_user_data_dir() / "institution_names.json"


def _remote_registry_target() -> tuple[str, str] | None:
    if os.getenv(_USER_REGISTRY_ENV):
        return None
    return normalizer_sidecars.remote_sidecar_target()


def normalize_key(value: str) -> str:
    """Normalize institution name to lookup key format."""
    normalized = _TOKEN_RE.sub(" ", str(value or "").strip().lower())
    return _WS_RE.sub(" ", normalized).strip()


def _load_user_registry(force: bool = False) -> dict[str, str]:
    path = user_registry_path()
    remote_target = _remote_registry_target()
    with _USER_REGISTRY_CACHE_LOCK:
        cache = _USER_REGISTRY_CACHES.setdefault(path, _UserRegistryCache())
        content: str | None
        signature: str
        if remote_target is not None:
            content = normalizer_sidecars.read_text(
                "institution_names.json",
                target_info=remote_target,
                missing_ok=True,
            )
            if content is None:
                if force or cache.signature is not None:
                    cache.signature = None
                    cache.names = {}
                return cache.names
            signature = "remote:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
        else:
            try:
                stat = path.stat()
            except FileNotFoundError:
                if force or cache.signature is not None:
                    cache.signature = None
                    cache.names = {}
                return cache.names

            signature = f"local:{stat.st_mtime_ns}"
            content = path.read_text(encoding="utf-8")

        if not force and cache.signature == signature:
            return cache.names

        names: dict[str, str] = {}
        try:
            payload = json.loads(content)
            raw_names = payload.get("canonical_names", {})
            if not isinstance(raw_names, dict):
                raise ValueError("'canonical_names' must be a JSON object")
            for raw_key, raw_value in raw_names.items():
                key = normalize_key(str(raw_key))
                value = str(raw_value or "").strip()
                if not key or not value:
                    continue
                names[key] = value
        except Exception as exc:
            logger.warning(
                "Failed to load user institution registry path=%s error=%s", path, exc
            )
            names = {}

        cache.signature = signature
        cache.names = names
        return cache.names


def _write_user_registry(names: dict[str, str]) -> None:
    path = user_registry_path()
    remote_target = _remote_registry_target()
    with _USER_REGISTRY_CACHE_LOCK:
        payload = {"canonical_names": dict(sorted(names.items()))}
        rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if remote_target is not None:
            normalizer_sidecars.write_text(
                "institution_names.json",
                rendered,
                target_info=remote_target,
            )
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(rendered, encoding="utf-8")
        _load_user_registry(force=True)


def user_canonical_names() -> dict[str, str]:
    """Return the user-space canonical registry."""
    return dict(_load_user_registry())


def register_user_institution(
    canonical_name: str, aliases: list[str] | None = None
) -> dict[str, object]:
    """Append new institution names to the user-space registry."""
    canonical_label = str(canonical_name or "").strip()
    if not canonical_label:
        raise ValueError("canonical_name is required")

    requested_keys: list[str] = []
    for raw_name in [canonical_label, *(aliases or [])]:
        key = normalize_key(raw_name)
        if not key:
            continue
        if key in CANONICAL_NAMES:
            raise ValueError(
                f"Institution name '{raw_name}' already exists in the built-in registry"
            )
        requested_keys.append(key)

    if not requested_keys:
        raise ValueError("at least one non-empty institution name is required")

    with _USER_REGISTRY_CACHE_LOCK:
        current = dict(_load_user_registry())
        added_keys: list[str] = []
        for key in requested_keys:
            existing = current.get(key)
            if existing is None:
                current[key] = canonical_label
                added_keys.append(key)
                continue
            if existing != canonical_label:
                raise ValueError(
                    f"Institution name '{key}' is already registered to '{existing}' in the user registry"
                )

        _write_user_registry(current)
    return {
        "canonical_name": canonical_label,
        "registered_keys": sorted(set(requested_keys)),
        "added_keys": sorted(added_keys),
        "changed": bool(added_keys),
        "path": str(user_registry_path()),
    }


def canonicalize(institution_name: str) -> str:
    """Return canonical institution name or the stripped input if unknown."""
    stripped = str(institution_name or "").strip()
    if not stripped:
        return ""
    key = normalize_key(stripped)
    if key in CANONICAL_NAMES:
        return CANONICAL_NAMES[key]
    return _load_user_registry().get(key, stripped)


def is_known(institution_name: str) -> bool:
    """True when institution_name resolves through the built-in or user registry."""
    stripped = str(institution_name or "").strip()
    if not stripped:
        return False
    key = normalize_key(stripped)
    return key in CANONICAL_NAMES or key in _load_user_registry()


def _tokenize(value: str) -> set[str]:
    normalized = normalize_key(value)
    if not normalized:
        return set()
    return {token for token in normalized.split(" ") if token}


def similar_names(name_a: str, name_b: str) -> bool:
    """Textual similarity heuristic for audit reporting."""
    norm_a = normalize_key(name_a)
    norm_b = normalize_key(name_b)
    if not norm_a or not norm_b:
        return False
    if norm_a in norm_b or norm_b in norm_a:
        return True

    tokens_a = _tokenize(norm_a)
    tokens_b = _tokenize(norm_b)
    if not tokens_a or not tokens_b:
        return False
    shared = len(tokens_a & tokens_b)
    smaller = min(len(tokens_a), len(tokens_b))
    return shared * 2 >= smaller
