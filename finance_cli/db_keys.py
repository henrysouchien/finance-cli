"""Per-user SQLCipher key management."""

from __future__ import annotations

import base64
import contextvars
import importlib
import os
import secrets
from pathlib import Path

from . import crypto_envelope
from .exceptions import DBDEKNotFoundError, EnvelopeVersionError, InvalidCiphertextError

_CACHE: contextvars.ContextVar[dict[str, bytes] | None] = contextvars.ContextVar(
    "db_dek_cache",
    default=None,
)


def _namespace() -> str:
    return os.getenv("FINANCE_SECRETS_NAMESPACE", "finance-cli")


def _secret_ref(user_id: str) -> str:
    return f"{_namespace()}/users/{user_id}/db-key"


def _secrets_backend():
    return importlib.import_module("finance_cli.secrets_backend")


def __getattr__(name: str):
    if name == "secrets_backend":
        return _secrets_backend()
    raise AttributeError(name)


def user_db_key_secret_ref(user_id: str) -> str:
    """Return the legacy Secrets Manager ref for a user's DB DEK."""

    return _secret_ref(str(user_id))


def generate_dek() -> bytes:
    return secrets.token_bytes(32)


def _decode_dek(user_id: str, raw: str) -> bytes:
    dek = base64.b64decode(raw, validate=True)
    if len(dek) != 32:
        raise ValueError(f"DEK for user {user_id!r} has unexpected length")
    return dek


def provision_user_db_key(user_id: str, *, data_dir: Path | None = None) -> str:
    """Idempotently provision a per-user DEK secret."""
    ref = _secret_ref(user_id)
    secrets_backend = _secrets_backend()
    existing = secrets_backend.get_secret(ref, missing_ok=True)
    if existing is not None:
        _decode_dek(user_id, existing)
        return ref
    if crypto_envelope.has_db_dek(str(user_id), data_dir=data_dir):
        return ref

    dek = generate_dek()
    secrets_backend.put_secret(ref, base64.b64encode(dek).decode("ascii"))
    return ref


def _get_legacy_user_db_key(user_id: str) -> bytes:
    cache = _CACHE.get()
    if cache is not None and user_id in cache:
        return cache[user_id]

    raw = _secrets_backend().get_secret(_secret_ref(user_id), missing_ok=False)
    if raw is None:
        raise RuntimeError(f"No DEK found for user {user_id!r}")

    dek = _decode_dek(user_id, raw)
    if cache is not None:
        cache[user_id] = dek
    return dek


def get_user_db_key(user_id: str, *, data_dir: Path | None = None) -> bytes:
    """Return the raw 32-byte DEK for a user, preferring db-dek.enc."""
    try:
        return crypto_envelope.get_db_dek(str(user_id), data_dir=data_dir)
    except (DBDEKNotFoundError, EnvelopeVersionError, InvalidCiphertextError):
        return _get_legacy_user_db_key(str(user_id))


def begin_request_cache() -> contextvars.Token[dict[str, bytes] | None]:
    return _CACHE.set({})


def end_request_cache(token: contextvars.Token[dict[str, bytes] | None]) -> None:
    cache = _CACHE.get()
    if cache is not None:
        # Clear stale shared dict refs so post-cleanup callers miss and refetch
        # instead of receiving corrupted zero-byte "DEKs".
        cache.clear()
    _CACHE.reset(token)
    crypto_envelope.evict_caches()


def delete_user_db_key(user_id: str) -> None:
    _secrets_backend().delete_secret(_secret_ref(user_id), recovery_window_days=7)
