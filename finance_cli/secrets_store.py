"""Per-user Secrets Manager helpers for finance_cli."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Literal

from . import crypto_envelope, secrets_backend
from .exceptions import InvalidCiphertextError, ProviderSecretNotFoundError

Provider = Literal["anthropic", "stripe", "snaptrade"]


def _namespace() -> str:
    return str(os.getenv("FINANCE_SECRETS_NAMESPACE") or "finance-cli").strip() or "finance-cli"


def _secret_name(user_id: str, provider: Provider) -> str:
    return f"{_namespace()}/users/{user_id}/{provider}-api-key"


def _is_secrets_manager_ref(ref: str) -> bool:
    value = str(ref or "").strip()
    return bool(
        value.startswith("vault://")
        or value.startswith("arn:aws:secretsmanager:")
        or "/" in value
    )


def _provider_default_path(provider: Provider) -> tuple[str, ...]:
    if provider == "anthropic":
        return ("api_key",)
    if provider == "stripe":
        return ("secret_key",)
    if provider == "snaptrade":
        return ("user_secret",)
    raise ValueError(f"Unsupported provider: {provider!r}")


def _sm_fallback_enabled() -> bool:
    return str(os.getenv("FINANCE_CLI_PROVIDER_SM_FALLBACK") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _coerce_path(path: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(path, str):
        parts = [part for part in path.split("/") if part]
    else:
        parts = [str(part) for part in path]
    normalized = tuple(str(part).strip() for part in parts if str(part).strip())
    if not normalized:
        raise ValueError("provider secret path is required")
    return normalized


def resolve_secret_ref(ref: str, user_id: str, *, missing_ok: bool = False) -> str | None:
    """Resolve a provider secret reference to plaintext.

    Handles both P3 vault:// URIs and legacy Secrets Manager refs during the
    bake window. Plain environment variable names are intentionally rejected.
    """

    value = str(ref or "").strip()
    if value.startswith("vault://"):
        return crypto_envelope.get_provider_secret(str(user_id), value)
    if value.startswith("arn:aws:secretsmanager:") or "/" in value:
        return secrets_backend.get_secret(value, missing_ok=missing_ok)
    raise ValueError(f"Unrecognized secret ref: {ref!r}")


def migrate_provider_ref_to_vault(
    user_id: str,
    *,
    provider: Provider | Literal["plaid"],
    path: str | Sequence[str],
    plaintext: str,
    old_sm_ref: str,
) -> str:
    """Move one legacy SM-backed provider secret into providers.enc.

    The caller is responsible for updating the DB column or queue payload that
    held old_sm_ref. This helper writes the vault entry idempotently and
    schedules the legacy SM ref for soft-delete.
    """

    if str(old_sm_ref or "").startswith("vault://"):
        return str(old_sm_ref)
    path_parts = _coerce_path(path)
    vault_uri = crypto_envelope.format_vault_ref(str(user_id), str(provider), *path_parts)
    try:
        crypto_envelope.set_provider_secret(
            str(user_id),
            vault_uri,
            plaintext,
            migrated_from_ref=str(old_sm_ref),
            require_existing_match=True,
        )
    except InvalidCiphertextError:
        raise
    secrets_backend.delete_secret(str(old_sm_ref))
    return vault_uri


def store_user_api_key(user_id: str, provider: Provider, key: str) -> str:
    if _sm_fallback_enabled():
        name = _secret_name(str(user_id), provider)
        secrets_backend.put_secret(name, key, description=f"{provider} api key for user {user_id}")
        return name
    ref = crypto_envelope.format_vault_ref(str(user_id), provider, *_provider_default_path(provider))
    return crypto_envelope.set_provider_secret(str(user_id), ref, key)


def get_user_api_key(
    user_id: str,
    provider: Provider,
    *,
    secret_ref: str | None = None,
) -> str | None:
    del provider
    if secret_ref and _is_secrets_manager_ref(secret_ref):
        return resolve_secret_ref(secret_ref, str(user_id), missing_ok=True)
    return None


def delete_user_api_key(
    user_id: str,
    provider: Provider,
    *,
    secret_ref: str | None = None,
    force: bool = False,
) -> None:
    if secret_ref and str(secret_ref).strip():
        ref = str(secret_ref).strip()
    else:
        ref = crypto_envelope.format_vault_ref(str(user_id), provider, *_provider_default_path(provider))
    if ref.startswith("vault://"):
        try:
            crypto_envelope.delete_provider_secret(str(user_id), ref)
        except ProviderSecretNotFoundError:
            if secret_ref:
                raise
            secrets_backend.delete_secret(_secret_name(str(user_id), provider), force=force)
        else:
            if not secret_ref:
                secrets_backend.delete_secret(_secret_name(str(user_id), provider), force=force)
        return
    secrets_backend.delete_secret(ref, force=force)
