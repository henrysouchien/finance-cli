"""Backup bundle crypto helpers for encrypted v2 bundles."""

from __future__ import annotations

import base64
import hashlib
import hmac
import importlib
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from . import crypto_envelope

MAGIC = b"FCLIBDL\x02"
MAGIC_V3 = crypto_envelope.MAGIC
FORMAT_VERSION = 2
_AUTH_TAG_LEN = 16
_AES_KEY_LEN = 32
_NONCE_LEN = 12


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _namespace() -> str:
    return str(os.getenv("FINANCE_SECRETS_NAMESPACE") or "finance-cli").strip() or "finance-cli"


def _secrets_backend():
    return importlib.import_module("finance_cli.secrets_backend")


def _signing_key_ref(user_id: str) -> str:
    return f"{_namespace()}/users/{user_id}/backup-signing-key"


def _bundle_key_ref(user_id: str, bundle_id: str) -> str:
    return f"{_namespace()}/users/{user_id}/backup-keys/{bundle_id}"


def _canonical_manifest_payload(manifest: dict[str, Any]) -> bytes:
    payload = dict(manifest)
    payload.pop("signature", None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _decode_secret_bytes(*, raw: str | None, label: str) -> bytes:
    if raw is None:
        raise ValueError(f"Missing secret for {label}")
    try:
        decoded = base64.b64decode(raw, validate=True)
    except Exception as exc:
        raise ValueError(f"Secret for {label} is not valid base64") from exc
    if len(decoded) != _AES_KEY_LEN:
        raise ValueError(f"Secret for {label} has unexpected length")
    return decoded


def ensure_signing_key(user_id: str) -> str:
    """Idempotent: create the per-user backup signing key secret if absent."""
    secret_ref = _signing_key_ref(str(user_id))
    existing = _secrets_backend().get_secret(secret_ref, missing_ok=True)
    if existing is not None:
        _decode_secret_bytes(raw=existing, label=secret_ref)
        return secret_ref

    key = os.urandom(_AES_KEY_LEN)
    _secrets_backend().put_secret(
        secret_ref,
        base64.b64encode(key).decode("ascii"),
        description=f"backup signing key for user {user_id}",
    )
    return secret_ref


def sign_manifest(manifest: dict[str, Any], user_id: str) -> dict[str, Any]:
    """Return a signed manifest using the per-user HMAC signing key."""
    resolved_user_id = str(user_id)
    signed_manifest = dict(manifest)
    signed_manifest["user_id"] = resolved_user_id
    secret_ref = ensure_signing_key(resolved_user_id)
    signing_key = _decode_secret_bytes(
        raw=_secrets_backend().get_secret(secret_ref, missing_ok=False),
        label=secret_ref,
    )
    signature_hex = hmac.new(
        signing_key,
        _canonical_manifest_payload(signed_manifest),
        hashlib.sha256,
    ).hexdigest()
    signed_manifest["signature"] = {
        "alg": "HMAC-SHA256",
        "key_ref": secret_ref,
        "signature_hex": signature_hex,
    }
    return signed_manifest


def verify_manifest(manifest: dict[str, Any]) -> bool:
    """Verify the manifest HMAC using the key path derived from manifest.user_id."""
    if not isinstance(manifest, dict):
        return False

    user_id = str(manifest.get("user_id") or "").strip()
    signature = manifest.get("signature")
    if not user_id or not isinstance(signature, dict):
        return False
    if str(signature.get("alg") or "").strip() != "HMAC-SHA256":
        return False

    expected_ref = _signing_key_ref(user_id)
    try:
        signing_key = _decode_secret_bytes(
            raw=_secrets_backend().get_secret(expected_ref, missing_ok=False),
            label=expected_ref,
        )
    except Exception:
        return False

    provided_signature = str(signature.get("signature_hex") or "").strip().lower()
    if not provided_signature:
        return False
    computed_signature = hmac.new(
        signing_key,
        _canonical_manifest_payload(manifest),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed_signature, provided_signature)


def parse_bundle_header(encrypted_bundle_bytes: bytes) -> dict[str, Any]:
    """Parse the plaintext header from a v2 backup bundle."""
    if len(encrypted_bundle_bytes) < len(MAGIC) + 2 + 16 + 4 + _AUTH_TAG_LEN:
        raise ValueError("Backup bundle is truncated")
    if encrypted_bundle_bytes[: len(MAGIC)] != MAGIC:
        raise ValueError("Backup bundle magic mismatch")

    offset = len(MAGIC)
    version = int.from_bytes(encrypted_bundle_bytes[offset : offset + 2], "big")
    offset += 2
    if version != FORMAT_VERSION:
        raise ValueError(f"Unsupported backup bundle version: {version}")

    bundle_uuid = uuid.UUID(bytes=encrypted_bundle_bytes[offset : offset + 16])
    offset += 16
    header_len = int.from_bytes(encrypted_bundle_bytes[offset : offset + 4], "big")
    offset += 4
    header_end = offset + header_len
    if header_end > len(encrypted_bundle_bytes) - _AUTH_TAG_LEN:
        raise ValueError("Backup bundle header is truncated")

    try:
        header = json.loads(encrypted_bundle_bytes[offset:header_end].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Backup bundle header is invalid JSON") from exc
    if not isinstance(header, dict):
        raise ValueError("Backup bundle header must be an object")

    parsed = dict(header)
    parsed["bundle_id"] = str(bundle_uuid)
    parsed["version"] = version
    parsed["header_len"] = header_len
    parsed["payload_offset"] = header_end
    return parsed


def parse_bundle_header_v3(encrypted_bundle_bytes: bytes) -> dict[str, Any]:
    """Parse the plaintext header from a v3 backup bundle."""

    return crypto_envelope.parse_bundle_header_v3(encrypted_bundle_bytes)


def parse_bundle_header_any_version(encrypted_bundle_bytes: bytes) -> dict[str, Any]:
    """Parse a v2 or v3 plaintext bundle header by MAGIC."""

    if encrypted_bundle_bytes[: len(MAGIC)] == MAGIC:
        return parse_bundle_header(encrypted_bundle_bytes)
    if encrypted_bundle_bytes[: len(MAGIC_V3)] == MAGIC_V3:
        return parse_bundle_header_v3(encrypted_bundle_bytes)
    raise ValueError("Backup bundle magic mismatch")


def encrypt_bundle(plaintext_tar_bytes: bytes, user_id: str, bundle_id: str) -> bytes:
    """Encrypt tar bytes with a fresh per-bundle AES-256-GCM DEK."""
    resolved_user_id = str(user_id)
    resolved_bundle_id = str(uuid.UUID(str(bundle_id)))
    dek_secret_ref = _bundle_key_ref(resolved_user_id, resolved_bundle_id)
    dek = os.urandom(_AES_KEY_LEN)
    _secrets_backend().put_secret(
        dek_secret_ref,
        base64.b64encode(dek).decode("ascii"),
        description=f"backup bundle key for user {resolved_user_id} bundle {resolved_bundle_id}",
    )

    nonce = os.urandom(_NONCE_LEN)
    encrypted = AESGCM(dek).encrypt(nonce, plaintext_tar_bytes, None)
    ciphertext = encrypted[:-_AUTH_TAG_LEN]
    auth_tag = encrypted[-_AUTH_TAG_LEN:]
    header = {
        "user_id": resolved_user_id,
        "created_at": _utc_now_iso(),
        "dek_secret_ref": dek_secret_ref,
        "nonce_hex": nonce.hex(),
    }
    header_json = json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return (
        MAGIC
        + FORMAT_VERSION.to_bytes(2, "big")
        + uuid.UUID(resolved_bundle_id).bytes
        + len(header_json).to_bytes(4, "big")
        + header_json
        + ciphertext
        + auth_tag
    )


def encrypt_bundle_v3(
    plaintext_tar_bytes: bytes,
    user_id: str,
    bundle_id: str,
    *,
    mode: str,
    recovery_db_dek_present: bool,
) -> bytes:
    """Encrypt tar bytes using the KMS envelope v3 bundle format."""

    return crypto_envelope.encrypt_bundle_v3(
        plaintext_tar_bytes,
        user_id,
        bundle_id,
        mode=mode,
        recovery_db_dek_present=recovery_db_dek_present,
    )


def decrypt_bundle(encrypted_bundle_bytes: bytes, user_id: str) -> tuple[bytes, dict[str, Any]]:
    """Decrypt a v2 bundle after confirming the header user matches the caller."""
    header = parse_bundle_header(encrypted_bundle_bytes)
    resolved_user_id = str(user_id)
    header_user_id = str(header.get("user_id") or "").strip()
    if header_user_id != resolved_user_id:
        raise ValueError(
            f"Backup bundle belongs to {header_user_id!r}, not {resolved_user_id!r}"
        )

    dek_secret_ref = str(header.get("dek_secret_ref") or "").strip()
    nonce_hex = str(header.get("nonce_hex") or "").strip()
    if not dek_secret_ref or not nonce_hex:
        raise ValueError("Backup bundle header is missing encryption metadata")

    try:
        nonce = bytes.fromhex(nonce_hex)
    except ValueError as exc:
        raise ValueError("Backup bundle nonce is invalid") from exc
    if len(nonce) != _NONCE_LEN:
        raise ValueError("Backup bundle nonce has unexpected length")

    dek = _decode_secret_bytes(
        raw=_secrets_backend().get_secret(dek_secret_ref, missing_ok=False),
        label=dek_secret_ref,
    )
    payload_offset = int(header["payload_offset"])
    ciphertext = encrypted_bundle_bytes[payload_offset:-_AUTH_TAG_LEN]
    auth_tag = encrypted_bundle_bytes[-_AUTH_TAG_LEN:]
    if not ciphertext:
        raise ValueError("Backup bundle ciphertext is missing")
    try:
        plaintext = AESGCM(dek).decrypt(nonce, ciphertext + auth_tag, None)
    except InvalidTag as exc:
        raise ValueError("Backup bundle authentication failed") from exc
    return plaintext, header


def decrypt_bundle_v3(encrypted_bundle_bytes: bytes, user_id: str) -> tuple[bytes, dict[str, Any]]:
    """Decrypt a v3 bundle after confirming the header user matches the caller."""

    plaintext = crypto_envelope.decrypt_bundle_v3(encrypted_bundle_bytes, user_id)
    header = crypto_envelope.parse_bundle_header_v3(encrypted_bundle_bytes)
    return plaintext, header


def decrypt_bundle_any_version(encrypted_bundle_bytes: bytes, user_id: str) -> tuple[bytes, dict[str, Any]]:
    """Decrypt a v2 or v3 bundle by MAGIC."""

    if encrypted_bundle_bytes[: len(MAGIC)] == MAGIC:
        return decrypt_bundle(encrypted_bundle_bytes, user_id)
    if encrypted_bundle_bytes[: len(MAGIC_V3)] == MAGIC_V3:
        return decrypt_bundle_v3(encrypted_bundle_bytes, user_id)
    raise ValueError("Backup bundle magic mismatch")


def delete_bundle_key(user_id: str, bundle_id: str) -> None:
    """Schedule the per-bundle DEK for soft deletion."""
    _secrets_backend().delete_secret(
        _bundle_key_ref(str(user_id), str(bundle_id)),
        recovery_window_days=7,
    )
