"""KMS envelope encryption helpers for bundle v3 and future per-user artifacts."""

from __future__ import annotations

import base64
import contextvars
import fcntl
import importlib
import json
import os
import secrets
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .exceptions import (
    BackendMismatchError,
    CrossUserBundleError,
    DBDEKNotFoundError,
    EnvelopeVersionError,
    InvalidCiphertextError,
    KMSAccessDeniedError,
    KMSUnavailableError,
    ProviderSecretNotFoundError,
)
from .config import get_data_dir

MAGIC = b"FCLIBDL\x03"
FORMAT_VERSION = 3
DB_DEK_MAGIC = b"DDEK"
DB_DEK_VERSION = 1
PROVIDERS_MAGIC = b"PRVS"
PROVIDERS_VERSION = 1

_AES_KEY_LEN = 32
_AUTH_TAG_LEN = 16
_NONCE_LEN = 12
_PROVENANCE_FILENAME = ".envelope_provenance.json"
_SERVICE_NAME = "finance-cli"
_DB_DEK_CACHE_TTL_SECONDS = 300.0
_DB_DEK_KIND = "db-dek"
_PROVIDERS_KIND = "providers"
_SUPPORTED_PROVIDERS = frozenset({"anthropic", "plaid", "stripe", "snaptrade", "telegram"})
_DB_DEK_CACHE: contextvars.ContextVar[
    dict[tuple[str, str], tuple[float, bytes]] | None
] = contextvars.ContextVar("db_dek_envelope_cache", default=None)


class StorageBackend(Protocol):
    """Abstract store for KMS-protected per-user blobs."""

    name: str

    def get(self, user_id: str, kind: str) -> bytes | None: ...

    def put(self, user_id: str, kind: str, blob: bytes) -> None: ...

    def delete(self, user_id: str, kind: str) -> None: ...


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _kms(*, aws_region: str | None = None):
    region = aws_region if aws_region is not None else os.getenv("AWS_REGION", "us-east-1")
    return _boto3().client("kms", region_name=region)


def _boto3():
    return importlib.import_module("boto3")


def __getattr__(name: str):
    if name == "boto3":
        return _boto3()
    raise AttributeError(name)


def _kms_key_arn(*, kms_key_arn: str | None = None) -> str:
    arn = str(
        kms_key_arn if kms_key_arn else os.getenv("FINANCE_CLI_KMS_KEY_ARN") or ""
    ).strip()
    if not arn:
        raise ValueError("FINANCE_CLI_KMS_KEY_ARN must be set")
    return arn


def _kms_client(*, aws_region: str | None = None):
    if aws_region is None:
        return _kms()
    return _kms(aws_region=aws_region)


def _resolved_kms_key_arn(*, kms_key_arn: str | None = None) -> str:
    if kms_key_arn is None:
        return _kms_key_arn()
    return _kms_key_arn(kms_key_arn=kms_key_arn)


def _bundle_encryption_context(user_id: str, bundle_id: str) -> dict[str, str]:
    return {
        "user_id": str(user_id),
        "purpose": "bundle-dek",
        "bundle_id": str(bundle_id),
    }


def _db_dek_encryption_context(user_id: str) -> dict[str, str]:
    return {
        "user_id": str(user_id),
        "purpose": "db-dek",
    }


def _providers_encryption_context(user_id: str) -> dict[str, str]:
    return {
        "user_id": str(user_id),
        "purpose": "providers",
    }


def _db_dek_cache_ttl_seconds() -> float:
    raw = str(os.getenv("FINANCE_CLI_DB_DEK_CACHE_TTL_SECONDS") or "").strip()
    if not raw:
        return _DB_DEK_CACHE_TTL_SECONDS
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return _DB_DEK_CACHE_TTL_SECONDS


def _active_data_dir(data_dir: Path | None = None) -> Path:
    if data_dir is not None:
        return data_dir.expanduser().resolve()
    web_root = str(os.getenv("FINANCE_WEB_DATA_ROOT") or "").strip()
    if web_root:
        return Path(web_root).expanduser().resolve()
    return get_data_dir().expanduser().resolve()


def _cache_key(user_id: str, data_dir: Path) -> tuple[str, str]:
    return (str(data_dir.expanduser().resolve()), str(user_id))


def _get_cache() -> dict[tuple[str, str], tuple[float, bytes]]:
    cache = _DB_DEK_CACHE.get()
    if cache is None:
        cache = {}
        _DB_DEK_CACHE.set(cache)
    return cache


def _canonical_header_json(header: dict[str, Any]) -> bytes:
    return json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _map_kms_error(exc: Exception, *, action: str) -> Exception:
    BotoCoreError, ClientError = _botocore_exception_types()
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code") or "").strip()
        message = str(exc.response.get("Error", {}).get("Message") or "").strip()
        detail = message or code or exc.__class__.__name__
        access_denied_codes = {
            "AccessDenied",
            "AccessDeniedException",
            "KMSAccessDeniedException",
            "UnauthorizedOperation",
        }
        unavailable_codes = {
            "DependencyTimeoutException",
            "InternalFailure",
            "KMSInternalException",
            "LimitExceededException",
            "RequestLimitExceeded",
            "RequestTimeout",
            "ServiceUnavailable",
            "ServiceUnavailableException",
            "Throttling",
            "ThrottlingException",
            "TooManyRequestsException",
        }
        if code in access_denied_codes:
            return KMSAccessDeniedError(f"KMS {action} access denied: {detail}")
        if code == "InvalidCiphertextException":
            return InvalidCiphertextError(f"KMS {action} rejected ciphertext: {detail}")
        if code in unavailable_codes:
            return KMSUnavailableError(f"KMS {action} unavailable: {detail}")
        return KMSUnavailableError(f"KMS {action} failed: {detail}")
    if isinstance(exc, BotoCoreError):
        return KMSUnavailableError(f"KMS {action} unavailable: {exc}")
    return exc


def _botocore_exception_types():
    exceptions = importlib.import_module("botocore.exceptions")
    return exceptions.BotoCoreError, exceptions.ClientError


def _is_botocore_error(exc: Exception) -> bool:
    return isinstance(exc, _botocore_exception_types())


def _raise_mapped_kms_error(exc: Exception, *, action: str) -> None:
    if not _is_botocore_error(exc):
        raise exc
    raise _map_kms_error(exc, action=action) from exc


def secure_zero(b: bytearray) -> None:
    """Best-effort overwrite of mutable key material."""

    for idx in range(len(b)):
        b[idx] = 0


@dataclass(frozen=True)
class _ParsedV3Frame:
    aad: bytes
    header: dict[str, Any]
    header_json: bytes
    payload_with_tag: bytes
    bundle_id: str
    header_len: int
    payload_offset: int


@dataclass(frozen=True)
class _ParsedDBDEKFrame:
    aad: bytes
    user_id: str
    kek_ciphertext: bytes
    nonce: bytes
    payload_with_tag: bytes


@dataclass(frozen=True)
class VaultRef:
    user_id: str
    provider: str
    path: list[str]


@dataclass(frozen=True)
class _ParsedProvidersFrame:
    aad: bytes
    user_id: str
    kek_ciphertext: bytes
    nonce: bytes
    payload_with_tag: bytes


def _parse_v3_frame(bundle_bytes: bytes) -> _ParsedV3Frame:
    minimum_len = len(MAGIC) + 2 + 16 + 4 + _AUTH_TAG_LEN
    if len(bundle_bytes) < minimum_len:
        raise EnvelopeVersionError("Backup bundle v3 frame is truncated")
    if bundle_bytes[: len(MAGIC)] != MAGIC:
        raise EnvelopeVersionError("Backup bundle magic mismatch")

    offset = len(MAGIC)
    version = int.from_bytes(bundle_bytes[offset : offset + 2], "big")
    offset += 2
    if version != FORMAT_VERSION:
        raise EnvelopeVersionError(f"Unsupported backup bundle version: {version}")

    try:
        bundle_uuid = uuid.UUID(bytes=bundle_bytes[offset : offset + 16])
    except ValueError as exc:
        raise EnvelopeVersionError("Backup bundle UUID is invalid") from exc
    offset += 16

    header_len = int.from_bytes(bundle_bytes[offset : offset + 4], "big")
    offset += 4
    header_end = offset + header_len
    if header_len <= 0 or header_end > len(bundle_bytes) - _AUTH_TAG_LEN:
        raise EnvelopeVersionError("Backup bundle header is truncated")

    header_json = bundle_bytes[offset:header_end]
    try:
        header = json.loads(header_json.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidCiphertextError("Backup bundle header is invalid JSON") from exc
    if not isinstance(header, dict):
        raise InvalidCiphertextError("Backup bundle header must be an object")
    if _canonical_header_json(header) != header_json:
        raise InvalidCiphertextError("Backup bundle header is not canonical")

    return _ParsedV3Frame(
        aad=bundle_bytes[:header_end],
        header=header,
        header_json=header_json,
        payload_with_tag=bundle_bytes[header_end:],
        bundle_id=str(bundle_uuid),
        header_len=header_len,
        payload_offset=header_end,
    )


def _parse_db_dek_blob(blob: bytes) -> _ParsedDBDEKFrame:
    minimum_len = len(DB_DEK_MAGIC) + 2 + 2 + 4 + _NONCE_LEN + 4 + _AUTH_TAG_LEN
    if len(blob) < minimum_len:
        raise EnvelopeVersionError("db-dek.enc frame is truncated")
    if blob[: len(DB_DEK_MAGIC)] != DB_DEK_MAGIC:
        raise EnvelopeVersionError("db-dek.enc magic mismatch")

    offset = len(DB_DEK_MAGIC)
    version = int.from_bytes(blob[offset : offset + 2], "big")
    offset += 2
    if version != DB_DEK_VERSION:
        raise EnvelopeVersionError(f"Unsupported db-dek.enc version: {version}")

    user_id_len = int.from_bytes(blob[offset : offset + 2], "big")
    offset += 2
    user_id_end = offset + user_id_len
    if user_id_len <= 0 or user_id_end > len(blob):
        raise EnvelopeVersionError("db-dek.enc user_id is truncated")
    user_id_bytes = blob[offset:user_id_end]
    try:
        user_id = user_id_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidCiphertextError("db-dek.enc user_id is not UTF-8") from exc
    if not user_id:
        raise InvalidCiphertextError("db-dek.enc user_id is empty")
    offset = user_id_end
    aad = blob[:offset]

    if offset + 4 > len(blob):
        raise EnvelopeVersionError("db-dek.enc KMS ciphertext length is missing")
    kek_len = int.from_bytes(blob[offset : offset + 4], "big")
    offset += 4
    kek_end = offset + kek_len
    if kek_len <= 0 or kek_end > len(blob):
        raise EnvelopeVersionError("db-dek.enc KMS ciphertext is truncated")
    kek_ciphertext = blob[offset:kek_end]
    offset = kek_end

    nonce_end = offset + _NONCE_LEN
    if nonce_end > len(blob):
        raise EnvelopeVersionError("db-dek.enc nonce is truncated")
    nonce = blob[offset:nonce_end]
    offset = nonce_end

    if offset + 4 > len(blob):
        raise EnvelopeVersionError("db-dek.enc payload length is missing")
    payload_len = int.from_bytes(blob[offset : offset + 4], "big")
    offset += 4
    payload_end = offset + payload_len
    tag_end = payload_end + _AUTH_TAG_LEN
    if payload_len <= 0 or tag_end != len(blob):
        raise EnvelopeVersionError("db-dek.enc payload is truncated")
    payload_with_tag = blob[offset:payload_end] + blob[payload_end:tag_end]

    return _ParsedDBDEKFrame(
        aad=aad,
        user_id=user_id,
        kek_ciphertext=bytes(kek_ciphertext),
        nonce=bytes(nonce),
        payload_with_tag=bytes(payload_with_tag),
    )


def _parse_providers_blob(blob: bytes) -> _ParsedProvidersFrame:
    minimum_len = len(PROVIDERS_MAGIC) + 2 + 2 + 4 + _NONCE_LEN + 4 + _AUTH_TAG_LEN
    if len(blob) < minimum_len:
        raise EnvelopeVersionError("providers.enc frame is truncated")
    if blob[: len(PROVIDERS_MAGIC)] != PROVIDERS_MAGIC:
        raise EnvelopeVersionError("providers.enc magic mismatch")

    offset = len(PROVIDERS_MAGIC)
    version = int.from_bytes(blob[offset : offset + 2], "big")
    offset += 2
    if version != PROVIDERS_VERSION:
        raise EnvelopeVersionError(f"Unsupported providers.enc version: {version}")

    user_id_len = int.from_bytes(blob[offset : offset + 2], "big")
    offset += 2
    user_id_end = offset + user_id_len
    if user_id_len <= 0 or user_id_end > len(blob):
        raise EnvelopeVersionError("providers.enc user_id is truncated")
    user_id_bytes = blob[offset:user_id_end]
    try:
        user_id = user_id_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidCiphertextError("providers.enc user_id is not UTF-8") from exc
    if not user_id:
        raise InvalidCiphertextError("providers.enc user_id is empty")
    offset = user_id_end
    aad = blob[:offset]

    if offset + 4 > len(blob):
        raise EnvelopeVersionError("providers.enc KMS ciphertext length is missing")
    kek_len = int.from_bytes(blob[offset : offset + 4], "big")
    offset += 4
    kek_end = offset + kek_len
    if kek_len <= 0 or kek_end > len(blob):
        raise EnvelopeVersionError("providers.enc KMS ciphertext is truncated")
    kek_ciphertext = blob[offset:kek_end]
    offset = kek_end

    nonce_end = offset + _NONCE_LEN
    if nonce_end > len(blob):
        raise EnvelopeVersionError("providers.enc nonce is truncated")
    nonce = blob[offset:nonce_end]
    offset = nonce_end

    if offset + 4 > len(blob):
        raise EnvelopeVersionError("providers.enc payload length is missing")
    payload_len = int.from_bytes(blob[offset : offset + 4], "big")
    offset += 4
    payload_end = offset + payload_len
    tag_end = payload_end + _AUTH_TAG_LEN
    if payload_len <= 0 or tag_end != len(blob):
        raise EnvelopeVersionError("providers.enc payload is truncated")
    payload_with_tag = blob[offset:payload_end] + blob[payload_end:tag_end]

    return _ParsedProvidersFrame(
        aad=aad,
        user_id=user_id,
        kek_ciphertext=bytes(kek_ciphertext),
        nonce=bytes(nonce),
        payload_with_tag=bytes(payload_with_tag),
    )


def _encrypt_db_dek_blob(
    user_id: str,
    dek: bytes,
    *,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
) -> bytes:
    resolved_user_id = str(user_id)
    if len(dek) != _AES_KEY_LEN:
        raise ValueError(f"DEK for user {resolved_user_id!r} has unexpected length")

    key_arn = _resolved_kms_key_arn(kms_key_arn=kms_key_arn)
    enc_ctx = _db_dek_encryption_context(resolved_user_id)
    try:
        resp = _kms_client(aws_region=aws_region).generate_data_key(
            KeyId=key_arn,
            KeySpec="AES_256",
            EncryptionContext=enc_ctx,
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="GenerateDataKey")

    wrapping_dek = bytearray(resp["Plaintext"])
    if len(wrapping_dek) != _AES_KEY_LEN:
        secure_zero(wrapping_dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")

    user_id_bytes = resolved_user_id.encode("utf-8")
    aad = DB_DEK_MAGIC + DB_DEK_VERSION.to_bytes(2, "big") + len(user_id_bytes).to_bytes(2, "big") + user_id_bytes
    kek_ciphertext = bytes(resp["CiphertextBlob"])
    nonce = os.urandom(_NONCE_LEN)
    try:
        encrypted = AESGCM(bytes(wrapping_dek)).encrypt(nonce, dek, aad)
    finally:
        secure_zero(wrapping_dek)
    ciphertext = encrypted[:-_AUTH_TAG_LEN]
    auth_tag = encrypted[-_AUTH_TAG_LEN:]
    return (
        aad
        + len(kek_ciphertext).to_bytes(4, "big")
        + kek_ciphertext
        + nonce
        + len(ciphertext).to_bytes(4, "big")
        + ciphertext
        + auth_tag
    )


def _decrypt_db_dek_blob(
    blob: bytes,
    expected_user_id: str,
    *,
    aws_region: str | None = None,
) -> bytes:
    parsed = _parse_db_dek_blob(blob)
    expected = str(expected_user_id)
    if parsed.user_id != expected:
        raise InvalidCiphertextError(
            f"db-dek.enc belongs to {parsed.user_id!r}, not {expected!r}"
        )

    try:
        resp = _kms_client(aws_region=aws_region).decrypt(
            CiphertextBlob=parsed.kek_ciphertext,
            EncryptionContext=_db_dek_encryption_context(expected),
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="Decrypt")

    wrapping_dek = bytearray(resp["Plaintext"])
    if len(wrapping_dek) != _AES_KEY_LEN:
        secure_zero(wrapping_dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")
    try:
        dek = AESGCM(bytes(wrapping_dek)).decrypt(
            parsed.nonce,
            parsed.payload_with_tag,
            parsed.aad,
        )
    except InvalidTag as exc:
        raise InvalidCiphertextError("db-dek.enc authentication failed") from exc
    finally:
        secure_zero(wrapping_dek)
    if len(dek) != _AES_KEY_LEN:
        raise InvalidCiphertextError("db-dek.enc plaintext DEK has unexpected length")
    return bytes(dek)


def _encrypt_providers_blob(
    user_id: str,
    payload: dict[str, Any],
    *,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
) -> bytes:
    resolved_user_id = str(user_id)
    payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if not payload_bytes:
        raise ValueError("providers.enc payload is empty")

    key_arn = _resolved_kms_key_arn(kms_key_arn=kms_key_arn)
    enc_ctx = _providers_encryption_context(resolved_user_id)
    try:
        resp = _kms_client(aws_region=aws_region).generate_data_key(
            KeyId=key_arn,
            KeySpec="AES_256",
            EncryptionContext=enc_ctx,
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="GenerateDataKey")

    wrapping_dek = bytearray(resp["Plaintext"])
    if len(wrapping_dek) != _AES_KEY_LEN:
        secure_zero(wrapping_dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")

    user_id_bytes = resolved_user_id.encode("utf-8")
    aad = (
        PROVIDERS_MAGIC
        + PROVIDERS_VERSION.to_bytes(2, "big")
        + len(user_id_bytes).to_bytes(2, "big")
        + user_id_bytes
    )
    kek_ciphertext = bytes(resp["CiphertextBlob"])
    nonce = os.urandom(_NONCE_LEN)
    try:
        encrypted = AESGCM(bytes(wrapping_dek)).encrypt(nonce, payload_bytes, aad)
    finally:
        secure_zero(wrapping_dek)
    ciphertext = encrypted[:-_AUTH_TAG_LEN]
    auth_tag = encrypted[-_AUTH_TAG_LEN:]
    return (
        aad
        + len(kek_ciphertext).to_bytes(4, "big")
        + kek_ciphertext
        + nonce
        + len(ciphertext).to_bytes(4, "big")
        + ciphertext
        + auth_tag
    )


def _decrypt_providers_blob(
    blob: bytes,
    expected_user_id: str,
    *,
    aws_region: str | None = None,
) -> dict[str, Any]:
    parsed = _parse_providers_blob(blob)
    expected = str(expected_user_id)
    if parsed.user_id != expected:
        raise InvalidCiphertextError(
            f"providers.enc belongs to {parsed.user_id!r}, not {expected!r}"
        )

    try:
        resp = _kms_client(aws_region=aws_region).decrypt(
            CiphertextBlob=parsed.kek_ciphertext,
            EncryptionContext=_providers_encryption_context(expected),
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="Decrypt")

    wrapping_dek = bytearray(resp["Plaintext"])
    if len(wrapping_dek) != _AES_KEY_LEN:
        secure_zero(wrapping_dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")
    try:
        plaintext = AESGCM(bytes(wrapping_dek)).decrypt(
            parsed.nonce,
            parsed.payload_with_tag,
            parsed.aad,
        )
    except InvalidTag as exc:
        raise InvalidCiphertextError("providers.enc authentication failed") from exc
    finally:
        secure_zero(wrapping_dek)

    try:
        payload = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidCiphertextError("providers.enc payload is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise InvalidCiphertextError("providers.enc payload must be an object")
    if int(payload.get("schema_version") or 0) != 1:
        raise EnvelopeVersionError("Unsupported providers.enc schema_version")
    if str(payload.get("user_id") or "") != expected:
        raise InvalidCiphertextError(
            f"providers.enc payload belongs to {payload.get('user_id')!r}, not {expected!r}"
        )
    providers = payload.get("providers")
    if providers is None:
        payload["providers"] = {}
    elif not isinstance(providers, dict):
        raise InvalidCiphertextError("providers.enc providers field must be an object")
    if "_unknown_fields" not in payload or not isinstance(payload.get("_unknown_fields"), dict):
        payload["_unknown_fields"] = {}
    return payload


class UserEnvelopeLock:
    """Per-user advisory lock for envelope artifact writes."""

    def __init__(self, user_id: str, data_dir: Path | None = None) -> None:
        self.user_id = str(user_id)
        self.data_dir = _active_data_dir(data_dir)
        self.path = self.data_dir / self.user_id / ".envelope.lock"
        self._handle: Any | None = None

    def __enter__(self) -> "UserEnvelopeLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+b")
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def parse_bundle_header_v3(bundle_bytes: bytes) -> dict[str, Any]:
    """Parse the plaintext v3 header without decrypting the payload."""

    parsed = _parse_v3_frame(bundle_bytes)
    header = dict(parsed.header)
    header["bundle_id"] = parsed.bundle_id
    header["version"] = FORMAT_VERSION
    header["header_len"] = parsed.header_len
    header["payload_offset"] = parsed.payload_offset
    return header


def encrypt_bundle_v3(
    plaintext_tar: bytes,
    user_id: str,
    bundle_id: str,
    *,
    mode: str,
    recovery_db_dek_present: bool,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
) -> bytes:
    """Encrypt tar bytes as a v3 KMS envelope bundle."""

    resolved_user_id = str(user_id)
    resolved_bundle_id = str(uuid.UUID(str(bundle_id)))
    resolved_mode = str(mode).strip().lower()
    if resolved_mode not in {"portable", "compact"}:
        raise ValueError("mode must be 'portable' or 'compact'")

    enc_ctx = _bundle_encryption_context(resolved_user_id, resolved_bundle_id)
    key_arn = _resolved_kms_key_arn(kms_key_arn=kms_key_arn)
    try:
        resp = _kms_client(aws_region=aws_region).generate_data_key(
            KeyId=key_arn,
            KeySpec="AES_256",
            EncryptionContext=enc_ctx,
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="GenerateDataKey")

    dek = bytearray(resp["Plaintext"])
    if len(dek) != _AES_KEY_LEN:
        secure_zero(dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")
    kek_ciphertext = bytes(resp["CiphertextBlob"])
    payload_nonce = os.urandom(_NONCE_LEN)
    header = {
        "user_id": resolved_user_id,
        "created_at": _utc_now_iso(),
        "wrap_method": "kms-direct-v1",
        "kms_key_arn": key_arn,
        "kek_ciphertext_b64": base64.b64encode(kek_ciphertext).decode("ascii"),
        "encryption_context": enc_ctx,
        "payload_nonce_hex": payload_nonce.hex(),
        "mode": resolved_mode,
        "recovery_db_dek_present": bool(recovery_db_dek_present),
    }
    header_json = _canonical_header_json(header)
    aad = (
        MAGIC
        + FORMAT_VERSION.to_bytes(2, "big")
        + uuid.UUID(resolved_bundle_id).bytes
        + len(header_json).to_bytes(4, "big")
        + header_json
    )
    try:
        payload_with_tag = AESGCM(bytes(dek)).encrypt(payload_nonce, plaintext_tar, aad)
    finally:
        secure_zero(dek)
    return aad + payload_with_tag


def decrypt_bundle_v3(
    bundle_bytes: bytes,
    expected_user_id: str,
    *,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
) -> bytes:
    """Decrypt a v3 bundle after cross-user pre-checks."""

    parsed = _parse_v3_frame(bundle_bytes)
    expected = str(expected_user_id)
    header_user_id = str(parsed.header.get("user_id") or "").strip()
    if header_user_id != expected:
        raise CrossUserBundleError(f"Backup bundle belongs to {header_user_id!r}, not {expected!r}")

    enc_ctx = parsed.header.get("encryption_context")
    if not isinstance(enc_ctx, dict):
        raise InvalidCiphertextError("Backup bundle header is missing encryption context")
    ctx_user_id = str(enc_ctx.get("user_id") or "").strip()
    if ctx_user_id != expected:
        raise CrossUserBundleError(f"Backup bundle context belongs to {ctx_user_id!r}, not {expected!r}")
    if str(enc_ctx.get("purpose") or "") != "bundle-dek":
        raise InvalidCiphertextError("Backup bundle encryption context has wrong purpose")
    ctx_bundle_id = str(enc_ctx.get("bundle_id") or "").strip()
    if not ctx_bundle_id:
        raise InvalidCiphertextError("Backup bundle encryption context is missing bundle_id")

    try:
        kek_ciphertext = base64.b64decode(
            str(parsed.header.get("kek_ciphertext_b64") or ""),
            validate=True,
        )
    except Exception as exc:
        raise InvalidCiphertextError("Backup bundle KMS ciphertext is invalid base64") from exc
    try:
        payload_nonce = bytes.fromhex(str(parsed.header.get("payload_nonce_hex") or ""))
    except ValueError as exc:
        raise InvalidCiphertextError("Backup bundle payload nonce is invalid") from exc
    if len(payload_nonce) != _NONCE_LEN:
        raise InvalidCiphertextError("Backup bundle payload nonce has unexpected length")

    try:
        resp = _kms_client(aws_region=aws_region).decrypt(
            CiphertextBlob=kek_ciphertext,
            EncryptionContext=_bundle_encryption_context(expected, ctx_bundle_id),
        )
    except Exception as exc:
        _raise_mapped_kms_error(exc, action="Decrypt")

    dek = bytearray(resp["Plaintext"])
    if len(dek) != _AES_KEY_LEN:
        secure_zero(dek)
        raise InvalidCiphertextError("KMS returned a DEK with unexpected length")
    try:
        return AESGCM(bytes(dek)).decrypt(payload_nonce, parsed.payload_with_tag, parsed.aad)
    except InvalidTag as exc:
        raise InvalidCiphertextError("Backup bundle authentication failed") from exc
    finally:
        secure_zero(dek)


def _blob_filename(kind: str) -> str:
    normalized = str(kind).strip()
    if normalized == "db-dek":
        return "db-dek.enc"
    if normalized == "providers":
        return "providers.enc"
    return f"{normalized}.enc"


def _provenance_path(data_dir: Path, user_id: str) -> Path:
    return data_dir / str(user_id) / _PROVENANCE_FILENAME


def _read_provenance(data_dir: Path, user_id: str) -> dict[str, Any]:
    path = _provenance_path(data_dir, user_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_provenance(data_dir: Path, user_id: str, payload: dict[str, Any]) -> None:
    path = _provenance_path(data_dir, user_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _verify_provenance(data_dir: Path, user_id: str, kind: str, backend_name: str) -> None:
    provenance = _read_provenance(data_dir, user_id)
    recorded = str(provenance.get(kind) or "").strip()
    if recorded and recorded != backend_name:
        raise BackendMismatchError(
            f"{kind} is recorded in {recorded!r}, but active backend is {backend_name!r}"
        )


def _record_provenance(data_dir: Path, user_id: str, kind: str, backend_name: str) -> None:
    _verify_provenance(data_dir, user_id, kind, backend_name)
    provenance = _read_provenance(data_dir, user_id)
    provenance[kind] = backend_name
    provenance["recorded_at"] = _utc_now_iso()
    _write_provenance(data_dir, user_id, provenance)


def _clear_provenance(data_dir: Path, user_id: str, kind: str, backend_name: str) -> None:
    _verify_provenance(data_dir, user_id, kind, backend_name)
    provenance = _read_provenance(data_dir, user_id)
    if kind in provenance:
        provenance.pop(kind, None)
        provenance["recorded_at"] = _utc_now_iso()
        _write_provenance(data_dir, user_id, provenance)


class FileStorageBackend:
    """File-backed storage for KMS-protected artifacts."""

    name = "file"

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir.expanduser().resolve()

    def _path(self, user_id: str, kind: str) -> Path:
        return self.data_dir / str(user_id) / _blob_filename(kind)

    def get(self, user_id: str, kind: str) -> bytes | None:
        _verify_provenance(self.data_dir, str(user_id), str(kind), self.name)
        path = self._path(str(user_id), str(kind))
        if not path.exists():
            return None
        return path.read_bytes()

    def put(self, user_id: str, kind: str, blob: bytes) -> None:
        resolved_user_id = str(user_id)
        resolved_kind = str(kind)
        _verify_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)
        path = self._path(resolved_user_id, resolved_kind)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(blob)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_path, 0o600)
            os.replace(tmp_path, path)
            _fsync_dir(path.parent)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
        _record_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)

    def delete(self, user_id: str, kind: str) -> None:
        resolved_user_id = str(user_id)
        resolved_kind = str(kind)
        _verify_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)
        try:
            self._path(resolved_user_id, resolved_kind).unlink()
        except FileNotFoundError:
            pass
        _clear_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class KeychainStorageBackend:
    """Base64-encoded blob storage in an OS keychain via keyring."""

    def __init__(self, data_dir: Path, *, name: str, keyring_module: Any | None = None) -> None:
        self.data_dir = data_dir.expanduser().resolve()
        self.name = name
        self._keyring = keyring_module if keyring_module is not None else _load_keyring()

    def _account(self, user_id: str, kind: str) -> str:
        resolved_kind = str(kind)
        if resolved_kind == "db-dek":
            return f"db-dek-{user_id}"
        return f"{resolved_kind}-{user_id}"

    def get(self, user_id: str, kind: str) -> bytes | None:
        resolved_user_id = str(user_id)
        resolved_kind = str(kind)
        _verify_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)
        raw = self._keyring.get_password(_SERVICE_NAME, self._account(resolved_user_id, resolved_kind))
        if raw is None:
            return None
        try:
            return base64.b64decode(str(raw), validate=True)
        except Exception as exc:
            raise ValueError(f"{resolved_kind} keychain blob is not valid base64") from exc

    def put(self, user_id: str, kind: str, blob: bytes) -> None:
        resolved_user_id = str(user_id)
        resolved_kind = str(kind)
        _verify_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)
        self._keyring.set_password(
            _SERVICE_NAME,
            self._account(resolved_user_id, resolved_kind),
            base64.b64encode(blob).decode("ascii"),
        )
        _record_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)

    def delete(self, user_id: str, kind: str) -> None:
        resolved_user_id = str(user_id)
        resolved_kind = str(kind)
        _verify_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)
        try:
            self._keyring.delete_password(_SERVICE_NAME, self._account(resolved_user_id, resolved_kind))
        except Exception:
            pass
        _clear_provenance(self.data_dir, resolved_user_id, resolved_kind, self.name)


class MacOSKeychainBackend(KeychainStorageBackend):
    def __init__(self, data_dir: Path, *, keyring_module: Any | None = None) -> None:
        super().__init__(data_dir, name="keychain:macos", keyring_module=keyring_module)


class LibsecretKeychainBackend(KeychainStorageBackend):
    def __init__(self, data_dir: Path, *, keyring_module: Any | None = None) -> None:
        super().__init__(data_dir, name="keychain:libsecret", keyring_module=keyring_module)


class WinVaultKeychainBackend(KeychainStorageBackend):
    def __init__(self, data_dir: Path, *, keyring_module: Any | None = None) -> None:
        super().__init__(data_dir, name="keychain:windows", keyring_module=keyring_module)


def _load_keyring() -> Any:
    try:
        import keyring
    except ImportError as exc:
        raise RuntimeError("keyring is not installed") from exc
    return keyring


def _detect_keychain_backend(data_dir: Path) -> StorageBackend | None:
    try:
        keyring = _load_keyring()
        backend = keyring.get_keyring()
    except Exception:
        return None

    fqcn = f"{backend.__class__.__module__}.{backend.__class__.__name__}".lower()
    if "macos" in fqcn:
        return MacOSKeychainBackend(data_dir, keyring_module=keyring)
    if "secretservice" in fqcn or "libsecret" in fqcn or "linux_kernel_keyring" in fqcn:
        return LibsecretKeychainBackend(data_dir, keyring_module=keyring)
    if "winvault" in fqcn:
        return WinVaultKeychainBackend(data_dir, keyring_module=keyring)
    return None


def select_backend(
    user_id: str,
    data_dir: Path,
    *,
    backend: str | None = None,
) -> StorageBackend:
    """Select the active storage backend for this environment."""

    del user_id
    resolved_data_dir = data_dir.expanduser().resolve()
    requested = str(
        backend if backend is not None else os.getenv("FINANCE_CLI_ENVELOPE_BACKEND") or "auto"
    ).strip().lower()
    source = "backend" if backend is not None else "FINANCE_CLI_ENVELOPE_BACKEND"
    if requested in {"file", "files"}:
        return FileStorageBackend(resolved_data_dir)
    if requested in {"macos", "macos-keychain", "keychain:macos"}:
        return MacOSKeychainBackend(resolved_data_dir)
    if requested in {"libsecret", "secretservice", "keychain:libsecret"}:
        return LibsecretKeychainBackend(resolved_data_dir)
    if requested in {"winvault", "windows", "keychain:windows"}:
        return WinVaultKeychainBackend(resolved_data_dir)
    if requested == "keychain":
        backend = _detect_keychain_backend(resolved_data_dir)
        if backend is None:
            raise RuntimeError("No allowed keychain backend is available")
        return backend
    if requested not in {"auto", ""}:
        raise ValueError(f"Unsupported {source}={requested!r}")

    return _detect_keychain_backend(resolved_data_dir) or FileStorageBackend(resolved_data_dir)


def _select_backend(
    user_id: str,
    data_dir: Path,
    *,
    backend: str | None = None,
) -> StorageBackend:
    if backend is None:
        return select_backend(user_id, data_dir)
    return select_backend(user_id, data_dir, backend=backend)


def install_db_dek_blob(
    user_id: str,
    blob_bytes: bytes,
    *,
    data_dir: Path | None = None,
    backend: str | None = None,
) -> None:
    """Install received db-dek.enc bytes into the active backend without decrypting."""

    resolved_user_id = str(user_id)
    parsed = _parse_db_dek_blob(bytes(blob_bytes))
    if parsed.user_id != resolved_user_id:
        raise InvalidCiphertextError(
            f"db-dek.enc belongs to {parsed.user_id!r}, not {resolved_user_id!r}"
        )
    resolved_data_dir = _active_data_dir(data_dir)
    storage_backend = _select_backend(resolved_user_id, resolved_data_dir, backend=backend)
    with UserEnvelopeLock(resolved_user_id, resolved_data_dir):
        storage_backend.put(resolved_user_id, _DB_DEK_KIND, bytes(blob_bytes))
    evict_caches(resolved_user_id)


def get_db_dek(
    user_id: str,
    *,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
) -> bytes:
    """Load and KMS-decrypt db-dek.enc for user_id, cached with a short TTL."""

    resolved_user_id = str(user_id)
    resolved_data_dir = _active_data_dir(data_dir)
    key = _cache_key(resolved_user_id, resolved_data_dir)
    now = time.monotonic()
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        expires_at, dek = cached
        if expires_at > now:
            return dek
        cache.pop(key, None)

    storage_backend = _select_backend(resolved_user_id, resolved_data_dir, backend=backend)
    blob = storage_backend.get(resolved_user_id, _DB_DEK_KIND)
    if blob is None:
        raise DBDEKNotFoundError(f"db-dek.enc is not provisioned for user {resolved_user_id!r}")

    dek = _decrypt_db_dek_blob(blob, resolved_user_id, aws_region=aws_region)
    ttl = _db_dek_cache_ttl_seconds()
    if ttl > 0:
        cache[key] = (now + ttl, dek)
    return dek


def provision_db_dek(
    user_id: str,
    *,
    dek: bytes | None = None,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
) -> None:
    """Idempotently provision db-dek.enc, optionally wrapping an existing DEK."""

    resolved_user_id = str(user_id)
    resolved_data_dir = _active_data_dir(data_dir)
    plaintext = bytes(dek) if dek is not None else secrets.token_bytes(_AES_KEY_LEN)
    if len(plaintext) != _AES_KEY_LEN:
        raise ValueError(f"DEK for user {resolved_user_id!r} has unexpected length")

    storage_backend = _select_backend(resolved_user_id, resolved_data_dir, backend=backend)
    with UserEnvelopeLock(resolved_user_id, resolved_data_dir):
        existing = storage_backend.get(resolved_user_id, _DB_DEK_KIND)
        if existing is not None:
            existing_plaintext = _decrypt_db_dek_blob(
                existing,
                resolved_user_id,
                aws_region=aws_region,
            )
            if existing_plaintext != plaintext:
                raise InvalidCiphertextError(
                    "Existing db-dek.enc plaintext differs from the requested DEK; manual intervention required"
                )
            cache = _get_cache()
            ttl = _db_dek_cache_ttl_seconds()
            if ttl > 0:
                cache[_cache_key(resolved_user_id, resolved_data_dir)] = (time.monotonic() + ttl, plaintext)
            return

        blob = _encrypt_db_dek_blob(
            resolved_user_id,
            plaintext,
            kms_key_arn=kms_key_arn,
            aws_region=aws_region,
        )
        storage_backend.put(resolved_user_id, _DB_DEK_KIND, blob)
    cache = _get_cache()
    ttl = _db_dek_cache_ttl_seconds()
    if ttl > 0:
        cache[_cache_key(resolved_user_id, resolved_data_dir)] = (time.monotonic() + ttl, plaintext)


def rotate_db_dek(
    user_id: str,
    *,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
) -> None:
    """Rewrap the existing SQLCipher DEK into a fresh db-dek.enc envelope."""

    resolved_user_id = str(user_id)
    resolved_data_dir = _active_data_dir(data_dir)
    storage_backend = _select_backend(resolved_user_id, resolved_data_dir, backend=backend)
    with UserEnvelopeLock(resolved_user_id, resolved_data_dir):
        existing = storage_backend.get(resolved_user_id, _DB_DEK_KIND)
        if existing is None:
            raise DBDEKNotFoundError(f"db-dek.enc is not provisioned for user {resolved_user_id!r}")
        plaintext = _decrypt_db_dek_blob(existing, resolved_user_id, aws_region=aws_region)
        storage_backend.put(
            resolved_user_id,
            _DB_DEK_KIND,
            _encrypt_db_dek_blob(
                resolved_user_id,
                plaintext,
                kms_key_arn=kms_key_arn,
                aws_region=aws_region,
            ),
        )
    cache = _get_cache()
    ttl = _db_dek_cache_ttl_seconds()
    if ttl > 0:
        cache[_cache_key(resolved_user_id, resolved_data_dir)] = (time.monotonic() + ttl, plaintext)


def has_db_dek(
    user_id: str,
    *,
    data_dir: Path | None = None,
    backend: str | None = None,
) -> bool:
    resolved_user_id = str(user_id)
    resolved_data_dir = _active_data_dir(data_dir)
    storage_backend = _select_backend(resolved_user_id, resolved_data_dir, backend=backend)
    return storage_backend.get(resolved_user_id, _DB_DEK_KIND) is not None


def parse_vault_ref(ref: str) -> VaultRef:
    """Parse a provider vault reference of shape vault://{user_id}/{provider}/{path}."""

    raw = str(ref or "").strip()
    parsed = urlsplit(raw)
    if parsed.scheme != "vault":
        raise ValueError(f"Secret ref is not a vault URI: {ref!r}")
    if parsed.query or parsed.fragment:
        raise ValueError(f"Vault URI must not include query or fragment: {ref!r}")
    user_id = unquote(parsed.netloc).strip()
    if not user_id:
        raise ValueError(f"Vault URI is missing user_id: {ref!r}")
    segments = [unquote(part) for part in parsed.path.split("/") if part]
    if len(segments) < 2:
        raise ValueError(f"Vault URI must include provider and path: {ref!r}")
    provider = segments[0].strip().lower()
    if provider not in _SUPPORTED_PROVIDERS:
        raise ValueError(f"Unsupported vault provider: {provider!r}")
    path = [part.strip() for part in segments[1:]]
    if any(not part for part in path):
        raise ValueError(f"Vault URI path contains an empty segment: {ref!r}")
    return VaultRef(user_id=user_id, provider=provider, path=path)


def format_vault_ref(user_id: str, provider: str, *path: str) -> str:
    resolved_user_id = str(user_id).strip()
    resolved_provider = str(provider).strip().lower()
    if not resolved_user_id:
        raise ValueError("user_id is required")
    if resolved_provider not in _SUPPORTED_PROVIDERS:
        raise ValueError(f"Unsupported vault provider: {resolved_provider!r}")
    resolved_path = [str(part).strip() for part in path]
    if not resolved_path or any(not part for part in resolved_path):
        raise ValueError("vault path must contain at least one non-empty segment")
    encoded = "/".join(quote(part, safe="") for part in (resolved_provider, *resolved_path))
    return f"vault://{quote(resolved_user_id, safe='')}/{encoded}"


def _providers_enabled() -> bool:
    return str(os.getenv("FINANCE_CLI_PROVIDERS_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _providers_backend(
    user_id: str,
    data_dir: Path | None = None,
    *,
    backend: str | None = None,
) -> tuple[StorageBackend, Path]:
    if not _providers_enabled():
        raise ProviderSecretNotFoundError("providers.enc is server-only")
    resolved_data_dir = _active_data_dir(data_dir)
    storage_backend = _select_backend(str(user_id), resolved_data_dir, backend=backend)
    if storage_backend.name != "file":
        raise ProviderSecretNotFoundError("providers.enc is server-only")
    return storage_backend, resolved_data_dir


def _empty_providers_payload(user_id: str) -> dict[str, Any]:
    now = _utc_now_iso()
    return {
        "schema_version": 1,
        "user_id": str(user_id),
        "updated_at": now,
        "rotated_at": now,
        "providers": {},
        "_unknown_fields": {},
    }


def _load_providers_payload(
    user_id: str,
    backend: StorageBackend,
    *,
    aws_region: str | None = None,
) -> dict[str, Any] | None:
    blob = backend.get(str(user_id), _PROVIDERS_KIND)
    if blob is None:
        return None
    return _decrypt_providers_blob(blob, str(user_id), aws_region=aws_region)


def _walk_provider_path(payload: dict[str, Any], vault_ref: VaultRef) -> Any:
    current: Any = payload.get("providers", {}).get(vault_ref.provider)
    for segment in vault_ref.path:
        if not isinstance(current, dict) or segment not in current:
            raise ProviderSecretNotFoundError(f"Provider secret not found: {format_vault_ref(vault_ref.user_id, vault_ref.provider, *vault_ref.path)}")
        current = current[segment]
    return current


def _provider_leaf_container(
    payload: dict[str, Any],
    vault_ref: VaultRef,
    *,
    create: bool,
) -> tuple[dict[str, Any], str]:
    providers = payload.setdefault("providers", {})
    if not isinstance(providers, dict):
        raise InvalidCiphertextError("providers.enc providers field must be an object")
    if create:
        provider_payload = providers.setdefault(vault_ref.provider, {})
    else:
        provider_payload = providers.get(vault_ref.provider)
    if not isinstance(provider_payload, dict):
        raise ProviderSecretNotFoundError(f"Provider secret not found: {format_vault_ref(vault_ref.user_id, vault_ref.provider, *vault_ref.path)}")

    current = provider_payload
    for segment in vault_ref.path[:-1]:
        next_value = current.get(segment)
        if next_value is None and create:
            next_value = {}
            current[segment] = next_value
        if not isinstance(next_value, dict):
            raise ProviderSecretNotFoundError(f"Provider secret path is not a container: {format_vault_ref(vault_ref.user_id, vault_ref.provider, *vault_ref.path)}")
        current = next_value
    return current, vault_ref.path[-1]


def _stamp_provider_metadata(container: dict[str, Any], provider: str, path: list[str], migrated_from_ref: str | None) -> None:
    now = _utc_now_iso()
    container.setdefault("added_at", now)
    if provider == "plaid" and len(path) >= 3 and path[0] == "items":
        container.setdefault("linked_at", now)
    if migrated_from_ref:
        container["migrated_from_ref"] = str(migrated_from_ref)


def get_provider_secret(
    user_id: str,
    ref: str,
    *,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
) -> Any:
    resolved_user_id = str(user_id)
    vault_ref = parse_vault_ref(ref)
    if vault_ref.user_id != resolved_user_id:
        raise ProviderSecretNotFoundError(
            f"Provider secret belongs to {vault_ref.user_id!r}, not {resolved_user_id!r}"
        )
    storage_backend, _data_dir = _providers_backend(
        resolved_user_id,
        data_dir=data_dir,
        backend=backend,
    )
    payload = _load_providers_payload(resolved_user_id, storage_backend, aws_region=aws_region)
    if payload is None:
        raise ProviderSecretNotFoundError(f"Provider secret not found: {ref}")
    return _walk_provider_path(payload, vault_ref)


def set_provider_secret(
    user_id: str,
    ref: str,
    value: Any,
    *,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
    migrated_from_ref: str | None = None,
    require_existing_match: bool = False,
) -> str:
    resolved_user_id = str(user_id)
    vault_ref = parse_vault_ref(ref)
    if vault_ref.user_id != resolved_user_id:
        raise ProviderSecretNotFoundError(
            f"Provider secret belongs to {vault_ref.user_id!r}, not {resolved_user_id!r}"
        )
    storage_backend, resolved_data_dir = _providers_backend(
        resolved_user_id,
        data_dir=data_dir,
        backend=backend,
    )
    with UserEnvelopeLock(resolved_user_id, resolved_data_dir):
        payload = _load_providers_payload(
            resolved_user_id,
            storage_backend,
            aws_region=aws_region,
        )
        if payload is None:
            payload = _empty_providers_payload(resolved_user_id)
        container, leaf = _provider_leaf_container(payload, vault_ref, create=True)
        existing = container.get(leaf)
        if require_existing_match and existing is not None and existing != value:
            raise InvalidCiphertextError(
                "Existing provider secret plaintext differs from the requested value; manual intervention required"
            )
        container[leaf] = value
        _stamp_provider_metadata(container, vault_ref.provider, vault_ref.path, migrated_from_ref)
        payload["updated_at"] = _utc_now_iso()
        storage_backend.put(
            resolved_user_id,
            _PROVIDERS_KIND,
            _encrypt_providers_blob(
                resolved_user_id,
                payload,
                kms_key_arn=kms_key_arn,
                aws_region=aws_region,
            ),
        )
    return ref


def delete_provider_secret(
    user_id: str,
    ref: str,
    *,
    data_dir: Path | None = None,
    kms_key_arn: str | None = None,
    aws_region: str | None = None,
    backend: str | None = None,
) -> None:
    resolved_user_id = str(user_id)
    vault_ref = parse_vault_ref(ref)
    if vault_ref.user_id != resolved_user_id:
        raise ProviderSecretNotFoundError(
            f"Provider secret belongs to {vault_ref.user_id!r}, not {resolved_user_id!r}"
        )
    storage_backend, resolved_data_dir = _providers_backend(
        resolved_user_id,
        data_dir=data_dir,
        backend=backend,
    )
    with UserEnvelopeLock(resolved_user_id, resolved_data_dir):
        payload = _load_providers_payload(
            resolved_user_id,
            storage_backend,
            aws_region=aws_region,
        )
        if payload is None:
            return
        try:
            container, leaf = _provider_leaf_container(payload, vault_ref, create=False)
        except ProviderSecretNotFoundError:
            return
        container.pop(leaf, None)
        payload["updated_at"] = _utc_now_iso()
        storage_backend.put(
            resolved_user_id,
            _PROVIDERS_KIND,
            _encrypt_providers_blob(
                resolved_user_id,
                payload,
                kms_key_arn=kms_key_arn,
                aws_region=aws_region,
            ),
        )


def evict_caches(user_id: str | None = None) -> None:
    cache = _DB_DEK_CACHE.get()
    if cache is None:
        return
    if user_id is None:
        cache.clear()
        return
    resolved_user_id = str(user_id)
    for key in list(cache):
        if key[1] == resolved_user_id:
            cache.pop(key, None)
