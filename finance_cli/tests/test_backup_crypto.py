from __future__ import annotations

import base64
import hashlib
import hmac
import json
import uuid

import pytest
import boto3
from moto import mock_aws

from finance_cli import backup_crypto, crypto_envelope, secrets_backend
from finance_cli.exceptions import InvalidCiphertextError


@pytest.fixture(autouse=True)
def _mock_backup_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_SECRETS_NAMESPACE", "finance-cli-test")
    secrets_backend._client = None
    with mock_aws():
        key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="test")["KeyMetadata"]["Arn"]
        monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
        yield
    secrets_backend._client = None


def _manifest(*, user_id: str = "alice") -> dict:
    return {
        "schema_version": 2,
        "bundle_id": str(uuid.uuid4()),
        "user_id": user_id,
        "created_at": "2026-04-15T12:34:56Z",
        "finance_cli_version": "1.2.3",
        "sqlcipher_db": True,
        "db_integrity_check": "ok",
        "migration_version": 73,
        "files": [
            {"path": "finance.db", "size_bytes": 12345},
            {"path": "rules.yaml", "size_bytes": 42},
        ],
    }


def _canonical_manifest_payload(manifest: dict) -> bytes:
    payload = dict(manifest)
    payload.pop("signature", None)
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _header_bounds(bundle: bytes) -> tuple[int, int, int]:
    header_len_offset = len(backup_crypto.MAGIC_V3) + 2 + 16
    header_offset = header_len_offset + 4
    header_len = int.from_bytes(bundle[header_len_offset:header_offset], "big")
    return header_len_offset, header_offset, header_offset + header_len


def _canonical_header(header: dict) -> bytes:
    return json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _replace_v3_header(bundle: bytes, header: dict) -> bytes:
    header_len_offset, _header_offset, header_end = _header_bounds(bundle)
    header_json = _canonical_header(header)
    return (
        bundle[:header_len_offset]
        + len(header_json).to_bytes(4, "big")
        + header_json
        + bundle[header_end:]
    )


def test_encrypt_decrypt_roundtrip() -> None:
    bundle_id = str(uuid.uuid4())
    encrypted = backup_crypto.encrypt_bundle(b"test tar bytes", "alice", bundle_id)

    plaintext, header = backup_crypto.decrypt_bundle(encrypted, "alice")

    assert plaintext == b"test tar bytes"
    assert header["user_id"] == "alice"
    assert header["bundle_id"] == bundle_id
    assert header["dek_secret_ref"] == f"finance-cli-test/users/alice/backup-keys/{bundle_id}"


def test_encrypt_decrypt_v3_roundtrip() -> None:
    bundle_id = str(uuid.uuid4())
    encrypted = backup_crypto.encrypt_bundle_v3(
        b"test tar bytes",
        "alice",
        bundle_id,
        mode="portable",
        recovery_db_dek_present=True,
    )

    plaintext, header = backup_crypto.decrypt_bundle_v3(encrypted, "alice")

    assert encrypted[: len(backup_crypto.MAGIC_V3)] == backup_crypto.MAGIC_V3
    assert plaintext == b"test tar bytes"
    assert header["user_id"] == "alice"
    assert header["bundle_id"] == bundle_id
    assert header["mode"] == "portable"
    assert header["recovery_db_dek_present"] is True


def test_decrypt_bundle_any_version_dispatches_by_magic() -> None:
    v2 = backup_crypto.encrypt_bundle(b"v2 tar bytes", "alice", str(uuid.uuid4()))
    v3 = backup_crypto.encrypt_bundle_v3(
        b"v3 tar bytes",
        "alice",
        str(uuid.uuid4()),
        mode="compact",
        recovery_db_dek_present=False,
    )

    assert backup_crypto.decrypt_bundle_any_version(v2, "alice")[0] == b"v2 tar bytes"
    assert backup_crypto.decrypt_bundle_any_version(v3, "alice")[0] == b"v3 tar bytes"


@pytest.mark.parametrize(
    ("mode", "recovery_present"),
    [("portable", True), ("compact", False)],
)
def test_v3_mode_round_trips(mode: str, recovery_present: bool) -> None:
    encrypted = backup_crypto.encrypt_bundle_v3(
        b"test tar bytes",
        "alice",
        str(uuid.uuid4()),
        mode=mode,
        recovery_db_dek_present=recovery_present,
    )

    _plaintext, header = backup_crypto.decrypt_bundle_v3(encrypted, "alice")

    assert header["mode"] == mode
    assert header["recovery_db_dek_present"] is recovery_present


@pytest.mark.parametrize(
    ("field", "value"),
    [("mode", "compacu"), ("recovery_db_dek_present", True)],
)
def test_v3_aad_authenticates_mode_and_recovery_fields(field: str, value) -> None:
    encrypted = backup_crypto.encrypt_bundle_v3(
        b"test tar bytes",
        "alice",
        str(uuid.uuid4()),
        mode="compact",
        recovery_db_dek_present=False,
    )
    header = backup_crypto.parse_bundle_header_v3(encrypted)
    header.pop("bundle_id")
    header.pop("version")
    header.pop("header_len")
    header.pop("payload_offset")
    header[field] = value
    tampered = _replace_v3_header(encrypted, header)

    with pytest.raises(InvalidCiphertextError):
        crypto_envelope.decrypt_bundle_v3(tampered, "alice")


def test_tampered_ciphertext_rejected() -> None:
    encrypted = bytearray(backup_crypto.encrypt_bundle(b"abcdef", "alice", str(uuid.uuid4())))
    encrypted[-17] ^= 0x01

    with pytest.raises(ValueError, match="authentication failed"):
        backup_crypto.decrypt_bundle(bytes(encrypted), "alice")


def test_tampered_manifest_rejected() -> None:
    manifest = backup_crypto.sign_manifest(_manifest(), "alice")
    manifest["migration_version"] = 999

    assert backup_crypto.verify_manifest(manifest) is False


def test_header_user_mismatch_rejected() -> None:
    encrypted = backup_crypto.encrypt_bundle(b"abcdef", "alice", str(uuid.uuid4()))

    with pytest.raises(ValueError, match="belongs to"):
        backup_crypto.decrypt_bundle(encrypted, "bob")


def test_signing_key_created_on_first_backup() -> None:
    secret_ref = "finance-cli-test/users/alice/backup-signing-key"
    assert secrets_backend.get_secret(secret_ref, missing_ok=True) is None

    manifest = backup_crypto.sign_manifest(_manifest(), "alice")

    assert manifest["signature"]["key_ref"] == secret_ref
    assert secrets_backend.get_secret(secret_ref, missing_ok=True) is not None


def test_verify_uses_manifest_user_id_not_signature_key_ref() -> None:
    alice_ref = backup_crypto.ensure_signing_key("alice")
    mallory_ref = backup_crypto.ensure_signing_key("mallory")
    alice_key = base64.b64decode(secrets_backend.get_secret(alice_ref), validate=True)
    mallory_key = base64.b64decode(secrets_backend.get_secret(mallory_ref), validate=True)
    assert alice_key != mallory_key

    attack_manifest = _manifest(user_id="alice")
    attack_manifest["signature"] = {
        "alg": "HMAC-SHA256",
        "key_ref": mallory_ref,
        "signature_hex": hmac.new(
            mallory_key,
            _canonical_manifest_payload(attack_manifest),
            hashlib.sha256,
        ).hexdigest(),
    }

    assert backup_crypto.verify_manifest(attack_manifest) is False
