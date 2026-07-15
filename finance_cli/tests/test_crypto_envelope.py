from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
import uuid
from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws

from finance_cli import crypto_envelope
from finance_cli.exceptions import (
    BackendMismatchError,
    CrossUserBundleError,
    InvalidCiphertextError,
    KMSAccessDeniedError,
    KMSUnavailableError,
    ProviderSecretNotFoundError,
)


def _configure_kms(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    client = boto3.client("kms", region_name="us-east-1")
    key_arn = client.create_key(Description="test envelope key")["KeyMetadata"]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    return key_arn


def _header_bounds(bundle: bytes) -> tuple[int, int, int]:
    header_len_offset = len(crypto_envelope.MAGIC) + 2 + 16
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


def _replace_header(bundle: bytes, header: dict) -> bytes:
    header_len_offset, _header_offset, header_end = _header_bounds(bundle)
    header_json = _canonical_header(header)
    return (
        bundle[:header_len_offset]
        + len(header_json).to_bytes(4, "big")
        + header_json
        + bundle[header_end:]
    )


@pytest.fixture()
def kms_env(monkeypatch: pytest.MonkeyPatch):
    with mock_aws():
        _configure_kms(monkeypatch)
        yield


def test_encrypt_decrypt_bundle_v3_round_trip(kms_env) -> None:
    bundle_id = str(uuid.uuid4())
    encrypted = crypto_envelope.encrypt_bundle_v3(
        b"tar bytes",
        "alice",
        bundle_id,
        mode="portable",
        recovery_db_dek_present=True,
    )

    plaintext = crypto_envelope.decrypt_bundle_v3(encrypted, "alice")
    header = crypto_envelope.parse_bundle_header_v3(encrypted)

    assert plaintext == b"tar bytes"
    assert header["user_id"] == "alice"
    assert header["bundle_id"] == bundle_id
    assert header["mode"] == "portable"
    assert header["recovery_db_dek_present"] is True


def test_encryption_context_mismatch_raises_invalid_ciphertext(kms_env) -> None:
    encrypted = crypto_envelope.encrypt_bundle_v3(
        b"tar bytes",
        "alice",
        str(uuid.uuid4()),
        mode="compact",
        recovery_db_dek_present=False,
    )
    header = crypto_envelope.parse_bundle_header_v3(encrypted)
    header["encryption_context"]["bundle_id"] = str(uuid.uuid4())
    tampered = _replace_header(encrypted, header)

    with pytest.raises(InvalidCiphertextError):
        crypto_envelope.decrypt_bundle_v3(tampered, "alice")


def test_aad_tampering_rejected(kms_env) -> None:
    encrypted = bytearray(
        crypto_envelope.encrypt_bundle_v3(
            b"tar bytes",
            "alice",
            str(uuid.uuid4()),
            mode="compact",
            recovery_db_dek_present=False,
        )
    )
    _header_len_offset, header_offset, header_end = _header_bounds(bytes(encrypted))
    idx = bytes(encrypted).index(b"compact", header_offset, header_end)
    encrypted[idx + len("compac")] = ord("u")

    with pytest.raises(InvalidCiphertextError):
        crypto_envelope.decrypt_bundle_v3(bytes(encrypted), "alice")


def test_header_user_id_flip_rejected_before_kms(kms_env) -> None:
    encrypted = crypto_envelope.encrypt_bundle_v3(
        b"tar bytes",
        "alice",
        str(uuid.uuid4()),
        mode="compact",
        recovery_db_dek_present=False,
    )
    header = crypto_envelope.parse_bundle_header_v3(encrypted)
    header["user_id"] = "bob"
    tampered = _replace_header(encrypted, header)

    with pytest.raises(CrossUserBundleError):
        crypto_envelope.decrypt_bundle_v3(tampered, "alice")


def test_kms_unavailability_handling(monkeypatch: pytest.MonkeyPatch) -> None:
    class UnavailableKMS:
        def generate_data_key(self, **_kwargs):
            raise EndpointConnectionError(endpoint_url="https://kms.us-east-1.amazonaws.com")

    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", "arn:aws:kms:us-east-1:123:key/test")
    monkeypatch.setattr(crypto_envelope, "_kms", lambda: UnavailableKMS())

    with pytest.raises(KMSUnavailableError):
        crypto_envelope.encrypt_bundle_v3(
            b"tar bytes",
            "alice",
            str(uuid.uuid4()),
            mode="compact",
            recovery_db_dek_present=False,
        )


def test_kms_access_denied_handling(monkeypatch: pytest.MonkeyPatch) -> None:
    class DeniedKMS:
        def generate_data_key(self, **_kwargs):
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
                "GenerateDataKey",
            )

    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", "arn:aws:kms:us-east-1:123:key/test")
    monkeypatch.setattr(crypto_envelope, "_kms", lambda: DeniedKMS())

    with pytest.raises(KMSAccessDeniedError):
        crypto_envelope.encrypt_bundle_v3(
            b"tar bytes",
            "alice",
            str(uuid.uuid4()),
            mode="compact",
            recovery_db_dek_present=False,
        )


def test_file_backend_round_trip_and_provenance(tmp_path: Path) -> None:
    backend = crypto_envelope.FileStorageBackend(tmp_path)

    assert backend.get("alice", "db-dek") is None
    backend.put("alice", "db-dek", b"encrypted db dek")

    assert backend.get("alice", "db-dek") == b"encrypted db dek"
    provenance = json.loads((tmp_path / "alice" / ".envelope_provenance.json").read_text(encoding="utf-8"))
    assert provenance["db-dek"] == "file"
    assert "recorded_at" in provenance


def test_keychain_backend_round_trip_and_mismatch(tmp_path: Path) -> None:
    class FakeKeyring:
        def __init__(self) -> None:
            self.store: dict[tuple[str, str], str] = {}

        def get_password(self, service: str, account: str) -> str | None:
            return self.store.get((service, account))

        def set_password(self, service: str, account: str, value: str) -> None:
            self.store[(service, account)] = value

        def delete_password(self, service: str, account: str) -> None:
            self.store.pop((service, account), None)

    keychain = crypto_envelope.MacOSKeychainBackend(tmp_path, keyring_module=FakeKeyring())
    keychain.put("alice", "db-dek", b"encrypted db dek")

    assert keychain.get("alice", "db-dek") == b"encrypted db dek"
    provenance = json.loads((tmp_path / "alice" / ".envelope_provenance.json").read_text(encoding="utf-8"))
    assert provenance["db-dek"] == "keychain:macos"

    with pytest.raises(BackendMismatchError):
        crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")


def _replace_db_dek_user_id(blob: bytes, replacement: str) -> bytes:
    offset = len(crypto_envelope.DB_DEK_MAGIC) + 2
    user_id_len = int.from_bytes(blob[offset : offset + 2], "big")
    offset += 2
    replacement_bytes = replacement.encode("utf-8")
    assert len(replacement_bytes) == user_id_len
    return blob[:offset] + replacement_bytes + blob[offset + user_id_len :]


def test_db_dek_provision_get_rotate_round_trip(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    dek = b"\x11" * 32

    crypto_envelope.provision_db_dek("alice", dek=dek, data_dir=tmp_path)
    before = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")

    assert crypto_envelope.get_db_dek("alice", data_dir=tmp_path) == dek

    crypto_envelope.rotate_db_dek("alice", data_dir=tmp_path)
    after = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")

    assert after != before
    assert crypto_envelope.get_db_dek("alice", data_dir=tmp_path) == dek


def test_db_dek_encryption_context_mismatch_raises(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    crypto_envelope.provision_db_dek("alice", dek=b"\x12" * 32, data_dir=tmp_path)
    blob = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")
    assert blob is not None

    tampered = _replace_db_dek_user_id(blob, "alixe")
    crypto_envelope.FileStorageBackend(tmp_path).put("alixe", "db-dek", tampered)

    with pytest.raises(InvalidCiphertextError):
        crypto_envelope.get_db_dek("alixe", data_dir=tmp_path)


def test_install_db_dek_blob_routes_through_backend_put(
    kms_env,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    crypto_envelope.provision_db_dek("alice", dek=b"\x13" * 32, data_dir=tmp_path)
    blob = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")
    assert blob is not None
    calls: list[tuple[str, str, bytes]] = []

    class FakeBackend:
        name = "fake"

        def get(self, user_id: str, kind: str) -> bytes | None:
            return None

        def put(self, user_id: str, kind: str, payload: bytes) -> None:
            calls.append((user_id, kind, payload))

        def delete(self, user_id: str, kind: str) -> None:
            raise AssertionError("delete should not be called")

    monkeypatch.setattr(crypto_envelope, "select_backend", lambda _user_id, _data_dir: FakeBackend())

    crypto_envelope.install_db_dek_blob("alice", blob, data_dir=tmp_path)

    assert calls == [("alice", "db-dek", blob)]


def test_db_dek_cache_ttl_eviction(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_CLI_DB_DEK_CACHE_TTL_SECONDS", "10")
    now = {"value": 100.0}
    monkeypatch.setattr(crypto_envelope.time, "monotonic", lambda: now["value"])
    dek1 = b"\x21" * 32
    dek2 = b"\x22" * 32

    crypto_envelope.provision_db_dek("alice", dek=dek1, data_dir=tmp_path)
    assert crypto_envelope.get_db_dek("alice", data_dir=tmp_path) == dek1

    replacement = crypto_envelope._encrypt_db_dek_blob("alice", dek2)
    crypto_envelope.FileStorageBackend(tmp_path).put("alice", "db-dek", replacement)
    assert crypto_envelope.get_db_dek("alice", data_dir=tmp_path) == dek1

    now["value"] = 111.0
    assert crypto_envelope.get_db_dek("alice", data_dir=tmp_path) == dek2


def _configure_providers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
    crypto_envelope.evict_caches()


def test_provider_secret_round_trip(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_providers(monkeypatch, tmp_path)
    ref = crypto_envelope.format_vault_ref("1", "anthropic", "api_key")

    assert crypto_envelope.set_provider_secret("1", ref, "sk-ant-test") == ref
    assert crypto_envelope.get_provider_secret("1", ref) == "sk-ant-test"

    blob = crypto_envelope.FileStorageBackend(tmp_path).get("1", "providers")
    assert blob is not None
    parsed = crypto_envelope._parse_providers_blob(blob)
    assert parsed.user_id == "1"


def test_telegram_provider_secret_round_trip(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_providers(monkeypatch, tmp_path)
    ref = crypto_envelope.format_vault_ref("1", "telegram", "bot_token")

    crypto_envelope.set_provider_secret("1", ref, "123:ABC")

    assert crypto_envelope.get_provider_secret("1", ref) == "123:ABC"


def test_vault_ref_parsing_edge_cases() -> None:
    parsed = crypto_envelope.parse_vault_ref("vault://1/plaid/items/item_123/access_token")
    assert parsed.user_id == "1"
    assert parsed.provider == "plaid"
    assert parsed.path == ["items", "item_123", "access_token"]
    assert crypto_envelope.format_vault_ref("1", "stripe", "publishable_key") == "vault://1/stripe/publishable_key"
    telegram = crypto_envelope.parse_vault_ref("vault://1/telegram/bot_token")
    assert telegram.user_id == "1"
    assert telegram.provider == "telegram"
    assert telegram.path == ["bot_token"]

    with pytest.raises(ValueError):
        crypto_envelope.parse_vault_ref("finance-cli/users/1/stripe-api-key")
    with pytest.raises(ValueError):
        crypto_envelope.format_vault_ref("1", "plaid")


def test_provider_server_only_guard_when_env_unset(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("FINANCE_CLI_PROVIDERS_ENABLED", raising=False)
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path))

    with pytest.raises(ProviderSecretNotFoundError, match="server-only"):
        crypto_envelope.get_provider_secret("1", "vault://1/anthropic/api_key")


def test_provider_schema_version_one_reads_back(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_providers(monkeypatch, tmp_path)
    payload = {
        "schema_version": 1,
        "user_id": "1",
        "updated_at": "2026-04-30T12:00:00Z",
        "rotated_at": "2026-04-30T12:00:00Z",
        "providers": {"snaptrade": {"user_secret": "snap-secret"}},
        "_unknown_fields": {"future": True},
    }
    blob = crypto_envelope._encrypt_providers_blob("1", payload)
    crypto_envelope.FileStorageBackend(tmp_path).put("1", "providers", blob)

    assert crypto_envelope.get_provider_secret("1", "vault://1/snaptrade/user_secret") == "snap-secret"


def test_concurrent_provider_writes_serialize(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_providers(monkeypatch, tmp_path)

    refs = [
        crypto_envelope.format_vault_ref("1", "stripe", "secret_key"),
        crypto_envelope.format_vault_ref("1", "stripe", "publishable_key"),
        crypto_envelope.format_vault_ref("1", "plaid", "items", "item_1", "access_token"),
    ]

    def write(pair: tuple[str, str]) -> None:
        ref, value = pair
        crypto_envelope.set_provider_secret("1", ref, value)

    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(write, zip(refs, ["sk_live", "pk_live", "access"], strict=True)))

    assert crypto_envelope.get_provider_secret("1", refs[0]) == "sk_live"
    assert crypto_envelope.get_provider_secret("1", refs[1]) == "pk_live"
    assert crypto_envelope.get_provider_secret("1", refs[2]) == "access"


def test_provider_wrong_user_raises(kms_env, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_providers(monkeypatch, tmp_path)
    ref = crypto_envelope.format_vault_ref("1", "anthropic", "api_key")
    crypto_envelope.set_provider_secret("1", ref, "sk-ant")

    with pytest.raises(ProviderSecretNotFoundError):
        crypto_envelope.get_provider_secret("2", ref)
