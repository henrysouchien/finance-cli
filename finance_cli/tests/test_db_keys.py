from __future__ import annotations

import asyncio
import base64
import importlib
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope
from finance_cli import db_keys as db_keys_module
from finance_cli import secrets_backend
from finance_cli.exceptions import DBDEKNotFoundError

_TEST_DEK = b"\x00" * 32


@pytest.fixture()
def real_db_keys(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    secrets_backend._client = None
    module = importlib.reload(db_keys_module)
    yield module
    secrets_backend._client = None
    db_keys_module.get_user_db_key = lambda _user_id, **_kwargs: _TEST_DEK
    db_keys_module.provision_user_db_key = lambda _user_id, **_kwargs: None
    db_keys_module.delete_user_db_key = lambda _user_id: None


def _configure_kms(monkeypatch: pytest.MonkeyPatch, data_dir: Path) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("FINANCE_CLI_DATA_DIR", str(data_dir))
    monkeypatch.setenv("FINANCE_CLI_ENVELOPE_BACKEND", "file")
    key_arn = boto3.client("kms", region_name="us-east-1").create_key(Description="test db keys")[
        "KeyMetadata"
    ]["Arn"]
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", key_arn)
    crypto_envelope.evict_caches()


def _put_sm_dek(user_id: str, dek: bytes) -> str:
    ref = f"finance-cli/users/{user_id}/db-key"
    secrets_backend.put_secret(ref, base64.b64encode(dek).decode("ascii"))
    return ref


def _force_sm_fallback(real_db_keys, monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_db_dek(*_args, **_kwargs):
        raise DBDEKNotFoundError("db-dek.enc missing for test")

    monkeypatch.setattr(real_db_keys.crypto_envelope, "get_db_dek", missing_db_dek)


@mock_aws
def test_provision_idempotent(real_db_keys) -> None:
    ref1 = real_db_keys.provision_user_db_key("u1")
    ref2 = real_db_keys.provision_user_db_key("u1")

    assert ref1 == "finance-cli/users/u1/db-key"
    assert ref2 == ref1
    assert len(real_db_keys.get_user_db_key("u1")) == 32


def test_dek_cache_per_request(real_db_keys, monkeypatch: pytest.MonkeyPatch) -> None:
    _force_sm_fallback(real_db_keys, monkeypatch)
    payloads = {
        "alice": base64.b64encode(b"\x01" * 32).decode("ascii"),
        "bob": base64.b64encode(b"\x02" * 32).decode("ascii"),
    }
    calls: dict[str, int] = {"alice": 0, "bob": 0}

    def fake_get_secret(secret_name: str, *, missing_ok: bool = False) -> str | None:
        del missing_ok
        user_id = secret_name.split("/")[-2]
        calls[user_id] += 1
        return payloads[user_id]

    monkeypatch.setattr(real_db_keys.secrets_backend, "get_secret", fake_get_secret)

    async def fetch_twice(user_id: str) -> tuple[bytes, bytes]:
        token = real_db_keys.begin_request_cache()
        try:
            return (
                real_db_keys.get_user_db_key(user_id),
                real_db_keys.get_user_db_key(user_id),
            )
        finally:
            real_db_keys.end_request_cache(token)

    async def run_tasks() -> tuple[tuple[bytes, bytes], tuple[bytes, bytes]]:
        return await asyncio.gather(fetch_twice("alice"), fetch_twice("bob"))

    alice_values, bob_values = asyncio.run(run_tasks())

    assert alice_values == (b"\x01" * 32, b"\x01" * 32)
    assert bob_values == (b"\x02" * 32, b"\x02" * 32)
    assert calls == {"alice": 1, "bob": 1}


def test_dek_cache_cleared(real_db_keys, monkeypatch: pytest.MonkeyPatch) -> None:
    _force_sm_fallback(real_db_keys, monkeypatch)
    payload = base64.b64encode(b"\x03" * 32).decode("ascii")
    calls = 0

    def fake_get_secret(secret_name: str, *, missing_ok: bool = False) -> str | None:
        del secret_name, missing_ok
        nonlocal calls
        calls += 1
        return payload

    monkeypatch.setattr(real_db_keys.secrets_backend, "get_secret", fake_get_secret)

    token = real_db_keys.begin_request_cache()
    try:
        assert real_db_keys.get_user_db_key("alice") == b"\x03" * 32
        assert real_db_keys.get_user_db_key("alice") == b"\x03" * 32
    finally:
        real_db_keys.end_request_cache(token)

    token = real_db_keys.begin_request_cache()
    try:
        assert real_db_keys.get_user_db_key("alice") == b"\x03" * 32
    finally:
        real_db_keys.end_request_cache(token)

    assert calls == 2


def test_end_request_cache_does_not_corrupt_stale_refs(
    real_db_keys, monkeypatch: pytest.MonkeyPatch
) -> None:
    _force_sm_fallback(real_db_keys, monkeypatch)
    real_dek = b"\x42" * 32
    fetch_calls = {"n": 0}

    def fake_get_secret(ref: str, *, missing_ok: bool = False) -> str | None:
        del ref, missing_ok
        fetch_calls["n"] += 1
        return base64.b64encode(real_dek).decode("ascii")

    monkeypatch.setattr(real_db_keys.secrets_backend, "get_secret", fake_get_secret)

    child_dek: dict[str, bytes] = {}

    async def _child() -> None:
        child_dek["value"] = real_db_keys.get_user_db_key("1")

    async def scenario() -> None:
        token = real_db_keys.begin_request_cache()
        assert real_db_keys.get_user_db_key("1") == real_dek
        assert fetch_calls["n"] == 1

        child_task = asyncio.create_task(_child())
        real_db_keys.end_request_cache(token)
        await child_task

    asyncio.run(scenario())

    assert child_dek["value"] == real_dek
    assert fetch_calls["n"] == 2


@mock_aws
def test_delete_soft_deletes(real_db_keys) -> None:
    ref = real_db_keys.provision_user_db_key("u-delete")

    real_db_keys.delete_user_db_key("u-delete")

    described = secrets_backend._get_client().describe_secret(SecretId=ref)
    assert described["Name"] == ref
    assert described["DeletedDate"] is not None


@mock_aws
def test_dek_length_validation(real_db_keys) -> None:
    ref = "finance-cli/users/u-bad/db-key"
    secrets_backend.put_secret(ref, base64.b64encode(b"\x04" * 31).decode("ascii"))

    with pytest.raises(ValueError, match="unexpected length"):
        real_db_keys.get_user_db_key("u-bad")


@mock_aws
def test_vault_first_read(real_db_keys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_kms(monkeypatch, tmp_path)
    _put_sm_dek("u-vault", b"\x31" * 32)
    crypto_envelope.provision_db_dek("u-vault", dek=b"\x32" * 32, data_dir=tmp_path)

    assert real_db_keys.get_user_db_key("u-vault") == b"\x32" * 32


@mock_aws
def test_sm_fallback_when_vault_missing(real_db_keys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_kms(monkeypatch, tmp_path)
    _put_sm_dek("u-sm", b"\x33" * 32)

    assert real_db_keys.get_user_db_key("u-sm") == b"\x33" * 32


@mock_aws
def test_sm_and_vault_both_work_during_migration(
    real_db_keys,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_kms(monkeypatch, tmp_path)
    dek = b"\x34" * 32
    _put_sm_dek("u-both", dek)
    crypto_envelope.provision_db_dek("u-both", dek=dek, data_dir=tmp_path)

    assert real_db_keys._get_legacy_user_db_key("u-both") == dek
    assert real_db_keys.get_user_db_key("u-both") == dek


@mock_aws
def test_vault_only_after_migration(real_db_keys, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_kms(monkeypatch, tmp_path)
    dek = b"\x35" * 32
    ref = _put_sm_dek("u-cutover", dek)
    crypto_envelope.provision_db_dek("u-cutover", dek=dek, data_dir=tmp_path)

    real_db_keys.delete_user_db_key("u-cutover")

    described = secrets_backend._get_client().describe_secret(SecretId=ref)
    assert described["DeletedDate"] is not None
    assert real_db_keys.get_user_db_key("u-cutover") == dek


@mock_aws
def test_vault_read_accepts_explicit_data_dir(
    real_db_keys,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_data_dir = tmp_path / "active"
    explicit_data_dir = tmp_path / "explicit"
    _configure_kms(monkeypatch, active_data_dir)
    dek = b"\x36" * 32
    ref = _put_sm_dek("u-explicit", dek)
    crypto_envelope.provision_db_dek("u-explicit", dek=dek, data_dir=explicit_data_dir)

    real_db_keys.delete_user_db_key("u-explicit")

    described = secrets_backend._get_client().describe_secret(SecretId=ref)
    assert described["DeletedDate"] is not None
    assert real_db_keys.get_user_db_key("u-explicit", data_dir=explicit_data_dir) == dek
