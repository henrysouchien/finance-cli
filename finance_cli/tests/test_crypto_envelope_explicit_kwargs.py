from __future__ import annotations

import uuid
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from finance_cli import crypto_envelope
from finance_cli.exceptions import ProviderSecretNotFoundError


_CONFIG_ENVS = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "FINANCE_CLI_KMS_KEY_ARN",
    "FINANCE_CLI_ENVELOPE_BACKEND",
    "FINANCE_CLI_PROVIDERS_ENABLED",
    "FINANCE_WEB_DATA_ROOT",
)


def _clear_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _CONFIG_ENVS:
        monkeypatch.delenv(name, raising=False)
    crypto_envelope.evict_caches()


def _create_kms_key(region: str) -> str:
    client = boto3.client("kms", region_name=region)
    return client.create_key(Description="explicit kwargs envelope key")["KeyMetadata"]["Arn"]


def test_kms_key_arn_explicit_arg_precedes_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_KMS_KEY_ARN", "arn:aws:kms:us-east-1:123:key/from-env")

    assert (
        crypto_envelope._kms_key_arn(kms_key_arn="arn:aws:kms:us-west-2:123:key/explicit")
        == "arn:aws:kms:us-west-2:123:key/explicit"
    )


def test_kms_explicit_region_precedes_env(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str | None]] = []
    sentinel = object()

    def fake_client(service_name: str, *, region_name: str | None = None):
        calls.append((service_name, region_name))
        return sentinel

    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setattr(crypto_envelope.boto3, "client", fake_client)

    assert crypto_envelope._kms(aws_region="us-west-2") is sentinel
    assert calls == [("kms", "us-west-2")]


def test_db_dek_entry_points_accept_explicit_config_without_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = "us-west-2"
    dek = b"\x41" * 32
    restore_root = tmp_path / "restore"

    with mock_aws():
        _clear_config_env(monkeypatch)
        key_arn = _create_kms_key(region)

        crypto_envelope.provision_db_dek(
            "alice",
            dek=dek,
            data_dir=tmp_path,
            kms_key_arn=key_arn,
            aws_region=region,
            backend="file",
        )
        assert crypto_envelope.has_db_dek("alice", data_dir=tmp_path, backend="file")

        crypto_envelope.evict_caches("alice")
        assert (
            crypto_envelope.get_db_dek(
                "alice",
                data_dir=tmp_path,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
            == dek
        )

        before = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")
        assert before is not None
        crypto_envelope.rotate_db_dek(
            "alice",
            data_dir=tmp_path,
            kms_key_arn=key_arn,
            aws_region=region,
            backend="file",
        )
        after = crypto_envelope.FileStorageBackend(tmp_path).get("alice", "db-dek")

        assert after is not None
        assert after != before
        crypto_envelope.evict_caches("alice")
        assert (
            crypto_envelope.get_db_dek(
                "alice",
                data_dir=tmp_path,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
            == dek
        )

        crypto_envelope.install_db_dek_blob(
            "alice",
            after,
            data_dir=restore_root,
            backend="file",
        )
        assert crypto_envelope.has_db_dek("alice", data_dir=restore_root, backend="file")
        crypto_envelope.evict_caches("alice")
        assert (
            crypto_envelope.get_db_dek(
                "alice",
                data_dir=restore_root,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
            == dek
        )


def test_bundle_v3_entry_points_accept_explicit_config_without_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = "us-west-2"

    with mock_aws():
        _clear_config_env(monkeypatch)
        key_arn = _create_kms_key(region)
        bundle_id = str(uuid.uuid4())

        encrypted = crypto_envelope.encrypt_bundle_v3(
            b"tar bytes",
            "alice",
            bundle_id,
            mode="portable",
            recovery_db_dek_present=True,
            kms_key_arn=key_arn,
            aws_region=region,
        )

        assert (
            crypto_envelope.decrypt_bundle_v3(
                encrypted,
                "alice",
                kms_key_arn=key_arn,
                aws_region=region,
            )
            == b"tar bytes"
        )
        header = crypto_envelope.parse_bundle_header_v3(encrypted)
        assert header["kms_key_arn"] == key_arn


def test_provider_secret_entry_points_accept_explicit_config_without_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = "us-west-2"

    with mock_aws():
        _clear_config_env(monkeypatch)
        monkeypatch.setenv("FINANCE_CLI_PROVIDERS_ENABLED", "1")
        key_arn = _create_kms_key(region)
        ref = crypto_envelope.format_vault_ref("alice", "stripe", "secret_key")

        assert (
            crypto_envelope.set_provider_secret(
                "alice",
                ref,
                "sk-test",
                data_dir=tmp_path,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
            == ref
        )
        assert (
            crypto_envelope.get_provider_secret(
                "alice",
                ref,
                data_dir=tmp_path,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
            == "sk-test"
        )

        crypto_envelope.delete_provider_secret(
            "alice",
            ref,
            data_dir=tmp_path,
            kms_key_arn=key_arn,
            aws_region=region,
            backend="file",
        )

        with pytest.raises(ProviderSecretNotFoundError):
            crypto_envelope.get_provider_secret(
                "alice",
                ref,
                data_dir=tmp_path,
                kms_key_arn=key_arn,
                aws_region=region,
                backend="file",
            )
