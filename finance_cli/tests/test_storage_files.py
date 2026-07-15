from __future__ import annotations

import pytest

from finance_cli import storage_files
from finance_cli.storage_client.auth import JWTAuthProvider
from finance_cli.storage_client.errors import PathInvalidError


class _StaticSecretsClient:
    def __init__(self, private_key_pem: str, kid: str) -> None:
        self.private_key_pem = private_key_pem
        self.kid = kid

    def get_secret_value(self, *, SecretId: str):
        return {"SecretString": self.private_key_pem}

    def describe_secret(self, *, SecretId: str):
        return {"Tags": [{"Key": "kid", "Value": self.kid}]}


def _auth(local_storage_proxy):
    return JWTAuthProvider(
        refresh_interval_seconds=60 * 60,
        secrets_client=_StaticSecretsClient(local_storage_proxy.private_key_pem, local_storage_proxy.kid),
    )


def test_read_write_round_trip(local_storage_proxy) -> None:
    provider = _auth(local_storage_proxy)
    try:
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-roundtrip",
            product="finance_cli",
            relative_path="agent_memory.md",
            content=b"# Memory\n",
            auth_provider=provider,
        )

        assert storage_files.read_file(
            local_storage_proxy.target,
            user_id="synthetic-files-roundtrip",
            product="finance_cli",
            relative_path="agent_memory.md",
            auth_provider=provider,
        ) == b"# Memory\n"
    finally:
        provider.close()


def test_write_file_streams_chunks(local_storage_proxy) -> None:
    provider = _auth(local_storage_proxy)
    content = b"abcdefghijklmnopqrstuvwxyz"
    try:
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-chunks",
            product="finance_cli",
            relative_path="uploads/chunked.csv",
            content=content,
            chunk_size=5,
            auth_provider=provider,
        )

        assert storage_files.read_file(
            local_storage_proxy.target,
            user_id="synthetic-files-chunks",
            product="finance_cli",
            relative_path="uploads/chunked.csv",
            auth_provider=provider,
        ) == content
    finally:
        provider.close()


def test_user_normalizer_files_are_allowed(local_storage_proxy) -> None:
    provider = _auth(local_storage_proxy)
    content = b"PRIMARY_KEY = 'demo_bank'\n"
    try:
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="normalizers/demo_bank.py",
            content=content,
            auth_provider=provider,
        )

        assert storage_files.read_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="normalizers/demo_bank.py",
            auth_provider=provider,
        ) == content

        assert storage_files.list_files(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            prefix="normalizers",
            auth_provider=provider,
        ) == ["normalizers/demo_bank.py"]

        storage_files.delete_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="normalizers/demo_bank.py",
            auth_provider=provider,
        )

        assert storage_files.list_files(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            prefix="normalizers",
            auth_provider=provider,
        ) == []

        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="normalizers/.staging/demo_bank.py",
            content=content,
            auth_provider=provider,
        )
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="normalizers/.staging/.test_passes.json",
            content=b"{}\n",
            auth_provider=provider,
        )
        assert storage_files.list_files(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            prefix="normalizers/.staging",
            auth_provider=provider,
        ) == [
            "normalizers/.staging/.test_passes.json",
            "normalizers/.staging/demo_bank.py",
        ]
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-normalizers",
            product="finance_cli",
            relative_path="institution_names.json",
            content=b'{"canonical_names": {"demo": "Demo Bank"}}\n',
            auth_provider=provider,
        )

        with pytest.raises(PathInvalidError, match="relative_path_not_allowed"):
            storage_files.write_file(
                local_storage_proxy.target,
                user_id="synthetic-files-normalizers",
                product="finance_cli",
                relative_path="normalizers/readme.txt",
                content=b"not a module\n",
                auth_provider=provider,
            )
    finally:
        provider.close()


def test_list_and_delete_files(local_storage_proxy) -> None:
    provider = _auth(local_storage_proxy)
    try:
        storage_files.write_file(
            local_storage_proxy.target,
            user_id="synthetic-files-list",
            product="finance_cli",
            relative_path="sessions/2026-05-01.md",
            content=b"note\n",
            auth_provider=provider,
        )

        assert storage_files.list_files(
            local_storage_proxy.target,
            user_id="synthetic-files-list",
            product="finance_cli",
            prefix="sessions",
            auth_provider=provider,
        ) == ["sessions/2026-05-01.md"]

        storage_files.delete_file(
            local_storage_proxy.target,
            user_id="synthetic-files-list",
            product="finance_cli",
            relative_path="sessions/2026-05-01.md",
            auth_provider=provider,
        )

        assert storage_files.list_files(
            local_storage_proxy.target,
            user_id="synthetic-files-list",
            product="finance_cli",
            prefix="sessions",
            auth_provider=provider,
        ) == []
    finally:
        provider.close()


def test_path_validation_rejects_obviously_bad_paths(local_storage_proxy) -> None:
    provider = _auth(local_storage_proxy)
    try:
        with pytest.raises(PathInvalidError):
            storage_files.write_file(
                local_storage_proxy.target,
                user_id="synthetic-files-bad-path",
                product="finance_cli",
                relative_path="../finance.db",
                content=b"bad",
                auth_provider=provider,
            )
    finally:
        provider.close()
