from __future__ import annotations

import json
import logging
import sys
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path

import grpc
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
STORAGE_SERVER_SRC = ROOT / "services" / "storage_server" / "src"
if str(STORAGE_SERVER_SRC) not in sys.path:
    sys.path.insert(0, str(STORAGE_SERVER_SRC))
FINANCE_WEB_ROOT = ROOT / "finance-web"
if str(FINANCE_WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(FINANCE_WEB_ROOT))

_TEST_DEK = b"\x00" * 32


@pytest.fixture(scope="session", autouse=True)
def _stub_db_keys():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    monkeypatch.setattr("finance_cli.db_keys.get_user_db_key", lambda _user_id, **_kwargs: _TEST_DEK)
    monkeypatch.setattr("finance_cli.db_keys.provision_user_db_key", lambda _user_id, **_kwargs: None)
    monkeypatch.setattr("finance_cli.db_keys.delete_user_db_key", lambda _user_id: None)
    monkeypatch.setattr(
        "finance_cli.user_provisioning.provision_user_db_key",
        lambda _user_id, **_kwargs: None,
    )
    yield
    monkeypatch.undo()


@pytest.fixture(autouse=True)
def _clean_data_dir_env(monkeypatch):
    """Prevent .env from polluting test state.

    Tests that call main() trigger load_dotenv(), which would set
    FINANCE_CLI_DATA_DIR from the project .env file.  Disable dotenv
    loading entirely and ensure FINANCE_CLI_DATA_DIR is clean.
    """
    monkeypatch.delenv("FINANCE_CLI_DATA_DIR", raising=False)
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")


@pytest.fixture(autouse=True)
def _reset_runtime_cli_settings():
    from finance_cli.config import set_runtime_cli_settings
    from finance_cli.db import set_db_encryption_mode_override

    previous_settings = set_runtime_cli_settings(None)
    previous_encryption_mode = set_db_encryption_mode_override(None)
    try:
        yield
    finally:
        set_runtime_cli_settings(previous_settings)
        set_db_encryption_mode_override(previous_encryption_mode)


@pytest.fixture(autouse=True)
def _reset_finance_cli_logging():
    logger = logging.getLogger("finance_cli")
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)
    try:
        yield
    finally:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        logger.propagate = True
        logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def _suppress_alerts(monkeypatch):
    """Prevent error-capture and cost-tracking alerts from firing during tests."""
    monkeypatch.setattr("finance_cli.error_capture._HAS_ALERTS", False)
    monkeypatch.setattr("finance_cli.cost_tracking._HAS_ALERTS", False)


@dataclass(frozen=True)
class LocalStorageProxy:
    target: str
    kid: str
    private_key_pem: str
    public_key_pem: str
    runtime: object

    def __iter__(self):
        yield self.target
        yield self.kid
        yield self.private_key_pem


class _StaticSecretsClient:
    def __init__(self, private_key_pem: str, kid: str) -> None:
        self.private_key_pem = private_key_pem
        self.kid = kid

    def get_secret_value(self, *, SecretId: str):
        return {"SecretString": self.private_key_pem}

    def describe_secret(self, *, SecretId: str):
        return {"Tags": [{"Key": "kid", "Value": self.kid}]}


def auth_provider_for_test(private_key_pem: str, kid: str, secrets_client_stub=None):
    from finance_cli.storage_client.auth import JWTAuthProvider

    return JWTAuthProvider(
        refresh_interval_seconds=60 * 60,
        secrets_client=secrets_client_stub or _StaticSecretsClient(private_key_pem, kid),
    )


@pytest.fixture()
def storage_connection_factory(local_storage_proxy: LocalStorageProxy):
    from finance_cli.storage_client import StorageConnection

    providers = []
    connections = []

    def make_connection(
        *,
        user_id: str | None = None,
        scopes: list[str] | None = None,
        **kwargs,
    ) -> StorageConnection:
        provider = auth_provider_for_test(
            local_storage_proxy.private_key_pem,
            local_storage_proxy.kid,
        )
        providers.append(provider)
        conn = StorageConnection(
            local_storage_proxy.target,
            user_id=user_id or f"synthetic-{uuid.uuid4().hex[:12]}",
            scopes=scopes,
            auth_provider=provider,
            **kwargs,
        )
        connections.append(conn)
        return conn

    try:
        yield make_connection
    finally:
        for conn in connections:
            conn.close()
        for provider in providers:
            provider.close()


@pytest.fixture()
def local_storage_proxy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalStorageProxy:
    from storage_server import crypto_envelope
    from storage_server.config import Config
    from storage_server import server as storage_server_server

    kid = "test-kid"
    private_key = ed25519.Ed25519PrivateKey.generate()
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    public_key_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")

    public_keys_dir = tmp_path / "public_keys"
    public_keys_dir.mkdir()
    (public_keys_dir / f"{kid}.pem").write_text(public_key_pem, encoding="ascii")

    issuer_products_file = tmp_path / "issuer_products.yaml"
    issuer_products_file.write_text(f"{kid}:\n  - finance_cli\n", encoding="utf-8")

    data_root = tmp_path / "data"
    data_root.mkdir()
    logs = tmp_path / "logs"
    logs.mkdir()

    def dek_path(user_id: str, data_dir: Path | None) -> Path:
        assert data_dir is not None
        return Path(data_dir) / user_id / "db-dek.enc"

    def has_db_dek(user_id: str, *, data_dir: Path | None = None, backend: str | None = None) -> bool:
        del backend
        return dek_path(user_id, data_dir).exists()

    def provision_db_dek(
        user_id: str,
        *,
        dek: bytes | None = None,
        data_dir: Path | None = None,
        kms_key_arn: str | None = None,
        aws_region: str | None = None,
        backend: str | None = None,
    ) -> None:
        del kms_key_arn, aws_region, backend
        path = dek_path(user_id, data_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(dek or _TEST_DEK)

    def get_db_dek(
        user_id: str,
        *,
        data_dir: Path | None = None,
        kms_key_arn: str | None = None,
        aws_region: str | None = None,
        backend: str | None = None,
    ) -> bytes:
        del kms_key_arn, aws_region, backend
        path = dek_path(user_id, data_dir)
        if not path.exists():
            provision_db_dek(user_id, data_dir=data_dir)
        return path.read_bytes()

    def install_db_dek_blob(
        user_id: str,
        blob_bytes: bytes,
        *,
        data_dir: Path | None = None,
        backend: str | None = None,
    ) -> None:
        del backend
        path = dek_path(user_id, data_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bytes(blob_bytes))

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
        del kms_key_arn, aws_region
        header = {
            "user_id": user_id,
            "bundle_id": bundle_id,
            "mode": mode,
            "recovery_db_dek_present": recovery_db_dek_present,
        }
        header_bytes = json.dumps(header, sort_keys=True).encode("utf-8")
        return b"TESTBDL3" + len(header_bytes).to_bytes(4, "big") + header_bytes + plaintext_tar

    def decrypt_bundle_v3(
        bundle_bytes: bytes,
        expected_user_id: str,
        *,
        kms_key_arn: str | None = None,
        aws_region: str | None = None,
    ) -> bytes:
        del kms_key_arn, aws_region
        if not bundle_bytes.startswith(b"TESTBDL3"):
            raise ValueError("bad bundle")
        header_len = int.from_bytes(bundle_bytes[8:12], "big")
        header = json.loads(bundle_bytes[12 : 12 + header_len])
        if header["user_id"] != expected_user_id:
            raise PermissionError("cross_user_bundle")
        return bundle_bytes[12 + header_len :]

    monkeypatch.setattr(crypto_envelope, "_kms", lambda *args, **kwargs: object())
    monkeypatch.setattr(crypto_envelope, "_kms_key_arn", lambda *args, **kwargs: "alias/test")
    monkeypatch.setattr(crypto_envelope, "has_db_dek", has_db_dek)
    monkeypatch.setattr(crypto_envelope, "provision_db_dek", provision_db_dek)
    monkeypatch.setattr(crypto_envelope, "get_db_dek", get_db_dek)
    monkeypatch.setattr(crypto_envelope, "install_db_dek_blob", install_db_dek_blob)
    monkeypatch.setattr(crypto_envelope, "encrypt_bundle_v3", encrypt_bundle_v3)
    monkeypatch.setattr(crypto_envelope, "decrypt_bundle_v3", decrypt_bundle_v3)
    monkeypatch.setattr(storage_server_server, "kms_sanity_check", lambda config: None)

    config = Config(
        synthetic_only=True,
        listen_address="127.0.0.1:0",
        data_root=data_root,
        public_keys_dir=public_keys_dir,
        issuer_products_file=issuer_products_file,
        kms_key_alias="alias/test",
        kms_region="us-east-2",
        session_idle_timeout_seconds=1,
        session_max_lifetime_seconds=600,
        session_tombstone_seconds=600,
        max_sessions_per_user=3,
        max_total_sessions=50,
        write_chunk_size_bytes=64 * 1024,
        plaid_owner_rate_limit_per_minute=10,
        plaid_owner_rate_burst=10,
        sqlite_busy_timeout_ms=5000,
        audit_log_path=logs / "audit.log",
        access_log_path=logs / "access.log",
        health_log_path=logs / "health.log",
    )
    runtime = storage_server_server.build_server(config, skip_kms_sanity=True, start_reaper=False)
    runtime.start()
    waiter = threading.Thread(target=runtime.wait_for_termination, name="storage-proxy-test", daemon=True)
    waiter.start()

    ready_channel = grpc.insecure_channel(runtime.target)
    grpc.channel_ready_future(ready_channel).result(timeout=5)
    ready_channel.close()
    try:
        yield LocalStorageProxy(
            target=runtime.target,
            kid=kid,
            private_key_pem=private_key_pem,
            public_key_pem=public_key_pem,
            runtime=runtime,
        )
    finally:
        runtime.stop(grace=0)
        waiter.join(timeout=2)
