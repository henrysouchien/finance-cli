from __future__ import annotations

import sqlite3
import tarfile

from finance_cli import storage_files
from finance_cli.storage_client.auth import JWTAuthProvider
from finance_cli.storage_client.sync_snapshot import export_sync_snapshot


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


def test_export_sync_snapshot_streams_tarball(local_storage_proxy, storage_connection_factory, tmp_path) -> None:
    user_id = "synthetic-client-sync-snapshot"
    conn = storage_connection_factory(user_id=user_id)
    provider = _auth(local_storage_proxy)
    try:
        conn.executescript(
            """
            CREATE TABLE schema_version (version INTEGER NOT NULL);
            INSERT INTO schema_version VALUES (63);
            CREATE TABLE _sync_changelog (
                id INTEGER PRIMARY KEY,
                table_name TEXT,
                op TEXT,
                pk_json TEXT,
                old_json TEXT,
                new_json TEXT,
                origin_session_id TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO _sync_changelog
                (id, table_name, op, pk_json, old_json, new_json, origin_session_id)
            VALUES
                (11, 'plaid_items', 'INSERT', '{}', NULL, '{}', 'server');
            CREATE TABLE sync_state (
                id INTEGER PRIMARY KEY,
                last_applied_op_id INTEGER,
                install_id TEXT,
                subscriber_status TEXT
            );
            INSERT INTO sync_state VALUES (0, 9, 'old-install', 'degraded');
            CREATE TABLE plaid_items (
                id INTEGER PRIMARY KEY,
                access_token_ref TEXT NOT NULL
            );
            INSERT INTO plaid_items (access_token_ref) VALUES ('secret-ref');
            """
        )
        conn.commit()
        conn.close()

        storage_files.write_file(
            local_storage_proxy.target,
            user_id=user_id,
            product="finance_cli",
            relative_path="agent_memory.md",
            content=b"# Memory\n",
            auth_provider=provider,
        )

        snapshot = export_sync_snapshot(
            local_storage_proxy.target,
            user_id=user_id,
            auth_provider=provider,
        )
        try:
            assert snapshot.snapshot_op_id == 11
            assert snapshot.schema_version == 63
            assert snapshot.snapshot_id

            with tarfile.open(snapshot.path, "r:gz") as tar:
                tar.extractall(tmp_path, filter="data")

            assert (tmp_path / "agent_memory.md").read_text(encoding="utf-8") == "# Memory\n"
            with sqlite3.connect(str(tmp_path / "finance.db")) as snapshot_conn:
                assert snapshot_conn.execute("SELECT access_token_ref FROM plaid_items").fetchone()[0] is None
                assert snapshot_conn.execute(
                    "SELECT last_applied_op_id, install_id, subscriber_status FROM sync_state WHERE id = 0"
                ).fetchone() == (0, "", "healthy")
        finally:
            snapshot.path.unlink(missing_ok=True)
    finally:
        conn.close()
        provider.close()
