from __future__ import annotations

import asyncio
import io
import json
import sqlite3
import tarfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import httpx
import pytest

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import EngagementRequiredError
from finance_cli.sync import config as sync_config
from finance_cli.sync.engine import SyncEngine, _proxy_timeout
from finance_cli.sync.exceptions import (
    SyncAuthError,
    SyncCatchupFailedError,
    SyncConflictError,
    SyncSchemaMismatchError,
    SyncServerUnreachableError,
)


def _patch_cashnerd_paths(monkeypatch, base_dir: Path) -> None:
    import finance_cli.sync.engine as sync_engine

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_CONFIG_PATH", base_dir / "config.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AUTH_DIR", base_dir / "auth")
    monkeypatch.setattr(
        sync_config, "CASHNERD_TOKEN_PATH", base_dir / "auth" / "token.json"
    )
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(
        sync_config, "CASHNERD_DB_PATH", base_dir / "data" / "finance.db"
    )
    monkeypatch.setattr(
        sync_config, "CASHNERD_RULES_PATH", base_dir / "data" / "rules.yaml"
    )
    monkeypatch.setattr(
        sync_config, "CASHNERD_UPLOADS_DIR", base_dir / "data" / "uploads"
    )
    monkeypatch.setattr(
        sync_config, "CASHNERD_SKILL_STATE_PATH", base_dir / "data" / "skill_state.json"
    )
    monkeypatch.setattr(
        sync_config, "CASHNERD_AGENT_MEMORY_PATH", base_dir / "data" / "agent_memory.md"
    )
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_DIR", base_dir / "sync")
    monkeypatch.setattr(
        sync_config,
        "CASHNERD_PENDING_CHANGESET_PATH",
        base_dir / "sync" / "pending_changeset.json",
    )
    monkeypatch.setattr(
        sync_config, "CASHNERD_SYNC_LOG_PATH", base_dir / "sync" / "sync_log.json"
    )

    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", sync_config.CASHNERD_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)
    monkeypatch.setattr(
        sync_engine, "CASHNERD_RULES_PATH", sync_config.CASHNERD_RULES_PATH
    )
    monkeypatch.setattr(
        sync_engine, "CASHNERD_SKILL_STATE_PATH", sync_config.CASHNERD_SKILL_STATE_PATH
    )
    monkeypatch.setattr(
        sync_engine,
        "CASHNERD_AGENT_MEMORY_PATH",
        sync_config.CASHNERD_AGENT_MEMORY_PATH,
    )
    monkeypatch.setattr(
        sync_engine,
        "CASHNERD_PENDING_CHANGESET_PATH",
        sync_config.CASHNERD_PENDING_CHANGESET_PATH,
    )
    monkeypatch.setattr(
        sync_engine, "CASHNERD_SYNC_LOG_PATH", sync_config.CASHNERD_SYNC_LOG_PATH
    )


class DummyAuth:
    def __init__(self) -> None:
        self.sync_token = "sync-token"
        self.recorded_user_id: str | None = None
        self.invalidations = 0

    async def get_credential(self) -> str:
        return "google-credential"

    async def get_sync_token(self, *, force_refresh: bool = False) -> str:
        return self.sync_token if not force_refresh else f"{self.sync_token}-fresh"

    def record_sync_session(self, *, token: str, user_id: str | None = None) -> None:
        self.sync_token = token
        self.recorded_user_id = user_id

    def invalidate_sync_token(self) -> None:
        self.invalidations += 1


def _snapshot_tar(
    tmp_path: Path,
    *,
    txn_id: str = "txn-sync",
    description: str = "Pulled",
    clear_fts: bool = False,
    reset_epoch: str | None = None,
) -> bytes:
    staging = tmp_path / "snapshot"
    staging.mkdir(exist_ok=True)
    db_path = staging / "finance.db"
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES (?, '2026-04-16', ?, -123, 'manual', 1)
            """,
            (txn_id, description),
        )
        if reset_epoch is not None:
            conn.execute(
                """
                UPDATE sync_reset_state
                   SET reset_epoch = ?
                 WHERE id = 0
                """,
                (reset_epoch,),
            )
        conn.commit()
        if clear_fts:
            conn.execute("INSERT INTO txn_fts(txn_fts) VALUES('delete-all')")
            conn.commit()
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    (staging / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tar.add(db_path, arcname="finance.db")
        tar.add(staging / "rules.yaml", arcname="rules.yaml")
    return buffer.getvalue()


def _local_reset_epoch(db_path: Path) -> str:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT reset_epoch FROM sync_reset_state WHERE id = 0"
        ).fetchone()
    return str(row[0])


def _local_txn_description(db_path: Path, txn_id: str) -> str | None:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT description FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()
    return str(row[0]) if row is not None else None


def test_pull_downloads_snapshot_updates_sync_state_and_updates_config(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    auth = DummyAuth()
    config = sync_config.SyncConfig(server_url="https://cashnerd.example")
    tar_bytes = _snapshot_tar(tmp_path, clear_fts=True)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/sync/auth"
        assert json.loads(request.content.decode("utf-8")) == {
            "credential": "google-credential"
        }
        return httpx.Response(
            200,
            headers={
                "X-CashNerd-Sync-Token": "sync-token-2",
                "X-CashNerd-User-Id": "42",
                "X-CashNerd-Schema-Version": "59",
                "X-CashNerd-Op-Id": "1",
                "X-CashNerd-Pull-Timestamp": "2026-04-16T12:00:00Z",
            },
            content=tar_bytes,
        )

    engine = SyncEngine(
        config,
        auth,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert asyncio.run(engine.pull()) is True
    assert sync_config.CASHNERD_DB_PATH.exists()
    assert sync_config.CASHNERD_RULES_PATH.exists()
    assert config.user_id == "42"
    assert config.schema_version == 59
    assert config.last_sync_ts == "2026-04-16T12:00:00Z"
    assert auth.recorded_user_id == "42"

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        pulled = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-sync'"
        ).fetchone()
        sync_state = conn.execute(
            """
            SELECT last_applied_op_id, install_id, subscriber_status
              FROM sync_state
             WHERE id = 0
            """
        ).fetchone()
        changelog_count = conn.execute(
            "SELECT COUNT(*) FROM _sync_changelog"
        ).fetchone()
        fts_row = conn.execute(
            """
            SELECT t.description
              FROM txn_fts f
              JOIN transactions t ON t.rowid = f.rowid
             WHERE txn_fts MATCH 'Pulled'
            """
        ).fetchone()

    assert pulled == ("Pulled",)
    assert fts_row == ("Pulled",)
    assert sync_state[0] == 1
    assert sync_state[1]
    assert sync_state[2] == "healthy"
    assert changelog_count == (0,)


def test_pull_removes_stale_sqlite_sidecars(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    auth = DummyAuth()
    config = sync_config.SyncConfig(server_url="https://cashnerd.example")
    tar_bytes = _snapshot_tar(tmp_path)
    sync_config.ensure_dirs()
    sync_config.CASHNERD_DB_PATH.with_name(
        f"{sync_config.CASHNERD_DB_PATH.name}-wal"
    ).write_bytes(b"stale-wal")
    sync_config.CASHNERD_DB_PATH.with_name(
        f"{sync_config.CASHNERD_DB_PATH.name}-shm"
    ).write_bytes(b"stale-shm")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "X-CashNerd-Sync-Token": "sync-token-2",
                "X-CashNerd-User-Id": "42",
                "X-CashNerd-Schema-Version": "59",
                "X-CashNerd-Op-Id": "1",
                "X-CashNerd-Pull-Timestamp": "2026-04-16T12:00:00Z",
            },
            content=tar_bytes,
        )

    engine = SyncEngine(
        config,
        auth,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert asyncio.run(engine.pull()) is True
    wal_path = sync_config.CASHNERD_DB_PATH.with_name(
        f"{sync_config.CASHNERD_DB_PATH.name}-wal"
    )
    shm_path = sync_config.CASHNERD_DB_PATH.with_name(
        f"{sync_config.CASHNERD_DB_PATH.name}-shm"
    )
    if wal_path.exists():
        assert wal_path.read_bytes() != b"stale-wal"
    if shm_path.exists():
        assert shm_path.read_bytes() != b"stale-shm"
    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-sync'"
        ).fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()
    assert integrity == ("ok",)
    assert row == ("Pulled",)


def test_pull_raises_auth_error_when_login_is_missing(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")

    class MissingLoginAuth(DummyAuth):
        async def get_credential(self) -> str:
            raise SyncAuthError(
                "Not authenticated. Run: python3 -m finance_cli.sync.login"
            )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        MissingLoginAuth(),
    )

    with pytest.raises(SyncAuthError, match="python3 -m finance_cli.sync.login"):
        asyncio.run(engine.pull())


def test_push_includes_causal_token_and_clears_changelog(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 77,
                   install_id = 'install-123'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "status": "applied",
                "applied_count": 1,
                "new_pull_timestamp": "2026-04-16T13:00:00Z",
                "new_op_id": 88,
                "server_op_id": 88,
            },
        )

    config = sync_config.SyncConfig(
        server_url="https://cashnerd.example",
        last_sync_ts="2026-04-16T12:00:00Z",
        schema_version=59,
    )
    engine = SyncEngine(
        config,
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    body = asyncio.run(engine.push())

    assert body["status"] == "applied"
    assert captured["json"]["push_id"]
    assert captured["json"]["schema_version"] == 59
    assert captured["json"]["last_seen_op_id"] == 77
    assert captured["json"]["install_id"] == "install-123"
    assert "rules_yaml" not in captured["json"]
    assert "rules_sha256" not in captured["json"]
    assert len(captured["json"]["changeset"]) == 1
    assert config.last_sync_ts == "2026-04-16T13:00:00Z"

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        changelog_count = conn.execute(
            "SELECT COUNT(*) FROM _sync_changelog"
        ).fetchone()
        last_applied = conn.execute(
            "SELECT last_applied_op_id FROM sync_state WHERE id = 0"
        ).fetchone()
    assert changelog_count == (0,)
    assert last_applied == (77,)


def test_push_reuses_pending_payload_and_clears_only_sent_changelog(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 77,
                   install_id = 'install-123'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-first', '2026-04-16', 'First local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    def failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("response lost", request=request)

    config = sync_config.SyncConfig(
        server_url="https://cashnerd.example",
        last_sync_ts="2026-04-16T12:00:00Z",
        schema_version=59,
    )
    engine = SyncEngine(
        config,
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(failing_handler)),
    )

    with pytest.raises(SyncServerUnreachableError):
        asyncio.run(engine.push())

    pending = json.loads(
        sync_config.CASHNERD_PENDING_CHANGESET_PATH.read_text(encoding="utf-8")
    )
    assert pending["push_id"]
    assert [change["pk"]["id"] for change in pending["changeset"]] == ["txn-first"]
    first_max_id = int(pending["client_changelog_max_id"])

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-second', '2026-04-16', 'Second local change', -555, 'manual', 1)
            """
        )
        conn.commit()

    captured: dict[str, object] = {}

    def retry_handler(request: httpx.Request) -> httpx.Response:
        captured["json"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "status": "applied",
                "applied_count": 1,
                "new_pull_timestamp": "2026-04-16T13:00:00Z",
                "new_op_id": 88,
                "server_op_id": 88,
            },
        )

    retry_engine = SyncEngine(
        config,
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(retry_handler)),
    )

    body = asyncio.run(retry_engine.push())

    assert body["status"] == "applied"
    assert captured["json"]["push_id"] == pending["push_id"]
    assert [change["pk"]["id"] for change in captured["json"]["changeset"]] == [
        "txn-first"
    ]
    assert not sync_config.CASHNERD_PENDING_CHANGESET_PATH.exists()
    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        remaining = conn.execute(
            """
            SELECT id, json_extract(pk_json, '$.id') AS txn_id
              FROM _sync_changelog
             ORDER BY id
            """
        ).fetchall()
    assert remaining == [(first_max_id + 1, "txn-second")]


def test_read_changeset_excludes_downstream_only_cost_ledger(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, request_id)
            VALUES ('plaid', 'transactions_sync', 123, 'req-local-cost')
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
    )

    changeset = engine._read_changeset()

    assert [change["table"] for change in changeset] == ["transactions"]
    assert changeset[0]["pk"] == {"id": "txn-local"}


def test_push_clears_downstream_only_changelog_when_no_pushable_changes(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, request_id)
            VALUES ('plaid', 'transactions_sync', 123, 'req-local-cost')
            """
        )
        conn.commit()

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
    )

    result = asyncio.run(engine.push())

    assert result == {"status": "no_changes"}
    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        changelog_count = conn.execute(
            "SELECT COUNT(*) FROM _sync_changelog"
        ).fetchone()
    assert changelog_count == (0,)


def test_push_waits_for_subscriber_after_412_and_retries_once(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 10,
                   install_id = 'install-123',
                   subscriber_status = 'healthy'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    requests: list[dict[str, object]] = []
    responses = iter(
        [
            httpx.Response(412, json={"server_op_id": 12}),
            httpx.Response(
                200, json={"status": "applied", "new_op_id": 12, "server_op_id": 12}
            ),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content.decode("utf-8")))
        return next(responses)

    engine = SyncEngine(
        sync_config.SyncConfig(
            server_url="https://cashnerd.example",
            last_sync_ts="2026-04-16T12:00:00Z",
            schema_version=59,
        ),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def run_push() -> dict[str, object]:
        async def bump_cursor_later() -> None:
            await asyncio.sleep(0.05)
            with connect(sync_config.CASHNERD_DB_PATH) as conn:
                conn.execute(
                    "UPDATE sync_state SET last_applied_op_id = 12 WHERE id = 0"
                )
                conn.commit()

        task = asyncio.create_task(bump_cursor_later())
        try:
            return await engine.push()
        finally:
            await task

    body = asyncio.run(run_push())

    assert body["status"] == "applied"
    assert len(requests) == 2
    assert requests[0]["last_seen_op_id"] == 10
    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        row = conn.execute("SELECT COUNT(*) FROM _sync_changelog").fetchone()
    assert row == (0,)


def test_push_raises_catchup_failed_when_subscriber_does_not_advance(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 10,
                   install_id = 'install-123',
                   subscriber_status = 'degraded'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    engine = SyncEngine(
        sync_config.SyncConfig(
            server_url="https://cashnerd.example",
            last_sync_ts="2026-04-16T12:00:00Z",
            schema_version=59,
        ),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(412, json={"server_op_id": 12})
            )
        ),
    )

    with pytest.raises(SyncCatchupFailedError):
        asyncio.run(engine.push())


def test_force_pull_non_strict_uses_op_id_guard_and_preserves_install_id(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 50,
                   install_id = 'install-persist',
                   subscriber_status = 'healthy'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local-newer', '2026-04-16', 'Newer Local', -200, 'manual', 1)
            """
        )
        conn.commit()
    sync_config.CASHNERD_RULES_PATH.write_text("local rules\n", encoding="utf-8")

    tar_bytes = _snapshot_tar(
        tmp_path, txn_id="txn-stale", description="Stale Snapshot"
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "X-CashNerd-Sync-Token": "sync-token-2",
                "X-CashNerd-User-Id": "42",
                "X-CashNerd-Schema-Version": "59",
                "X-CashNerd-Op-Id": "10",
                "X-CashNerd-Pull-Timestamp": "2026-04-16T12:00:00Z",
            },
            content=tar_bytes,
        )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    asyncio.run(engine.force_pull())

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-local-newer'"
        ).fetchone()
        missing = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-stale'"
        ).fetchone()
        sync_state = conn.execute(
            "SELECT last_applied_op_id, install_id FROM sync_state WHERE id = 0"
        ).fetchone()
    assert row == ("Newer Local",)
    assert missing is None
    assert sync_state == (50, "install-persist")
    assert engine.install_id == "install-persist"


def test_force_pull_strict_installs_snapshot_when_op_id_rewinds(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 50,
                   install_id = 'install-persist',
                   subscriber_status = 'healthy'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local-newer', '2026-04-16', 'Newer Local', -200, 'manual', 1)
            """
        )
        conn.commit()
    sync_config.CASHNERD_RULES_PATH.write_text("local rules\n", encoding="utf-8")

    tar_bytes = _snapshot_tar(
        tmp_path, txn_id="txn-restored", description="Restored Snapshot"
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "X-CashNerd-Sync-Token": "sync-token-2",
                "X-CashNerd-User-Id": "42",
                "X-CashNerd-Schema-Version": "59",
                "X-CashNerd-Op-Id": "10",
                "X-CashNerd-Pull-Timestamp": "2026-04-16T12:00:00Z",
            },
            content=tar_bytes,
        )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    asyncio.run(engine._force_pull_strict())

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        local_row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-local-newer'"
        ).fetchone()
        restored_row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-restored'"
        ).fetchone()
        sync_state = conn.execute(
            "SELECT last_applied_op_id, install_id FROM sync_state WHERE id = 0"
        ).fetchone()
    assert local_row is None
    assert restored_row == ("Restored Snapshot",)
    assert sync_state == (10, "install-persist")
    assert engine.install_id == "install-persist"


def test_pull_strict_rebootstraps_when_reset_epoch_mismatches(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 50,
                   install_id = 'install-persist',
                   subscriber_status = 'healthy'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            UPDATE sync_reset_state
               SET reset_epoch = 'local-epoch'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local-newer', '2026-04-16', 'Newer Local', -200, 'manual', 1)
            """
        )
        conn.commit()

    tar_bytes = _snapshot_tar(
        tmp_path,
        txn_id="txn-server-reset",
        description="Server Reset",
        reset_epoch="server-epoch",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/sync/schema-version":
            return httpx.Response(
                200,
                json={
                    "schema_version": 59,
                    "migration_count": 59,
                    "reset_epoch": "server-epoch",
                },
            )
        if request.url.path == "/api/sync/auth":
            return httpx.Response(
                200,
                headers={
                    "X-CashNerd-Sync-Token": "sync-token-2",
                    "X-CashNerd-User-Id": "42",
                    "X-CashNerd-Schema-Version": "59",
                    "X-CashNerd-Op-Id": "10",
                    "X-CashNerd-Pull-Timestamp": "2026-04-16T12:00:00Z",
                },
                content=tar_bytes,
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    engine = SyncEngine(
        sync_config.SyncConfig(
            server_url="https://cashnerd.example", schema_version=59
        ),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    assert asyncio.run(engine.pull()) is True

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        local_row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-local-newer'"
        ).fetchone()
        reset_row = conn.execute(
            "SELECT description FROM transactions WHERE id = 'txn-server-reset'"
        ).fetchone()
        sync_state = conn.execute(
            "SELECT last_applied_op_id, install_id FROM sync_state WHERE id = 0"
        ).fetchone()
        reset_epoch = conn.execute(
            "SELECT reset_epoch FROM sync_reset_state WHERE id = 0"
        ).fetchone()[0]
    assert local_row is None
    assert reset_row == ("Server Reset",)
    assert sync_state == (10, "install-persist")
    assert reset_epoch == "server-epoch"


def test_push_raises_conflict_and_schema_mismatch(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET install_id = 'install-123',
                   last_applied_op_id = 10
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Local change', -444, 'manual', 1)
            """
        )
        conn.commit()

    responses = iter(
        [
            httpx.Response(
                409,
                json={"status": "conflict", "conflicts": [{"table": "transactions"}]},
            ),
            httpx.Response(
                412,
                json={"server_schema_version": 59, "client_schema_version": 56},
            ),
        ]
    )

    engine = SyncEngine(
        sync_config.SyncConfig(
            server_url="https://cashnerd.example",
            last_sync_ts="2026-04-16T12:00:00Z",
            schema_version=56,
        ),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: next(responses))
        ),
    )

    with pytest.raises(SyncConflictError):
        asyncio.run(engine.push())

    with pytest.raises(SyncSchemaMismatchError):
        asyncio.run(engine.push())


def test_push_raises_engagement_required_and_clears_pending_change(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.ensure_dirs()
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-engagement', '2026-05-29', 'Needs engagement', -100, 'manual', 1)
            """
        )
        conn.commit()

    engine = SyncEngine(
        sync_config.SyncConfig(
            server_url="https://cashnerd.example",
            last_sync_ts="2026-05-29T12:00:00Z",
            schema_version=db_module.SCHEMA_VERSION,
        ),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    403,
                    json={
                        "detail": {
                            "error": "engagement_required",
                            "message": "Your CashNerd membership has lapsed.",
                        }
                    },
                )
            )
        ),
    )

    with pytest.raises(EngagementRequiredError) as exc_info:
        asyncio.run(engine.push())

    assert exc_info.value.user_message == "Your CashNerd membership has lapsed."
    assert not sync_config.CASHNERD_PENDING_CHANGESET_PATH.exists()


def test_proxy_tool_waits_for_subscriber_after_success(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={
                        "_op_id": 123,
                        "result": {"data": {"ok": True}, "summary": {}},
                    },
                )
            )
        ),
    )

    calls: list[tuple[int, float]] = []

    async def fake_wait(target_op_id: int, timeout: float) -> None:
        calls.append((target_op_id, timeout))

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(engine.proxy_tool("plaid_sync", {"account_id": "acct-1"}))

    assert payload == {"data": {"ok": True}, "summary": {}}
    assert calls == [(123, 30.0)]


def test_proxy_tool_plaid_link_opens_browser_then_exchanges(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    requests: list[dict[str, Any]] = []
    opened: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={
                    "_op_id": 101,
                    "result": {
                        "data": {
                            "ready": True,
                            "session": {
                                "link_token": "link-token-123",
                                "hosted_link_url": "https://plaid.test/link",
                                "requested_products": ["transactions", "liabilities"],
                            },
                        },
                        "summary": {"ready": True, "waited": False},
                    },
                },
            )
        return httpx.Response(
            200,
            json={
                "_op_id": 102,
                "result": {
                    "data": {"plaid_item_id": "item-linked", "status": "active"},
                    "summary": {"plaid_item_id": "item-linked", "status": "active"},
                },
            },
        )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        browser_opener=opened.append,
    )
    calls: list[tuple[int, float]] = []

    async def fake_wait(target_op_id: int, timeout: float) -> None:
        calls.append((target_op_id, timeout))

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(
        engine.proxy_tool(
            "plaid_link",
            {
                "wait": True,
                "open_browser": True,
                "timeout": 15,
                "allow_duplicate": True,
                "include_liabilities": True,
            },
        )
    )

    assert opened == ["https://plaid.test/link"]
    assert [request["tool_name"] for request in requests] == [
        "plaid_link",
        "plaid_exchange",
    ]
    assert requests[0]["arguments"]["wait"] is False
    assert requests[0]["arguments"]["open_browser"] is False
    assert requests[1]["arguments"] == {
        "link_token": "link-token-123",
        "requested_products": ["transactions", "liabilities"],
        "timeout": 15,
        "allow_duplicate_institution": True,
    }
    assert payload["data"]["session"]["link_token"] == "link-token-123"
    assert payload["data"]["linked_item"]["plaid_item_id"] == "item-linked"
    assert payload["summary"]["waited"] is True
    assert calls == [(101, 30.0), (102, 30.0)]


def test_proxy_tool_plaid_link_wait_without_browser_returns_handoff(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={
                "_op_id": 101,
                "result": {
                    "data": {
                        "session": {
                            "link_token": "link-token-123",
                            "hosted_link_url": "https://plaid.test/link",
                        }
                    },
                    "summary": {"ready": True, "waited": False},
                },
            },
        )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    async def fake_wait(_target_op_id: int, timeout: float) -> None:
        return None

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(
        engine.proxy_tool("plaid_link", {"wait": True, "open_browser": False})
    )

    assert len(requests) == 1
    assert requests[0]["arguments"]["wait"] is False
    assert payload["data"]["session"]["hosted_link_url"] == "https://plaid.test/link"
    assert "requires open_browser=True" in payload["summary"]["warnings"][0]


def test_proxy_tool_plaid_link_preserves_exchange_error(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    responses = iter(
        [
            httpx.Response(
                200,
                json={
                    "_op_id": 101,
                    "result": {
                        "data": {
                            "session": {
                                "link_token": "link-token-123",
                                "hosted_link_url": "https://plaid.test/link",
                            }
                        },
                        "summary": {"ready": True, "waited": False},
                    },
                },
            ),
            httpx.Response(
                200,
                json={
                    "data": {"error": "Timed out waiting for Plaid Link completion"},
                    "summary": {
                        "errors": ["Timed out waiting for Plaid Link completion"]
                    },
                },
            ),
        ]
    )
    opened: list[str] = []
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _request: next(responses))
        ),
        browser_opener=opened.append,
    )

    async def fake_wait(_target_op_id: int, timeout: float) -> None:
        return None

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(
        engine.proxy_tool("plaid_link", {"wait": True, "open_browser": True})
    )

    assert opened == ["https://plaid.test/link"]
    assert payload["data"]["session"]["link_token"] == "link-token-123"
    assert (
        payload["data"]["exchange_error"]["data"]["error"]
        == "Timed out waiting for Plaid Link completion"
    )
    assert payload["summary"]["linked"] is False
    assert payload["summary"]["errors"] == [
        "Timed out waiting for Plaid Link completion"
    ]


def test_proxy_tool_uploads_preferences_bundle(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    bundle_path = tmp_path / "preferences.tar.gz"
    bundle_path.write_bytes(b"bundle-bytes")
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        seen["path"] = request.url.path
        seen["authorization"] = request.headers.get("authorization")
        seen["content_type"] = request.headers.get("content-type")
        seen["body"] = body
        return httpx.Response(
            200,
            json={
                "_op_id": 123,
                "result": {"data": {"ok": True}, "summary": {}},
            },
        )

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    calls: list[tuple[int, float]] = []

    async def fake_wait(target_op_id: int, timeout: float) -> None:
        calls.append((target_op_id, timeout))

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(
        engine.proxy_tool(
            "db_import_preferences",
            {"bundle_path": str(bundle_path), "mode": "merge", "dry_run": False},
        )
    )

    assert payload == {"data": {"ok": True}, "summary": {}}
    assert seen["path"] == "/api/sync/proxy-tool-upload"
    assert seen["authorization"] == "Bearer sync-token"
    assert str(seen["content_type"]).startswith("multipart/form-data; boundary=")
    assert b'name="tool_name"' in seen["body"]
    assert b"db_import_preferences" in seen["body"]
    assert b'name="arguments"' in seen["body"]
    assert b'"dry_run":false' in seen["body"]
    assert b'"bundle_path"' not in seen["body"]
    assert b"bundle-bytes" in seen["body"]
    assert calls == [(123, 30.0)]


def test_proxy_tool_upload_missing_preferences_bundle_returns_error(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
    )

    payload = asyncio.run(
        engine.proxy_tool(
            "db_import_preferences", {"bundle_path": str(tmp_path / "missing.tar.gz")}
        )
    )

    assert "Preferences bundle not found" in payload["data"]["error"]


def test_proxy_tool_appends_warning_when_subscriber_wait_times_out(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={
                        "_op_id": 123,
                        "result": {"data": {"ok": True}, "summary": {}},
                    },
                )
            )
        ),
    )

    async def fake_wait(_target_op_id: int, timeout: float) -> None:
        del timeout
        raise asyncio.TimeoutError

    monkeypatch.setattr(engine, "_wait_for_subscriber", fake_wait)

    payload = asyncio.run(engine.proxy_tool("plaid_sync", {"account_id": "acct-1"}))

    assert payload["summary"]["warnings"] == [
        "Server mutation succeeded but local subscriber hasn't caught up within 30s; next tool call may see stale data briefly."
    ]


def test_proxy_tool_handles_501_gracefully(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    501,
                    json={
                        "error": "Sync proxy-tool remains deferred after Phase 1 core sync API."
                    },
                )
            )
        ),
    )

    payload = asyncio.run(engine.proxy_tool("plaid_sync", {"account_id": "acct-1"}))

    assert "upload path" in payload["data"]["error"]


def test_proxy_tool_db_restore_uploads_and_strict_rebootstraps(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    bundle_path = tmp_path / "restore.tar.gz"
    bundle_path.write_bytes(b"restore-bundle")
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/sync/proxy-tool-upload":
            seen["upload_body"] = request.read()
            return httpx.Response(
                200,
                json={
                    "_op_id": 0,
                    "result": {
                        "data": {
                            "restored": True,
                            "dry_run": False,
                            "_sync_reset": {
                                "restored": True,
                                "local_rebootstrap_required": True,
                                "reset_epoch": "server-epoch",
                            },
                        },
                        "summary": {"restored": True},
                    },
                },
            )
        if request.url.path == "/api/sync/auth":
            return httpx.Response(
                200,
                content=_snapshot_tar(
                    tmp_path, txn_id="restore-pulled", reset_epoch="server-epoch"
                ),
                headers={
                    "X-CashNerd-Sync-Token": "new-sync-token",
                    "X-CashNerd-User-Id": "42",
                    "X-CashNerd-Op-Id": "0",
                    "X-CashNerd-Schema-Version": str(db_module.SCHEMA_VERSION),
                    "X-CashNerd-Pull-Timestamp": "2026-05-27T12:00:00Z",
                },
            )
        raise AssertionError(f"unexpected request path: {request.url.path}")

    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    stop_calls = 0

    async def fake_stop() -> None:
        nonlocal stop_calls
        stop_calls += 1

    monkeypatch.setattr(engine, "stop_subscriber", fake_stop)

    payload = asyncio.run(
        engine.proxy_tool(
            "db_restore", {"bundle_path": str(bundle_path), "dry_run": False}
        )
    )

    assert stop_calls == 1
    assert b"db_restore" in seen["upload_body"]
    assert payload["data"]["local_rebootstrapped"] is True
    assert payload["summary"]["local_rebootstrapped"] is True
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute(
            "SELECT reset_epoch FROM sync_reset_state WHERE id = 0"
        ).fetchone()
        txn = conn.execute(
            "SELECT description FROM transactions WHERE id = 'restore-pulled'"
        ).fetchone()
    assert row[0] == "server-epoch"
    assert txn[0] == "Pulled"


def test_two_local_clients_rebootstrap_after_server_restore(
    monkeypatch, tmp_path: Path
) -> None:
    import finance_cli.sync.engine as sync_engine_module

    monkeypatch.setattr(
        sync_engine_module,
        "optional_lease_scope",
        lambda *_args, **_kwargs: nullcontext(),
    )
    state = {
        "reset_epoch": "epoch-1",
        "txn_id": "before-restore",
        "description": "Before restore",
    }
    auth_calls = 0
    schema_calls = 0

    def snapshot() -> bytes:
        nonlocal auth_calls
        auth_calls += 1
        snapshot_root = tmp_path / f"snapshot-{auth_calls}"
        snapshot_root.mkdir()
        return _snapshot_tar(
            snapshot_root,
            txn_id=str(state["txn_id"]),
            description=str(state["description"]),
            reset_epoch=str(state["reset_epoch"]),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal schema_calls
        if request.url.path == "/api/sync/auth":
            return httpx.Response(
                200,
                content=snapshot(),
                headers={
                    "X-CashNerd-Sync-Token": "new-sync-token",
                    "X-CashNerd-User-Id": "42",
                    "X-CashNerd-Op-Id": "0",
                    "X-CashNerd-Schema-Version": str(db_module.SCHEMA_VERSION),
                    "X-CashNerd-Pull-Timestamp": "2026-05-27T12:00:00Z",
                },
            )
        if request.url.path == "/api/sync/schema-version":
            schema_calls += 1
            return httpx.Response(
                200,
                json={
                    "schema_version": db_module.SCHEMA_VERSION,
                    "reset_epoch": state["reset_epoch"],
                },
            )
        if request.url.path == "/api/sync/proxy-tool-upload":
            body = request.read()
            assert b"db_restore" in body
            state.update(
                {
                    "reset_epoch": "epoch-2",
                    "txn_id": "after-restore",
                    "description": "After restore",
                }
            )
            return httpx.Response(
                200,
                json={
                    "_op_id": 0,
                    "result": {
                        "data": {
                            "restored": True,
                            "dry_run": False,
                            "_sync_reset": {
                                "restored": True,
                                "local_rebootstrap_required": True,
                                "reset_epoch": "epoch-2",
                            },
                        },
                        "summary": {"restored": True},
                    },
                },
            )
        raise AssertionError(f"unexpected request path: {request.url.path}")

    transport = httpx.MockTransport(handler)
    engine_a = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=transport),
    )
    engine_b = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(transport=transport),
    )
    bundle_path = tmp_path / "restore.tar.gz"
    bundle_path.write_bytes(b"restore-bundle")

    def use_client(name: str) -> Path:
        base_dir = tmp_path / name / ".cashnerd"
        _patch_cashnerd_paths(monkeypatch, base_dir)
        return sync_config.CASHNERD_DB_PATH

    db_a = use_client("client-a")
    assert asyncio.run(engine_a.pull()) is True
    db_b = use_client("client-b")
    assert asyncio.run(engine_b.pull()) is True

    assert _local_reset_epoch(db_a) == "epoch-1"
    assert _local_reset_epoch(db_b) == "epoch-1"

    use_client("client-a")
    payload = asyncio.run(
        engine_a.proxy_tool(
            "db_restore", {"bundle_path": str(bundle_path), "dry_run": False}
        )
    )

    assert payload["data"]["local_rebootstrapped"] is True
    assert _local_reset_epoch(db_a) == "epoch-2"
    assert _local_txn_description(db_a, "after-restore") == "After restore"

    use_client("client-b")
    assert asyncio.run(engine_b.pull()) is True

    assert schema_calls >= 1
    assert _local_reset_epoch(db_b) == "epoch-2"
    assert _local_txn_description(db_b, "after-restore") == "After restore"
    assert _local_txn_description(db_b, "before-restore") is None


def test_proxy_tool_handles_remote_sync_unsupported_501(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    501,
                    json={"detail": {"error": "sync_unsupported_for_remote"}},
                )
            )
        ),
    )

    payload = asyncio.run(engine.proxy_tool("plaid_sync", {"account_id": "acct-1"}))

    assert "Remote storage sync is not enabled" in payload["data"]["error"]


def test_proxy_tool_raises_engagement_required(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    403,
                    json={
                        "detail": {
                            "error": "engagement_required",
                            "message": "Plaid sync requires an active CashNerd engagement.",
                        }
                    },
                )
            )
        ),
    )

    with pytest.raises(EngagementRequiredError) as exc_info:
        asyncio.run(engine.proxy_tool("plaid_sync", {"account_id": "acct-1"}))

    assert (
        exc_info.value.user_message
        == "Plaid sync requires an active CashNerd engagement."
    )


def test_proxy_tool_raises_for_422(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    engine = SyncEngine(
        sync_config.SyncConfig(server_url="https://cashnerd.example"),
        DummyAuth(),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    422, json={"detail": "Tool 'bad' is not available via sync proxy"}
                )
            )
        ),
    )

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(engine.proxy_tool("bad", {}))


@pytest.mark.parametrize(
    ("tool_name", "arguments", "expected"),
    [
        ("plaid_link", {"timeout": 15}, 45.0),
        ("plaid_link", {"timeout": "bad"}, 330.0),
        ("plaid_exchange", {}, 330.0),
        ("plaid_exchange", {"timeout": 20}, 50.0),
        ("setup_connect", {"timeout": 20, "skip_sync": True}, 50.0),
        ("setup_connect", {"timeout": 20, "skip_sync": False}, 320.0),
        ("monthly_run", {"sync": True, "ai": True}, 600.0),
        ("monthly_run", {"sync": True, "ai": False}, 300.0),
        ("db_import_preferences", {}, 300.0),
        ("db_restore", {}, 300.0),
        ("plaid_sync", {}, 180.0),
        ("plaid_balance_refresh", {}, 180.0),
        ("stripe_sync", {}, 180.0),
        ("schwab_sync", {}, 180.0),
        ("plaid_status", {}, 120.0),
    ],
)
def test_proxy_timeout_map(
    tool_name: str, arguments: dict[str, object], expected: float
) -> None:
    assert _proxy_timeout(tool_name, arguments) == expected
