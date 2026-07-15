from __future__ import annotations

import io
import sqlite3
import tarfile
import threading
import time
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.sync import config as sync_config
from finance_cli.sync.bootstrap_lock import InstallBootstrapLock
from finance_cli.sync.engine import SyncEngine


def _patch_paths(monkeypatch, base_dir: Path) -> None:
    import finance_cli.mcp_local as mcp_local
    import finance_cli.sync.engine as sync_engine

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", base_dir / "data" / "finance.db")
    monkeypatch.setattr(sync_config, "CASHNERD_RULES_PATH", base_dir / "data" / "rules.yaml")
    monkeypatch.setattr(sync_engine, "CASHNERD_DIR", sync_config.CASHNERD_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(sync_engine, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)
    monkeypatch.setattr(sync_engine, "CASHNERD_RULES_PATH", sync_config.CASHNERD_RULES_PATH)
    monkeypatch.setattr(mcp_local, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)


def _snapshot_tar(tmp_path: Path) -> bytes:
    staging = tmp_path / "snapshot"
    staging.mkdir(exist_ok=True)
    db_path = staging / "finance.db"
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-stale', '2026-04-16', 'Stale Snapshot', -111, 'manual', 1)
            """
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(FULL)")
    (staging / "rules.yaml").write_text("keyword_rules: []\n", encoding="utf-8")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        tar.add(db_path, arcname="finance.db")
        tar.add(staging / "rules.yaml", arcname="rules.yaml")
    return buffer.getvalue()


class DummyAuth:
    async def get_credential(self) -> str:
        return "cred"

    async def get_sync_token(self, *, force_refresh: bool = False) -> str:
        return "token-fresh" if force_refresh else "token"

    def record_sync_session(self, *, token: str, user_id: str | None = None) -> None:
        return None

    def invalidate_sync_token(self) -> None:
        return None


def test_install_bootstrap_lock_serializes_concurrent_acquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "bootstrap.lock"
    order: list[str] = []
    first_inside = threading.Event()
    release_first = threading.Event()

    def worker(name: str) -> None:
        with InstallBootstrapLock(lock_path):
            order.append(name)
            if name == "first":
                first_inside.set()
                release_first.wait(1.0)

    first = threading.Thread(target=worker, args=("first",), daemon=True)
    second = threading.Thread(target=worker, args=("second",), daemon=True)
    first.start()
    assert first_inside.wait(1.0)
    second.start()
    time.sleep(0.05)
    assert order == ["first"]
    release_first.set()
    first.join(1.0)
    second.join(1.0)
    assert order == ["first", "second"]


def test_locked_commit_snapshot_discards_stale_tarball_when_local_cursor_is_newer(monkeypatch, tmp_path: Path) -> None:
    _patch_paths(monkeypatch, tmp_path / ".cashnerd")
    sync_config.CASHNERD_DATA_DIR.mkdir(parents=True, exist_ok=True)
    initialize_database(sync_config.CASHNERD_DB_PATH)
    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE sync_state
               SET last_applied_op_id = 50,
                   install_id = 'install-persist'
             WHERE id = 0
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-local', '2026-04-16', 'Current Local', -222, 'manual', 1)
            """
        )
        conn.commit()

    engine = SyncEngine(sync_config.SyncConfig(server_url="https://cashnerd.example"), DummyAuth())
    install_id = engine._locked_commit_snapshot(_snapshot_tar(tmp_path), 10, "user-bootstrap")

    with sqlite3.connect(str(sync_config.CASHNERD_DB_PATH)) as conn:
        local_row = conn.execute("SELECT description FROM transactions WHERE id = 'txn-local'").fetchone()
        stale_row = conn.execute("SELECT description FROM transactions WHERE id = 'txn-stale'").fetchone()
        sync_state = conn.execute(
            "SELECT last_applied_op_id, install_id FROM sync_state WHERE id = 0"
        ).fetchone()
    assert install_id == "install-persist"
    assert local_row == ("Current Local",)
    assert stale_row is None
    assert sync_state == (50, "install-persist")


def test_reconcile_sessions_on_startup_restores_leftover_backup(monkeypatch, tmp_path: Path) -> None:
    _patch_paths(monkeypatch, tmp_path / ".cashnerd")
    import finance_cli.mcp_local as mcp_local

    backup = sync_config.CASHNERD_DATA_DIR / "sessions.old"
    backup.mkdir(parents=True, exist_ok=True)
    (backup / "2026-04-16.md").write_text("restored note\n", encoding="utf-8")

    mcp_local._reconcile_sessions_on_startup()

    restored = sync_config.CASHNERD_DATA_DIR / "sessions" / "2026-04-16.md"
    assert restored.exists()
    assert restored.read_text(encoding="utf-8") == "restored note\n"
    assert not backup.exists()
