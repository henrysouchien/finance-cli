from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database
from finance_cli.sync_protocol import REPLICATED_TABLES


def test_migration_058_creates_change_feed_schema_and_seed_row(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        changelog_columns = {
            row["name"]: row["type"]
            for row in conn.execute("PRAGMA table_info(_sync_changelog)").fetchall()
        }
        table_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        sync_state = conn.execute(
            """
            SELECT id, last_applied_op_id, install_id, subscriber_status
            FROM sync_state
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert changelog_columns["origin_session_id"] == "TEXT"
    assert {"_meta_state", "sync_state"} <= table_names
    assert sync_state["id"] == 0
    assert sync_state["last_applied_op_id"] == 0
    assert sync_state["install_id"] == ""
    assert sync_state["subscriber_status"] == "healthy"
    assert "_sync_log__meta_state_insert" in trigger_names
    assert "_sync_log__meta_state_update" in trigger_names
    assert "_sync_log__meta_state_delete" in trigger_names
    assert "_sync_log_tax_config_update" in trigger_names
    assert REPLICATED_TABLES <= table_names


def test_migration_058_stamps_origin_session_and_skips_stream_session(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="install-123") as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-origin', '2026-04-16', 'Origin', -500, 'manual', 1)
            """
        )
        conn.commit()

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT table_name, op, pk_json, origin_session_id
            FROM _sync_changelog
            WHERE table_name = 'transactions'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        assert row["table_name"] == "transactions"
        assert row["op"] == "INSERT"
        assert json.loads(row["pk_json"]) == {"id": "txn-origin"}
        assert row["origin_session_id"] == "install-123"

    with connect(db_path, session_id="__STREAM__") as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-stream', '2026-04-16', 'Stream', -700, 'manual', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO _meta_state (key, sha256, updated_at)
            VALUES ('rules.yaml', 'abc123', '2026-04-16T12:00:00Z')
            """
        )
        conn.commit()

    with connect(db_path) as conn:
        tx_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM _sync_changelog
            WHERE json_extract(pk_json, '$.id') = 'txn-stream'
            """
        ).fetchone()[0]
        meta_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM _sync_changelog
            WHERE table_name = '_meta_state'
              AND json_extract(pk_json, '$.key') = 'rules.yaml'
            """
        ).fetchone()[0]

    assert tx_count == 0
    assert meta_count == 0
