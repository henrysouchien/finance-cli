from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_067_creates_internal_operation_log(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(_operation_log)").fetchall()
        }
        conn.execute(
            """
            INSERT INTO _operation_log (
                op_type,
                surface,
                tool_name,
                status,
                started_at,
                finished_at,
                start_changelog_id,
                end_changelog_id,
                request_json,
                result_json
            ) VALUES (
                'tool_invocation',
                'sync_proxy',
                'plaid_status',
                'success',
                '2026-05-26T10:00:00Z',
                '2026-05-26T10:00:01Z',
                10,
                10,
                '{"argument_keys":[]}',
                '{"has_errors":false}'
            )
            """
        )
        conn.commit()
        row = conn.execute(
            """
            SELECT op_type, surface, tool_name, status, request_json, result_json
              FROM _operation_log
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "op_type",
        "surface",
        "tool_name",
        "status",
        "started_at",
        "finished_at",
        "duration_ms",
        "start_changelog_id",
        "end_changelog_id",
        "request_json",
        "result_json",
        "error_json",
        "idempotency_key",
        "created_at",
    } <= columns
    assert row["op_type"] == "tool_invocation"
    assert row["surface"] == "sync_proxy"
    assert row["tool_name"] == "plaid_status"
    assert row["status"] == "success"
    assert json.loads(row["request_json"]) == {"argument_keys": []}
    assert json.loads(row["result_json"]) == {"has_errors": False}
