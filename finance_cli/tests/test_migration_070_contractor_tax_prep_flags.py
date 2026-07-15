from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_070_creates_synced_contractor_tax_prep_flags_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(contractor_tax_prep_flags)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO contractors (id, name, entity_type, is_active)
            VALUES ('contractor-1', 'Jane Doe', 'individual', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO contractor_tax_prep_flags (
                id, contractor_id, tax_year, flag_type, status, reason, source,
                payment_snapshot_json
            ) VALUES (
                'flag-1', 'contractor-1', 2026, 'january_1099_prep', 'active',
                'Approaching threshold', 'agent', '{"non_card_paid_cents":55000}'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'contractor_tax_prep_flags'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "id",
        "contractor_id",
        "tax_year",
        "flag_type",
        "status",
        "reason",
        "source",
        "payment_snapshot_json",
        "resolved_at",
        "created_at",
        "updated_at",
    } <= columns
    assert "contractor_tax_prep_flags_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_contractor_tax_prep_flags_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"id": "flag-1"}
    assert json.loads(changelog["new_json"])["tax_year"] == 2026
    assert changelog["origin_session_id"] == "local-test"
