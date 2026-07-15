from __future__ import annotations

import json
from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


SPECIAL_SYNCED_TABLES_068_076 = [
    "user_strategy_preferences",
    "account_alert_rules",
    "contractor_tax_prep_flags",
    "spending_freeze_flags",
    "card_paydown_flags",
    "retirement_contribution_targets",
    "hysa_transfer_flags",
    "savings_automations",
    "transaction_dispute_workflows",
]


def test_migration_068_creates_synced_strategy_preferences_table(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path, session_id="local-test") as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(user_strategy_preferences)").fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'").fetchall()
        }
        conn.execute(
            """
            INSERT INTO user_strategy_preferences (
                domain, strategy, rationale, source, evidence_json
            ) VALUES (
                'debt', 'snowball', 'Small wins', 'user', '{"confirmed":true}'
            )
            """
        )
        conn.commit()
        changelog = conn.execute(
            """
            SELECT op, pk_json, new_json, origin_session_id
              FROM _sync_changelog
             WHERE table_name = 'user_strategy_preferences'
             ORDER BY id DESC
             LIMIT 1
            """
        ).fetchone()

    assert max(versions) == db_module.SCHEMA_VERSION
    assert {
        "domain",
        "strategy",
        "rationale",
        "source",
        "evidence_json",
        "created_at",
        "updated_at",
    } <= columns
    assert "user_strategy_preferences_touch_updated_at" in trigger_names
    for op in ("insert", "update", "delete"):
        assert f"_sync_log_user_strategy_preferences_{op}" in trigger_names
    assert changelog["op"] == "INSERT"
    assert json.loads(changelog["pk_json"]) == {"domain": "debt"}
    assert json.loads(changelog["new_json"])["strategy"] == "snowball"
    assert changelog["origin_session_id"] == "local-test"


def test_special_synced_migration_invariants_repair_current_version_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        for table_name in SPECIAL_SYNCED_TABLES_068_076:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

    initialize_database(db_path)

    with connect(db_path) as conn:
        table_names = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        trigger_names = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            ).fetchall()
        }
        version_counts = {
            int(row["version"]): int(row["n"])
            for row in conn.execute(
                """
                SELECT version, COUNT(*) AS n
                  FROM schema_version
                 WHERE version BETWEEN 68 AND 76
                 GROUP BY version
                """
            ).fetchall()
        }

    assert set(SPECIAL_SYNCED_TABLES_068_076) <= table_names
    assert "_sync_log_user_strategy_preferences_insert" in trigger_names
    assert "_sync_log_transaction_dispute_workflows_delete" in trigger_names
    assert version_counts == {version: 1 for version in range(68, 77)}
