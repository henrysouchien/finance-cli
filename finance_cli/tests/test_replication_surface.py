from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.sync_protocol import (
    CHANGELOG_TABLES,
    DOWNSTREAM_ONLY_TABLES,
    NON_REPLICATED_WRITABLE_TABLES,
    READ_ONLY_TABLES,
    REPLICATED_TABLES,
)


def test_replicated_tables_covers_live_db_minus_excluded(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        internal_prefixes = ("sqlite_", "_sync_changelog", "sync_state", "txn_fts", "_meta_state_history")
        all_user_tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
            if not any(str(row["name"]).startswith(prefix) for prefix in internal_prefixes)
        }
        expected_surface = (
            set(REPLICATED_TABLES)
            | set(DOWNSTREAM_ONLY_TABLES)
            | set(NON_REPLICATED_WRITABLE_TABLES)
            | set(READ_ONLY_TABLES)
        )
        missing_classification = all_user_tables - expected_surface

        assert not missing_classification, (
            f"New table(s) added without classification: {sorted(missing_classification)}. "
            "Update REPLICATED_TABLES, DOWNSTREAM_ONLY_TABLES, or NON_REPLICATED_WRITABLE_TABLES."
        )

        for table in CHANGELOG_TABLES:
            for op in ("insert", "update", "delete"):
                trigger_name = f"_sync_log_{table}_{op}"
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                    (trigger_name,),
                ).fetchone()
                assert row is not None, f"Missing {trigger_name}"
