from __future__ import annotations

from pathlib import Path

from finance_cli import db as db_module
from finance_cli.db import connect, initialize_database


def test_migration_057_seeds_plaid_daily_limit(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        versions = {
            int(row["version"])
            for row in conn.execute("SELECT version FROM schema_version").fetchall()
        }
        row = conn.execute(
            """
            SELECT provider, period, limit_usd6, action
            FROM cost_limits
            WHERE provider = 'plaid'
              AND period = 'daily'
            """
        ).fetchone()

    assert row is not None
    assert row["provider"] == "plaid"
    assert row["period"] == "daily"
    assert row["limit_usd6"] == 1_000_000
    assert row["action"] == "warn"
    assert max(versions) == db_module.SCHEMA_VERSION
