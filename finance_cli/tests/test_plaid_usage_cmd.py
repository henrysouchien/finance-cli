from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from finance_cli.commands import plaid_cmd
from finance_cli.db import connect, initialize_database


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def test_plaid_usage_day_json_shape_and_summary(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES
                ('plaid', 'accounts_balance_get', 100000, datetime('now')),
                ('plaid', 'accounts_balance_get', 100000, datetime('now')),
                ('plaid', 'transactions_sync', 0, datetime('now')),
                ('plaid', 'transactions_sync', 0, datetime('now')),
                ('plaid', 'item_get', 0, datetime('now'))
            """
        )
        conn.commit()

        result = plaid_cmd.handle_usage(SimpleNamespace(day=True, month=False), conn)

    assert result["data"] == {
        "period": "day",
        "start": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "totals": {"calls": 5, "cost_usd6": 200_000},
        "by_operation": [
            {"operation": "accounts_balance_get", "calls": 2, "cost_usd6": 200_000},
            {"operation": "transactions_sync", "calls": 2, "cost_usd6": 0},
            {"operation": "item_get", "calls": 1, "cost_usd6": 0},
        ],
    }
    assert result["summary"] == {
        "calls": 5,
        "cost_usd": "0.20",
        "limit_usd": "1.00",
        "pct_used": 20.0,
    }


def test_plaid_usage_month_json_shape_and_summary(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES
                ('plaid', 'accounts_balance_get', 100000, datetime('now', 'start of month')),
                ('plaid', 'item_get', 0, datetime('now', 'start of month'))
            """
        )
        conn.commit()

        result = plaid_cmd.handle_usage(SimpleNamespace(day=False, month=True), conn)

    assert result["data"] == {
        "period": "month",
        "start": datetime.now(timezone.utc).strftime("%Y-%m-01T00:00:00Z"),
        "totals": {"calls": 2, "cost_usd6": 100_000},
        "by_operation": [
            {"operation": "accounts_balance_get", "calls": 1, "cost_usd6": 100_000},
            {"operation": "item_get", "calls": 1, "cost_usd6": 0},
        ],
    }
    assert result["summary"] == {
        "calls": 2,
        "cost_usd": "0.10",
        "limit_usd": "10.00",
        "pct_used": 1.0,
    }


def test_plaid_usage_default_json_shape_and_cli_sections(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES
                ('plaid', 'accounts_balance_get', 100000, datetime('now')),
                ('plaid', 'transactions_sync', 0, datetime('now'))
            """
        )
        conn.commit()

        default_result = plaid_cmd.handle_usage(SimpleNamespace(day=False, month=False), conn)

    assert default_result["data"]["day"] == {
        "period": "day",
        "start": datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z"),
        "totals": {"calls": 2, "cost_usd6": 100_000},
        "by_operation": [
            {"operation": "accounts_balance_get", "calls": 1, "cost_usd6": 100_000},
            {"operation": "transactions_sync", "calls": 1, "cost_usd6": 0},
        ],
    }
    assert default_result["data"]["month"] == {
        "period": "month",
        "start": datetime.now(timezone.utc).strftime("%Y-%m-01T00:00:00Z"),
        "totals": {"calls": 2, "cost_usd6": 100_000},
        "by_operation": [
            {"operation": "accounts_balance_get", "calls": 1, "cost_usd6": 100_000},
            {"operation": "transactions_sync", "calls": 1, "cost_usd6": 0},
        ],
    }
    assert default_result["summary"] == {
        "day": {
            "calls": 2,
            "cost_usd": "0.10",
            "limit_usd": "1.00",
            "pct_used": 10.0,
        },
        "month": {
            "calls": 2,
            "cost_usd": "0.10",
            "limit_usd": "10.00",
            "pct_used": 1.0,
        },
    }
    assert "Plaid usage" in default_result["cli_report"]
    assert "Today" in default_result["cli_report"]
    assert "This month" in default_result["cli_report"]
    assert "daily limit $1.00" in default_result["cli_report"]
    assert "monthly limit $10.00" in default_result["cli_report"]

    with connect(db_path) as conn:
        day_result = plaid_cmd.handle_usage(SimpleNamespace(day=True, month=False), conn)
    assert "Today" in day_result["cli_report"]
    assert "This month" not in day_result["cli_report"]

    with connect(db_path) as conn:
        month_result = plaid_cmd.handle_usage(SimpleNamespace(day=False, month=True), conn)
    assert "This month" in month_result["cli_report"]
    assert "Today" not in month_result["cli_report"]
