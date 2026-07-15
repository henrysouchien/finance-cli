from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

import finance_cli.mcp_server as mcp_server
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@pytest.fixture()
def conn(db_path: Path):
    handle = connect(db_path)
    yield handle
    handle.close()


def _count(conn, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
    return int(row["cnt"] if row else 0)


def _event_count(conn, event: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM analytics_events WHERE event = ?",
        (event,),
    ).fetchone()
    return int(row["cnt"] if row else 0)


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_account(
    conn,
    *,
    institution: str = "Test Bank",
    name: str = "Checking",
    account_type: str = "checking",
    source: str = "manual",
    is_business: int = 0,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, source, is_active, is_business
        ) VALUES (?, ?, ?, ?, ?, 1, ?)
        """,
        (account_id, institution, name, account_type, source, is_business),
    )
    conn.commit()
    return account_id


def _seed_txn(
    conn,
    *,
    description: str,
    txn_date: str,
    amount_cents: int,
    account_id: str | None = None,
    category_id: str | None = None,
    use_type: str | None = None,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, use_type, is_active, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, description, amount_cents, category_id, use_type),
    )
    conn.commit()
    return txn_id


def _seed_recurring_series(conn, *, description: str = "Netflix", amount_cents: int = -1599) -> None:
    category_id = _seed_category(conn, "Streaming")
    today = date.today()
    for days_ago in (60, 30, 0):
        _seed_txn(
            conn,
            description=description,
            txn_date=(today - timedelta(days=days_ago)).isoformat(),
            amount_cents=amount_cents,
            category_id=category_id,
            use_type="Personal",
        )


def test_txn_add_dry_run_does_not_persist(db_path: Path, conn) -> None:
    before = _count(conn, "transactions")

    result = mcp_server.txn_add(
        amount=25.50,
        date=date.today().isoformat(),
        description="Dry run txn",
        dry_run=True,
    )

    assert result["data"]["dry_run"] is True
    assert result["summary"]["total_transactions"] == 1

    with connect(db_path) as check_conn:
        assert _count(check_conn, "transactions") == before


def test_budget_set_dry_run_rolls_back_and_skips_analytics(db_path: Path, conn) -> None:
    _seed_category(conn, "Dining")
    analytics_before = _event_count(conn, "feature.budget_set")

    result = mcp_server.budget_set(category="Dining", amount=500, period="monthly", dry_run=True)

    assert result["data"]["dry_run"] is True
    assert result["data"]["budget_id"]

    with connect(db_path) as check_conn:
        assert _count(check_conn, "budgets") == 0
        assert _event_count(check_conn, "feature.budget_set") == analytics_before


def test_account_set_business_dry_run_returns_preview_without_persisting(db_path: Path, conn) -> None:
    account_id = _seed_account(conn, is_business=0)
    _seed_txn(
        conn,
        description="Office Depot",
        txn_date=date.today().isoformat(),
        amount_cents=-5000,
        account_id=account_id,
        use_type=None,
    )

    result = mcp_server.account_set_business(
        id=account_id,
        is_business=True,
        backfill=True,
        dry_run=True,
    )

    assert result["data"]["dry_run"] is True
    assert result["data"]["account"]["is_business"] == 1
    assert result["data"]["backfill"]["transactions_updated"] == 1

    with connect(db_path) as check_conn:
        row = check_conn.execute(
            "SELECT is_business FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        txn_row = check_conn.execute(
            "SELECT use_type FROM transactions WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        assert int(row["is_business"]) == 0
        assert txn_row["use_type"] is None


def test_loan_add_dry_run_returns_preview_without_persisting(db_path: Path, conn) -> None:
    loans_before = _count(conn, "manual_loans")

    result = mcp_server.loan_add(
        creditor="Family",
        amount=500.0,
        start_date="2026-01-01",
        monthly_payment=50.0,
        due_day=15,
        dry_run=True,
    )

    assert result["data"]["dry_run"] is True
    assert result["data"]["loan"]["creditor_name"] == "Family"
    assert result["data"]["loan"]["balance"] == 500.0
    assert result["data"]["disbursement"]["amount"] == 500.0

    with connect(db_path) as check_conn:
        assert _count(check_conn, "manual_loans") == loans_before
        assert _count(check_conn, "loan_disbursements") == 0


def test_loan_add_idempotency_key_returns_existing_row(db_path: Path, conn) -> None:
    first = mcp_server.loan_add(
        creditor="Family",
        amount=500.0,
        start_date="2026-01-01",
        monthly_payment=50.0,
        due_day=15,
        idempotency_key="loan-add-1",
    )
    second = mcp_server.loan_add(
        creditor="Family",
        amount=500.0,
        start_date="2026-01-01",
        monthly_payment=50.0,
        due_day=15,
        idempotency_key="loan-add-1",
    )

    assert second["data"]["loan"]["id"] == first["data"]["loan"]["id"]
    assert second["data"]["already_existed"] is True

    with connect(db_path) as check_conn:
        assert _count(check_conn, "manual_loans") == 1
        assert _count(check_conn, "loan_disbursements") == 1


def test_loan_add_without_idempotency_key_keeps_normal_behavior(db_path: Path, conn) -> None:
    first = mcp_server.loan_add(
        creditor="Family",
        amount=500.0,
        start_date="2026-01-01",
        monthly_payment=50.0,
        due_day=15,
    )
    second = mcp_server.loan_add(
        creditor="Family",
        amount=500.0,
        start_date="2026-01-01",
        monthly_payment=50.0,
        due_day=15,
    )

    assert first["data"]["loan"]["id"] != second["data"]["loan"]["id"]

    with connect(db_path) as check_conn:
        assert _count(check_conn, "manual_loans") == 2
        assert _count(check_conn, "loan_disbursements") == 2


def test_provider_switch_dry_run_rolls_back_wrapper_commit(db_path: Path, conn) -> None:
    account_id = _seed_account(conn, institution="Switch Bank", source="schwab")

    result = mcp_server.provider_switch(institution="Switch Bank", provider="plaid", dry_run=True)

    assert result["data"]["dry_run"] is True
    assert result["data"]["deactivated_count"] == 1

    with connect(db_path) as check_conn:
        routing_count = _count(check_conn, "provider_routing")
        row = check_conn.execute(
            "SELECT is_active FROM accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        assert routing_count == 0
        assert int(row["is_active"]) == 1


def test_subs_detect_dry_run_rolls_back_and_skips_analytics(db_path: Path, conn) -> None:
    _seed_recurring_series(conn)
    analytics_before = _event_count(conn, "feature.subscription_detected")

    result = mcp_server.subs_detect(dry_run=True)

    assert result["data"]["dry_run"] is True
    assert result["data"]["detected"] >= 1

    with connect(db_path) as check_conn:
        assert _count(check_conn, "subscriptions") == 0
        assert _event_count(check_conn, "feature.subscription_detected") == analytics_before


def test_monthly_run_dry_run_skips_pruning(db_path: Path) -> None:
    with patch("finance_cli.commands.monthly_cmd.prune_analytics") as prune_analytics, \
         patch("finance_cli.commands.monthly_cmd.prune_perf_samples") as prune_perf_samples, \
         patch("finance_cli.commands.monthly_cmd.prune_frontend_logs") as prune_frontend_logs, \
         patch("finance_cli.commands.monthly_cmd.prune_errors") as prune_errors, \
         patch("finance_cli.commands.monthly_cmd.prune_cost_ledger") as prune_cost_ledger:
        result = mcp_server.monthly_run(
            dry_run=True,
            skip=["dedup", "categorize", "detect"],
            summary_only=False,
        )

    assert result["data"]["month"]
    prune_analytics.assert_not_called()
    prune_perf_samples.assert_not_called()
    prune_frontend_logs.assert_not_called()
    prune_errors.assert_not_called()
    prune_cost_ledger.assert_not_called()
