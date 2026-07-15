from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import transaction_disputes
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(conn, account_id: str = "card-1") -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Card Bank', 'Rewards', 'credit_card', -50000, 1)
        """,
        (account_id,),
    )


def _seed_txn(
    conn,
    txn_id: str,
    *,
    account_id: str = "card-1",
    date: str = "2026-05-01",
    description: str = "ACME STORE",
    amount_cents: int = -5_000,
    is_active: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, is_active, source
        ) VALUES (?, ?, ?, ?, ?, ?, 'manual')
        """,
        (txn_id, account_id, date, description, amount_cents, is_active),
    )


def test_txn_dispute_workflow_is_idempotent_for_duplicate_charge(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-1", date="2026-05-01")
        _seed_txn(conn, "txn-2", date="2026-05-05")
        conn.commit()

        preview = transaction_disputes.txn_dispute_workflow(
            conn,
            transaction_id="txn-1",
            duplicate_transaction_id="txn-2",
            dry_run=True,
        )
        preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM transaction_dispute_workflows"
        ).fetchone()["n"]
        first = transaction_disputes.txn_dispute_workflow(
            conn,
            transaction_id="txn-1",
            duplicate_transaction_id="txn-2",
            note="Looks duplicated.",
        )
        second = transaction_disputes.txn_dispute_workflow(
            conn,
            transaction_id="txn-1",
            duplicate_transaction_id="txn-2",
            note="User confirmed duplicate.",
            source="user",
        )
        rows = conn.execute(
            """
            SELECT id, note, source, amount_cents, dispute_reason
              FROM transaction_dispute_workflows
            """
        ).fetchall()

    assert preview["summary"]["prepared"] == 0
    assert preview["data"]["workflow"]["snapshot"]["duplicate"]["id"] == "txn-2"
    assert preview_count == 0
    assert first["summary"]["prepared"] == 1
    assert first["summary"]["amount_cents"] == 5_000
    assert second["data"]["workflow"]["id"] == first["data"]["workflow"]["id"]
    assert len(rows) == 1
    assert rows[0]["note"] == "User confirmed duplicate."
    assert rows[0]["source"] == "user"
    assert rows[0]["amount_cents"] == 5_000
    assert rows[0]["dispute_reason"] == "duplicate_charge"


def test_txn_dispute_workflow_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_account(conn, "card-2")
        _seed_txn(conn, "txn-1", account_id="card-1", date="2026-05-01")
        _seed_txn(conn, "txn-2", account_id="card-1", date="2026-05-20")
        _seed_txn(conn, "txn-3", account_id="card-2", date="2026-05-02")
        _seed_txn(conn, "txn-4", account_id="card-1", date="2026-05-02", amount_cents=-6_000)
        _seed_txn(conn, "income-1", account_id="card-1", amount_cents=5_000)
        conn.commit()

        with pytest.raises(ValidationError, match="transaction not found"):
            transaction_disputes.txn_dispute_workflow(conn, transaction_id="missing")
        with pytest.raises(ValidationError, match="expense transaction"):
            transaction_disputes.txn_dispute_workflow(
                conn,
                transaction_id="income-1",
                dispute_reason="unrecognized_merchant",
            )
        with pytest.raises(ValidationError, match="duplicate_transaction_id is required"):
            transaction_disputes.txn_dispute_workflow(conn, transaction_id="txn-1")
        with pytest.raises(ValidationError, match="same account"):
            transaction_disputes.txn_dispute_workflow(
                conn,
                transaction_id="txn-1",
                duplicate_transaction_id="txn-3",
            )
        with pytest.raises(ValidationError, match="same amount"):
            transaction_disputes.txn_dispute_workflow(
                conn,
                transaction_id="txn-1",
                duplicate_transaction_id="txn-4",
            )
        with pytest.raises(ValidationError, match="within 7 days"):
            transaction_disputes.txn_dispute_workflow(
                conn,
                transaction_id="txn-1",
                duplicate_transaction_id="txn-2",
            )


def test_txn_dispute_workflow_allows_unrecognized_merchant_without_duplicate(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-1")
        conn.commit()

        result = transaction_disputes.txn_dispute_workflow(
            conn,
            transaction_id="txn-1",
            dispute_reason="unrecognized_merchant",
        )

    assert result["summary"]["prepared"] == 1
    assert result["data"]["workflow"]["duplicate_transaction_id"] is None
    assert result["data"]["workflow"]["dispute_reason"] == "unrecognized_merchant"


def test_txn_dispute_workflow_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "txn_dispute_workflow" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "txn_dispute_workflow" in DB_WRITE_TOOLS
