from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import card_paydown_flags
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_card(conn, *, account_id: str = "card-1", balance_cents: int = -250_000) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            card_ending, balance_current_cents, balance_limit_cents, is_active
        ) VALUES (?, 'High Bank', 'Rewards', 'credit_card', '1234', ?, 500000, 1)
        """,
        (account_id, balance_cents),
    )


def _seed_cash(conn, *, account_id: str = "checking-1", account_type: str = "checking") -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Cash Bank', 'Checking', ?, 300000, 1)
        """,
        (account_id, account_type),
    )


def _seed_liability(conn, *, account_id: str = "card-1") -> str:
    liability_id = "liability-card-1"
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active, apr_purchase,
            minimum_payment_cents, intro_apr_end_date
        ) VALUES (?, ?, 'credit', 1, 24.99, 7500, '2099-12-31')
        """,
        (liability_id, account_id),
    )
    return liability_id


def test_flag_card_for_paydown_is_idempotent_and_snapshots_card(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_card(conn)
        _seed_cash(conn)
        liability_id = _seed_liability(conn)
        conn.commit()

        first = card_paydown_flags.flag_card_for_paydown(
            conn,
            account_id="card-1",
            suggested_payment_cents=50_000,
            cash_source_account_id="checking-1",
            interest_saved_annual_cents=12_000,
        )
        second = card_paydown_flags.flag_card_for_paydown(
            conn,
            account_id="card-1",
            suggested_payment_cents=75_000,
            cash_source_account_id="checking-1",
            interest_saved_annual_cents=15_000,
            reason="Use bonus cash on this card first.",
            source="user",
        )
        rows = conn.execute(
            "SELECT id, reason, suggested_payment_cents, source FROM card_paydown_flags"
        ).fetchall()

    snapshot = first["data"]["flag"]["snapshot"]
    assert first["summary"]["flagged"] == 1
    assert snapshot["account_label"] == "High Bank Rewards 1234"
    assert snapshot["balance_cents"] == 250_000
    assert snapshot["apr_purchase"] == 24.99
    assert snapshot["minimum_payment_cents"] == 7_500
    assert snapshot["liability_id"] == liability_id
    assert snapshot["cash_source_account_id"] == "checking-1"
    assert second["data"]["flag"]["id"] == first["data"]["flag"]["id"]
    assert len(rows) == 1
    assert rows[0]["suggested_payment_cents"] == 75_000
    assert rows[0]["reason"] == "Use bonus cash on this card first."
    assert rows[0]["source"] == "user"


def test_flag_card_for_paydown_dry_run_does_not_write(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_card(conn)
        conn.commit()

        result = card_paydown_flags.flag_card_for_paydown(
            conn,
            account_id="card-1",
            dry_run=True,
        )
        row_count = conn.execute("SELECT COUNT(*) AS n FROM card_paydown_flags").fetchone()["n"]

    assert result["summary"]["flagged"] == 0
    assert result["data"]["dry_run"] is True
    assert row_count == 0


def test_flag_card_for_paydown_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_cash(conn, account_id="checking-1")
        _seed_cash(conn, account_id="savings-1", account_type="savings")
        _seed_card(conn, account_id="card-1")
        _seed_card(conn, account_id="paid-off", balance_cents=0)
        conn.commit()

        with pytest.raises(ValueError, match="account not found"):
            card_paydown_flags.flag_card_for_paydown(conn, account_id="missing")
        with pytest.raises(ValueError, match="credit_card"):
            card_paydown_flags.flag_card_for_paydown(conn, account_id="checking-1")
        with pytest.raises(ValueError, match="positive balance"):
            card_paydown_flags.flag_card_for_paydown(conn, account_id="paid-off")
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            card_paydown_flags.flag_card_for_paydown(
                conn,
                account_id="card-1",
                suggested_payment_cents=-1,
            )


def test_list_and_clear_card_paydown_flags(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_card(conn, account_id="card-1")
        _seed_card(conn, account_id="card-2", balance_cents=-150_000)
        conn.commit()
        first = card_paydown_flags.flag_card_for_paydown(conn, account_id="card-1")
        second = card_paydown_flags.flag_card_for_paydown(conn, account_id="card-2")

        cleared = card_paydown_flags.clear_card_paydown_flag(
            conn,
            flag_id=first["data"]["flag"]["id"],
        )
        active = card_paydown_flags.list_card_paydown_flags(conn)
        all_flags = card_paydown_flags.list_card_paydown_flags(conn, status="all")

    assert cleared["summary"]["cleared"] == 1
    assert cleared["data"]["flag"]["status"] == "resolved"
    assert active["summary"] == {"count": 1, "status": "active"}
    assert active["data"]["flags"][0]["id"] == second["data"]["flag"]["id"]
    assert all_flags["summary"] == {"count": 2, "status": "all"}


def test_card_paydown_tools_are_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS, NO_SYNC_TOOLS

    assert "card_paydown_flags_list" in gateway_tools.READ_ONLY_TOOLS
    assert "card_paydown_flags_list" not in gateway_tools.BRIDGE_TOOLS
    assert "card_paydown_flags_list" in NO_SYNC_TOOLS
    assert {"flag_card_for_paydown", "clear_card_paydown_flag"} <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert {"flag_card_for_paydown", "clear_card_paydown_flag"} <= DB_WRITE_TOOLS
