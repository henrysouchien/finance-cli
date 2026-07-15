from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import spending_freeze
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(conn, *, account_id: str = "checking-1", is_active: int = 1) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Test Bank', 'Checking', 'checking', 75000, ?)
        """,
        (account_id, is_active),
    )


def _seed_category(conn, *, category_id: str = "dining", name: str = "Dining") -> None:
    conn.execute(
        """
        INSERT INTO categories (id, name, level, is_system)
        VALUES (?, ?, 0, 0)
        """,
        (category_id, name),
    )


def test_set_spending_freeze_flag_is_idempotent_and_can_scope_to_category(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_category(conn)
        conn.commit()

        first = spending_freeze.set_spending_freeze_flag(
            conn,
            category_id="dining",
            account_id="checking-1",
            bill_name="Rent",
            bill_amount_cents=250_000,
            due_date="2099-06-01",
            target_balance_after_cents=-5_000,
        )
        second = spending_freeze.set_spending_freeze_flag(
            conn,
            category_id="dining",
            account_id="checking-1",
            bill_name="Rent",
            bill_amount_cents=250_000,
            due_date="2099-06-01",
            target_balance_after_cents=-5_000,
            reason="Pause dining until rent clears.",
            source="user",
        )
        rows = conn.execute("SELECT id, scope, reason, source FROM spending_freeze_flags").fetchall()

    flag = first["data"]["flag"]
    assert first["summary"]["configured"] == 1
    assert flag["scope"] == "category"
    assert flag["category_id"] == "dining"
    assert flag["category_name"] == "Dining"
    assert flag["account_id"] == "checking-1"
    assert flag["bill_amount_cents"] == 250_000
    assert flag["target_balance_after_cents"] == -5_000
    assert flag["hold_until"] == "2099-06-01"
    assert second["data"]["flag"]["id"] == first["data"]["flag"]["id"]
    assert len(rows) == 1
    assert rows[0]["scope"] == "category"
    assert rows[0]["reason"] == "Pause dining until rent clears."
    assert rows[0]["source"] == "user"


def test_set_spending_freeze_flag_dry_run_does_not_write(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = spending_freeze.set_spending_freeze_flag(
            conn,
            scope="all_nonessential",
            hold_until="2099-06-01",
            dry_run=True,
        )
        row_count = conn.execute("SELECT COUNT(*) AS n FROM spending_freeze_flags").fetchone()["n"]

    assert result["summary"]["configured"] == 0
    assert result["data"]["dry_run"] is True
    assert result["data"]["flag"]["scope"] == "all_nonessential"
    assert row_count == 0


def test_set_spending_freeze_flag_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, account_id="inactive", is_active=0)
        conn.commit()

        with pytest.raises(ValueError, match="category_id is required"):
            spending_freeze.set_spending_freeze_flag(conn, scope="category", hold_until="2099-06-01")
        with pytest.raises(ValueError, match="account must be active"):
            spending_freeze.set_spending_freeze_flag(
                conn,
                scope="account",
                account_id="inactive",
                hold_until="2099-06-01",
            )
        with pytest.raises(ValueError, match="hold_until must not be in the past"):
            spending_freeze.set_spending_freeze_flag(conn, hold_until="2000-01-01")
        with pytest.raises(ValueError, match="source must be one of"):
            spending_freeze.set_spending_freeze_flag(conn, source="chat", hold_until="2099-06-01")


def test_list_and_clear_spending_freeze_flags(db_path: Path) -> None:
    with connect(db_path) as conn:
        first = spending_freeze.set_spending_freeze_flag(
            conn,
            hold_until="2099-06-01",
            bill_name="Rent",
        )
        second = spending_freeze.set_spending_freeze_flag(
            conn,
            hold_until="2099-06-02",
            bill_name="Insurance",
        )

        cleared = spending_freeze.clear_spending_freeze_flag(
            conn,
            flag_id=first["data"]["flag"]["id"],
        )
        active = spending_freeze.list_spending_freeze_flags(conn)
        all_flags = spending_freeze.list_spending_freeze_flags(conn, status="all")

    assert cleared["summary"]["cleared"] == 1
    assert cleared["data"]["flag"]["status"] == "resolved"
    assert active["summary"] == {"count": 1, "status": "active"}
    assert active["data"]["flags"][0]["id"] == second["data"]["flag"]["id"]
    assert all_flags["summary"] == {"count": 2, "status": "all"}


def test_spending_freeze_tools_are_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS, NO_SYNC_TOOLS

    assert "spending_freeze_flags_list" in gateway_tools.READ_ONLY_TOOLS
    assert "spending_freeze_flags_list" not in gateway_tools.BRIDGE_TOOLS
    assert "spending_freeze_flags_list" in NO_SYNC_TOOLS
    assert {"set_spending_freeze_flag", "clear_spending_freeze_flag"} <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert {"set_spending_freeze_flag", "clear_spending_freeze_flag"} <= DB_WRITE_TOOLS
