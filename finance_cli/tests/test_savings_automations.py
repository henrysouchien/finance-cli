from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import savings_automations
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_goal(
    conn,
    goal_id: str = "goal-1",
    *,
    name: str = "House Fund",
    is_active: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO goals (
            id, name, metric, target_cents, starting_cents, direction, deadline, is_active
        ) VALUES (?, ?, 'liquid_cash', 2000000, 500000, 'up', '2030-01-01', ?)
        """,
        (goal_id, name, is_active),
    )


def _seed_account(
    conn,
    account_id: str,
    *,
    account_type: str,
    balance_cents: int = 500_000,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Cash Bank', ?, ?, ?, 1)
        """,
        (account_id, account_id, account_type, balance_cents),
    )


def test_setup_savings_automation_is_idempotent_and_snapshots_goal(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_goal(conn)
        _seed_account(conn, "checking-1", account_type="checking")
        _seed_account(conn, "savings-1", account_type="savings")
        conn.commit()

        preview = savings_automations.setup_savings_automation(
            conn,
            goal_id="goal-1",
            amount_cents=50_000,
            start_date="2026-06-01",
            day_of_month=1,
            source_account_id="checking-1",
            destination_account_id="savings-1",
            projected_end_balance_cents=2_100_000,
            reason="Lock in the monthly pace.",
            dry_run=True,
        )
        preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM savings_automations"
        ).fetchone()["n"]

        first = savings_automations.setup_savings_automation(
            conn,
            goal_id="goal-1",
            amount_cents=50_000,
            start_date="2026-06-01",
            day_of_month=1,
            source_account_id="checking-1",
            destination_account_id="savings-1",
            projected_end_balance_cents=2_100_000,
            reason="Lock in the monthly pace.",
        )
        second = savings_automations.setup_savings_automation(
            conn,
            goal_id="goal-1",
            amount_cents=75_000,
            start_date="2026-07-01",
            cadence="biweekly",
            funding_method="paycheck_split",
            source_account_id="checking-1",
            destination_account_id="savings-1",
            reason="Updated after bonus.",
            source="user",
        )
        rows = conn.execute(
            """
            SELECT id, amount_cents, cadence, funding_method, reason, source
              FROM savings_automations
            """
        ).fetchall()

    assert preview["summary"]["configured"] == 0
    assert preview["data"]["automation"]["snapshot"]["goal_name"] == "House Fund"
    assert preview_count == 0
    assert first["summary"]["configured"] == 1
    assert first["data"]["automation"]["target_amount_cents"] == 2_000_000
    assert first["data"]["automation"]["goal_date"] == "2030-01-01"
    assert second["data"]["automation"]["id"] == first["data"]["automation"]["id"]
    assert len(rows) == 1
    assert rows[0]["amount_cents"] == 75_000
    assert rows[0]["cadence"] == "biweekly"
    assert rows[0]["funding_method"] == "paycheck_split"
    assert rows[0]["reason"] == "Updated after bonus."
    assert rows[0]["source"] == "user"


def test_setup_savings_automation_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_goal(conn)
        _seed_goal(conn, "inactive-goal", name="Inactive House Fund", is_active=0)
        _seed_account(conn, "checking-1", account_type="checking")
        _seed_account(conn, "savings-1", account_type="savings")
        _seed_account(conn, "card-1", account_type="credit_card")
        conn.commit()

        with pytest.raises(ValidationError, match="goal not found"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="missing",
                amount_cents=50_000,
                start_date="2026-06-01",
            )
        with pytest.raises(ValidationError, match="goal must be active"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="inactive-goal",
                amount_cents=50_000,
                start_date="2026-06-01",
            )
        with pytest.raises(ValidationError, match="greater than 0"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="goal-1",
                amount_cents=0,
                start_date="2026-06-01",
            )
        with pytest.raises(ValidationError, match="YYYY-MM-DD"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="goal-1",
                amount_cents=50_000,
                start_date="06/01/2026",
            )
        with pytest.raises(ValidationError, match="funding_method"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="goal-1",
                amount_cents=50_000,
                start_date="2026-06-01",
                funding_method="wire",
            )
        with pytest.raises(ValidationError, match="source_account_id"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="goal-1",
                amount_cents=50_000,
                start_date="2026-06-01",
                source_account_id="savings-1",
            )
        with pytest.raises(ValidationError, match="destination_account_id"):
            savings_automations.setup_savings_automation(
                conn,
                goal_id="goal-1",
                amount_cents=50_000,
                start_date="2026-06-01",
                destination_account_id="card-1",
            )


def test_setup_savings_automation_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    assert "setup_savings_automation" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "setup_savings_automation" in DB_WRITE_TOOLS
