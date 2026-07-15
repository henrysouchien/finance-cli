from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import retirement_targets
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_set_monthly_retirement_target_is_idempotent_and_updates_monthly_plans(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        preview = retirement_targets.set_monthly_retirement_target(
            conn,
            tax_year="2026",
            account_type="Roth IRA",
            monthly_target_cents=100_000,
            start_month="2026-10",
            end_month="2026-12",
            room_remaining_cents=300_000,
            estimated_tax_savings_cents=75_000,
            reason="Use Q4 surplus before year-end.",
            dry_run=True,
        )
        preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM retirement_contribution_targets"
        ).fetchone()["n"]
        preview_plans = conn.execute("SELECT COUNT(*) AS n FROM monthly_plans").fetchone()["n"]

        first = retirement_targets.set_monthly_retirement_target(
            conn,
            tax_year="2026",
            account_type="Roth IRA",
            monthly_target_cents=100_000,
            start_month="2026-10",
            end_month="2026-12",
            room_remaining_cents=300_000,
            estimated_tax_savings_cents=75_000,
            reason="Use Q4 surplus before year-end.",
        )
        second = retirement_targets.set_monthly_retirement_target(
            conn,
            tax_year="2026",
            account_type="roth_ira",
            monthly_target_cents=125_000,
            start_month="2026-10",
            end_month="2026-12",
            room_remaining_cents=375_000,
            estimated_tax_savings_cents=90_000,
            reason="Increase after updated contribution room.",
        )
        target_rows = conn.execute(
            """
            SELECT id, account_type, monthly_target_cents, room_remaining_cents,
                   estimated_tax_savings_cents, reason
              FROM retirement_contribution_targets
            """
        ).fetchall()
        monthly_rows = conn.execute(
            """
            SELECT month, investment_target_cents
              FROM monthly_plans
             WHERE month BETWEEN '2026-10' AND '2026-12'
             ORDER BY month
            """
        ).fetchall()

    assert preview["summary"]["dry_run"] is True
    assert preview["summary"]["set"] == 0
    assert preview["data"]["target"]["payload"]["total_planned_cents"] == 300_000
    assert preview_count == 0
    assert preview_plans == 0
    assert first["summary"]["set"] == 1
    assert first["summary"]["monthly_plans_updated"] == 3
    assert first["data"]["target"]["payload"]["months_count"] == 3
    assert second["data"]["target"]["id"] == first["data"]["target"]["id"]
    assert second["data"]["target"]["payload"]["total_planned_cents"] == 375_000
    assert len(target_rows) == 1
    assert target_rows[0]["account_type"] == "roth_ira"
    assert target_rows[0]["monthly_target_cents"] == 125_000
    assert target_rows[0]["room_remaining_cents"] == 375_000
    assert target_rows[0]["estimated_tax_savings_cents"] == 90_000
    assert target_rows[0]["reason"] == "Increase after updated contribution room."
    assert [(row["month"], row["investment_target_cents"]) for row in monthly_rows] == [
        ("2026-10", 125_000),
        ("2026-11", 125_000),
        ("2026-12", 125_000),
    ]


def test_set_monthly_retirement_target_can_skip_monthly_plan_updates(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        result = retirement_targets.set_monthly_retirement_target(
            conn,
            tax_year="2026",
            account_type="sep_ira",
            monthly_target_cents=50_000,
            start_month="2026-10",
            end_month="2026-12",
            update_monthly_plans=False,
        )
        plan_count = conn.execute("SELECT COUNT(*) AS n FROM monthly_plans").fetchone()["n"]

    assert result["summary"]["monthly_plans_updated"] == 0
    assert result["data"]["target"]["payload"]["update_monthly_plans"] is False
    assert plan_count == 0


def test_setup_monthly_transfer_goal_reuses_retirement_target_state(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        preview = retirement_targets.setup_monthly_transfer_goal(
            conn,
            tax_year="2026",
            monthly_transfer_cents=100_000,
            room_remaining_cents=300_000,
            start_month="2026-10",
            end_month="2026-12",
            dry_run=True,
        )
        preview_count = conn.execute(
            "SELECT COUNT(*) AS n FROM retirement_contribution_targets"
        ).fetchone()["n"]

        result = retirement_targets.setup_monthly_transfer_goal(
            conn,
            tax_year="2026",
            monthly_transfer_cents=100_000,
            room_remaining_cents=300_000,
            start_month="2026-10",
            end_month="2026-12",
            estimated_tax_savings_cents=60_000,
        )
        target = conn.execute(
            """
            SELECT account_type, monthly_target_cents, room_remaining_cents, reason
              FROM retirement_contribution_targets
             LIMIT 1
            """
        ).fetchone()
        monthly_rows = conn.execute(
            """
            SELECT month, investment_target_cents
              FROM monthly_plans
             WHERE month BETWEEN '2026-10' AND '2026-12'
             ORDER BY month
            """
        ).fetchall()

    assert preview["summary"]["dry_run"] is True
    assert preview_count == 0
    assert result["summary"]["set"] == 1
    assert result["data"]["target"]["account_type"] == "roth_ira"
    assert result["data"]["target"]["estimated_tax_savings_cents"] == 60_000
    assert target["account_type"] == "roth_ira"
    assert target["monthly_target_cents"] == 100_000
    assert target["room_remaining_cents"] == 300_000
    assert target["reason"] == "Monthly retirement transfer goal."
    assert [(row["month"], row["investment_target_cents"]) for row in monthly_rows] == [
        ("2026-10", 100_000),
        ("2026-11", 100_000),
        ("2026-12", 100_000),
    ]


def test_set_monthly_retirement_target_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="account_type"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="brokerage",
                monthly_target_cents=100_000,
                start_month="2026-10",
                end_month="2026-12",
            )
        with pytest.raises(ValidationError, match="greater than 0"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=0,
                start_month="2026-10",
                end_month="2026-12",
            )
        with pytest.raises(ValidationError, match="YYYY-MM"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="10/2026",
                end_month="2026-12",
            )
        with pytest.raises(ValidationError, match="tax_year 2026"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="2027-10",
                end_month="2027-12",
            )
        with pytest.raises(ValidationError, match="greater than or equal"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="2026-12",
                end_month="2026-10",
            )
        with pytest.raises(ValidationError, match="greater than or equal to 0"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="2026-10",
                end_month="2026-12",
                room_remaining_cents=-1,
            )
        with pytest.raises(ValidationError, match="exceeds room_remaining_cents"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="2026-10",
                end_month="2026-12",
                room_remaining_cents=250_000,
            )
        with pytest.raises(ValidationError, match="plus room_remaining_cents"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=50_000,
                start_month="2026-10",
                end_month="2026-12",
                room_remaining_cents=300_000,
                annual_limit_cents=400_000,
                contributed_ytd_cents=150_000,
            )
        with pytest.raises(ValidationError, match="YYYY-MM-DD"):
            retirement_targets.set_monthly_retirement_target(
                conn,
                tax_year="2026",
                account_type="roth_ira",
                monthly_target_cents=100_000,
                start_month="2026-10",
                end_month="2026-12",
                deadline="12/31/2026",
            )


def test_monthly_retirement_target_tool_is_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS

    expected = {"set_monthly_retirement_target", "setup_monthly_transfer_goal"}
    assert expected <= gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert expected <= DB_WRITE_TOOLS
