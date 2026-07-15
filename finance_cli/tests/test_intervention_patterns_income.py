from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.interventions.income import (
    evaluate_i1_income_slowdown,
    evaluate_i2_income_concentration_risk,
    evaluate_i3_seasonal_income_alert,
    evaluate_i4_missed_billable_detection,
    evaluate_i5_pricing_signal,
)
from finance_cli.interventions.registry import Move, Priority


NOW = datetime(2026, 4, 9, 12, 0, 0)
SEASONAL_NOW = datetime(2026, 6, 15, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(conn, *, balance_current_cents: int = 100_000) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Bank', 'checking', 'checking', ?, 1)
        """,
        (account_id, balance_current_cents),
    )
    conn.commit()
    return account_id


def _category_id(conn, name: str, *, is_income: int = 1) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, is_income),
    )
    conn.commit()
    return category_id


def _seed_income(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    txn_date: str,
    category_name: str = "Income: Business",
    is_active: int = 1,
    is_payment: int = 0,
    use_type: str | None = None,
    project_id: str | None = None,
    notes: str | None = None,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            use_type, project_id, notes, is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'invoice', ?, ?, ?, ?, ?, ?, ?, 1, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            amount_cents,
            _category_id(conn, category_name),
            use_type,
            project_id,
            notes,
            is_payment,
            is_active,
        ),
    )
    conn.commit()
    return txn_id


def _seed_income_growth_interest(conn, *, strategy: str = "raise_rates") -> None:
    conn.execute(
        """
        INSERT INTO user_strategy_preferences (
            domain, strategy, rationale, source, evidence_json
        ) VALUES ('income', ?, 'User wants to grow income.', 'user', '{}')
        """,
        (strategy,),
    )
    conn.commit()


def _seed_project(conn, *, name: str = "Client A") -> str:
    project_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO projects (id, name, is_active)
        VALUES (?, ?, 1)
        """,
        (project_id, name),
    )
    conn.commit()
    return project_id


def _seed_business_expense(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    txn_date: str,
    category_name: str = "Travel",
    description: str = "client meeting",
    use_type: str = "Business",
    project_id: str | None = None,
    is_active: int = 1,
    is_payment: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id,
            use_type, project_id, is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            _category_id(conn, category_name, is_income=0),
            use_type,
            project_id,
            is_payment,
            is_active,
        ),
    )
    conn.commit()
    return txn_id


def _seed_contractor_payment(conn, *, transaction_id: str) -> None:
    contractor_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO contractors (id, name, entity_type, is_active)
        VALUES (?, 'Contractor LLC', 'llc', 1)
        """,
        (contractor_id,),
    )
    conn.execute(
        """
        INSERT INTO contractor_payments (id, contractor_id, transaction_id, tax_year)
        VALUES (?, ?, ?, 2026)
        """,
        (uuid.uuid4().hex, contractor_id, transaction_id),
    )
    conn.commit()


def test_i1_fires_on_month_over_month_drop(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date, amount in (
            ("2025-10-15", 100_000),
            ("2025-11-15", 100_000),
            ("2025-12-15", 100_000),
            ("2026-01-15", 100_000),
            ("2026-02-15", 100_000),
            ("2026-03-15", 70_000),
        ):
            _seed_income(conn, account_id=account_id, amount_cents=amount, txn_date=txn_date)

        intervention = evaluate_i1_income_slowdown(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-1"
    assert "30% drop" in intervention.headline


def test_i1_fires_on_three_month_drop(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date, amount in (
            ("2025-10-15", 200_000),
            ("2025-11-15", 200_000),
            ("2025-12-15", 200_000),
            ("2026-01-15", 120_000),
            ("2026-02-15", 120_000),
            ("2026-03-15", 120_000),
        ):
            _seed_income(conn, account_id=account_id, amount_cents=amount, txn_date=txn_date)

        intervention = evaluate_i1_income_slowdown(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-1"
    assert "40% drop" in intervention.headline


def test_i2_income_concentration_risk_fires_for_top_source_over_50_percent(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn, balance_current_cents=300_000)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=300_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=100_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-2"
    assert intervention.move is Move.DIAGNOSE
    assert intervention.priority is Priority.MEDIUM
    assert intervention.dollar_impact_cents == 900_000
    assert "75% of your income comes from Client A." in intervention.headline
    assert "roughly 4 weeks" in intervention.headline
    assert "Last 3 complete months income: $12,000.00." in intervention.detail_bullets
    assert "Client A income: $9,000.00 ($3,000.00/mo)." in intervention.detail_bullets
    assert "Current checking/savings buffer: $3,000.00." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "income_mix"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {"months": 3}


def test_i2_income_concentration_risk_does_not_round_partial_share_to_100_percent(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn, balance_current_cents=300_000)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=33_300,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=1,
            txn_date="2026-03-20",
            category_name="Client B",
        )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "99% of your income comes from Client A." in intervention.headline
    assert "100% of your income" not in intervention.headline


def test_i2_income_concentration_risk_fires_at_50_percent_threshold(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=250_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=150_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=100_000,
                txn_date=f"{month}-25",
                category_name="Client C",
            )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-2"
    assert "50% of your income comes from Client A." in intervention.headline


def test_i2_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn, balance_current_cents=300_000)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=300_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=100_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "I-2" for item in result.interventions)
    assert any(item.pattern_id == "I-2" for item in result.get_for_surface("action_queue"))


def test_i2_suppresses_when_no_source_reaches_50_percent(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=150_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=160_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=170_000,
                txn_date=f"{month}-25",
                category_name="Client C",
            )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i2_suppresses_when_three_complete_month_history_is_incomplete(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for month in ("2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=300_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=100_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i2_ignores_inactive_and_payment_income_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for month in ("2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=400_000,
                txn_date=f"{month}-10",
                category_name="Inactive Client",
                is_active=0,
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=400_000,
                txn_date=f"{month}-11",
                category_name="Payment Client",
                is_payment=1,
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=150_000,
                txn_date=f"{month}-15",
                category_name="Client A",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=160_000,
                txn_date=f"{month}-20",
                category_name="Client B",
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=170_000,
                txn_date=f"{month}-25",
                category_name="Client C",
            )

        intervention = evaluate_i2_income_concentration_risk(conn, build_context(conn, now=NOW))

    assert intervention is None


def _seed_august_slow_season_evidence(
    conn,
    *,
    account_id: str,
    baseline_cents: int = 500_000,
    slow_month_cents: int = 300_000,
) -> None:
    for year in (2024, 2025):
        for month in ("05", "06", "07"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=baseline_cents,
                txn_date=f"{year}-{month}-15",
                category_name="Income: Client Work",
            )
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=slow_month_cents,
            txn_date=f"{year}-08-15",
            category_name="Income: Client Work",
        )
    for month in ("03", "04", "05"):
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=600_000,
            txn_date=f"2026-{month}-15",
            category_name="Income: Client Work",
        )


def test_i3_seasonal_income_alert_fires_six_to_eight_weeks_before_slow_month(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_august_slow_season_evidence(conn, account_id=account_id)

        intervention = evaluate_i3_seasonal_income_alert(conn, build_context(conn, now=SEASONAL_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-3"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.MEDIUM
    assert intervention.dollar_impact_cents == 300_000
    assert (
        "Your August income is historically 40% lower. Slow season starts in 7 weeks - "
        "recommend banking $3,000.00 extra now."
    ) in intervention.headline
    assert "Historical pre-season baseline: $5,000.00/mo across 2 years." in intervention.detail_bullets
    assert "Historical August income: $3,000.00/mo." in intervention.detail_bullets
    assert "Recent 3-month average income: $6,000.00/mo." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "goal_set"
    assert intervention.action.params == {
        "name": "August slow-season buffer",
        "metric": "liquid_cash",
        "target": 3000.0,
        "direction": "up",
        "deadline": "2026-07-31",
    }


def test_i3_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_august_slow_season_evidence(conn, account_id=account_id)

        result = run_engine(conn, now=SEASONAL_NOW)

    assert any(item.pattern_id == "I-3" for item in result.interventions)
    assert any(item.pattern_id == "I-3" for item in result.get_for_surface("action_queue"))


def test_i3_suppresses_when_slow_season_is_outside_lead_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_august_slow_season_evidence(conn, account_id=account_id)

        intervention = evaluate_i3_seasonal_income_alert(
            conn,
            build_context(conn, now=datetime(2026, 7, 10, 12, 0, 0)),
        )

    assert intervention is None


def test_i3_suppresses_without_two_historical_years(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for month in ("05", "06", "07"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=500_000,
                txn_date=f"2025-{month}-15",
                category_name="Income: Client Work",
            )
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=300_000,
            txn_date="2025-08-15",
            category_name="Income: Client Work",
        )
        for month in ("03", "04", "05"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=600_000,
                txn_date=f"2026-{month}-15",
                category_name="Income: Client Work",
            )

        intervention = evaluate_i3_seasonal_income_alert(conn, build_context(conn, now=SEASONAL_NOW))

    assert intervention is None


def test_i3_suppresses_when_historical_dip_is_below_twenty_percent(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_august_slow_season_evidence(
            conn,
            account_id=account_id,
            baseline_cents=500_000,
            slow_month_cents=425_000,
        )

        intervention = evaluate_i3_seasonal_income_alert(conn, build_context(conn, now=SEASONAL_NOW))

    assert intervention is None


def test_i3_ignores_inactive_and_payment_income_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_august_slow_season_evidence(conn, account_id=account_id)
        for year in (2024, 2025):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=500_000,
                txn_date=f"{year}-08-20",
                category_name="Inactive August Income",
                is_active=0,
            )
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=500_000,
                txn_date=f"{year}-08-21",
                category_name="Payment August Income",
                is_payment=1,
            )

        intervention = evaluate_i3_seasonal_income_alert(conn, build_context(conn, now=SEASONAL_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-3"
    assert "historically 40% lower" in intervention.headline


def test_i4_missed_billable_detection_fires_for_business_travel_cluster(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        txn_ids = [
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-12_000,
                txn_date="2026-03-10",
                category_name="Travel",
            ),
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-18_000,
                txn_date="2026-03-11",
                category_name="Travel",
            ),
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-20_000,
                txn_date="2026-03-12",
                category_name="Travel",
            ),
        ]
        _seed_business_expense(
            conn,
            account_id=account_id,
            amount_cents=-70_000,
            txn_date="2026-03-13",
            category_name="Travel",
            use_type="Personal",
        )
        _seed_business_expense(
            conn,
            account_id=account_id,
            amount_cents=-80_000,
            txn_date="2026-03-14",
            category_name="Travel",
            project_id=_seed_project(conn),
        )

        intervention = evaluate_i4_missed_billable_detection(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-4"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.tiers == (1, 2)
    assert intervention.priority is Priority.MEDIUM
    assert intervention.headline == (
        "3 business travel expenses have no invoice/project link. "
        "$500.00 worth. Worth checking if these were billable?"
    )
    assert intervention.dollar_impact_cents == 50_000
    assert "Categories: Travel." in intervention.detail_bullets
    assert "Window: 2026-03-10 through 2026-03-12." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.label == "Preview billable project tags"
    assert intervention.action.tool == "bulk_tag_billable_expenses"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {
        "ids": txn_ids,
        "project": "Billable Review",
        "overwrite_existing_project": False,
        "dry_run": True,
    }


def test_i4_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for day in (10, 11, 12):
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-15_000,
                txn_date=f"2026-03-{day}",
                category_name="Client Meals",
            )

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "I-4" for item in result.interventions)
    assert any(item.pattern_id == "I-4" for item in result.get_for_surface("action_queue"))


def test_i4_suppresses_when_billable_cluster_is_too_small(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for day in (10, 11):
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-20_000,
                txn_date=f"2026-03-{day}",
                category_name="Travel",
            )

        intervention = evaluate_i4_missed_billable_detection(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i4_suppresses_already_project_linked_expenses(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        project_id = _seed_project(conn)
        for day in (10, 11, 12):
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-20_000,
                txn_date=f"2026-03-{day}",
                category_name="Travel",
                project_id=project_id,
            )

        intervention = evaluate_i4_missed_billable_detection(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i4_suppresses_contractor_payment_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_expense(
            conn,
            account_id=account_id,
            amount_cents=-20_000,
            txn_date="2026-03-10",
            category_name="Software",
        )
        _seed_business_expense(
            conn,
            account_id=account_id,
            amount_cents=-20_000,
            txn_date="2026-03-11",
            category_name="Software",
        )
        contractor_txn_id = _seed_business_expense(
            conn,
            account_id=account_id,
            amount_cents=-20_000,
            txn_date="2026-03-12",
            category_name="Software",
        )
        _seed_contractor_payment(conn, transaction_id=contractor_txn_id)

        intervention = evaluate_i4_missed_billable_detection(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i4_picks_largest_plausibly_billable_cluster(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for day in (10, 11, 12):
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-10_000,
                txn_date=f"2026-03-{day}",
                category_name="Travel",
            )
        software_ids = [
            _seed_business_expense(
                conn,
                account_id=account_id,
                amount_cents=-30_000,
                txn_date=f"2026-03-{day}",
                category_name="Software",
            )
            for day in (13, 14, 15)
        ]

        intervention = evaluate_i4_missed_billable_detection(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-4"
    assert "3 business software expenses" in intervention.headline
    assert intervention.dollar_impact_cents == 90_000
    assert intervention.action is not None
    assert intervention.action.params["ids"] == software_ids


def _seed_tracked_income_months(
    conn,
    *,
    account_id: str,
    project_id: str | None = None,
    months: tuple[str, ...] = ("2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03"),
    amount_cents: int = 120_000,
    hours: int = 10,
) -> None:
    for month in months:
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=amount_cents,
            txn_date=f"{month}-15",
            category_name="Income: Project Work",
            project_id=project_id,
            notes=json.dumps({"tracked_hours": hours}),
        )


def test_i5_pricing_signal_fires_for_stable_tracked_rate_with_growth_interest(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        project_id = _seed_project(conn, name="Client A")
        _seed_income_growth_interest(conn)
        _seed_tracked_income_months(conn, account_id=account_id, project_id=project_id)

        intervention = evaluate_i5_pricing_signal(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "I-5"
    assert intervention.move is Move.COMPARE
    assert intervention.tiers == (1,)
    assert intervention.priority is Priority.LOW
    assert intervention.headline == (
        "Your effective rate works out to $120/hr based on tracked time. "
        "You haven't raised it in 6 months. Worth testing a 10% bump on the next project?"
    )
    assert intervention.detail_bullets == (
        "Income stream: Client A.",
        "Tracked sample: 60 hours and $7,200.00 over 6 complete months.",
        "Monthly effective rate range: $120/hr to $120/hr.",
        "A 10% test rate would be about $132/hr.",
    )
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 144_000


def test_i5_runs_through_engine_but_not_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_income_growth_interest(conn)
        _seed_tracked_income_months(conn, account_id=account_id)

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "I-5" for item in result.interventions)
    assert all(item.pattern_id != "I-5" for item in result.get_for_surface("action_queue"))


def test_i5_suppresses_without_income_growth_interest(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_tracked_income_months(conn, account_id=account_id)

        intervention = evaluate_i5_pricing_signal(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i5_suppresses_without_tracked_hours_metadata(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_income_growth_interest(conn)
        for month in ("2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03"):
            _seed_income(
                conn,
                account_id=account_id,
                amount_cents=120_000,
                txn_date=f"{month}-15",
                category_name="Income: Project Work",
            )

        intervention = evaluate_i5_pricing_signal(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i5_suppresses_when_six_complete_tracked_months_are_missing(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_income_growth_interest(conn)
        _seed_tracked_income_months(
            conn,
            account_id=account_id,
            months=("2025-11", "2025-12", "2026-01", "2026-02", "2026-03"),
        )

        intervention = evaluate_i5_pricing_signal(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_i5_suppresses_when_effective_rate_changed(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_income_growth_interest(conn)
        _seed_tracked_income_months(
            conn,
            account_id=account_id,
            months=("2025-10", "2025-11", "2025-12", "2026-01", "2026-02"),
        )
        _seed_income(
            conn,
            account_id=account_id,
            amount_cents=180_000,
            txn_date="2026-03-15",
            category_name="Income: Project Work",
            notes=json.dumps({"tracked_hours": 10}),
        )

        intervention = evaluate_i5_pricing_signal(conn, build_context(conn, now=NOW))

    assert intervention is None
