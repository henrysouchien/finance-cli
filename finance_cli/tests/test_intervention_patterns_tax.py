from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move, Priority
from finance_cli.interventions.tax import (
    evaluate_t1_quarterly_estimated_tax_warning,
    evaluate_t2_untagged_business_deductions,
    evaluate_t3_mileage_gap_warning,
    evaluate_t4_home_office_deduction_unclaimed,
    evaluate_t5_1099_contractor_threshold_warning,
    evaluate_t6_end_of_year_tax_acceleration,
    evaluate_t7_business_deduction_streak,
)


NOW = datetime(2026, 4, 9, 12, 0, 0)
T1_NOW = datetime(2026, 6, 1, 12, 0, 0)
T3_NOW = datetime(2026, 8, 15, 12, 0, 0)
T6_NOW = datetime(2026, 11, 15, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(conn) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type, balance_current_cents, is_active
        ) VALUES (?, 'Bank', 'checking', 'checking', 100000, 1)
        """,
        (account_id,),
    )
    conn.commit()
    return account_id


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, 0, 0, 0)
        """,
        (category_id, name),
    )
    conn.execute(
        """
        INSERT INTO schedule_c_map (
            id, category_id, schedule_c_line, line_number, deduction_pct, tax_year
        ) VALUES (?, ?, 'Other expenses', '27a', 1.0, 2026)
        """,
        (uuid.uuid4().hex, category_id),
    )
    conn.commit()
    return category_id


def _income_category_id(conn, name: str = "Income: Business") -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, 1, 0, 0)
        """,
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_transaction(
    conn,
    *,
    account_id: str,
    category_name: str,
    amount_cents: int,
    txn_date: str,
    use_type: str | None,
    description: str = "seed",
    is_reviewed: int = 1,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, use_type,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1, ?, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            _category_id(conn, category_name),
            use_type,
            is_reviewed,
        ),
    )
    conn.commit()
    return txn_id


def _seed_business_income(
    conn,
    *,
    account_id: str,
    amount_cents: int = 200_000,
    txn_date: str = "2026-02-15",
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, category_id, use_type,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, 'client payment', ?, ?, 'Business', 0, 1, 1, 'manual')
        """,
        (txn_id, account_id, txn_date, amount_cents, _income_category_id(conn)),
    )
    conn.commit()
    return txn_id


def _seed_housing_expense(
    conn,
    *,
    account_id: str,
    amount_cents: int = -200_000,
    txn_date: str = "2026-02-01",
    category_name: str = "Rent",
    use_type: str | None = "Personal",
) -> str:
    return _seed_transaction(
        conn,
        account_id=account_id,
        category_name=category_name,
        amount_cents=amount_cents,
        txn_date=txn_date,
        use_type=use_type,
    )


def _seed_business_expense(
    conn,
    *,
    account_id: str,
    amount_cents: int = -200_000,
    txn_date: str = "2026-02-15",
    category_name: str = "Software & Subscriptions",
) -> str:
    return _seed_transaction(
        conn,
        account_id=account_id,
        category_name=category_name,
        amount_cents=amount_cents,
        txn_date=txn_date,
        use_type="Business",
    )


def _seed_estimated_tax_payment(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    txn_date: str = "2026-05-20",
    description: str = "IRS Direct Pay 1040-ES",
) -> str:
    return _seed_transaction(
        conn,
        account_id=account_id,
        category_name="Taxes",
        amount_cents=amount_cents,
        txn_date=txn_date,
        use_type="Personal",
        description=description,
    )


def _seed_mileage(conn, *, trip_date: str, miles: float = 600.0) -> str:
    mileage_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO mileage_log (
            id, trip_date, miles, destination, business_purpose,
            vehicle_name, tax_year, round_trip
        ) VALUES (?, ?, ?, 'Client site', 'Client meeting', 'primary', ?, 0)
        """,
        (mileage_id, trip_date, miles, int(trip_date[:4])),
    )
    conn.commit()
    return mileage_id


def _seed_prior_year_quarterly_mileage(
    conn,
    *,
    q1: float = 600.0,
    q2: float = 600.0,
    q3: float = 600.0,
    q4: float = 600.0,
) -> None:
    for trip_date, miles in (
        ("2025-02-15", q1),
        ("2025-05-15", q2),
        ("2025-08-15", q3),
        ("2025-11-15", q4),
    ):
        _seed_mileage(conn, trip_date=trip_date, miles=miles)


def _set_tax_config(conn, *, year: int, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO tax_config (tax_year, config_key, config_value)
        VALUES (?, ?, ?)
        """,
        (year, key, value),
    )
    conn.commit()


def _seed_contractor(
    conn,
    *,
    name: str = "Alex Writer",
    entity_type: str = "individual",
    tin_last4: str | None = None,
    is_active: int = 1,
) -> str:
    contractor_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO contractors (id, name, tin_last4, entity_type, is_active)
        VALUES (?, ?, ?, ?, ?)
        """,
        (contractor_id, name, tin_last4, entity_type, is_active),
    )
    conn.commit()
    return contractor_id


def _seed_contractor_payment(
    conn,
    *,
    contractor_id: str,
    account_id: str,
    amount_cents: int,
    txn_date: str,
    paid_via_card: bool = False,
) -> str:
    txn_id = _seed_transaction(
        conn,
        account_id=account_id,
        category_name="1099 Contract Labor",
        amount_cents=amount_cents,
        txn_date=txn_date,
        use_type="Business",
    )
    conn.execute(
        """
        INSERT INTO contractor_payments (
            id, contractor_id, transaction_id, tax_year, paid_via_card
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            uuid.uuid4().hex,
            contractor_id,
            txn_id,
            int(txn_date[:4]),
            1 if paid_via_card else 0,
        ),
    )
    conn.commit()
    return txn_id


def _seed_active_contractor_prep_flag(conn, *, contractor_id: str, tax_year: int = 2026) -> None:
    conn.execute(
        """
        INSERT INTO contractor_tax_prep_flags (
            id, contractor_id, tax_year, flag_type, status, reason,
            source, payment_snapshot_json
        ) VALUES (?, ?, ?, 'january_1099_prep', 'active', 'Already flagged', 'agent', '{}')
        """,
        (uuid.uuid4().hex, contractor_id, tax_year),
    )
    conn.commit()


def _seed_retirement_target(
    conn,
    *,
    account_type: str = "sep_ira",
    tax_year: int = 2026,
    status: str = "active",
    contributed_ytd_cents: int | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO retirement_contribution_targets (
            id, tax_year, account_type, status, monthly_target_cents,
            start_month, end_month, room_remaining_cents, annual_limit_cents,
            contributed_ytd_cents, estimated_tax_savings_cents, deadline,
            reason, source, payload_json, idempotency_key
        ) VALUES (?, ?, ?, ?, 100000, '2026-10', '2026-12', 300000,
                  7200000, ?, 90000, '2026-12-31',
                  'Seed target', 'agent', '{}', ?)
        """,
        (
            uuid.uuid4().hex,
            tax_year,
            account_type,
            status,
            contributed_ytd_cents,
            f"retirement_target:{tax_year}:{account_type}:{status}:{uuid.uuid4().hex}",
        ),
    )
    conn.commit()


def test_t1_quarterly_estimated_tax_warning_fires_with_due_date_and_gap(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)
        _seed_business_expense(conn, account_id=account_id, amount_cents=-200_000)

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-1"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.HIGH
    assert intervention.dollar_impact_cents == 120_000
    assert "2026-Q2 estimated tax due in 14 days" in intervention.headline
    assert "~$1,200.00 based on YTD business profit" in intervention.headline
    assert "Set aside $1,200.00 now" in intervention.headline
    assert "Due date: 2026-06-15." in intervention.detail_bullets
    assert "YTD Schedule C net profit through 2026-06-01: $8,000.00." in intervention.detail_bullets
    assert "Annualized profit estimate: $16,000.00." in intervention.detail_bullets
    assert "Tax-like payments observed since 2026-04-16: $0.00." in intervention.detail_bullets
    assert "Estimate method: 30% default rate." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "biz_estimated_tax"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {
        "est_quarter": "2026-Q2",
        "include_se": True,
        "rate": 0.3,
    }


def test_t1_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)

        result = run_engine(conn, now=T1_NOW)

    assert any(item.pattern_id == "T-1" for item in result.interventions)
    assert any(item.pattern_id == "T-1" for item in result.get_for_surface("action_queue"))


def test_t1_suppresses_when_due_date_is_not_within_30_days(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)

        intervention = evaluate_t1_quarterly_estimated_tax_warning(
            conn,
            build_context(conn, now=datetime(2026, 5, 1, 12, 0, 0)),
        )

    assert intervention is None


def test_t1_january_window_targets_prior_tax_year_q4(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2025-12-15",
        )

        intervention = evaluate_t1_quarterly_estimated_tax_warning(
            conn,
            build_context(conn, now=datetime(2026, 1, 5, 12, 0, 0)),
        )

    assert intervention is not None
    assert intervention.pattern_id == "T-1"
    assert "2025-Q4 estimated tax due in 10 days" in intervention.headline
    assert intervention.dollar_impact_cents == 75_000
    assert intervention.action is not None
    assert intervention.action.params["est_quarter"] == "2025-Q4"


def test_t1_suppresses_without_positive_business_profit(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_expense(conn, account_id=account_id, amount_cents=-200_000)

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is None


def test_t1_does_not_treat_property_tax_as_estimated_tax_payment(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)
        _seed_business_expense(conn, account_id=account_id, amount_cents=-200_000)
        _seed_estimated_tax_payment(
            conn,
            account_id=account_id,
            amount_cents=-120_000,
            description="County property tax payment",
        )

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 120_000
    assert "Tax-like payments observed since 2026-04-16: $0.00." in intervention.detail_bullets


def test_t1_suppresses_when_current_payment_window_is_fully_paid(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)
        _seed_business_expense(conn, account_id=account_id, amount_cents=-200_000)
        _seed_estimated_tax_payment(conn, account_id=account_id, amount_cents=-120_000)

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is None


def test_t1_warns_on_remaining_gap_after_partial_tax_payment(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)
        _seed_business_expense(conn, account_id=account_id, amount_cents=-200_000)
        _seed_estimated_tax_payment(conn, account_id=account_id, amount_cents=-40_000)

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 80_000
    assert "Set aside $800.00 now" in intervention.headline
    assert "Tax-like payments observed since 2026-04-16: $400.00." in intervention.detail_bullets


def test_t1_ignores_future_dated_business_income(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000, txn_date="2026-06-10")

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is None


def test_t1_uses_configured_tax_rate_for_estimate_and_action(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=1_000_000)
        _set_tax_config(conn, year=2026, key="estimated_tax_rate", value="0.24")

        intervention = evaluate_t1_quarterly_estimated_tax_warning(conn, build_context(conn, now=T1_NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 120_000
    assert "~$1,200.00 based on YTD business profit" in intervention.headline
    assert "Estimate method: 24% configured rate." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.params["rate"] == 0.24


def test_t2_fires_for_null_and_personal_use_type_matches(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_transaction(conn, account_id=account_id, category_name="Software & Subscriptions", amount_cents=-12_000, txn_date="2026-01-10", use_type=None)
        _seed_transaction(conn, account_id=account_id, category_name="Software & Subscriptions", amount_cents=-8_000, txn_date="2026-03-10", use_type="Personal")

        intervention = evaluate_t2_untagged_business_deductions(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-2"
    assert intervention.dollar_impact_cents == 20_000


def test_t2_does_not_fire_for_business_use_type(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_transaction(conn, account_id=account_id, category_name="Software & Subscriptions", amount_cents=-12_000, txn_date="2026-01-10", use_type="Business")

        intervention = evaluate_t2_untagged_business_deductions(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t3_mileage_gap_warning_fires_for_zero_current_quarter_miles(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)

        intervention = evaluate_t3_mileage_gap_warning(conn, build_context(conn, now=T3_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-3"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.MEDIUM
    assert intervention.dollar_impact_cents == 12_600
    assert "You logged 0 miles this quarter" in intervention.headline
    assert "averaged 600 mi/quarter" in intervention.headline
    assert "$420.00 in deductions = $126.00 kept" in intervention.headline
    assert "Current quarter checked: 2026-Q3 through 2026-08-15." in intervention.detail_bullets
    assert "2025 quarterly mileage: Q1: 600 mi, Q2: 600 mi, Q3: 600 mi, Q4: 600 mi." in intervention.detail_bullets
    assert "Mileage rate: 70 cents/mi; tax-rate assumption: 30%." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "biz_mileage_add"
    assert intervention.action.build_stub is True
    assert intervention.action.params == {
        "date": "2026-08-15",
        "vehicle": "primary",
        "round_trip": False,
        "notes": "Prompted by T-3 mileage gap warning for 2026-Q3.",
    }


def test_t3_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)

        result = run_engine(conn, now=T3_NOW)

    assert any(item.pattern_id == "T-3" for item in result.interventions)
    assert any(item.pattern_id == "T-3" for item in result.get_for_surface("action_queue"))


def test_t3_suppresses_when_current_quarter_has_mileage(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)
        _seed_mileage(conn, trip_date="2026-07-20", miles=25.0)

        intervention = evaluate_t3_mileage_gap_warning(conn, build_context(conn, now=T3_NOW))

    assert intervention is None


def test_t3_requires_each_prior_year_quarter_to_have_500_miles(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn, q2=499.9)

        intervention = evaluate_t3_mileage_gap_warning(conn, build_context(conn, now=T3_NOW))

    assert intervention is None


def test_t3_ignores_early_quarter_zero_mileage_noise(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)

        intervention = evaluate_t3_mileage_gap_warning(
            conn,
            build_context(conn, now=datetime(2026, 7, 10, 12, 0, 0)),
        )

    assert intervention is None


def test_t3_suppresses_when_mileage_tracking_is_disabled(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)
        _set_tax_config(conn, year=2026, key="mileage_tracking", value="disabled")

        intervention = evaluate_t3_mileage_gap_warning(conn, build_context(conn, now=T3_NOW))

    assert intervention is None


def test_t3_uses_configured_tax_rate_for_tax_kept_math(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_prior_year_quarterly_mileage(conn)
        _set_tax_config(conn, year=2026, key="estimated_tax_rate", value="0.24")

        intervention = evaluate_t3_mileage_gap_warning(conn, build_context(conn, now=T3_NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 10_080
    assert "$420.00 in deductions = $100.80 kept" in intervention.headline
    assert "Mileage rate: 70 cents/mi; tax-rate assumption: 24%." in intervention.detail_bullets


def test_t4_home_office_deduction_unclaimed_fires_for_business_income_and_rent(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=300_000)
        for txn_date in ("2026-01-01", "2026-02-01", "2026-03-01"):
            _seed_housing_expense(conn, account_id=account_id, txn_date=txn_date)

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-4"
    assert intervention.move is Move.DIAGNOSE
    assert intervention.priority is Priority.MEDIUM
    assert intervention.dollar_impact_cents == 22_500
    assert "Home office isn't claimed." in intervention.headline
    assert "($2,000.00/mo)" in intervention.headline
    assert "~$750.00/yr you're not deducting = $225.00 kept" in intervention.headline
    assert "Business income observed: $3,000.00." in intervention.detail_bullets
    assert (
        "Personal rent/mortgage average: $2,000.00/mo across 3 observed months."
        in intervention.detail_bullets
    )
    assert "Confirm dedicated office square footage before saving home-office tax config." in (
        intervention.detail_bullets
    )
    assert intervention.action is not None
    assert intervention.action.tool == "setup_home_office_tracking"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {
        "year": "2026",
        "sqft": 150,
        "method": "simplified",
        "dry_run": True,
    }


def test_t4_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id)
        _seed_housing_expense(conn, account_id=account_id)

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "T-4" for item in result.interventions)
    assert any(item.pattern_id == "T-4" for item in result.get_for_surface("action_queue"))


def test_t4_suppresses_without_business_income(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_housing_expense(conn, account_id=account_id)

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t4_suppresses_without_personal_housing_spend(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id)

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t4_ignores_future_dated_income_and_housing(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, txn_date="2026-12-15")
        _seed_housing_expense(conn, account_id=account_id, txn_date="2026-12-01")

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t4_suppresses_when_home_office_is_already_configured(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id)
        _seed_housing_expense(conn, account_id=account_id)
        _set_tax_config(conn, year=2026, key="home_office_method", value="simplified")

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t4_suppresses_rent_or_utilities_split_rule_conflicts(
    db_path: Path,
    tmp_path: Path,
) -> None:
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        """
        split_rules:
          - match:
              category: Rent
            business_pct: 25
            business_category: Rent
            personal_category: Rent
        """,
        encoding="utf-8",
    )
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id)
        _seed_housing_expense(conn, account_id=account_id)

        intervention = evaluate_t4_home_office_deduction_unclaimed(
            conn,
            build_context(conn, now=NOW, rules_path=rules_path),
        )

    assert intervention is None


def test_t4_caps_preview_deduction_by_business_income_and_uses_configured_tax_rate(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(conn, account_id=account_id, amount_cents=50_000)
        _seed_housing_expense(conn, account_id=account_id)
        _set_tax_config(conn, year=2026, key="estimated_tax_rate", value="0.24")

        intervention = evaluate_t4_home_office_deduction_unclaimed(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 12_000
    assert "~$500.00/yr you're not deducting = $120.00 kept" in intervention.headline
    assert (
        "Preview uses 150 sqft at $5.00/sqft, capped at 300 sqft and by business income; "
        "tax-rate assumption: 24%."
    ) in intervention.detail_bullets


def test_t5_1099_contractor_threshold_warning_fires_for_approaching_threshold(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Alex Writer")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-55_000,
            txn_date="2026-03-01",
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-5"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.HIGH
    assert intervention.dollar_impact_cents == 55_000
    assert "You've paid Alex Writer $550.00 this year." in intervention.headline
    assert "One more invoice crosses $600" in intervention.headline
    assert "Worth collecting their W-9 now if you don't have it." in intervention.headline
    assert "Current tax year: 2026." in intervention.detail_bullets
    assert "Non-card contractor payments: $550.00 across 1 linked transactions." in intervention.detail_bullets
    assert "Card/processor payments excluded from 1099-NEC threshold: $0.00." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "flag_contractor_january_prep"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {
        "contractor_id": contractor_id,
        "tax_year": "2026",
        "reason": "Contractor is approaching the non-card 1099-NEC reporting threshold.",
        "source": "agent",
        "dry_run": False,
    }


def test_t5_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Alex Writer")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-55_000,
            txn_date="2026-03-01",
        )

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "T-5" for item in result.interventions)
    assert any(item.pattern_id == "T-5" for item in result.get_for_surface("action_queue"))


def test_t5_uses_crossed_threshold_copy_and_tin_on_file_copy(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Jordan Dev", tin_last4="1234")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-70_000,
            txn_date="2026-03-01",
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "You've paid Jordan Dev $700.00 this year." in intervention.headline
    assert "That crosses the $600 1099-NEC threshold" in intervention.headline
    assert "TIN is already on file; flag this for January prep now." in intervention.headline
    assert intervention.action is not None
    assert intervention.action.params["reason"] == (
        "Contractor has crossed the non-card 1099-NEC reporting threshold."
    )


def test_t5_excludes_card_paid_amounts_from_threshold(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Card Processor Payee")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-100_000,
            txn_date="2026-03-01",
            paid_via_card=True,
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t5_counts_only_non_card_linked_transactions(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Mixed Channel")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-80_000,
            txn_date="2026-03-01",
            paid_via_card=True,
        )
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-55_000,
            txn_date="2026-03-02",
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "Non-card contractor payments: $550.00 across 1 linked transactions." in intervention.detail_bullets
    assert "Card/processor payments excluded from 1099-NEC threshold: $800.00." in intervention.detail_bullets


def test_t5_suppresses_corporation_contractors(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Corp Vendor", entity_type="corporation")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-100_000,
            txn_date="2026-03-01",
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t5_suppresses_contractors_with_active_january_prep_flag(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        contractor_id = _seed_contractor(conn, name="Already Flagged")
        _seed_contractor_payment(
            conn,
            contractor_id=contractor_id,
            account_id=account_id,
            amount_cents=-55_000,
            txn_date="2026-03-01",
        )
        _seed_active_contractor_prep_flag(conn, contractor_id=contractor_id)

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t5_chooses_highest_non_card_contractor(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        smaller_id = _seed_contractor(conn, name="Smaller Vendor")
        larger_id = _seed_contractor(conn, name="Larger Vendor")
        _seed_contractor_payment(
            conn,
            contractor_id=smaller_id,
            account_id=account_id,
            amount_cents=-55_000,
            txn_date="2026-03-01",
        )
        _seed_contractor_payment(
            conn,
            contractor_id=larger_id,
            account_id=account_id,
            amount_cents=-59_000,
            txn_date="2026-03-02",
        )

        intervention = evaluate_t5_1099_contractor_threshold_warning(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "You've paid Larger Vendor $590.00 this year." in intervention.headline
    assert intervention.action is not None
    assert intervention.action.params["contractor_id"] == larger_id


def test_t6_end_of_year_tax_acceleration_sets_sep_target_for_q4_profit(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-11-01",
        )

        intervention = evaluate_t6_end_of_year_tax_acceleration(conn, build_context(conn, now=T6_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-6"
    assert intervention.move is Move.PRESCRIBE
    assert intervention.tiers == (2,)
    assert intervention.priority is Priority.MEDIUM
    assert intervention.headline == (
        "Year-end tax move: you have $7,200.00 of SEP IRA room left. "
        "Maxing it before Dec 31 saves you $2,160.00 this year."
    )
    assert intervention.dollar_impact_cents == 216_000
    assert "YTD Schedule C net profit: $40,000.00." in intervention.detail_bullets
    assert (
        "Q4 net profit through 2026-11-15: $30,000.00 (75% of YTD profit)."
        in intervention.detail_bullets
    )
    assert "Known SEP contributions this year: $0.00." in intervention.detail_bullets
    assert "Tax-rate assumption: 30%." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "set_monthly_retirement_target"
    assert intervention.action.build_stub is False
    assert intervention.action.params == {
        "tax_year": "2026",
        "account_type": "sep_ira",
        "monthly_target_cents": 360_000,
        "start_month": "2026-11",
        "end_month": "2026-12",
        "room_remaining_cents": 720_000,
        "annual_limit_cents": 720_000,
        "contributed_ytd_cents": 0,
        "estimated_tax_savings_cents": 216_000,
        "deadline": "2026-12-31",
        "reason": "Q4 Schedule C profit creates year-end SEP IRA deduction room.",
        "update_monthly_plans": True,
        "dry_run": False,
    }


def test_t6_runs_through_engine_and_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-11-01",
        )

        result = run_engine(conn, now=T6_NOW)

    assert any(item.pattern_id == "T-6" for item in result.interventions)
    assert any(item.pattern_id == "T-6" for item in result.get_for_surface("action_queue"))


def test_t6_suppresses_outside_q4(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=4_000_000,
            txn_date="2026-09-15",
        )

        intervention = evaluate_t6_end_of_year_tax_acceleration(
            conn,
            build_context(conn, now=datetime(2026, 9, 30, 12, 0, 0)),
        )

    assert intervention is None


def test_t6_requires_q4_income_concentration(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-11-01",
        )

        intervention = evaluate_t6_end_of_year_tax_acceleration(conn, build_context(conn, now=T6_NOW))

    assert intervention is None


def test_t6_suppresses_when_active_sep_target_exists(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-11-01",
        )
        _seed_retirement_target(conn, account_type="sep_ira", status="active")

        intervention = evaluate_t6_end_of_year_tax_acceleration(conn, build_context(conn, now=T6_NOW))

    assert intervention is None


def test_t6_reduces_room_by_known_resolved_sep_contributions(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-11-01",
        )
        _seed_retirement_target(
            conn,
            account_type="sep_ira",
            status="resolved",
            contributed_ytd_cents=200_000,
        )

        intervention = evaluate_t6_end_of_year_tax_acceleration(conn, build_context(conn, now=T6_NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 156_000
    assert "Known SEP contributions this year: $2,000.00." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.params["room_remaining_cents"] == 520_000
    assert intervention.action.params["monthly_target_cents"] == 260_000


def test_t6_suppresses_when_sep_room_is_already_used(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=1_000_000,
            txn_date="2026-02-15",
        )
        _seed_business_income(
            conn,
            account_id=account_id,
            amount_cents=3_000_000,
            txn_date="2026-11-01",
        )
        _seed_retirement_target(
            conn,
            account_type="sep_ira",
            status="resolved",
            contributed_ytd_cents=720_000,
        )

        intervention = evaluate_t6_end_of_year_tax_acceleration(conn, build_context(conn, now=T6_NOW))

    assert intervention is None


def test_t7_business_deduction_streak_reinforces_reviewed_schedule_c_expenses(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        _set_tax_config(conn, year=2026, key="estimated_tax_rate", value="0.24")
        for txn_date, amount_cents in (
            ("2026-04-04", -10_000),
            ("2026-04-06", -20_000),
            ("2026-04-09", -30_000),
        ):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=amount_cents,
                txn_date=txn_date,
                use_type="Business",
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "T-7"
    assert intervention.move is Move.COACH
    assert intervention.tiers == (2,)
    assert intervention.priority is Priority.LOW
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 14_400
    assert intervention.headline == (
        "3 reviewed Schedule C expenses this week. That's $600.00 documented for 2026, "
        "about $144.00 kept at your tax-rate assumption."
    )
    assert "Window checked: 2026-04-03 through 2026-04-09." in intervention.detail_bullets
    assert (
        "Top reviewed category: Software & Subscriptions ($600.00 deductible)."
        in intervention.detail_bullets
    )
    assert "Tax-rate assumption: 24%." in intervention.detail_bullets


def test_t7_runs_through_engine_without_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2026-04-04", "2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Professional Fees",
                amount_cents=-10_000,
                txn_date=txn_date,
                use_type="Business",
            )

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "T-7" for item in result.interventions)
    assert all(item.pattern_id != "T-7" for item in result.get_for_surface("action_queue"))


def test_t7_requires_reviewed_business_expenses(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2026-04-04", "2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=-10_000,
                txn_date=txn_date,
                use_type="Business",
                is_reviewed=0,
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t7_requires_business_use_type(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2026-04-04", "2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=-10_000,
                txn_date=txn_date,
                use_type="Personal",
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t7_requires_three_recent_reviewed_expenses(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=-20_000,
                txn_date=txn_date,
                use_type="Business",
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t7_does_not_count_prior_tax_year_inside_lookback_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2025-12-30", "2026-01-01", "2026-01-03"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=-20_000,
                txn_date=txn_date,
                use_type="Business",
            )

        intervention = evaluate_t7_business_deduction_streak(
            conn,
            build_context(conn, now=datetime(2026, 1, 3, 12, 0, 0)),
        )

    assert intervention is None


def test_t7_requires_meaningful_deduction_amount(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        for txn_date in ("2026-04-04", "2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Software & Subscriptions",
                amount_cents=-4_000,
                txn_date=txn_date,
                use_type="Business",
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_t7_uses_schedule_c_deduction_percentage(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_account(conn)
        dining_id = _category_id(conn, "Dining")
        conn.execute(
            """
            INSERT INTO schedule_c_map (
                id, category_id, schedule_c_line, line_number, deduction_pct, tax_year
            ) VALUES (?, ?, 'Deductible meals', '24b', 0.5, 2026)
            ON CONFLICT(category_id, tax_year) DO UPDATE
                SET deduction_pct = excluded.deduction_pct
            """,
            (uuid.uuid4().hex, dining_id),
        )
        conn.commit()
        for txn_date in ("2026-04-04", "2026-04-06", "2026-04-09"):
            _seed_transaction(
                conn,
                account_id=account_id,
                category_name="Dining",
                amount_cents=-10_000,
                txn_date=txn_date,
                use_type="Business",
            )

        intervention = evaluate_t7_business_deduction_streak(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.dollar_impact_cents == 4_500
    assert "Top reviewed category: Dining ($150.00 deductible)." in intervention.detail_bullets
