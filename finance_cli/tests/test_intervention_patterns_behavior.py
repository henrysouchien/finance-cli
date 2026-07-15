from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.behavior import (
    evaluate_b1_subscription_drift,
    evaluate_b2_lifestyle_creep,
    evaluate_b3_one_off_vs_trend,
    evaluate_b4_end_of_month_spending_pattern,
    evaluate_b5_discipline_streak,
    evaluate_b6_subscription_bundle_opportunity,
    evaluate_b7_q4_budget_drag,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move, Priority


NOW = datetime(2026, 6, 20, 12, 0, 0)
B4_PROMPT_NOW = datetime(2026, 6, 21, 12, 0, 0)
Q4_PLANNING_NOW = datetime(2026, 9, 15, 12, 0, 0)


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)
    return db_path


def _category_id(conn, name: str, *, is_income: bool = False) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, ?, 0, 0)
        """,
        (category_id, name, 1 if is_income else 0),
    )
    return category_id


def _seed_subscription_charges(
    conn,
    *,
    description: str = "Acme Cloud",
    amounts: tuple[int, int, int, int, int, int, int],
    dates: tuple[str, str, str, str, str, str, str] = (
        "2025-11-10",
        "2025-12-10",
        "2026-01-10",
        "2026-02-10",
        "2026-03-10",
        "2026-04-10",
        "2026-05-10",
    ),
) -> None:
    category_id = _category_id(conn, "Software")
    for txn_date, amount_cents in zip(dates, amounts):
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, is_payment,
                is_active, is_reviewed, source
            ) VALUES (?, ?, ?, ?, ?, 0, 1, 1, 'manual')
            """,
            (uuid.uuid4().hex, txn_date, description, -abs(amount_cents), category_id),
        )
    conn.commit()


def _seed_goal(conn) -> str:
    goal_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO goals (
            id, name, metric, target_cents, starting_cents, direction, is_active
        ) VALUES (?, 'Emergency fund', 'liquid_cash', 600000, 0, 'up', 1)
        """,
        (goal_id,),
    )
    conn.commit()
    return goal_id


def _seed_account(conn, account_id: str = "card-1") -> str:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active
        ) VALUES (?, 'Card Bank', 'Rewards', 'credit_card', -50000, 1)
        """,
        (account_id,),
    )
    conn.commit()
    return account_id


def _seed_subscription(
    conn,
    *,
    vendor_name: str,
    amount_cents: int,
    frequency: str = "monthly",
    account_id: str | None = "card-1",
    use_type: str | None = "Personal",
    is_active: int = 1,
) -> str:
    subscription_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO subscriptions (
            id, vendor_name, amount_cents, frequency, account_id,
            use_type, is_active, is_auto_detected
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        """,
        (subscription_id, vendor_name, amount_cents, frequency, account_id, use_type, is_active),
    )
    conn.commit()
    return subscription_id


def _seed_monthly_budget(
    conn,
    *,
    category_id: str,
    amount_cents: int,
    effective_from: str = "2025-01-01",
    effective_to: str | None = None,
) -> str:
    budget_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO budgets (
            id, category_id, period, amount_cents, effective_from, effective_to, use_type
        ) VALUES (?, ?, 'monthly', ?, ?, ?, 'Personal')
        """,
        (budget_id, category_id, amount_cents, effective_from, effective_to),
    )
    conn.commit()
    return budget_id


def _seed_b2_month(
    conn,
    *,
    month: str,
    income_cents: int = 500_000,
    dining_cents: int,
    travel_cents: int,
    rent_cents: int = 100_000,
) -> None:
    income_category = _category_id(conn, "Income: Salary", is_income=True)
    dining_category = _category_id(conn, "Dining")
    travel_category = _category_id(conn, "Travel")
    rent_category = _category_id(conn, "Rent")
    rows = [
        (f"{month}-01", "Salary", income_cents, income_category, None),
        (f"{month}-05", "Rent", -rent_cents, rent_category, "Personal"),
        (f"{month}-10", "Dining", -dining_cents, dining_category, "Personal"),
        (f"{month}-15", "Travel", -travel_cents, travel_category, "Personal"),
    ]
    for txn_date, description, amount_cents, category_id, use_type in rows:
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, use_type,
                is_payment, is_active, is_reviewed, source
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 1, 'manual')
            """,
            (uuid.uuid4().hex, txn_date, description, amount_cents, category_id, use_type),
        )
    conn.commit()


def _seed_b2_lifestyle_creep(conn, *, current_income_cents: int = 500_000) -> None:
    for month in ("2025-12", "2026-01", "2026-02"):
        _seed_b2_month(conn, month=month, dining_cents=50_000, travel_cents=20_000)
    for month in ("2026-03", "2026-04", "2026-05"):
        _seed_b2_month(
            conn,
            month=month,
            income_cents=current_income_cents,
            dining_cents=90_000,
            travel_cents=50_000,
        )


def _seed_b7_q4_history(
    conn,
    *,
    category_id: str,
    budget_cents: int = 30_000,
    actuals_by_year: dict[int, int] | None = None,
) -> None:
    actuals = actuals_by_year or {2024: 50_000, 2025: 45_000}
    _seed_monthly_budget(
        conn,
        category_id=category_id,
        amount_cents=budget_cents,
        effective_from="2024-01-01",
    )
    for year, actual_cents in actuals.items():
        for month in (10, 11, 12):
            _seed_b3_expense(
                conn,
                category_id=category_id,
                txn_date=f"{year}-{month:02d}-10",
                amount_cents=actual_cents,
                description="Q4 dining",
            )


def _seed_b7_budgetless_spending_history(
    conn,
    *,
    category_id: str,
    baseline_cents: int = 30_000,
    actuals_by_year: dict[int, int] | None = None,
) -> None:
    actuals = actuals_by_year or {2024: 50_000, 2025: 45_000}
    for year, actual_cents in actuals.items():
        for month in (7, 8, 9):
            _seed_b3_expense(
                conn,
                category_id=category_id,
                txn_date=f"{year}-{month:02d}-10",
                amount_cents=baseline_cents,
                description="Baseline dining",
            )
        for month in (10, 11, 12):
            _seed_b3_expense(
                conn,
                category_id=category_id,
                txn_date=f"{year}-{month:02d}-10",
                amount_cents=actual_cents,
                description="Q4 dining",
            )


def _seed_b3_expense(
    conn,
    *,
    category_id: str,
    txn_date: str,
    amount_cents: int,
    description: str,
    use_type: str | None = "Personal",
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, use_type,
            is_payment, is_active, is_reviewed, source
        ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 1, 'manual')
        """,
        (uuid.uuid4().hex, txn_date, description, -abs(amount_cents), category_id, use_type),
    )
    conn.commit()


def _seed_b4_month(
    conn,
    *,
    month: str,
    category_id: str,
    first_cents: int,
    late_cents: int,
    use_type: str | None = "Personal",
) -> None:
    first_date = f"{month}-05"
    late_day = "25"
    for txn_date, amount_cents, description in (
        (first_date, first_cents, "early discretionary"),
        (f"{month}-{late_day}", late_cents, "late discretionary"),
    ):
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, use_type,
                is_payment, is_active, is_reviewed, source
            ) VALUES (?, ?, ?, ?, ?, ?, 0, 1, 1, 'manual')
            """,
            (
                uuid.uuid4().hex,
                txn_date,
                description,
                -abs(amount_cents),
                category_id,
                use_type,
            ),
        )
    conn.commit()


def test_b1_subscription_drift_fires_at_15_percent_growth(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_subscription_charges(
            conn,
            amounts=(20_000, 20_000, 20_000, 23_000, 23_000, 23_000, 23_000),
        )

        intervention = evaluate_b1_subscription_drift(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-1"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.priority is Priority.MEDIUM
    assert "$230.00/mo in matched subs, up from $200.00/mo six months ago" in intervention.headline
    assert "$360.00/yr of extra recurring burn" in intervention.headline
    assert intervention.dollar_impact_cents == 36_000
    assert intervention.action is not None
    assert intervention.action.tool == "subs_audit"
    assert intervention.action.params == {}


def test_b1_subscription_drift_requires_15_percent_growth(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_subscription_charges(
            conn,
            amounts=(10_000, 10_000, 10_000, 11_499, 11_499, 11_499, 11_499),
        )

        intervention = evaluate_b1_subscription_drift(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b1_subscription_drift_ignores_current_partial_month(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_subscription_charges(
            conn,
            amounts=(20_000, 20_000, 20_000, 20_000, 20_000, 20_000, 23_000),
            dates=(
                "2025-11-10",
                "2025-12-10",
                "2026-01-10",
                "2026-02-10",
                "2026-03-10",
                "2026-04-10",
                "2026-06-10",
            ),
        )

        intervention = evaluate_b1_subscription_drift(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b1_runs_through_engine_and_receives_tier4_ladder(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        _seed_subscription_charges(
            conn,
            amounts=(20_000, 20_000, 20_000, 23_000, 23_000, 23_000, 23_000),
        )

        result = run_engine(conn, now=NOW)

    b1 = next(item for item in result.interventions if item.pattern_id == "B-1")
    assert b1.tier4_ladder is not None
    assert "Emergency fund" in b1.tier4_ladder
    assert b1.goal_link is not None


def test_b6_subscription_bundle_opportunity_fires_for_apple_one_savings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Apple Music", amount_cents=1_099)
        _seed_subscription(conn, vendor_name="Apple TV+", amount_cents=1_299)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-6"
    assert intervention.move is Move.COMPARE
    assert intervention.priority is Priority.LOW
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 4_836
    assert intervention.headline == (
        "Apple Music and Apple TV billing separate ($23.98/mo). "
        "Apple One Individual is $19.95/mo. $48.36/yr to switch."
    )
    assert "https://www.apple.com/apple-one/" in intervention.detail_bullets[1]


def test_b6_subscription_bundle_opportunity_uses_apple_family_price_signal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Apple Music", amount_cents=1_699)
        _seed_subscription(conn, vendor_name="Apple TV+", amount_cents=1_299)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-6"
    assert intervention.dollar_impact_cents == 4_836
    assert intervention.headline == (
        "Apple Music and Apple TV billing separate ($29.98/mo). "
        "Apple One Family is $25.95/mo. $48.36/yr to switch."
    )


def test_b6_subscription_bundle_opportunity_fires_for_disney_hulu_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Disney+", amount_cents=1_199)
        _seed_subscription(conn, vendor_name="Hulu", amount_cents=1_199)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-6"
    assert intervention.dollar_impact_cents == 13_188
    assert intervention.headline == (
        "Disney+ and Hulu billing separate ($23.98/mo). "
        "Disney+, Hulu Bundle is $12.99/mo. $131.88/yr to switch."
    )


def test_b6_runs_through_engine_but_not_action_queue(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Apple Music", amount_cents=1_099)
        _seed_subscription(conn, vendor_name="Apple TV+", amount_cents=1_299)

        result = run_engine(conn, now=NOW)

    assert any(item.pattern_id == "B-6" for item in result.interventions)
    assert all(item.pattern_id != "B-6" for item in result.get_for_surface("action_queue"))


def test_b6_requires_bundle_savings(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Apple Arcade", amount_cents=699)
        _seed_subscription(conn, vendor_name="iCloud+", amount_cents=99)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b6_suppresses_different_account_subscriptions(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_account(conn, "card-2")
        _seed_subscription(conn, vendor_name="Disney+", amount_cents=1_199, account_id="card-1")
        _seed_subscription(conn, vendor_name="Hulu", amount_cents=1_199, account_id="card-2")

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b6_suppresses_accountless_subscriptions(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_subscription(conn, vendor_name="Disney+", amount_cents=1_199, account_id=None)
        _seed_subscription(conn, vendor_name="Hulu", amount_cents=1_199, account_id=None)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b6_suppresses_business_subscriptions(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Apple Music", amount_cents=1_099, use_type="Business")
        _seed_subscription(conn, vendor_name="Apple TV+", amount_cents=1_299, use_type="Business")

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b6_suppresses_unclassified_subscriptions(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Disney+", amount_cents=1_199, use_type=None)
        _seed_subscription(conn, vendor_name="Hulu", amount_cents=1_199, use_type=None)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b6_ignores_existing_bundle_rows(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_subscription(conn, vendor_name="Disney+ Hulu Bundle", amount_cents=1_299)
        _seed_subscription(conn, vendor_name="Hulu", amount_cents=1_199)

        intervention = evaluate_b6_subscription_bundle_opportunity(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b7_q4_budget_drag_fires_for_two_prior_q4_overruns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(conn, category_id=dining_id)

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-7"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.priority is Priority.LOW
    assert intervention.tiers == (4,)
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 52_500
    assert intervention.headline == (
        "Q4 has run over budget in 2024 and 2025 by about $525.00. "
        "That is the seasonal drag on Emergency fund; plan the buffer before October."
    )
    assert "2024 Q4: $1,500.00 actual vs $900.00 budget." in intervention.detail_bullets
    assert "2025 Q4: $1,350.00 actual vs $900.00 budget." in intervention.detail_bullets
    assert "Biggest recurring Q4 pressure: Dining ($1,050.00 total overage)." in intervention.detail_bullets


def test_b7_q4_budget_drag_falls_back_to_spending_baseline_without_budgets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_budgetless_spending_history(conn, category_id=dining_id)

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-7"
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 52_500
    assert intervention.headline == (
        "Q4 spending has run above its July-September baseline in 2024 and 2025 by about $525.00. "
        "That is the seasonal drag on Emergency fund; plan the buffer before October."
    )
    assert (
        "2024 Q4: $1,500.00 actual vs $900.00 July-September baseline."
        in intervention.detail_bullets
    )
    assert (
        "2025 Q4: $1,350.00 actual vs $900.00 July-September baseline."
        in intervention.detail_bullets
    )
    assert "Biggest recurring Q4 pressure: Dining ($1,050.00 total overage)." in intervention.detail_bullets


def test_b7_runs_through_engine_and_receives_tier4_ladder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(conn, category_id=dining_id)

        result = run_engine(conn, now=Q4_PLANNING_NOW)

    b7 = next(item for item in result.interventions if item.pattern_id == "B-7")
    assert b7.action is None
    assert b7.tier4_ladder is not None
    assert "Emergency fund" in b7.tier4_ladder
    assert all(item.pattern_id != "B-7" for item in result.get_for_surface("action_queue"))


def test_b7_requires_explicit_active_goal(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(conn, category_id=dining_id)

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is None


def test_b7_requires_two_prior_q4_overruns(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(conn, category_id=dining_id, actuals_by_year={2025: 45_000})

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is None


def test_b7_suppresses_outside_q4_planning_window(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(conn, category_id=dining_id)

        intervention = evaluate_b7_q4_budget_drag(
            conn,
            build_context(conn, now=datetime(2026, 1, 15, 12, 0, 0)),
        )

    assert intervention is None


def test_b7_does_not_use_spending_baseline_when_complete_budgets_do_not_overrun(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_monthly_budget(conn, category_id=dining_id, amount_cents=50_000)
        _seed_b7_budgetless_spending_history(conn, category_id=dining_id)

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is None


def test_b7_requires_meaningful_q4_overage(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        dining_id = _category_id(conn, "Dining")
        _seed_b7_q4_history(
            conn,
            category_id=dining_id,
            actuals_by_year={2024: 33_000, 2025: 34_000},
        )

        intervention = evaluate_b7_q4_budget_drag(conn, build_context(conn, now=Q4_PLANNING_NOW))

    assert intervention is None


def test_b2_lifestyle_creep_fires_when_spending_up_and_income_flat(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_b2_lifestyle_creep(conn)

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-2"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.MEDIUM
    assert "Spending's up $700.00/mo over six months" in intervention.headline
    assert "Dining (+$400.00/mo) and Travel (+$300.00/mo)" in intervention.headline
    assert "Pulling Dining back to last quarter's level frees $400.00/mo" in intervention.headline
    assert intervention.dollar_impact_cents == 480_000
    assert intervention.action is not None
    assert intervention.action.tool == "spending_trends"
    assert intervention.action.params == {"months": 6, "view": "personal", "categories": ["Dining", "Travel"]}


def test_b2_lifestyle_creep_does_not_fire_when_income_increased(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_b2_lifestyle_creep(conn, current_income_cents=600_000)

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b2_lifestyle_creep_counts_uncategorized_baseline_spend(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        income_category = _category_id(conn, "Income: Salary", is_income=True)
        dining_category = _category_id(conn, "Dining")
        for month in ("2025-12", "2026-01", "2026-02"):
            conn.execute(
                """
                INSERT INTO transactions (
                    id, date, description, amount_cents, category_id, is_payment,
                    is_active, is_reviewed, source
                ) VALUES (?, ?, 'Salary', 500000, ?, 0, 1, 1, 'manual')
                """,
                (uuid.uuid4().hex, f"{month}-01", income_category),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    id, date, description, amount_cents, category_id, use_type,
                    is_payment, is_active, is_reviewed, source
                ) VALUES (?, ?, 'Uncategorized spend', -170000, NULL, 'Personal', 0, 1, 0, 'manual')
                """,
                (uuid.uuid4().hex, f"{month}-10"),
            )
        for month in ("2026-03", "2026-04", "2026-05"):
            conn.execute(
                """
                INSERT INTO transactions (
                    id, date, description, amount_cents, category_id, is_payment,
                    is_active, is_reviewed, source
                ) VALUES (?, ?, 'Salary', 500000, ?, 0, 1, 1, 'manual')
                """,
                (uuid.uuid4().hex, f"{month}-01", income_category),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    id, date, description, amount_cents, category_id, use_type,
                    is_payment, is_active, is_reviewed, source
                ) VALUES (?, ?, 'Dining', -170000, ?, 'Personal', 0, 1, 1, 'manual')
                """,
                (uuid.uuid4().hex, f"{month}-10", dining_category),
            )
        conn.commit()

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b2_lifestyle_creep_allows_lumpy_flat_income(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        for month in ("2025-12", "2026-01", "2026-02"):
            _seed_b2_month(
                conn,
                month=month,
                income_cents=1_500_000 if month == "2025-12" else 0,
                dining_cents=50_000,
                travel_cents=20_000,
            )
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b2_month(
                conn,
                month=month,
                income_cents=1_500_000 if month == "2026-03" else 0,
                dining_cents=90_000,
                travel_cents=50_000,
            )

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-2"


def test_b2_lifestyle_creep_uses_income_down_copy(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_b2_lifestyle_creep(conn, current_income_cents=400_000)

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert "Income is down." in intervention.headline


def test_b2_lifestyle_creep_ignores_current_partial_month_spike(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        for month in ("2025-12", "2026-01", "2026-02", "2026-03", "2026-04", "2026-05"):
            _seed_b2_month(conn, month=month, dining_cents=50_000, travel_cents=20_000)
        _seed_b2_month(conn, month="2026-06", dining_cents=200_000, travel_cents=100_000)

        intervention = evaluate_b2_lifestyle_creep(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b2_runs_through_engine_and_receives_tier4_ladder(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        _seed_b2_lifestyle_creep(conn)

        result = run_engine(conn, now=NOW)

    b2 = next(item for item in result.interventions if item.pattern_id == "B-2")
    assert b2.tier4_ladder is not None
    assert "Emergency fund" in b2.tier4_ladder
    assert b2.goal_link is not None


def test_b3_one_off_vs_trend_reassures_single_hit_overage(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Home")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        for month in ("2026-03", "2026-04"):
            _seed_b3_expense(conn, category_id=category_id, txn_date=f"{month}-08", amount_cents=40_000, description="Routine home")
        _seed_b3_expense(
            conn,
            category_id=category_id,
            txn_date="2026-05-10",
            amount_cents=45_000,
            description="Appliance repair",
        )
        _seed_b3_expense(
            conn,
            category_id=category_id,
            txn_date="2026-05-18",
            amount_cents=25_000,
            description="Routine home",
        )

        intervention = evaluate_b3_one_off_vs_trend(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-3"
    assert intervention.move is Move.DIAGNOSE
    assert intervention.priority is Priority.LOW
    assert intervention.action is None
    assert intervention.dollar_impact_cents == 0
    assert "Your May was $200.00 over on Home" in intervention.headline
    assert "$450.00 was a one-time Appliance repair" in intervention.headline
    assert "Not a trend" in intervention.headline


def test_b3_requires_thirty_percent_over_budget(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Shopping")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=35_000, description="Suit")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-12", amount_cents=27_000, description="Shoes")

        intervention = evaluate_b3_one_off_vs_trend(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b3_requires_single_transaction_to_explain_overage(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        for day in range(1, 8):
            _seed_b3_expense(
                conn,
                category_id=category_id,
                txn_date=f"2026-05-{day:02d}",
                amount_cents=10_000,
                description=f"Restaurant {day}",
            )

        intervention = evaluate_b3_one_off_vs_trend(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b3_suppresses_when_same_category_has_actual_trend(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Travel")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-04-07", amount_cents=70_000, description="Hotel")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=45_000, description="Flight")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-18", amount_cents=25_000, description="Taxi")

        intervention = evaluate_b3_one_off_vs_trend(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b3_runs_through_engine_without_action(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Home")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=45_000, description="Appliance repair")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-18", amount_cents=25_000, description="Routine home")

        result = run_engine(conn, now=NOW)

    b3 = next(item for item in result.interventions if item.pattern_id == "B-3")
    assert b3.action is None
    assert b3.tier4_ladder is None
    assert all(item.pattern_id != "B-3" for item in result.get_for_surface("action_queue"))


def test_b4_end_of_month_spending_pattern_fires_for_late_month_spike(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=16_000,
            )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is not None
    assert intervention.pattern_id == "B-4"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.priority is Priority.MEDIUM
    assert "runs 60% higher in the last 10 days" in intervention.headline
    assert "The late-month pattern is consistent" in intervention.headline
    assert "tired" not in intervention.headline
    assert "First 10 days average: $100.00." in intervention.detail_bullets
    assert "Last 10 days average: $160.00." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "add_late_month_buffer_budget"
    assert intervention.action.params == {
        "amount_cents": 6_000,
        "category_name": "Late-Month Buffer",
        "effective_from": "2026-07-01",
        "dry_run": False,
    }


def test_b4_runs_through_engine_and_action_queue(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=16_000,
            )

        result = run_engine(conn, now=B4_PROMPT_NOW)

    assert any(item.pattern_id == "B-4" for item in result.interventions)
    assert any(item.pattern_id == "B-4" for item in result.get_for_surface("action_queue"))


def test_b4_suppresses_before_current_late_month_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=16_000,
            )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=NOW),
        )

    assert intervention is None


def test_b4_requires_three_complete_months(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=16_000,
            )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is None


def test_b4_requires_every_month_to_clear_40_percent_lift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        _seed_b4_month(conn, month="2026-03", category_id=category_id, first_cents=10_000, late_cents=16_000)
        _seed_b4_month(conn, month="2026-04", category_id=category_id, first_cents=10_000, late_cents=13_999)
        _seed_b4_month(conn, month="2026-05", category_id=category_id, first_cents=10_000, late_cents=16_000)

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is None


def test_b4_ignores_current_partial_month_only_spike(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=10_000,
            )
        _seed_b4_month(
            conn,
            month="2026-06",
            category_id=category_id,
            first_cents=10_000,
            late_cents=40_000,
        )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is None


def test_b4_excludes_essential_categories(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Rent")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=20_000,
            )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is None


def test_b4_excludes_business_spend(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        for month in ("2026-03", "2026-04", "2026-05"):
            _seed_b4_month(
                conn,
                month=month,
                category_id=category_id,
                first_cents=10_000,
                late_cents=20_000,
                use_type="Business",
            )

        intervention = evaluate_b4_end_of_month_spending_pattern(
            conn,
            build_context(conn, now=B4_PROMPT_NOW),
        )

    assert intervention is None


def test_b5_discipline_streak_fires_for_three_under_budget_months(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=32_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-04-10", amount_cents=35_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=38_000, description="Dining")

        intervention = evaluate_b5_discipline_streak(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "B-5"
    assert intervention.move is Move.COACH
    assert intervention.priority is Priority.MEDIUM
    assert "Three months in a row under budget on Dining" in intervention.headline
    assert "lock $350.00 as your new target" in intervention.headline
    assert "Frees up $150.00/mo for your goal" in intervention.headline
    assert intervention.dollar_impact_cents == 180_000
    assert intervention.action is not None
    assert intervention.action.tool == "budget_update"
    assert intervention.action.params == {
        "category": "Dining",
        "amount": 350.0,
        "period": "monthly",
        "view": "personal",
    }


def test_b5_discipline_streak_requires_all_three_months_under_budget(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=32_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-04-10", amount_cents=51_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=38_000, description="Dining")

        intervention = evaluate_b5_discipline_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b5_discipline_streak_requires_real_spend_each_month(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Books")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=20_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=5_000, description="Books")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=5_000, description="Books")

        intervention = evaluate_b5_discipline_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b5_suppresses_when_current_open_budget_is_already_lower(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        category_id = _category_id(conn, "Dining")
        _seed_monthly_budget(
            conn,
            category_id=category_id,
            amount_cents=50_000,
            effective_from="2026-03-01",
            effective_to="2026-05-31",
        )
        _seed_monthly_budget(
            conn,
            category_id=category_id,
            amount_cents=30_000,
            effective_from="2026-06-01",
        )
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=32_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-04-10", amount_cents=35_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=38_000, description="Dining")

        intervention = evaluate_b5_discipline_streak(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_b5_runs_through_engine_and_receives_tier4_ladder(tmp_path: Path, monkeypatch) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    with connect(db_path) as conn:
        _seed_goal(conn)
        category_id = _category_id(conn, "Dining")
        _seed_monthly_budget(conn, category_id=category_id, amount_cents=50_000)
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-03-10", amount_cents=32_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-04-10", amount_cents=35_000, description="Dining")
        _seed_b3_expense(conn, category_id=category_id, txn_date="2026-05-10", amount_cents=38_000, description="Dining")

        result = run_engine(conn, now=NOW)

    b5 = next(item for item in result.interventions if item.pattern_id == "B-5")
    assert b5.tier4_ladder is not None
    assert "Emergency fund" in b5.tier4_ladder
    assert b5.goal_link is not None
