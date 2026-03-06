from __future__ import annotations

import calendar
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import finance_cli.forecasting as forecasting
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _month_start(offset_from_current: int) -> date:
    current = date.today().replace(day=1)
    month_index = (current.year * 12) + (current.month - 1) + int(offset_from_current)
    year, month_zero = divmod(month_index, 12)
    return date(year, month_zero + 1, 1)


def _month_date(offset_from_current: int, day: int = 10) -> str:
    start = _month_start(offset_from_current)
    last_day = calendar.monthrange(start.year, start.month)[1]
    safe_day = max(1, min(day, last_day))
    return date(start.year, start.month, safe_day).isoformat()


def _seed_category(conn, name: str, *, is_income: int = 0) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
        VALUES (?, ?, NULL, 0, ?, 0, 0)
        """,
        (category_id, name, is_income),
    )
    return category_id


def _seed_pl_map(conn, category_id: str, section: str, display_order: int = 10) -> None:
    conn.execute(
        """
        INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
        VALUES (?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, category_id, section, display_order),
    )


def _seed_business_account(
    conn,
    *,
    account_type: str = "checking",
    balance_cents: int = 0,
    is_business: int = 1,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business, source
        ) VALUES (?, 'Test Bank', 'Biz Account', ?, ?, 1, ?, 'manual')
        """,
        (account_id, account_type, balance_cents, is_business),
    )
    return account_id


def _seed_txn(
    conn,
    *,
    account_id: str,
    category_id: str,
    amount_cents: int,
    txn_date: str,
    description: str,
    source: str = "manual",
    source_category: str | None = None,
    use_type: str = "Business",
    is_payment: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents,
            category_id, source_category, category_source,
            use_type, is_active, is_payment, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'user', ?, 1, ?, ?)
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            category_id,
            source_category,
            use_type,
            is_payment,
            source,
        ),
    )
    return txn_id


def _set_revenue_streams(monkeypatch, streams: list[dict]) -> None:
    monkeypatch.setattr(forecasting, "load_rules", lambda: SimpleNamespace(revenue_streams=streams))


def _seed_revenue_category(conn, name: str = "Revenue") -> str:
    category_id = _seed_category(conn, name, is_income=1)
    _seed_pl_map(conn, category_id, "revenue", 10)
    return category_id


def _seed_expense_category(conn, name: str, section: str, order: int) -> str:
    category_id = _seed_category(conn, name, is_income=0)
    _seed_pl_map(conn, category_id, section, order)
    return category_id


def test_revenue_by_stream_groups_source_and_keyword(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(
        monkeypatch,
        [
            {"name": "Stripe", "match": {"source": "stripe", "source_category": "charge"}},
            {"name": "Kartra", "match": {"keywords": ["KARTRA PAYOUT"]}},
        ],
    )
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=revenue_id,
            amount_cents=12_000,
            txn_date=_month_date(-1),
            description="Stripe transfer",
            source="stripe",
            source_category="charge",
        )
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=revenue_id,
            amount_cents=8_000,
            txn_date=_month_date(-1, 11),
            description="KARTRA PAYOUT FEB",
            source="manual",
        )
        conn.commit()

        rows = forecasting.revenue_by_stream(conn, months=2)

    by_stream = {row["stream"]: row for row in rows}
    assert by_stream["Stripe"]["gross_cents"] == 12_000
    assert by_stream["Kartra"]["gross_cents"] == 8_000


def test_revenue_by_stream_routes_unmatched_to_other(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Stripe", "match": {"source": "stripe", "source_category": "charge"}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=revenue_id,
            amount_cents=5_500,
            txn_date=_month_date(0),
            description="Consulting invoice paid",
        )
        conn.commit()

        rows = forecasting.revenue_by_stream(conn, months=1)

    assert len(rows) == 1
    assert rows[0]["stream"] == "Other"
    assert rows[0]["gross_cents"] == 5_500


def test_revenue_by_stream_uses_pl_section_map_scope(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        mapped_revenue_id = _seed_revenue_category(conn, "Mapped Revenue")
        unmapped_income_id = _seed_category(conn, "Income: Business", is_income=1)
        account_id = _seed_business_account(conn)

        _seed_txn(
            conn,
            account_id=account_id,
            category_id=mapped_revenue_id,
            amount_cents=9_000,
            txn_date=_month_date(0),
            description="CONSULTING RETAINER",
        )
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=unmapped_income_id,
            amount_cents=99_000,
            txn_date=_month_date(0, 12),
            description="CONSULTING (UNMAPPED)",
        )
        conn.commit()

        rows = forecasting.revenue_by_stream(conn, months=1)

    assert len(rows) == 1
    assert rows[0]["gross_cents"] == 9_000


def test_revenue_by_stream_empty_dataset_returns_empty(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Stripe", "match": {"source": "stripe", "source_category": "charge"}}])
    with connect(db_path) as conn:
        rows = forecasting.revenue_by_stream(conn, months=3)
    assert rows == []


def test_revenue_by_stream_empty_rules_puts_all_revenue_in_other(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=revenue_id,
            amount_cents=7_700,
            txn_date=_month_date(-1),
            description="ROYALTY PAYMENT",
            source="stripe",
            source_category="charge",
        )
        conn.commit()

        rows = forecasting.revenue_by_stream(conn, months=2)

    assert len(rows) == 1
    assert rows[0]["stream"] == "Other"


def test_revenue_by_stream_handles_apostrophe_in_stream_name(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(
        monkeypatch,
        [{"name": "Partner's Revenue", "match": {"keywords": ["PARTNER PAYOUT"]}}],
    )
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(
            conn,
            account_id=account_id,
            category_id=revenue_id,
            amount_cents=4_000,
            txn_date=_month_date(0),
            description="PARTNER PAYOUT MARCH",
        )
        conn.commit()

        rows = forecasting.revenue_by_stream(conn, months=1)

    assert rows[0]["stream"] == "Partner's Revenue"


def test_revenue_trend_positive_slope_and_projection(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(-2), description="CONSULTING A")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=20_000, txn_date=_month_date(-1), description="CONSULTING B")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=30_000, txn_date=_month_date(0), description="CONSULTING C")
        conn.commit()

        trend = forecasting.revenue_trend(conn, months=3)

    stream = trend["streams"][0]
    assert stream["trend_slope_cents"] is not None and stream["trend_slope_cents"] > 0
    assert stream["projected_next_month_cents"] == 40_000


def test_revenue_trend_negative_slope(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=30_000, txn_date=_month_date(-2), description="CONSULTING A")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=20_000, txn_date=_month_date(-1), description="CONSULTING B")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(0), description="CONSULTING C")
        conn.commit()

        trend = forecasting.revenue_trend(conn, months=3)

    stream = trend["streams"][0]
    assert stream["trend_slope_cents"] is not None and stream["trend_slope_cents"] < 0


def test_revenue_trend_flat_slope(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=15_000, txn_date=_month_date(-2), description="CONSULTING A")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=15_000, txn_date=_month_date(-1), description="CONSULTING B")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=15_000, txn_date=_month_date(0), description="CONSULTING C")
        conn.commit()

        trend = forecasting.revenue_trend(conn, months=3)

    stream = trend["streams"][0]
    assert stream["trend_slope_cents"] == 0
    assert stream["projected_next_month_cents"] == 15_000


def test_revenue_trend_single_month_guard_returns_none(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=25_000, txn_date=_month_date(0), description="CONSULTING")
        conn.commit()

        trend = forecasting.revenue_trend(conn, months=1)

    stream = trend["streams"][0]
    assert stream["trend_slope_cents"] is None
    assert stream["projected_next_month_cents"] is None


def test_revenue_trend_non_consecutive_months_fills_missing_with_zero(db_path: Path, monkeypatch) -> None:
    _set_revenue_streams(monkeypatch, [{"name": "Consulting", "match": {"keywords": ["CONSULTING"]}}])
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=12_000, txn_date=_month_date(-3), description="CONSULTING A")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=18_000, txn_date=_month_date(-1), description="CONSULTING B")
        conn.commit()

        trend = forecasting.revenue_trend(conn, months=4)

    stream = trend["streams"][0]
    monthly = {row["month"]: row["cents"] for row in stream["monthly_totals"]}
    assert len(monthly) == 4
    assert 0 in monthly.values()


def test_burn_rate_monthly_average_across_months(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        marketing_id = _seed_expense_category(conn, "Marketing", "opex_marketing", 30)
        account_id = _seed_business_account(conn)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=20_000, txn_date=_month_date(-1), description="Income 1")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(0), description="Income 2")
        _seed_txn(conn, account_id=account_id, category_id=marketing_id, amount_cents=-8_000, txn_date=_month_date(-1, 11), description="Ads 1")
        _seed_txn(conn, account_id=account_id, category_id=marketing_id, amount_cents=-6_000, txn_date=_month_date(0, 11), description="Ads 2")
        conn.commit()

        result = forecasting.burn_rate(conn, months=2)

    assert result["monthly_avg_income_cents"] == 15_000
    assert result["monthly_avg_expense_cents"] == 7_000
    assert result["monthly_net_burn_cents"] == -8_000


def test_burn_rate_by_section_sums_to_total(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        marketing_id = _seed_expense_category(conn, "Marketing", "opex_marketing", 30)
        tech_id = _seed_expense_category(conn, "Software", "opex_technology", 40)
        account_id = _seed_business_account(conn)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=8_000, txn_date=_month_date(-1), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=marketing_id, amount_cents=-6_000, txn_date=_month_date(-1, 11), description="Ads")
        _seed_txn(conn, account_id=account_id, category_id=tech_id, amount_cents=-4_000, txn_date=_month_date(-1, 12), description="Tech")
        _seed_txn(conn, account_id=account_id, category_id=marketing_id, amount_cents=-2_000, txn_date=_month_date(0, 11), description="Ads")
        _seed_txn(conn, account_id=account_id, category_id=tech_id, amount_cents=-8_000, txn_date=_month_date(0, 12), description="Tech")
        conn.commit()

        result = forecasting.burn_rate(conn, months=2)

    assert sum(row["monthly_avg_cents"] for row in result["by_section"]) == result["monthly_avg_expense_cents"]


def test_burn_rate_empty_dataset_returns_zeroes(db_path: Path) -> None:
    with connect(db_path) as conn:
        result = forecasting.burn_rate(conn, months=3)
    assert result["monthly_avg_income_cents"] == 0
    assert result["monthly_avg_expense_cents"] == 0
    assert result["monthly_net_burn_cents"] == 0
    assert result["by_section"] == []


def test_runway_positive_burn_returns_finite_months(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        account_id = _seed_business_account(conn, account_type="checking", balance_cents=120_000)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(-1), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(0), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=opex_id, amount_cents=-40_000, txn_date=_month_date(-1, 11), description="Expense")
        _seed_txn(conn, account_id=account_id, category_id=opex_id, amount_cents=-40_000, txn_date=_month_date(0, 11), description="Expense")
        conn.commit()

        result = forecasting.runway(conn, months=2)

    assert result["monthly_net_burn_cents"] == 30_000
    assert result["runway_months"] == 4.0
    assert result["runway_date"] is not None


def test_runway_profitable_business_returns_none(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        account_id = _seed_business_account(conn, account_type="checking", balance_cents=80_000)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=30_000, txn_date=_month_date(0), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=opex_id, amount_cents=-5_000, txn_date=_month_date(0, 11), description="Expense")
        conn.commit()

        result = forecasting.runway(conn, months=1)

    assert result["monthly_net_burn_cents"] < 0
    assert result["runway_months"] is None
    assert result["runway_date"] is None


def test_runway_zero_burn_returns_none(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        account_id = _seed_business_account(conn, account_type="checking", balance_cents=50_000)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date=_month_date(-1), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=opex_id, amount_cents=-10_000, txn_date=_month_date(-1, 11), description="Expense")
        conn.commit()

        result = forecasting.runway(conn, months=1)

    assert result["monthly_net_burn_cents"] == 0
    assert result["runway_months"] is None
    assert result["runway_date"] is None


def test_runway_no_business_accounts_has_zero_liquid_balance(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        account_id = _seed_business_account(conn, account_type="checking", balance_cents=90_000, is_business=0)

        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=5_000, txn_date=_month_date(-1), description="Income")
        _seed_txn(conn, account_id=account_id, category_id=opex_id, amount_cents=-25_000, txn_date=_month_date(-1, 10), description="Expense")
        conn.commit()

        result = forecasting.runway(conn, months=1)

    assert result["liquid_balance_cents"] == 0


def test_runway_counts_only_business_checking_and_savings(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        txn_account = _seed_business_account(conn, account_type="checking", balance_cents=100_000)
        _seed_business_account(conn, account_type="savings", balance_cents=50_000)
        _seed_business_account(conn, account_type="credit_card", balance_cents=-500_000)
        _seed_business_account(conn, account_type="investment", balance_cents=2_000_000)

        _seed_txn(conn, account_id=txn_account, category_id=revenue_id, amount_cents=5_000, txn_date=_month_date(-1), description="Income")
        _seed_txn(conn, account_id=txn_account, category_id=opex_id, amount_cents=-10_000, txn_date=_month_date(-1, 10), description="Expense")
        conn.commit()

        result = forecasting.runway(conn, months=1)

    assert result["liquid_balance_cents"] == 150_000


def test_runway_excludes_hash_alias_balances(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        opex_id = _seed_expense_category(conn, "Ops", "opex_other", 80)
        canonical_id = _seed_business_account(conn, account_type="checking", balance_cents=120_000)
        hash_id = _seed_business_account(conn, account_type="checking", balance_cents=30_000)

        _seed_txn(
            conn,
            account_id=canonical_id,
            category_id=revenue_id,
            amount_cents=5_000,
            txn_date=_month_date(0),
            description="Income",
        )
        _seed_txn(
            conn,
            account_id=canonical_id,
            category_id=opex_id,
            amount_cents=-10_000,
            txn_date=_month_date(0, 11),
            description="Expense",
        )
        conn.commit()

        no_alias = forecasting.runway(conn, months=1)
        assert no_alias["liquid_balance_cents"] == 150_000

        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_id, canonical_id),
        )
        conn.commit()

        with_alias = forecasting.runway(conn, months=1)
        assert with_alias["liquid_balance_cents"] == 120_000
        assert with_alias["monthly_net_burn_cents"] == no_alias["monthly_net_burn_cents"]


def test_seasonal_pattern_computes_month_of_year_averages(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date="2024-01-15", description="Jan 2024")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=30_000, txn_date="2025-01-15", description="Jan 2025")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=20_000, txn_date="2025-02-15", description="Feb 2025")
        conn.commit()

        pattern = forecasting.seasonal_pattern(conn)

    by_month = {row["month_number"]: row for row in pattern["months"]}
    assert by_month[1]["avg_revenue_cents"] == 20_000
    assert by_month[1]["data_points"] == 2
    assert by_month[2]["avg_revenue_cents"] == 20_000


def test_seasonal_pattern_confidence_levels_and_sparse_output(db_path: Path) -> None:
    with connect(db_path) as conn:
        revenue_id = _seed_revenue_category(conn)
        account_id = _seed_business_account(conn)
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=10_000, txn_date="2023-03-10", description="Mar 2023")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=12_000, txn_date="2024-03-10", description="Mar 2024")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=14_000, txn_date="2025-03-10", description="Mar 2025")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=20_000, txn_date="2024-04-10", description="Apr 2024")
        _seed_txn(conn, account_id=account_id, category_id=revenue_id, amount_cents=18_000, txn_date="2025-04-10", description="Apr 2025")
        conn.commit()

        pattern = forecasting.seasonal_pattern(conn)

    assert len(pattern["months"]) == 12
    by_month = {row["month_number"]: row for row in pattern["months"]}
    assert by_month[3]["confidence"] == "high"
    assert by_month[4]["confidence"] == "low"
    assert by_month[5]["confidence"] == "none"
