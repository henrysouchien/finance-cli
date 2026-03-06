from __future__ import annotations

import calendar
import json
import uuid
from argparse import Namespace
from datetime import date
from pathlib import Path

import pytest

from finance_cli.__main__ import main
from finance_cli.commands import biz_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_category(conn, name: str, parent_name: str | None = None, is_income: int = 0) -> str:
    parent_id = None
    if parent_name:
        parent = conn.execute("SELECT id FROM categories WHERE name = ? LIMIT 1", (parent_name,)).fetchone()
        if parent is None:
            parent_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
                VALUES (?, ?, NULL, 0, 0, 0, 0)
                """,
                (parent_id, parent_name),
            )
        else:
            parent_id = str(parent["id"])

    category_id = uuid.uuid4().hex
    level = 1 if parent_id else 0
    conn.execute(
        """
        INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
        VALUES (?, ?, ?, ?, ?, 0, 0)
        """,
        (category_id, name, parent_id, level, is_income),
    )
    return category_id


def _category_id(conn, category_name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ? LIMIT 1", (category_name,)).fetchone()
    assert row is not None, f"Category not found: {category_name}"
    return str(row["id"])


def _seed_pl_map(conn, category_name: str, pl_section: str, display_order: int) -> None:
    conn.execute(
        """
        INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
        VALUES (?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, _category_id(conn, category_name), pl_section, display_order),
    )


def _seed_schedule_c_map(
    conn,
    category_name: str,
    line: str,
    line_number: str,
    deduction_pct: float,
    tax_year: int = 2025,
) -> None:
    conn.execute(
        """
        INSERT INTO schedule_c_map
            (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            uuid.uuid4().hex,
            _category_id(conn, category_name),
            line,
            line_number,
            deduction_pct,
            tax_year,
        ),
    )


def _seed_business_account(conn, *, balance_cents: int = 0, is_business: int = 1, name: str = "Business Checking") -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active, is_business, source
        ) VALUES (?, 'Test Bank', ?, 'checking', ?, 1, ?, 'manual')
        """,
        (account_id, name, balance_cents, is_business),
    )
    return account_id


def _seed_business_txn(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    date_str: str,
    category_name: str | None,
    use_type: str | None = "Business",
    is_payment: int = 0,
    description: str | None = None,
    source: str = "manual",
    source_category: str | None = None,
) -> str:
    txn_id = uuid.uuid4().hex
    category_id = _category_id(conn, category_name) if category_name else None
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents,
            category_id, source_category, category_source, use_type, is_active, is_payment, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'user', ?, 1, ?, ?)
        """,
        (
            txn_id,
            account_id,
            date_str,
            description or f"txn-{txn_id[:8]}",
            amount_cents,
            category_id,
            source_category,
            use_type,
            is_payment,
            source,
        ),
    )
    return txn_id


def _run_cli(args: list[str], capsys) -> tuple[int, dict]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _month_start(offset_from_current: int) -> date:
    current = date.today().replace(day=1)
    month_index = (current.year * 12) + (current.month - 1) + int(offset_from_current)
    year, month_zero = divmod(month_index, 12)
    return date(year, month_zero + 1, 1)


def _month_date(offset_from_current: int, day: int = 10) -> str:
    start = _month_start(offset_from_current)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return date(start.year, start.month, min(max(day, 1), last_day)).isoformat()


def _freeze_today(monkeypatch, year: int, month: int, day: int) -> None:
    class _FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(year, month, day)

    import finance_cli.forecasting as forecasting

    monkeypatch.setattr(biz_cmd, "date", _FrozenDate)
    monkeypatch.setattr(forecasting, "date", _FrozenDate)


def _pl_args(*, month: str | None = None, quarter: str | None = None, year: str | None = None, compare: bool = False) -> Namespace:
    return Namespace(month=month, quarter=quarter, year=year, compare=compare, format="json")


def _cashflow_args(*, month: str | None = None, quarter: str | None = None, year: str | None = None) -> Namespace:
    return Namespace(month=month, quarter=quarter, year=year, format="json")


def _tax_args(*, month: str | None = None, quarter: str | None = None, year: str | None = None) -> Namespace:
    return Namespace(month=month, quarter=quarter, year=year, format="json")


def _est_args(*, est_quarter: str | None = None, rate: float = 0.30) -> Namespace:
    return Namespace(est_quarter=est_quarter, rate=rate, format="json")


def _budget_set_args(
    *,
    section: str,
    amount: float,
    period: str = "monthly",
    effective_from: str | None = None,
) -> Namespace:
    return Namespace(section=section, amount=amount, period=period, effective_from=effective_from, format="json")


def _budget_status_args(*, month: str | None = None) -> Namespace:
    return Namespace(month=month, format="json")


def _find_line_item(result: dict, line_number: str) -> dict:
    for item in result["data"]["line_items"]:
        if str(item["line_number"]) == line_number:
            return item
    raise AssertionError(f"line item not found: {line_number}")


def _seed_pl_basics(conn, account_id: str) -> None:
    _seed_category(conn, "Test Revenue", parent_name="Income", is_income=1)
    _seed_category(conn, "Test COGS")
    _seed_category(conn, "Test Marketing")
    _seed_category(conn, "Test Software")
    _seed_pl_map(conn, "Test Revenue", "revenue", 10)
    _seed_pl_map(conn, "Test COGS", "cogs", 20)
    _seed_pl_map(conn, "Test Marketing", "opex_marketing", 30)
    _seed_pl_map(conn, "Test Software", "opex_technology", 40)

    _seed_business_txn(conn, account_id=account_id, amount_cents=100_000, date_str="2026-02-10", category_name="Test Revenue")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-20_000, date_str="2026-02-11", category_name="Test COGS")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-5_000, date_str="2026-02-12", category_name="Test Marketing")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-7_000, date_str="2026-02-13", category_name="Test Software")


def test_pl_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_pl_basics(conn, account_id)
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert result["data"]["gross_revenue_cents"] == 100_000
    assert result["data"]["cogs_cents"] == -20_000
    assert result["data"]["gross_profit_cents"] == 80_000
    assert result["data"]["total_opex_cents"] == -12_000
    assert result["data"]["net_income_cents"] == 68_000


def test_pl_monthly_period(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Monthly Revenue", is_income=1)
        _seed_pl_map(conn, "Monthly Revenue", "revenue", 10)
        _seed_business_txn(conn, account_id=account_id, amount_cents=10_000, date_str="2026-01-15", category_name="Monthly Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=20_000, date_str="2026-02-15", category_name="Monthly Revenue")
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-01"), conn)

    assert result["data"]["gross_revenue_cents"] == 10_000


def test_pl_quarterly_period(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Quarter Revenue", is_income=1)
        _seed_pl_map(conn, "Quarter Revenue", "revenue", 10)
        _seed_business_txn(conn, account_id=account_id, amount_cents=10_000, date_str="2026-01-15", category_name="Quarter Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=20_000, date_str="2026-03-15", category_name="Quarter Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=30_000, date_str="2026-04-01", category_name="Quarter Revenue")
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(quarter="2026-Q1"), conn)

    assert result["data"]["gross_revenue_cents"] == 30_000


def test_pl_yearly_period(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Year Revenue", is_income=1)
        _seed_pl_map(conn, "Year Revenue", "revenue", 10)
        _seed_business_txn(conn, account_id=account_id, amount_cents=50_000, date_str="2025-05-01", category_name="Year Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=80_000, date_str="2026-05-01", category_name="Year Revenue")
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(year="2025"), conn)

    assert result["data"]["gross_revenue_cents"] == 50_000


def test_pl_compare(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Compare Revenue", is_income=1)
        _seed_category(conn, "Compare COGS")
        _seed_pl_map(conn, "Compare Revenue", "revenue", 10)
        _seed_pl_map(conn, "Compare COGS", "cogs", 20)
        _seed_business_txn(conn, account_id=account_id, amount_cents=40_000, date_str="2026-02-10", category_name="Compare Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=30_000, date_str="2026-01-10", category_name="Compare Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-2_000, date_str="2026-01-11", category_name="Compare COGS")
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02", compare=True), conn)

    assert result["data"]["compare"] is not None
    by_section = {row["section"]: row for row in result["data"]["compare"]["section_totals"]}
    assert by_section["revenue"]["current_cents"] == 40_000
    assert by_section["revenue"]["prior_cents"] == 30_000
    assert by_section["cogs"]["current_cents"] == 0
    assert by_section["cogs"]["prior_cents"] == -2_000


def test_pl_empty(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Personal Only")
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=-1_000,
            date_str="2026-02-01",
            category_name="Personal Only",
            use_type="Personal",
        )
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert result["data"]["sections"] == {}
    assert result["data"]["gross_revenue_cents"] == 0
    assert result["data"]["cogs_cents"] == 0
    assert result["data"]["total_opex_cents"] == 0
    assert result["data"]["net_income_cents"] == 0


def test_pl_unmapped_categories(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Unmapped Biz Category")
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=-3_000,
            date_str="2026-02-12",
            category_name="Unmapped Biz Category",
        )
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert result["data"]["unmapped_count"] == 1
    assert result["data"]["unmapped"][0]["category_name"] == "Unmapped Biz Category"


def test_pl_unclassified_count(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Unclassified Category")
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=-2_000,
            date_str="2026-02-05",
            category_name="Unclassified Category",
            use_type=None,
        )
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert result["data"]["unclassified_count"] == 1


def test_pl_cli_report_sections(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_pl_basics(conn, account_id)
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert "INCOME STATEMENT" in result["cli_report"]
    assert "Revenue" in result["cli_report"]
    assert "Operating Expenses" in result["cli_report"]
    assert "NET INCOME" in result["cli_report"]


def test_pl_refund_reduces_revenue(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Refunded Revenue", is_income=1)
        _seed_pl_map(conn, "Refunded Revenue", "revenue", 10)
        _seed_business_txn(conn, account_id=account_id, amount_cents=10_000, date_str="2026-02-01", category_name="Refunded Revenue")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-3_000, date_str="2026-02-02", category_name="Refunded Revenue")
        conn.commit()
        result = biz_cmd.handle_pl(_pl_args(month="2026-02"), conn)

    assert result["data"]["gross_revenue_cents"] == 7_000


def test_cashflow_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Cashflow Category")
        _seed_business_txn(conn, account_id=account_id, amount_cents=50_000, date_str="2026-02-01", category_name="Cashflow Category")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-12_000, date_str="2026-02-02", category_name="Cashflow Category")
        _seed_business_txn(conn, account_id=account_id, amount_cents=5_000, date_str="2026-01-15", category_name="Cashflow Category")
        conn.commit()
        result = biz_cmd.handle_cashflow(_cashflow_args(month="2026-02"), conn)

    assert result["data"]["business_income_cents"] == 50_000
    assert result["data"]["business_expense_cents"] == -12_000
    assert result["data"]["net_operating_cash_flow_cents"] == 38_000


def test_cashflow_business_balances(db_path: Path) -> None:
    with connect(db_path) as conn:
        business_a = _seed_business_account(conn, balance_cents=123_456, name="Operating")
        business_b = _seed_business_account(conn, balance_cents=78_900, name="Stripe")
        _seed_business_account(conn, balance_cents=99_999, is_business=0, name="Personal")
        conn.commit()
        result = biz_cmd.handle_cashflow(_cashflow_args(month="2026-02"), conn)

    ids = {row["id"] for row in result["data"]["business_accounts"]}
    assert business_a in ids
    assert business_b in ids
    assert len(result["data"]["business_accounts"]) == 2


def _seed_tax_basics(conn, account_id: str) -> None:
    _seed_category(conn, "Tax Income", is_income=1)
    _seed_category(conn, "Tax COGS")
    _seed_category(conn, "Tax Advertising")
    _seed_category(conn, "Tax Meals")
    _seed_schedule_c_map(conn, "Tax COGS", "COGS (Part III)", "42", 1.0, tax_year=2025)
    _seed_schedule_c_map(conn, "Tax Advertising", "Advertising", "8", 1.0, tax_year=2025)
    _seed_schedule_c_map(conn, "Tax Meals", "Deductible meals", "24b", 0.5, tax_year=2025)

    _seed_business_txn(conn, account_id=account_id, amount_cents=100_000, date_str="2025-01-15", category_name="Tax Income")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-20_000, date_str="2025-01-16", category_name="Tax COGS")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-10_000, date_str="2025-01-20", category_name="Tax Advertising")
    _seed_business_txn(conn, account_id=account_id, amount_cents=-5_000, date_str="2025-02-20", category_name="Tax Meals")


def test_tax_schedule_c_flow(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_tax_basics(conn, account_id)
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert result["data"]["line_1_gross_receipts_cents"] == 100_000
    assert result["data"]["line_4_cogs_cents"] == 20_000
    assert result["data"]["line_7_gross_income_cents"] == 80_000
    assert result["data"]["line_28_total_expenses_cents"] == 12_500
    assert result["data"]["line_31_net_profit_cents"] == 67_500


def test_tax_meals_deduction(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Meals Income", is_income=1)
        _seed_category(conn, "Meals Expense")
        _seed_schedule_c_map(conn, "Meals Expense", "Deductible meals", "24b", 0.5, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=20_000, date_str="2025-03-01", category_name="Meals Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-5_001, date_str="2025-03-02", category_name="Meals Expense")
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    meals = _find_line_item(result, "24b")
    assert meals["actual_cents"] == 5_001
    assert meals["deductible_cents"] == 2_501


def test_tax_cogs_not_in_line_28(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Flow Income", is_income=1)
        _seed_category(conn, "Flow COGS")
        _seed_category(conn, "Flow Expense")
        _seed_schedule_c_map(conn, "Flow COGS", "COGS (Part III)", "42", 1.0, tax_year=2025)
        _seed_schedule_c_map(conn, "Flow Expense", "Advertising", "8", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=50_000, date_str="2025-01-10", category_name="Flow Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-10_000, date_str="2025-01-11", category_name="Flow COGS")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-3_000, date_str="2025-01-12", category_name="Flow Expense")
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert result["data"]["line_4_cogs_cents"] == 10_000
    assert result["data"]["line_28_total_expenses_cents"] == 3_000


def test_tax_quarterly_breakdown(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Quarter Income", is_income=1)
        _seed_category(conn, "Quarter Expense")
        _seed_schedule_c_map(conn, "Quarter Expense", "Advertising", "8", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=40_000, date_str="2025-02-01", category_name="Quarter Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-10_000, date_str="2025-02-02", category_name="Quarter Expense")
        _seed_business_txn(conn, account_id=account_id, amount_cents=20_000, date_str="2025-05-01", category_name="Quarter Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-2_000, date_str="2025-05-02", category_name="Quarter Expense")
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    by_q = {row["quarter"]: row for row in result["data"]["quarterly_breakdown"]}
    assert by_q[1]["net_profit_cents"] == 30_000
    assert by_q[2]["net_profit_cents"] == 18_000


def test_tax_line_sort_order(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Sort Income", is_income=1)
        _seed_category(conn, "Sort Ten")
        _seed_category(conn, "Sort Eight")
        _seed_category(conn, "Sort Seventeen")
        _seed_schedule_c_map(conn, "Sort Ten", "Commissions and fees", "10", 1.0, tax_year=2025)
        _seed_schedule_c_map(conn, "Sort Eight", "Advertising", "8", 1.0, tax_year=2025)
        _seed_schedule_c_map(conn, "Sort Seventeen", "Legal and professional services", "17", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=20_000, date_str="2025-01-10", category_name="Sort Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-1_000, date_str="2025-01-11", category_name="Sort Ten")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-1_000, date_str="2025-01-12", category_name="Sort Eight")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-1_000, date_str="2025-01-13", category_name="Sort Seventeen")
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert [item["line_number"] for item in result["data"]["line_items"]] == ["8", "10", "17"]


def test_tax_empty(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_business_account(conn)
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert result["data"]["line_1_gross_receipts_cents"] == 0
    assert result["data"]["line_28_total_expenses_cents"] == 0
    assert result["data"]["line_31_net_profit_cents"] == 0
    assert result["data"]["line_items"] == []


def test_tax_cli_report(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_tax_basics(conn, account_id)
        conn.commit()
        result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

    assert "SCHEDULE C SUMMARY" in result["cli_report"]
    assert "Part I: Income" in result["cli_report"]
    assert "Part II: Expenses" in result["cli_report"]
    assert "Line 30" in result["cli_report"]
    assert "N/A (not yet configured)" in result["cli_report"]


def test_estimated_tax_basic(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Est Income", is_income=1)
        _seed_category(conn, "Est Expense")
        _seed_schedule_c_map(conn, "Est Expense", "Advertising", "8", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=100_000, date_str="2025-01-15", category_name="Est Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-20_000, date_str="2025-02-15", category_name="Est Expense")
        conn.commit()
        result = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q2", rate=0.30), conn)

    assert result["data"]["ytd_net_profit_cents"] == 80_000
    assert result["data"]["annualized_profit_cents"] == 160_000
    assert result["data"]["estimated_annual_tax_cents"] == 48_000
    assert result["data"]["estimated_quarterly_payment_cents"] == 12_000


def test_estimated_tax_custom_rate(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Rate Income", is_income=1)
        _seed_category(conn, "Rate Expense")
        _seed_schedule_c_map(conn, "Rate Expense", "Advertising", "8", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=60_000, date_str="2025-01-05", category_name="Rate Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-10_000, date_str="2025-02-05", category_name="Rate Expense")
        conn.commit()
        result = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q2", rate=0.25), conn)

    assert result["data"]["ytd_net_profit_cents"] == 50_000
    assert result["data"]["annualized_profit_cents"] == 100_000
    assert result["data"]["estimated_annual_tax_cents"] == 25_000
    assert result["data"]["estimated_quarterly_payment_cents"] == 6_250


def test_estimated_tax_loss_floors_to_zero(db_path: Path) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Loss Income", is_income=1)
        _seed_category(conn, "Loss Expense")
        _seed_schedule_c_map(conn, "Loss Expense", "Advertising", "8", 1.0, tax_year=2025)
        _seed_business_txn(conn, account_id=account_id, amount_cents=10_000, date_str="2025-01-10", category_name="Loss Income")
        _seed_business_txn(conn, account_id=account_id, amount_cents=-20_000, date_str="2025-01-11", category_name="Loss Expense")
        conn.commit()
        result = biz_cmd.handle_estimated_tax(_est_args(est_quarter="2025-Q1", rate=0.30), conn)

    assert result["data"]["ytd_net_profit_cents"] == -10_000
    assert result["data"]["annualized_profit_cents"] == 0
    assert result["data"]["estimated_annual_tax_cents"] == 0
    assert result["data"]["estimated_quarterly_payment_cents"] == 0


def test_parse_period_rejects_multiple_arguments(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="Provide only one of --month, --quarter, or --year"):
            biz_cmd.handle_pl(_pl_args(month="2026-02", quarter="2026-Q1"), conn)


def test_cli_biz_forecast_json(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Forecast Revenue", is_income=1)
        _seed_pl_map(conn, "Forecast Revenue", "revenue", 10)
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=15_000,
            date_str=_month_date(-1),
            category_name="Forecast Revenue",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=25_000,
            date_str=_month_date(0),
            category_name="Forecast Revenue",
        )
        conn.commit()

    code, payload = _run_cli(["biz", "forecast", "--months", "2", "--format", "json"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "biz.forecast"
    assert len(payload["data"]["totals"]) == 2
    assert payload["summary"]["months"] == 2


def test_cli_biz_forecast_streams_json(db_path: Path, capsys) -> None:
    rules_path = db_path.parent / "rules.yaml"
    rules_path.write_text(
        """
revenue_streams:
  - name: Stripe
    match:
      source: stripe
      source_category: charge
  - name: Kartra
    match:
      keywords: ["KARTRA PAYOUT"]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Stream Revenue", is_income=1)
        _seed_pl_map(conn, "Stream Revenue", "revenue", 10)
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=11_000,
            date_str=_month_date(0),
            category_name="Stream Revenue",
            description="Stripe payout",
            source="stripe",
            source_category="charge",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=9_000,
            date_str=_month_date(0, 11),
            category_name="Stream Revenue",
            description="KARTRA PAYOUT batch",
        )
        conn.commit()

    code, payload = _run_cli(["biz", "forecast", "--months", "1", "--streams", "--format", "json"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "biz.forecast"
    streams = {row["name"] for row in payload["data"]["streams"]}
    assert {"Stripe", "Kartra"} <= streams


def test_cli_biz_runway_json(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn, balance_cents=120_000)
        _seed_category(conn, "Runway Revenue", is_income=1)
        _seed_category(conn, "Runway Expense")
        _seed_pl_map(conn, "Runway Revenue", "revenue", 10)
        _seed_pl_map(conn, "Runway Expense", "opex_other", 80)
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=10_000,
            date_str=_month_date(-1),
            category_name="Runway Revenue",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=10_000,
            date_str=_month_date(0),
            category_name="Runway Revenue",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=-40_000,
            date_str=_month_date(-1, 11),
            category_name="Runway Expense",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=-40_000,
            date_str=_month_date(0, 11),
            category_name="Runway Expense",
        )
        conn.commit()

    code, payload = _run_cli(["biz", "runway", "--months", "2", "--format", "json"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "biz.runway"
    assert payload["data"]["liquid_balance_cents"] == 120_000
    assert payload["data"]["monthly_net_burn_cents"] == 30_000
    assert payload["data"]["runway_months"] is not None


def test_cli_biz_seasonal_json(db_path: Path, capsys) -> None:
    with connect(db_path) as conn:
        account_id = _seed_business_account(conn)
        _seed_category(conn, "Seasonal Revenue", is_income=1)
        _seed_pl_map(conn, "Seasonal Revenue", "revenue", 10)
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=10_000,
            date_str="2024-01-10",
            category_name="Seasonal Revenue",
        )
        _seed_business_txn(
            conn,
            account_id=account_id,
            amount_cents=20_000,
            date_str="2025-01-10",
            category_name="Seasonal Revenue",
        )
        conn.commit()

    code, payload = _run_cli(["biz", "seasonal", "--format", "json"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "biz.seasonal"
    assert len(payload["data"]["months"]) == 12


class TestBizBudget:
    def test_budget_set_basic(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            result = biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=500,
                    period="monthly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            row = conn.execute(
                """
                SELECT id, pl_section, amount_cents, period, effective_from
                  FROM biz_section_budgets
                 WHERE id = ?
                """,
                (result["data"]["id"],),
            ).fetchone()

        assert row is not None
        assert result["data"]["pl_section"] == "opex_marketing"
        assert result["data"]["section_label"] == "Marketing"
        assert result["data"]["amount_cents"] == 50_000
        assert result["data"]["period"] == "monthly"
        assert result["data"]["effective_from"] == "2026-01-01"
        assert row["pl_section"] == "opex_marketing"
        assert row["amount_cents"] == 50_000

    def test_budget_status_no_budgets(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        assert result["data"]["rows"] == []
        assert result["summary"]["section_count"] == 0
        assert "BUSINESS BUDGET STATUS - 2026-01" in result["cli_report"]
        assert "  (none)" in result["cli_report"]

    def test_budget_status_with_actuals(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Budget Marketing")
            _seed_pl_map(conn, "Budget Marketing", "opex_marketing", 30)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-30_000,
                date_str="2026-01-10",
                category_name="Budget Marketing",
            )
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=500,
                    period="monthly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            conn.commit()
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        rows = {row["pl_section"]: row for row in result["data"]["rows"]}
        marketing = rows["opex_marketing"]
        assert marketing["budget_cents"] == 50_000
        assert marketing["monthly_budget_cents"] == 50_000
        assert marketing["actual_cents"] == 30_000
        assert marketing["remaining_cents"] == 20_000
        assert marketing["pct_used"] == 60.0

    def test_budget_status_without_budget_shows_actuals(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Budget Tech")
            _seed_pl_map(conn, "Budget Tech", "opex_technology", 40)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-10_000,
                date_str="2026-01-12",
                category_name="Budget Tech",
            )
            conn.commit()
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        rows = {row["pl_section"]: row for row in result["data"]["rows"]}
        tech = rows["opex_technology"]
        assert tech["budget_cents"] is None
        assert tech["monthly_budget_cents"] is None
        assert tech["actual_cents"] == 10_000
        assert tech["remaining_cents"] is None
        assert tech["pct_used"] is None

    def test_budget_invalid_section(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            with pytest.raises(ValueError, match="section must be one of"):
                biz_cmd.handle_biz_budget_set(
                    _budget_set_args(
                        section="revenue",
                        amount=500,
                        period="monthly",
                        effective_from="2026-01-01",
                    ),
                    conn,
                )

    def test_budget_cli_columns(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Budget Marketing")
            _seed_pl_map(conn, "Budget Marketing", "opex_marketing", 30)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-30_000,
                date_str="2026-01-10",
                category_name="Budget Marketing",
            )
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=500,
                    period="monthly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            conn.commit()
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        report = result["cli_report"]
        assert "Section" in report
        assert "Budget" in report
        assert "Actual" in report
        assert "Remaining" in report
        assert "Used%" in report
        assert "Marketing" in report
        assert "$500.00" in report
        assert "$300.00" in report
        assert "$200.00" in report
        assert "60.0%" in report

    def test_budget_zero_amount(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=0,
                    period="monthly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        row = next(row for row in result["data"]["rows"] if row["pl_section"] == "opex_marketing")
        assert row["monthly_budget_cents"] == 0
        assert row["pct_used"] is None
        assert row["remaining_cents"] is None
        assert "—" in result["cli_report"]

    def test_budget_quarterly_normalization(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=900,
                    period="quarterly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        row = next(row for row in result["data"]["rows"] if row["pl_section"] == "opex_marketing")
        assert row["budget_cents"] == 90_000
        assert row["monthly_budget_cents"] == 30_000

    def test_budget_yearly_normalization(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=12000,
                    period="yearly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        row = next(row for row in result["data"]["rows"] if row["pl_section"] == "opex_marketing")
        assert row["budget_cents"] == 1_200_000
        assert row["monthly_budget_cents"] == 100_000

    def test_budget_set_default_effective_from(self, db_path: Path, monkeypatch) -> None:
        _freeze_today(monkeypatch, 2026, 3, 17)
        with connect(db_path) as conn:
            result = biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=500,
                    period="monthly",
                    effective_from=None,
                ),
                conn,
            )

        assert result["data"]["effective_from"] == "2026-03-01"

    def test_budget_small_quarterly_zero_normalized(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=0.02,
                    period="quarterly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        row = next(row for row in result["data"]["rows"] if row["pl_section"] == "opex_marketing")
        assert row["budget_cents"] == 2
        assert row["monthly_budget_cents"] == 0
        assert row["pct_used"] is None
        assert "—" in result["cli_report"]

    def test_budget_refund_reduces_usage(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Budget Marketing")
            _seed_pl_map(conn, "Budget Marketing", "opex_marketing", 30)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-50_000,
                date_str="2026-01-10",
                category_name="Budget Marketing",
            )
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=10_000,
                date_str="2026-01-11",
                category_name="Budget Marketing",
            )
            biz_cmd.handle_biz_budget_set(
                _budget_set_args(
                    section="opex_marketing",
                    amount=500,
                    period="monthly",
                    effective_from="2026-01-01",
                ),
                conn,
            )
            conn.commit()
            result = biz_cmd.handle_biz_budget_status(_budget_status_args(month="2026-01"), conn)

        row = next(row for row in result["data"]["rows"] if row["pl_section"] == "opex_marketing")
        assert row["actual_cents"] == 40_000
        assert row["pct_used"] == 80.0

    def test_budget_set_invalid_date_rejected(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            with pytest.raises(ValueError, match="YYYY-MM-DD"):
                biz_cmd.handle_biz_budget_set(
                    _budget_set_args(
                        section="opex_marketing",
                        amount=500,
                        period="monthly",
                        effective_from="bad-date",
                    ),
                    conn,
                )


class TestGoldenOutputs:
    def test_pl_golden_output(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Golden Revenue", is_income=1)
            _seed_category(conn, "Golden Marketing")
            _seed_pl_map(conn, "Golden Revenue", "revenue", 10)
            _seed_pl_map(conn, "Golden Marketing", "opex_marketing", 30)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=500_000,
                date_str="2026-01-10",
                category_name="Golden Revenue",
            )
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-150_000,
                date_str="2026-01-11",
                category_name="Golden Marketing",
            )
            conn.commit()
            result = biz_cmd.handle_pl(_pl_args(month="2026-01"), conn)

        assert result["cli_report"].splitlines() == [
            "INCOME STATEMENT - January 2026",
            "",
            "Revenue",
            "  Golden Revenue                          $5,000.00",
            "  ----------------------------------------------------",
            "  Gross Revenue                           $5,000.00",
            "",
            "Cost of Goods Sold",
            "  (none)",
            "  ----------------------------------------------------",
            "  Gross Profit                            $5,000.00",
            "",
            "Operating Expenses",
            "  Marketing",
            "    Golden Marketing                      $1,500.00",
            "  ----------------------------------------------------",
            "  Total Operating Expenses                $1,500.00",
            "",
            "NET INCOME                                    $3,500.00",
            "",
            "WARNING: Unclassified transactions (NULL use_type): 0",
        ]

    def test_cashflow_golden_output(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn, balance_cents=900_000)
            _seed_category(conn, "Cashflow Revenue", is_income=1)
            _seed_category(conn, "Cashflow Expense")
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=200_000,
                date_str="2026-01-10",
                category_name="Cashflow Revenue",
            )
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-80_000,
                date_str="2026-01-11",
                category_name="Cashflow Expense",
            )
            conn.commit()
            result = biz_cmd.handle_cashflow(_cashflow_args(month="2026-01"), conn)

        assert result["cli_report"].splitlines() == [
            "CASH FLOW - January 2026",
            "",
            "Operating Activities",
            "  Business income received                $2,000.00",
            "  Business expenses paid                  ($800.00)",
            "  ----------------------------------------------------",
            "  Net Operating Cash Flow                 $1,200.00",
            "",
            "Business Account Balances",
            "  Test Bank Business Checking             $9,000.00",
            "",
            "WARNING: Unclassified transactions (NULL use_type): 0",
        ]

    def test_forecast_golden_output(self, db_path: Path, monkeypatch) -> None:
        _freeze_today(monkeypatch, 2026, 3, 15)
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_category(conn, "Forecast Revenue", is_income=1)
            _seed_pl_map(conn, "Forecast Revenue", "revenue", 10)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=100_000,
                date_str="2026-02-10",
                category_name="Forecast Revenue",
            )
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=120_000,
                date_str="2026-03-10",
                category_name="Forecast Revenue",
            )
            conn.commit()
            result = biz_cmd.handle_forecast(Namespace(months=2, streams=False, format="json"), conn)

        lines = result["cli_report"].splitlines()
        assert lines[:6] == [
            "REVENUE FORECAST - Last 2 month(s)",
            "",
            "Monthly Totals",
            "  2026-02           $1,000.00",
            "  2026-03           $1,200.00",
            "",
        ]
        assert lines[-1] == "WARNING: Unclassified transactions (NULL use_type): 0"
        assert "Trend: ↑ $200.00/mo" in result["cli_report"]
        assert "Projected next month: $1,400.00" in result["cli_report"]

    def test_runway_golden_output(self, db_path: Path, monkeypatch) -> None:
        _freeze_today(monkeypatch, 2026, 3, 15)
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn, balance_cents=120_000)
            _seed_category(conn, "Runway Expense")
            _seed_pl_map(conn, "Runway Expense", "opex_other", 80)
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-40_000,
                date_str="2026-02-10",
                category_name="Runway Expense",
            )
            _seed_business_txn(
                conn,
                account_id=account_id,
                amount_cents=-40_000,
                date_str="2026-03-10",
                category_name="Runway Expense",
            )
            conn.commit()
            result = biz_cmd.handle_runway(Namespace(months=2, format="json"), conn)

        report = result["cli_report"]
        assert "RUNWAY DASHBOARD - Last 2 month(s)" in report
        assert "Avg Monthly Expenses" in report and "$400.00" in report
        assert "Monthly Net Burn" in report and "$400.00" in report
        assert "Liquid Cash Balance" in report and "$1,200.00" in report
        assert "Runway: 3.00 months (through 2026-06-13)" in report
        assert "Expense Breakdown" in report
        assert "Other" in report and "$400.00" in report

    def test_seasonal_golden_empty(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            result = biz_cmd.handle_seasonal(Namespace(format="json"), conn)

        assert result["cli_report"].splitlines() == [
            "SEASONAL REVENUE PATTERN",
            "",
            "History months available: 0",
            "WARNING: Fewer than 12 historical months available; seasonality confidence is limited.",
            "",
            "  Month       Avg Revenue   Pts    Conf  Bar",
            "  ------------------------------------------------------------",
            "  Jan               $0.00     0    none  ",
            "  Feb               $0.00     0    none  ",
            "  Mar               $0.00     0    none  ",
            "  Apr               $0.00     0    none  ",
            "  May               $0.00     0    none  ",
            "  Jun               $0.00     0    none  ",
            "  Jul               $0.00     0    none  ",
            "  Aug               $0.00     0    none  ",
            "  Sep               $0.00     0    none  ",
            "  Oct               $0.00     0    none  ",
            "  Nov               $0.00     0    none  ",
            "  Dec               $0.00     0    none  ",
            "",
            "WARNING: Unclassified transactions (NULL use_type): 0",
        ]

    def test_tax_golden_with_amounts(self, db_path: Path) -> None:
        with connect(db_path) as conn:
            account_id = _seed_business_account(conn)
            _seed_tax_basics(conn, account_id)
            conn.commit()
            result = biz_cmd.handle_tax(_tax_args(year="2025"), conn)

        report = result["cli_report"]
        assert "SCHEDULE C SUMMARY - Tax Year 2025 (2025)" in report
        assert "Part I: Income" in report
        assert "Line 1   Gross receipts                          $1,000.00" in report
        assert "Line 4   Cost of goods sold                      ($200.00)" in report
        assert "Line 7   Gross income                              $800.00" in report
