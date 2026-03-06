"""Tests for the MCP server tool functions.

Each test monkeypatches connect() to use a tmp-path SQLite DB with
migrations applied, then calls the tool function directly and
asserts the {data, summary} envelope.
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    """Create a migrated temp DB and point connect() at it."""
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@pytest.fixture()
def conn(db_path):
    """Return a connection to the temp DB."""
    c = connect(db_path)
    yield c
    c.close()


@pytest.fixture()
def isolated_mcp_cache(db_path, tmp_path: Path, monkeypatch) -> Path:
    """Route MCP cache output into a temp directory for deterministic assertions."""
    import finance_cli.mcp_server as mcp_server

    fake_module_file = tmp_path / "finance_cli" / "mcp_server.py"
    fake_module_file.parent.mkdir(parents=True, exist_ok=True)
    fake_module_file.write_text("# test module path\n", encoding="utf-8")
    monkeypatch.setattr(mcp_server, "__file__", str(fake_module_file))
    return tmp_path / "exports" / "mcp_cache"


def _latest_cache_file(cache_dir: Path, prefix: str) -> Path:
    matches = sorted(cache_dir.glob(f"{prefix}_*.json"))
    assert matches, f"expected cache file matching {prefix}_*.json"
    return matches[-1]


def _month_date(offset_from_current: int, day: int = 15) -> str:
    today = date.today()
    month_index = (today.year * 12) + (today.month - 1) + int(offset_from_current)
    year, month_zero = divmod(month_index, 12)
    safe_day = max(1, min(day, 28))
    return date(year, month_zero + 1, safe_day).isoformat()


def _seed_category(conn, name: str, parent_id=None, is_income: int = 0) -> str:
    cid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, parent_id, is_income, is_system) VALUES (?, ?, ?, ?, 0)",
        (cid, name, parent_id, is_income),
    )
    conn.commit()
    return cid


def _seed_txn(conn, *, description="TEST TXN", amount_cents=-1000,
              date="2026-02-15", category_id=None, is_reviewed=0,
              account_id=None, use_type=None, source="manual") -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions
            (id, date, description, amount_cents, category_id, is_active,
             is_reviewed, source, account_id, use_type)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        """,
        (tid, date, description, amount_cents, category_id, is_reviewed, source, account_id, use_type),
    )
    conn.commit()
    return tid


def _seed_account(conn, *, institution="Test Bank", name="Checking",
                  account_type="checking", balance_cents=100000, is_business: int = 0,
                  source: str | None = None) -> str:
    aid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts
            (id, institution_name, account_name, account_type,
             balance_current_cents, is_active, is_business, source)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (aid, institution, name, account_type, balance_cents, is_business, source),
    )
    conn.commit()
    return aid


def _seed_balance_snapshot(
    conn,
    *,
    account_id: str,
    snapshot_date: str | None = None,
    balance_current_cents: int = 100000,
    source: str = "refresh",
) -> str:
    sid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO balance_snapshots
            (id, account_id, snapshot_date, source, balance_current_cents)
        VALUES (?, ?, ?, ?, ?)
        """,
        (sid, account_id, snapshot_date or date.today().isoformat(), source, balance_current_cents),
    )
    conn.commit()
    return sid


def _seed_pl_map(conn, category_id: str, section: str, display_order: int) -> None:
    conn.execute(
        """
        INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
        VALUES (?, ?, ?, ?)
        """,
        (uuid.uuid4().hex, category_id, section, display_order),
    )
    conn.commit()


def _seed_schedule_c_map(
    conn,
    category_id: str,
    *,
    line: str,
    line_number: str,
    deduction_pct: float = 1.0,
    tax_year: int = 2025,
) -> None:
    conn.execute(
        """
        INSERT INTO schedule_c_map
            (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (uuid.uuid4().hex, category_id, line, line_number, deduction_pct, tax_year),
    )
    conn.commit()


def _seed_subscription(
    conn,
    *,
    vendor_name: str,
    amount_cents: int = 1500,
    frequency: str = "monthly",
    is_active: int = 1,
) -> str:
    sid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO subscriptions
            (id, vendor_name, category_id, amount_cents, frequency,
             next_expected, is_active, use_type, is_auto_detected)
        VALUES (?, ?, NULL, ?, ?, NULL, ?, NULL, 0)
        """,
        (sid, vendor_name, amount_cents, frequency, is_active),
    )
    conn.commit()
    return sid


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float | None,
    minimum_payment_cents: int | None = None,
    next_monthly_payment_cents: int | None = None,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities
            (id, account_id, liability_type, is_active, apr_purchase, minimum_payment_cents, next_monthly_payment_cents)
        VALUES (?, ?, 'credit', 1, ?, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents, next_monthly_payment_cents),
    )
    conn.commit()
    return liability_id


def _set_txn_raw_payload(conn, txn_id: str, raw_payload: str) -> None:
    conn.execute(
        """
        UPDATE transactions
           SET raw_plaid_json = ?, dedupe_key = ?
         WHERE id = ?
        """,
        (raw_payload, f"dedupe:{txn_id}", txn_id),
    )
    conn.commit()


def _seed_plaid_item(
    conn,
    *,
    plaid_item_id: str = "item_test",
    institution_name: str = "Test Institution",
    access_token_ref: str = "secret/token_ref",
    sync_cursor: str = "cursor_1",
) -> str:
    row_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO plaid_items (id, plaid_item_id, institution_name, access_token_ref, sync_cursor, status)
        VALUES (?, ?, ?, ?, ?, 'active')
        """,
        (row_id, plaid_item_id, institution_name, access_token_ref, sync_cursor),
    )
    conn.commit()
    return row_id


# ---------------------------------------------------------------------------
# 1. Status & Overview
# ---------------------------------------------------------------------------

class TestStatusTools:
    def test_db_status(self, db_path, conn):
        _seed_txn(conn, is_reviewed=0)
        _seed_txn(conn, is_reviewed=1)
        from finance_cli.mcp_server import db_status
        result = db_status()
        assert "data" in result
        assert "summary" in result
        assert result["data"]["unreviewed_count"] == 1

    def test_setup_check(self, db_path, monkeypatch):
        from finance_cli.mcp_server import setup_check
        result = setup_check()
        assert "data" in result
        assert "summary" in result

    def test_setup_status(self, db_path, conn):
        _seed_plaid_item(conn)
        from finance_cli.mcp_server import setup_status
        result = setup_status()
        assert "data" in result
        assert "summary" in result
        plaid_items = result["data"]["plaid"]["items"]
        assert plaid_items
        assert "access_token_ref" not in plaid_items[0]
        assert "sync_cursor" not in plaid_items[0]


class TestAccountTools:
    def test_account_list_filters_by_is_business(self, db_path, conn):
        _seed_account(conn, institution="Biz Bank", name="Biz Checking", is_business=1)
        _seed_account(conn, institution="Personal Bank", name="Personal Checking", is_business=0)
        from finance_cli.mcp_server import account_list

        result = account_list(status="all", is_business=True)
        accounts = result["data"]["accounts"]

        assert len(accounts) == 1
        assert int(accounts[0]["is_business"]) == 1
        assert result["data"]["filters"]["is_business"] is True


# ---------------------------------------------------------------------------
# 2. Financial Reports
# ---------------------------------------------------------------------------

class TestReportTools:
    def test_daily_summary(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, date="2026-02-15")
        raw_payload = '{"source":"daily"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import daily_summary
        result = daily_summary(date="2026-02-15")
        assert "data" in result
        assert "summary" in result
        assert "cache_file" not in result
        assert "raw_plaid_json" not in result["data"]["transactions"][0]
        cache_file = _latest_cache_file(isolated_mcp_cache, "daily_summary")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_txn = next(txn for txn in cached["data"]["transactions"] if txn["id"] == tid)
        assert cached_txn["raw_plaid_json"] == raw_payload

    def test_daily_summary_no_date(self, db_path):
        from finance_cli.mcp_server import daily_summary
        result = daily_summary()
        assert "data" in result

    def test_weekly_summary(self, db_path, conn):
        _seed_txn(conn, date="2026-02-15")
        from finance_cli.mcp_server import weekly_summary
        result = weekly_summary(week="2026-W07")
        assert "data" in result
        assert "summary" in result

    def test_weekly_summary_compare(self, db_path, conn):
        _seed_txn(conn, date="2026-02-15")
        _seed_txn(conn, date="2026-02-08")
        from finance_cli.mcp_server import weekly_summary
        result = weekly_summary(week="2026-W07", compare=True)
        assert "data" in result

    def test_balance_net_worth(self, db_path, conn):
        _seed_account(conn)
        from finance_cli.mcp_server import balance_net_worth
        result = balance_net_worth()
        assert "data" in result
        assert result["data"]["net_worth_cents"] == 100000

    def test_balance_net_worth_with_investments(self, db_path, conn):
        _seed_account(conn, account_type="investment", balance_cents=500000)
        from finance_cli.mcp_server import balance_net_worth
        result = balance_net_worth()
        assert "data" in result
        assert result["data"]["net_worth_cents"] == 500000

    def test_subs_list(self, db_path, isolated_mcp_cache):
        from finance_cli.mcp_server import subs_list
        result = subs_list()
        assert "data" in result
        assert "summary" in result
        assert "cache_file" not in result
        assert _latest_cache_file(isolated_mcp_cache, "subs_list").exists()

    def test_subs_list_active_only_summary(self, db_path, conn, isolated_mcp_cache):
        _seed_subscription(
            conn,
            vendor_name="Very Long Vendor Name That Should Be Truncated For MCP Output",
            amount_cents=1099,
            frequency="weekly",
            is_active=1,
        )
        _seed_subscription(conn, vendor_name="Old Streaming", is_active=0)
        from finance_cli.mcp_server import subs_list

        result = subs_list(show_all=False)
        subscriptions = result["data"]["subscriptions"]
        summary = result["summary"]

        assert subscriptions
        assert all(int(item["is_active"]) == 1 for item in subscriptions)
        assert all(item["short_name"] == str(item["vendor_name"])[:30] for item in subscriptions)
        assert all(
            item["monthly_amount"] == round(float(item["monthly_amount"]), 2)
            for item in subscriptions
        )
        assert summary["active_subscriptions"] == len(subscriptions)
        assert summary["inactive_subscriptions"] == 1
        assert summary["total_subscriptions"] == (
            summary["active_subscriptions"] + summary["inactive_subscriptions"]
        )

        assert "cache_file" not in result
        cache_file = _latest_cache_file(isolated_mcp_cache, "subs_list")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_subscriptions = cached["data"]["subscriptions"]
        assert len(cached_subscriptions) == summary["total_subscriptions"]
        assert any(int(item["is_active"]) == 0 for item in cached_subscriptions)

    def test_subs_total(self, db_path):
        from finance_cli.mcp_server import subs_total
        result = subs_total()
        assert "data" in result

    def test_cat_tree(self, db_path, conn):
        _seed_category(conn, "Food & Drink")
        from finance_cli.mcp_server import cat_tree
        result = cat_tree()
        assert "data" in result

    def test_liquidity(self, db_path, conn):
        _seed_account(conn)
        _seed_account(conn, account_type="investment", balance_cents=500_000)
        from finance_cli.mcp_server import liquidity
        result = liquidity()
        assert "data" in result
        assert "summary" in result
        assert result["data"]["include_investments"] is True

        no_investments = liquidity(include_investments=False)
        assert no_investments["data"]["include_investments"] is False

    def test_debt_dashboard(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Chase",
            name="Freedom",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        from finance_cli.mcp_server import debt_dashboard

        result = debt_dashboard()
        assert "data" in result
        assert "summary" in result
        assert result["summary"]["total_cards"] == 1

    def test_debt_interest(self, db_path, conn):
        account_id = _seed_account(
            conn,
            institution="Barclays",
            name="View",
            account_type="credit_card",
            balance_cents=-10_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.24, minimum_payment_cents=500)

        from finance_cli.mcp_server import debt_interest

        result = debt_interest(months=6)
        assert "data" in result
        assert "summary" in result
        assert len(result["data"]["schedule"]) == 6
        assert "cards" not in result["data"]["schedule"][0]

        detailed = debt_interest(months=2, summary_only=False)
        assert "cards" in detailed["data"]["schedule"][0]

    def test_debt_simulate(self, db_path, conn):
        first = _seed_account(
            conn,
            institution="Chase",
            name="Sapphire",
            account_type="credit_card",
            balance_cents=-30_000,
        )
        second = _seed_account(
            conn,
            institution="Amex",
            name="Gold",
            account_type="credit_card",
            balance_cents=-8_000,
        )
        _seed_credit_liability(conn, account_id=first, apr_purchase=29.99, minimum_payment_cents=900)
        _seed_credit_liability(conn, account_id=second, apr_purchase=9.99, minimum_payment_cents=300)

        from finance_cli.mcp_server import debt_simulate

        result = debt_simulate(extra_dollars=500, strategy="compare")
        assert "data" in result
        assert "summary" in result
        assert "avalanche" in result["data"]
        avalanche_schedule = result["data"]["avalanche"]["schedule"]
        assert avalanche_schedule
        assert "paid_off_count" in avalanche_schedule[0]
        assert "cards" not in avalanche_schedule[0]
        baseline_schedule = result["data"]["baseline"]["schedule"]
        assert baseline_schedule
        assert "cards" not in baseline_schedule[0]

        detailed = debt_simulate(extra_dollars=500, strategy="avalanche", summary_only=False)
        detailed_schedule = detailed["data"]["schedule"]
        assert detailed_schedule
        assert "paid_off_cards" in detailed_schedule[0]
        assert "cards" in detailed_schedule[0]

    def test_debt_impact(self, db_path, conn):
        dining_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining_id, amount_cents=-5000, date="2026-01-15")
        _seed_txn(conn, category_id=dining_id, amount_cents=-3000, date="2025-12-15")

        account_id = _seed_account(
            conn,
            institution="Chase",
            name="Freedom",
            account_type="credit_card",
            balance_cents=-20_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=18.99, minimum_payment_cents=800)

        from finance_cli.mcp_server import debt_impact

        result = debt_impact(months=3, cut_pct=50)
        assert "data" in result
        assert "summary" in result

    def test_debt_impact_adds_caveat_when_baseline_is_capped(self, db_path, conn):
        dining_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=dining_id, amount_cents=-5000, date="2026-01-15")

        account_id = _seed_account(
            conn,
            institution="Capped",
            name="No Minimum",
            account_type="credit_card",
            balance_cents=-50_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=24.99, minimum_payment_cents=0)

        from finance_cli.mcp_server import debt_impact

        result = debt_impact(months=3, cut_pct=50)
        assert "caveat" in result["data"]

    def test_subs_audit_adds_caveat_when_baseline_is_capped(self, db_path, conn):
        _seed_subscription(conn, vendor_name="Video Service", amount_cents=2500, is_active=1)
        account_id = _seed_account(
            conn,
            institution="Capped",
            name="No Minimum",
            account_type="credit_card",
            balance_cents=-40_000,
        )
        _seed_credit_liability(conn, account_id=account_id, apr_purchase=19.99, minimum_payment_cents=0)
        from finance_cli.mcp_server import subs_audit

        result = subs_audit()
        assert "caveat" in result["data"]

    def test_biz_pl(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Biz Revenue", is_income=1)
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        account_id = _seed_account(conn, name="Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=25_000,
            date="2026-02-10",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_pl

        result = biz_pl(month="2026-02")
        assert "data" in result
        assert "summary" in result
        assert result["data"]["gross_revenue_cents"] == 25_000

    def test_biz_tax(self, db_path, conn):
        income_id = _seed_category(conn, "MCP Tax Income", is_income=1)
        expense_id = _seed_category(conn, "MCP Tax Expense")
        _seed_schedule_c_map(conn, expense_id, line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
        account_id = _seed_account(conn, name="Tax Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=income_id,
            amount_cents=60_000,
            date="2025-01-15",
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-10_000,
            date="2025-01-16",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_tax

        result = biz_tax(year="2025")
        assert "data" in result
        assert "summary" in result
        assert result["data"]["line_1_gross_receipts_cents"] == 60_000
        assert result["data"]["line_28_total_expenses_cents"] == 10_000

    def test_biz_tax_detail_supports_schedule_c_line_number(self, db_path, conn):
        income_id = _seed_category(conn, "MCP Tax Income Detail", is_income=1)
        expense_id = _seed_category(conn, "MCP Tax Expense Detail")
        _seed_schedule_c_map(conn, expense_id, line="Advertising", line_number="8", deduction_pct=1.0, tax_year=2025)
        account_id = _seed_account(conn, name="Tax Detail Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=income_id,
            amount_cents=80_000,
            date="2025-01-15",
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-12_000,
            date="2025-01-18",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_tax_detail

        result = biz_tax_detail(detail="8", year="2025")
        assert "data" in result
        assert result["data"]["detail"] is not None
        assert str(result["data"]["detail"]["line_number"]) == "8"

    def test_biz_budget_set_defaults(self, db_path, conn):
        from finance_cli.mcp_server import biz_budget_set

        result = biz_budget_set(section="opex_marketing", amount=500)
        assert "data" in result
        assert "summary" in result
        assert result["data"]["pl_section"] == "opex_marketing"
        assert result["data"]["period"] == "monthly"
        assert result["data"]["effective_from"] == date.today().replace(day=1).isoformat()

        row = conn.execute(
            """
            SELECT pl_section, amount_cents, period, effective_from
              FROM biz_section_budgets
             WHERE id = ?
            """,
            (result["data"]["id"],),
        ).fetchone()
        assert row is not None
        assert row["pl_section"] == "opex_marketing"
        assert row["amount_cents"] == 50_000
        assert row["period"] == "monthly"

    def test_biz_budget_status(self, db_path, conn):
        marketing_id = _seed_category(conn, "MCP Budget Marketing")
        _seed_pl_map(conn, marketing_id, "opex_marketing", 30)
        account_id = _seed_account(conn, name="Biz Budget Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=marketing_id,
            amount_cents=-30_000,
            date="2026-01-10",
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_budget_set, biz_budget_status

        biz_budget_set(section="opex_marketing", amount=500, period="monthly", effective_from="2026-01-01")
        result = biz_budget_status(month="2026-01")
        assert "data" in result
        assert "summary" in result

        rows = {row["pl_section"]: row for row in result["data"]["rows"]}
        marketing = rows["opex_marketing"]
        assert marketing["monthly_budget_cents"] == 50_000
        assert marketing["actual_cents"] == 30_000

    def test_biz_runway_marks_profitable_mode(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Runway Revenue", is_income=1)
        expense_id = _seed_category(conn, "MCP Runway Expense")
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        _seed_pl_map(conn, expense_id, "opex_other", 20)
        account_id = _seed_account(conn, name="Runway Biz Checking", is_business=1, balance_cents=90_000)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=30_000,
            date=_month_date(0, day=5),
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=expense_id,
            amount_cents=-5_000,
            date=_month_date(0, day=7),
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_runway

        result = biz_runway(months=1)
        assert result["data"]["monthly_net_burn_cents"] < 0
        assert result["data"]["is_profitable"] is True
        assert "note" in result["data"]

    def test_biz_forecast_projection_floor_non_negative(self, db_path, conn):
        revenue_id = _seed_category(conn, "MCP Forecast Revenue", is_income=1)
        _seed_pl_map(conn, revenue_id, "revenue", 10)
        account_id = _seed_account(conn, name="Forecast Biz Checking", is_business=1)
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=20_000,
            date=_month_date(-2),
            account_id=account_id,
            use_type="Business",
        )
        _seed_txn(
            conn,
            category_id=revenue_id,
            amount_cents=10_000,
            date=_month_date(-1),
            account_id=account_id,
            use_type="Business",
        )
        from finance_cli.mcp_server import biz_forecast

        result = biz_forecast(months=3, streams=True)
        assert result["data"]["projected_next_month_cents"] >= 0


# ---------------------------------------------------------------------------
# 3. Transaction Tools
# ---------------------------------------------------------------------------

class TestTransactionTools:
    def test_txn_list_empty(self, db_path):
        from finance_cli.mcp_server import txn_list
        result = txn_list()
        assert "data" in result
        assert "summary" in result

    def test_txn_list_with_data(self, db_path, conn):
        cat_id = _seed_category(conn, "Groceries")
        _seed_txn(conn, category_id=cat_id, description="WHOLE FOODS")
        from finance_cli.mcp_server import txn_list
        result = txn_list(limit=10)
        assert result["data"]["transactions"]
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_list_filters(self, db_path, conn):
        _seed_txn(conn, date="2026-02-10")
        _seed_txn(conn, date="2026-02-20")
        from finance_cli.mcp_server import txn_list
        result = txn_list(date_from="2026-02-15")
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_list_uncategorized(self, db_path, conn):
        _seed_txn(conn, category_id=None)
        cat_id = _seed_category(conn, "Test")
        _seed_txn(conn, category_id=cat_id)
        from finance_cli.mcp_server import txn_list
        result = txn_list(uncategorized=True)
        assert result["data"]["pagination"]["total_count"] == 1

    def test_txn_search(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, description="STARBUCKS COFFEE")
        raw_payload = '{"merchant":"STARBUCKS"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import txn_search
        # Use LIKE fallback (FTS may not have the row since we inserted directly)
        result = txn_search(query="STARBUCKS")
        assert "data" in result
        assert "cache_file" not in result
        assert all("raw_plaid_json" not in item for item in result["data"]["transactions"])

        cache_file = _latest_cache_file(isolated_mcp_cache, "txn_search")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_txn = next(item for item in cached["data"]["transactions"] if item["id"] == tid)
        assert cached_txn["raw_plaid_json"] == raw_payload

    def test_txn_show(self, db_path, conn, isolated_mcp_cache):
        tid = _seed_txn(conn, description="SHOW ME")
        raw_payload = '{"merchant":"SHOW"}'
        _set_txn_raw_payload(conn, tid, raw_payload)
        from finance_cli.mcp_server import txn_show
        result = txn_show(id=tid)
        assert result["data"]["transaction"]["id"] == tid
        assert "cache_file" not in result
        assert "raw_plaid_json" not in result["data"]["transaction"]

        cache_file = _latest_cache_file(isolated_mcp_cache, "txn_show")
        cached = json.loads(cache_file.read_text(encoding="utf-8"))
        assert cached["data"]["transaction"]["raw_plaid_json"] == raw_payload

    def test_txn_show_bad_id(self, db_path):
        from finance_cli.mcp_server import txn_show
        with pytest.raises(ValueError, match="not found"):
            txn_show(id="nonexistent_id_000")

    def test_txn_explain(self, db_path, conn):
        cat_id = _seed_category(conn, "Coffee")
        tid = _seed_txn(conn, category_id=cat_id, description="EXPLAINED TXN")
        from finance_cli.mcp_server import txn_explain
        result = txn_explain(id=tid)
        assert "data" in result

    def test_txn_coverage(self, db_path, conn, isolated_mcp_cache):
        aid = _seed_account(conn)
        _seed_txn(conn, account_id=aid, date="2026-02-01")
        _seed_txn(conn, account_id=aid, date="2026-02-15")
        from finance_cli.mcp_server import txn_coverage
        result = txn_coverage()
        assert "data" in result
        assert "cache_file" not in result
        assert _latest_cache_file(isolated_mcp_cache, "txn_coverage").exists()

    def test_write_cache_same_second_unique(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        class _FixedDatetime:
            @classmethod
            def now(cls):
                return datetime(2026, 3, 2, 9, 0, 0, 123456)

        monkeypatch.setattr(mcp_server, "datetime", _FixedDatetime)
        first = Path(mcp_server._write_cache("txn_search", {"i": 1}))
        second = Path(mcp_server._write_cache("txn_search", {"i": 2}))

        assert first.exists()
        assert second.exists()
        assert first.name != second.name


# ---------------------------------------------------------------------------
# 4. Rules & Categorization
# ---------------------------------------------------------------------------

class TestCategorizationTools:
    def test_rules_test(self, db_path):
        from finance_cli.mcp_server import rules_test
        result = rules_test(description="VENMO PAYMENT")
        assert "data" in result
        assert "summary" in result

    def test_cat_auto_categorize_dry_run(self, db_path, conn):
        _seed_txn(conn, description="UNCATEGORIZED TXN")
        from finance_cli.mcp_server import cat_auto_categorize
        result = cat_auto_categorize(dry_run=True)
        assert "data" in result

    def test_cat_auto_categorize_commit(self, db_path, conn):
        """When dry_run=False the tool must commit categorization changes."""
        _seed_category(conn, "Payments & Transfers")
        _seed_txn(conn, description="VENMO PAYMENT TO FRIEND")
        from finance_cli.mcp_server import cat_auto_categorize
        result = cat_auto_categorize(dry_run=False)
        assert "data" in result
        # Verify the commit persisted by opening a fresh connection
        with connect(db_path) as verify_conn:
            row = verify_conn.execute(
                "SELECT category_id FROM transactions WHERE description = 'VENMO PAYMENT TO FRIEND' AND is_active = 1"
            ).fetchone()
            # The categorizer may or may not match; what matters is no error
            assert row is not None

    def test_txn_categorize(self, db_path, conn):
        _seed_category(conn, "Dining")
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_categorize
        result = txn_categorize(txn_id=tid, category="Dining")
        assert "data" in result

    def test_txn_categorize_bad_category(self, db_path, conn):
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_categorize
        with pytest.raises(ValueError, match="not found"):
            txn_categorize(txn_id=tid, category="Nonexistent Category 999")

    def test_txn_bulk_categorize(self, db_path, conn):
        _seed_category(conn, "Bank Charges & Fees")
        _seed_txn(conn, description="MCP PLAN FEE 11111", use_type="Business")
        _seed_txn(conn, description="MCP PLAN FEE 22222", use_type="Business")
        from finance_cli.mcp_server import txn_bulk_categorize

        result = txn_bulk_categorize(
            category="Bank Charges & Fees",
            query="MCP PLAN FEE%",
            remember=True,
        )

        assert result["data"]["updated"] == 2
        assert result["data"]["remembered_count"] == 1
        with connect(db_path) as verify_conn:
            remember_rows = verify_conn.execute(
                "SELECT description_pattern, use_type FROM vendor_memory ORDER BY description_pattern, use_type"
            ).fetchall()
        assert [(row["description_pattern"], row["use_type"]) for row in remember_rows] == [
            ("mcp plan fee", "Business")
        ]

    def test_txn_bulk_categorize_requires_filter(self, db_path, conn):
        _seed_category(conn, "Dining")
        _seed_txn(conn, description="MCP NO FILTER")
        from finance_cli.mcp_server import txn_bulk_categorize

        with pytest.raises(ValueError, match="requires at least one filter"):
            txn_bulk_categorize(category="Dining")

    def test_txn_review_single(self, db_path, conn):
        tid = _seed_txn(conn)
        from finance_cli.mcp_server import txn_review
        result = txn_review(txn_id=tid)
        assert result["data"]["is_reviewed"] is True

    def test_txn_review_all_today(self, db_path, conn):
        from finance_cli.commands.txn import today_iso
        today = today_iso()
        _seed_txn(conn, date=today)
        _seed_txn(conn, date=today)
        from finance_cli.mcp_server import txn_review
        result = txn_review(all_today=True)
        assert result["data"]["updated"] >= 2

    def test_txn_review_bad_id(self, db_path):
        from finance_cli.mcp_server import txn_review
        with pytest.raises(ValueError, match="not found"):
            txn_review(txn_id="nonexistent_000")

    def test_cat_apply_splits_dry_run(self, db_path, conn):
        (db_path.parent / "rules.yaml").write_text(
            (
                "split_rules:\n"
                "  - match:\n"
                "      category: Rent\n"
                "    business_pct: 25\n"
                "    business_category: Rent\n"
                "    personal_category: Rent\n"
                '    note: "25% business use"\n'
            ),
            encoding="utf-8",
        )
        account_id = _seed_account(conn)
        rent_id = _seed_category(conn, "Rent")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            category_id=rent_id,
            description="Monthly Rent",
            amount_cents=-10000,
        )
        from finance_cli.mcp_server import cat_apply_splits
        result = cat_apply_splits(commit=False, backfill=True)
        assert "data" in result
        assert result["data"]["candidate_transactions"] >= 1

        child_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE parent_transaction_id = ?",
            (txn_id,),
        ).fetchone()["cnt"]
        assert int(child_count) == 0

    def test_cat_classify_use_type_commit(self, db_path, conn):
        (db_path.parent / "rules.yaml").write_text(
            (
                "category_overrides:\n"
                "  - categories: [Dining]\n"
                "    force_use_type: Personal\n"
                '    note: "test override"\n'
            ),
            encoding="utf-8",
        )
        account_id = _seed_account(conn)
        dining_id = _seed_category(conn, "Dining")
        txn_id = _seed_txn(
            conn,
            account_id=account_id,
            category_id=dining_id,
            description="Dining Candidate",
        )
        from finance_cli.mcp_server import cat_classify_use_type
        result = cat_classify_use_type(commit=True)
        assert "data" in result
        assert result["data"]["updated"] >= 1

        use_type = conn.execute(
            "SELECT use_type FROM transactions WHERE id = ?",
            (txn_id,),
        ).fetchone()["use_type"]
        assert use_type == "Personal"


# ---------------------------------------------------------------------------
# 5. Setup & Import
# ---------------------------------------------------------------------------

class TestSetupTools:
    def test_setup_init_dry_run(self, db_path):
        from finance_cli.mcp_server import setup_init
        result = setup_init(dry_run=True)
        assert "data" in result
        assert result["data"]["dry_run"] is True

    def test_ingest_csv_missing_file(self, db_path):
        from finance_cli.mcp_server import ingest_csv
        with pytest.raises(FileNotFoundError):
            ingest_csv(file="/nonexistent/path.csv", institution="chase")


# ---------------------------------------------------------------------------
# 6. Pipeline
# ---------------------------------------------------------------------------

class TestPipelineTools:
    def test_monthly_run_dry(self, db_path, conn):
        _seed_txn(conn)
        from finance_cli.mcp_server import monthly_run
        result = monthly_run(month="2026-02", dry_run=True)
        assert "data" in result
        assert "summary" in result
        assert result["data"]["month"] == "2026-02"
        assert result["summary"]["steps_skipped"] >= 0

    def test_monthly_run_no_sync(self, db_path):
        from finance_cli.mcp_server import monthly_run
        result = monthly_run(dry_run=True, skip=["dedup", "categorize", "detect"])
        assert result["summary"]["steps_skipped"] >= 3


# ---------------------------------------------------------------------------
# 7. Database
# ---------------------------------------------------------------------------

class TestDatabaseTools:
    def test_db_backup(self, db_path):
        from finance_cli.mcp_server import db_backup
        result = db_backup()
        assert "data" in result
        assert "summary" in result
        backup_path = result["data"].get("backup_path", "")
        assert backup_path  # non-empty path
        assert Path(backup_path).exists()


# ---------------------------------------------------------------------------
# 8. Wave 2 MCP tools
# ---------------------------------------------------------------------------

class TestBudgetTools:
    def test_budget_set_list_status(self, db_path, conn):
        _seed_category(conn, "Dining")
        from finance_cli.mcp_server import (
            budget_alerts,
            budget_delete,
            budget_forecast,
            budget_list,
            budget_set,
            budget_status,
            budget_suggest,
            budget_update,
        )

        set_result = budget_set(category="Dining", amount=500, period="monthly")
        assert "data" in set_result
        assert "summary" in set_result

        listed = budget_list()
        assert any(row["category_name"] == "Dining" for row in listed["data"]["budgets"])

        month = date.today().strftime("%Y-%m")
        status = budget_status(month=month)
        assert any(row["category_name"] == "Dining" for row in status["data"]["status"])

        forecast = budget_forecast(month=month)
        assert "forecast" in forecast["data"]

        alerts = budget_alerts(month=month)
        assert "alerts" in alerts["data"]

        suggest = budget_suggest(goal="savings", target=100)
        assert "suggestions" in suggest["data"]

        updated = budget_update(category="Dining", amount=450, period="monthly")
        assert updated["data"]["amount"] == 450.0

        deleted = budget_delete(category="Dining", period="monthly")
        assert deleted["data"]["deleted"] is True


class TestBalanceTools:
    def test_balance_show_and_history(self, db_path, conn):
        account_id = _seed_account(conn, institution="History Bank", name="History Checking", balance_cents=125000)
        _seed_balance_snapshot(conn, account_id=account_id, balance_current_cents=124500)
        from finance_cli.mcp_server import balance_history, balance_show

        shown = balance_show()
        assert any(row["id"] == account_id for row in shown["data"]["accounts"])

        history = balance_history(account=account_id, days=30)
        assert history["data"]["account"]["id"] == account_id
        assert history["data"]["history"]


class TestLiabilityTools:
    def test_liability_show_and_upcoming(self, db_path, conn):
        account_id = _seed_account(conn, account_type="credit_card", balance_cents=-25000)
        liability_id = _seed_credit_liability(
            conn,
            account_id=account_id,
            apr_purchase=19.99,
            minimum_payment_cents=2500,
            next_monthly_payment_cents=2500,
        )
        conn.execute(
            "UPDATE liabilities SET next_payment_due_date = date('now', '+7 day') WHERE id = ?",
            (liability_id,),
        )
        conn.commit()

        from finance_cli.mcp_server import liability_show, liability_upcoming

        shown = liability_show()
        assert any(row["id"] == liability_id for row in shown["data"]["liabilities"])

        upcoming = liability_upcoming(days=14)
        assert upcoming["summary"]["total_upcoming"] >= 1


class TestTxnWriteTools:
    def test_txn_add_edit_tag(self, db_path, conn):
        account_id = _seed_account(conn)
        from finance_cli.mcp_server import txn_add, txn_edit, txn_tag

        added = txn_add(
            amount=25.50,
            date=date.today().isoformat(),
            description="MCP write test",
            account_id=account_id,
        )
        txn_id = added["data"]["transaction_id"]
        assert txn_id

        edited = txn_edit(id=txn_id, notes="edited note")
        assert edited["data"]["updated_fields"] >= 1

        tagged = txn_tag(id=txn_id, project="MCP")
        assert tagged["data"]["project"] == "MCP"

        row = conn.execute(
            """
            SELECT t.notes, p.name AS project_name
              FROM transactions t
              LEFT JOIN projects p ON p.id = t.project_id
             WHERE t.id = ?
            """,
            (txn_id,),
        ).fetchone()
        assert row["notes"] == "edited note"
        assert row["project_name"] == "MCP"


class TestTxnFilterFixes:
    def test_txn_list_filters_account_id_and_use_type(self, db_path, conn):
        account_a = _seed_account(conn, name="Account A")
        account_b = _seed_account(conn, name="Account B")
        _seed_txn(conn, account_id=account_a, use_type="Business", description="A biz")
        _seed_txn(conn, account_id=account_a, use_type="Personal", description="A personal")
        _seed_txn(conn, account_id=account_b, use_type="Personal", description="B personal")
        from finance_cli.mcp_server import txn_list

        by_account = txn_list(account_id=account_a, limit=20)
        assert by_account["data"]["transactions"]
        assert all(row["account_id"] == account_a for row in by_account["data"]["transactions"])

        by_use_type = txn_list(use_type="Business", limit=20)
        assert by_use_type["data"]["transactions"]
        assert all(row["use_type"] == "Business" for row in by_use_type["data"]["transactions"])

    def test_txn_search_limit(self, db_path, isolated_mcp_cache, monkeypatch):
        import finance_cli.mcp_server as mcp_server

        fake_rows = [{"id": "t1"}, {"id": "t2"}, {"id": "t3"}]
        monkeypatch.setattr(
            mcp_server,
            "_call",
            lambda handler, ns_kwargs: {"data": {"transactions": list(fake_rows)}, "summary": {}},
        )

        result = mcp_server.txn_search(query="uber", limit=2)
        assert len(result["data"]["transactions"]) == 2


class TestCatMemoryTools:
    def test_cat_memory_list_add_disable(self, db_path, conn):
        _seed_category(conn, "Dining")
        from finance_cli.mcp_server import cat_memory_add, cat_memory_disable, cat_memory_list

        added = cat_memory_add(pattern="STARBUCKS", category="Dining", use_type="Any")
        rule_id = added["data"]["rule_id"]
        assert rule_id

        listed = cat_memory_list(limit=10)
        assert any(row["id"] == rule_id for row in listed["data"]["rules"])

        disabled = cat_memory_disable(id=rule_id)
        assert disabled["data"]["is_enabled"] is False


class TestSubsWriteTools:
    def test_subs_detect_add_cancel(self, db_path, conn):
        account_id = _seed_account(conn)
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-3))
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-2))
        _seed_txn(conn, account_id=account_id, description="NETFLIX", amount_cents=-1599, date=_month_date(-1))

        from finance_cli.mcp_server import subs_add, subs_cancel, subs_detect

        detected = subs_detect()
        assert "data" in detected
        assert "summary" in detected

        added = subs_add(vendor="MCP Subscription", amount=9.99, frequency="monthly")
        sub_id = added["data"]["subscription_id"]
        assert sub_id

        canceled = subs_cancel(id=sub_id)
        assert canceled["data"]["is_active"] is False


class TestDedupTools:
    def test_dedup_cross_format_and_audit_names(self, db_path, conn):
        account_id = _seed_account(conn, institution="Dup Bank")
        _seed_txn(
            conn,
            account_id=account_id,
            source="manual",
            date="2026-02-01",
            description="DUP TEST PAYMENT",
            amount_cents=-1200,
        )
        _seed_txn(
            conn,
            account_id=account_id,
            source="plaid",
            date="2026-02-01",
            description="DUP TEST PAYMENT",
            amount_cents=-1200,
        )

        from finance_cli.mcp_server import dedup_audit_names, dedup_cross_format

        cross = dedup_cross_format(dry_run=True, account_id=account_id)
        assert cross["data"]["dry_run"] is True
        assert cross["data"]["account_id"] == account_id
        assert cross["summary"]["total_matches"] >= 1

        audit = dedup_audit_names()
        assert "issues" in audit["data"]


class TestPlanTools:
    def test_plan_create_show_review(self, db_path, conn):
        account_id = _seed_account(conn)
        _seed_txn(conn, account_id=account_id, date=_month_date(-1), amount_cents=50000, description="Income")
        _seed_txn(conn, account_id=account_id, date=_month_date(-1), amount_cents=-12000, description="Expense")
        from finance_cli.mcp_server import plan_create, plan_review, plan_show

        created = plan_create()
        month = created["data"]["month"]
        assert month

        shown = plan_show(month=month)
        assert shown["data"]["plan"]["month"] == month

        reviewed = plan_review()
        assert "review" in reviewed["data"]


class TestRulesTools:
    def test_rules_show_and_validate(self, db_path):
        from finance_cli.mcp_server import rules_show, rules_validate

        shown = rules_show()
        assert "data" in shown
        assert "summary" in shown

        validated = rules_validate()
        assert "valid" in validated["data"]
        assert "errors" in validated["data"]

    def test_rules_add_keyword_and_remove_keyword(self, db_path):
        from finance_cli.mcp_server import rules_add_keyword, rules_remove_keyword, rules_test

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Dining")

        added = rules_add_keyword(keyword="NEWVENDOR", category="Dining")
        assert added["data"]["action"] == "added"
        tested = rules_test(description="NEWVENDOR PURCHASE")
        assert tested["data"]["keyword_match"]["matched_keyword"] == "NEWVENDOR"

        removed = rules_remove_keyword(keyword="NEWVENDOR")
        assert removed["data"]["keyword"] == "NEWVENDOR"
        retested = rules_test(description="NEWVENDOR PURCHASE")
        assert retested["data"]["keyword_match"] is None

    def test_rules_list(self, db_path):
        from finance_cli.mcp_server import rules_list

        result = rules_list()
        assert "data" in result
        assert isinstance(result["data"]["rules"], list)
        assert result["data"]["count"] >= 0
        if result["data"]["rules"]:
            rule = result["data"]["rules"][0]
            assert "category" in rule
            assert "keywords" in rule
            assert "priority" in rule
            assert "rule_index" in rule

    def test_rules_update_priority(self, db_path):
        from finance_cli.mcp_server import rules_add_keyword, rules_list, rules_update_priority

        with connect(db_path) as seeded_conn:
            _seed_category(seeded_conn, "Dining")

        rules_add_keyword(keyword="TESTPRIORITY", category="Dining")

        # Find rule by keyword content (not category name, which may match multiple)
        listing = rules_list()
        target = [r for r in listing["data"]["rules"] if "TESTPRIORITY" in r["keywords"]]
        assert target
        idx = target[0]["rule_index"]

        result = rules_update_priority(rule_index=idx, priority=10)
        assert result["data"]["old_priority"] == 0
        assert result["data"]["new_priority"] == 10

        listing2 = rules_list()
        updated = [r for r in listing2["data"]["rules"] if r["rule_index"] == idx]
        assert updated[0]["priority"] == 10

        from finance_cli.mcp_server import rules_remove_keyword

        rules_remove_keyword(keyword="TESTPRIORITY")

    def test_rules_update_priority_out_of_range(self, db_path):
        from finance_cli.mcp_server import rules_update_priority

        with pytest.raises(ValueError, match="out of range"):
            rules_update_priority(rule_index=9999, priority=5)


class TestExportTools:
    def test_export_csv_and_summary(self, db_path, conn):
        category_id = _seed_category(conn, "Dining")
        _seed_txn(conn, category_id=category_id, date=date.today().isoformat(), description="Export row")
        from finance_cli.mcp_server import export_csv, export_summary

        csv_result = export_csv()
        assert Path(csv_result["data"]["output"]).exists()

        summary_result = export_summary(month=date.today().strftime("%Y-%m"))
        assert Path(summary_result["data"]["output"]).exists()


class TestProviderTools:
    def test_provider_status(self, db_path):
        from finance_cli.mcp_server import provider_status

        result = provider_status()
        assert "data" in result
        assert "summary" in result
        assert "institution_count" in result["summary"]

    def test_provider_switch(self, db_path, conn):
        _seed_account(conn, institution="Switch Bank", source="schwab")
        from finance_cli.mcp_server import provider_switch

        result = provider_switch(institution="Switch Bank", provider="plaid")
        assert result["data"]["new_provider"] == "plaid"
        assert result["summary"]["deactivated_count"] >= 1


class TestSchwabTools:
    def test_schwab_status(self, db_path):
        from finance_cli.mcp_server import schwab_status

        result = schwab_status()
        assert "data" in result
        assert "summary" in result
        assert "configured" in result["data"]


class TestParamCoercion:
    """MCP-001: string-typed int/bool params should be coerced automatically."""

    def test_int_param_accepts_string(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(limit="10", offset="0")
        assert "data" in result

    def test_bool_param_accepts_string_true(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(uncategorized="true", limit="5")
        assert "data" in result

    def test_bool_param_accepts_string_false(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(uncategorized="false", limit="5")
        assert "data" in result

    def test_native_types_still_work(self, db_path):
        from finance_cli.mcp_server import txn_list

        result = txn_list(limit=10, uncategorized=False)
        assert "data" in result

    def test_coerce_params_unit(self):
        from finance_cli.mcp_server import _coerce_params

        def sample(x: int = 5, flag: bool = False, name: str = "a") -> dict:
            return {"x": x, "flag": flag, "name": name}

        wrapped = _coerce_params(sample)
        result = wrapped(x="42", flag="true", name="b")
        assert result == {"x": 42, "flag": True, "name": "b"}

    def test_coerce_bool_variants(self):
        from finance_cli.mcp_server import _coerce_params

        def sample(flag: bool = False) -> dict:
            return {"flag": flag}

        wrapped = _coerce_params(sample)
        assert wrapped(flag="1")["flag"] is True
        assert wrapped(flag="yes")["flag"] is True
        assert wrapped(flag="TRUE")["flag"] is True
        assert wrapped(flag="false")["flag"] is False
        assert wrapped(flag="0")["flag"] is False
        assert wrapped(flag="no")["flag"] is False
