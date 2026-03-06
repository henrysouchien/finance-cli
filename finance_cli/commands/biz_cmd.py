"""Business financial statement commands."""

from __future__ import annotations

import argparse
import calendar
import json
import re
import sqlite3
import uuid
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from ..forecasting import (
    revenue_trend,
    runway as forecasting_runway,
    seasonal_pattern,
)
from ..models import cents_to_dollars
from ..user_rules import load_rules
from .common import fmt_dollars

_QUARTER_RE = re.compile(r"^(\d{4})-Q([1-4])$")
_YEAR_RE = re.compile(r"^\d{4}$")
_LINE_NUMBER_RE = re.compile(r"^(\d+)([A-Za-z]*)$")
_TIN_LAST4_RE = re.compile(r"^\d{4}$")

_SECTION_LABELS = {
    "revenue": "Revenue",
    "cogs": "Cost of Goods Sold",
    "opex_marketing": "Marketing",
    "opex_technology": "Technology",
    "opex_professional": "Professional",
    "opex_facilities": "Facilities",
    "opex_people": "People",
    "opex_other": "Other",
}

_OPEX_SECTIONS = {
    "opex_marketing",
    "opex_technology",
    "opex_professional",
    "opex_facilities",
    "opex_people",
    "opex_other",
}

_BIZ_BUDGET_SECTIONS = (
    "cogs",
    "opex_marketing",
    "opex_technology",
    "opex_professional",
    "opex_facilities",
    "opex_people",
    "opex_other",
)
_BIZ_BUDGET_SECTION_SET = set(_BIZ_BUDGET_SECTIONS)
_BIZ_BUDGET_PERIOD_MONTHS = {"monthly": 1, "quarterly": 3, "yearly": 12}

_SE_TAX_PARAMS = {
    2025: {"ss_wage_base_cents": 17_610_000, "ss_rate": 0.124, "medicare_rate": 0.029},
    2026: {"ss_wage_base_cents": 18_450_000, "ss_rate": 0.124, "medicare_rate": 0.029},
}

_QBI_THRESHOLDS = {
    2025: {"single": 19_195_000, "mfj": 38_390_000},
    2026: {"single": 20_000_000, "mfj": 40_000_000},
}

_FED_BRACKETS_2025 = [
    (0, 11_600_00, 0.10),
    (11_600_00, 47_150_00, 0.12),
    (47_150_00, 100_525_00, 0.22),
    (100_525_00, 191_950_00, 0.24),
    (191_950_00, 243_725_00, 0.32),
    (243_725_00, 609_350_00, 0.35),
    (609_350_00, None, 0.37),
]

_FED_STANDARD_DEDUCTION_2025 = {"single": 15_000_00, "mfj": 30_000_00}

_NY_BRACKETS_2025 = [
    (0, 8_500_00, 0.04),
    (8_500_00, 11_700_00, 0.045),
    (11_700_00, 13_900_00, 0.0525),
    (13_900_00, 80_650_00, 0.0585),
    (80_650_00, 215_400_00, 0.0625),
    (215_400_00, 1_077_550_00, 0.0685),
    (1_077_550_00, None, 0.1025),
]

_NY_STANDARD_DEDUCTION_2025 = {"single": 8_000_00, "mfj": 16_050_00}

_NYC_BRACKETS_2025 = [
    (0, 12_000_00, 0.03078),
    (12_000_00, 25_000_00, 0.03762),
    (25_000_00, 50_000_00, 0.03819),
    (50_000_00, None, 0.03876),
]

_NYC_UBT_RATE = 0.04
_NYC_UBT_THRESHOLD_CENTS = 9_500_000

_TAX_ASSUMPTIONS = [
    "Assumes sole-proprietor single-member LLC with no other income sources.",
    "Does not account for AMT, NIIT, additional businesses, retirement contributions, or PTC.",
    "State and local calculations are estimates only; consult a tax professional.",
    "Tax brackets and thresholds based on IRS Rev. Proc. 2024-40 (TY 2025).",
]

_MILEAGE_RATES_CENTS = {
    2024: 67,
    2025: 70,
    2026: 70,
}

_CONTRACTOR_ENTITY_TYPES = {"individual", "llc", "partnership", "corporation"}


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("biz", help="Business accounting reports")
    biz_sub = parser.add_subparsers(dest="biz_command", required=True)

    period_parent = argparse.ArgumentParser(add_help=False)
    period_group = period_parent.add_mutually_exclusive_group()
    period_group.add_argument("--month", help="YYYY-MM")
    period_group.add_argument("--quarter", help="YYYY-Q1..Q4")
    period_group.add_argument("--year", help="YYYY")

    p_pl = biz_sub.add_parser("pl", parents=[format_parent, period_parent], help="Income statement (P&L)")
    p_pl.add_argument("--compare", action="store_true", help="Add prior period comparison")
    p_pl.set_defaults(func=handle_pl, command_name="biz.pl")

    p_cf = biz_sub.add_parser("cashflow", parents=[format_parent, period_parent], help="Cash flow statement")
    p_cf.set_defaults(func=handle_cashflow, command_name="biz.cashflow")

    p_tax = biz_sub.add_parser("tax", parents=[format_parent, period_parent], help="Schedule C tax report")
    p_tax.add_argument(
        "--detail",
        choices=["form-8829", "schedule-se", "qbi", "health-insurance", "s-corp", "transactions"],
        help="Show a detailed section",
    )
    p_tax.add_argument("--salary", type=float, help="Optional S-Corp salary override in dollars")
    p_tax.set_defaults(func=handle_tax, command_name="biz.tax")

    p_tax_setup = biz_sub.add_parser("tax-setup", parents=[format_parent], help="Configure tax assumptions")
    p_tax_setup.add_argument("--year", required=True, help="Tax year (YYYY)")
    p_tax_setup.add_argument("--method", choices=["simplified", "actual"], help="Home office method")
    p_tax_setup.add_argument("--sqft", type=int, help="Home office square footage")
    p_tax_setup.add_argument("--total-sqft", type=int, help="Total home square footage")
    p_tax_setup.add_argument("--filing-status", choices=["single", "mfj"], help="Filing status")
    p_tax_setup.add_argument("--state", help="State profile (NY or NY-NYC)")
    p_tax_setup.add_argument("--health-insurance-monthly", type=float, help="Monthly premium in dollars")
    p_tax_setup.add_argument("--w2-wages", type=float, help="W-2 wages in dollars")
    p_tax_setup.add_argument(
        "--mileage-method",
        choices=["standard", "actual"],
        help="Standard mileage rate or actual vehicle expenses",
    )
    p_tax_setup.set_defaults(func=handle_tax_setup, command_name="biz.tax_setup")

    p_tax_package = biz_sub.add_parser("tax-package", parents=[format_parent], help="Build full tax package export")
    p_tax_package.add_argument("--year", required=True, help="Tax year (YYYY)")
    p_tax_package.add_argument("--output", help="Optional file path (.json writes JSON; otherwise markdown)")
    p_tax_package.add_argument("--salary", type=float, help="Optional S-Corp salary override in dollars")
    p_tax_package.set_defaults(func=handle_tax_package, command_name="biz.tax_package")

    p_est = biz_sub.add_parser("estimated-tax", parents=[format_parent], help="Quarterly estimated tax")
    p_est.add_argument("--quarter", dest="est_quarter", help="YYYY-Q1..Q4")
    p_est.add_argument("--year", type=int, help="Tax year — shows full-year estimated tax")
    p_est.add_argument("--rate", type=float, help="Manual combined tax rate override")
    include_group = p_est.add_mutually_exclusive_group()
    include_group.add_argument("--include-se", dest="include_se", action="store_true", default=True)
    include_group.add_argument("--no-se", dest="include_se", action="store_false")
    p_est.add_argument("--salary", type=float, help="Optional S-Corp salary override in dollars")
    p_est.set_defaults(func=handle_estimated_tax, command_name="biz.estimated_tax")

    p_mileage = biz_sub.add_parser("mileage", help="Mileage log for vehicle deductions")
    mileage_sub = p_mileage.add_subparsers(dest="mileage_action", required=True)

    p_mileage_add = mileage_sub.add_parser("add", parents=[format_parent])
    p_mileage_add.add_argument("--date", required=True, help="Trip date (YYYY-MM-DD)")
    p_mileage_add.add_argument("--miles", required=True, type=float)
    p_mileage_add.add_argument("--destination", required=True)
    p_mileage_add.add_argument("--purpose", required=True, help="Business purpose")
    p_mileage_add.add_argument("--vehicle", default="primary")
    p_mileage_add.add_argument("--round-trip", action="store_true")
    p_mileage_add.add_argument("--notes")
    p_mileage_add.set_defaults(func=handle_mileage_add, command_name="biz.mileage_add")

    p_mileage_list = mileage_sub.add_parser("list", parents=[format_parent])
    p_mileage_list.add_argument("--year", help="Tax year (YYYY), default current")
    p_mileage_list.add_argument("--vehicle")
    p_mileage_list.add_argument("--limit", type=int, default=50)
    p_mileage_list.set_defaults(func=handle_mileage_list, command_name="biz.mileage_list")

    p_mileage_summary = mileage_sub.add_parser("summary", parents=[format_parent])
    p_mileage_summary.add_argument("--year", help="Tax year (YYYY), default current")
    p_mileage_summary.set_defaults(func=handle_mileage_summary, command_name="biz.mileage_summary")

    p_contractor = biz_sub.add_parser("contractor", help="1099 contractor tracking")
    contractor_sub = p_contractor.add_subparsers(dest="contractor_action", required=True)

    p_contractor_add = contractor_sub.add_parser("add", parents=[format_parent])
    p_contractor_add.add_argument("--name", required=True)
    p_contractor_add.add_argument("--tin-last4", help="Last 4 digits of TIN")
    p_contractor_add.add_argument(
        "--entity-type",
        default="individual",
        choices=["individual", "llc", "partnership", "corporation"],
    )
    p_contractor_add.add_argument("--notes")
    p_contractor_add.set_defaults(func=handle_contractor_add, command_name="biz.contractor_add")

    p_contractor_list = contractor_sub.add_parser("list", parents=[format_parent])
    p_contractor_list.add_argument("--year", help="Show payment totals for tax year")
    p_contractor_list.add_argument("--all", dest="include_inactive", action="store_true")
    p_contractor_list.set_defaults(func=handle_contractor_list, command_name="biz.contractor_list")

    p_contractor_link = contractor_sub.add_parser("link", parents=[format_parent])
    p_contractor_link.add_argument("--contractor-id", required=True)
    p_contractor_link.add_argument("--transaction-id", required=True)
    p_contractor_link.add_argument(
        "--paid-via-card",
        action="store_true",
        help="Payment was via credit card/processor (1099-K, not 1099-NEC)",
    )
    p_contractor_link.set_defaults(func=handle_contractor_link, command_name="biz.contractor_link")

    p_1099 = biz_sub.add_parser("1099-report", parents=[format_parent])
    p_1099.add_argument("--year", required=True, help="Tax year (YYYY)")
    p_1099.set_defaults(func=handle_1099_report, command_name="biz.1099_report")

    p_forecast = biz_sub.add_parser("forecast", parents=[format_parent], help="Revenue forecast and trend")
    p_forecast.add_argument("--months", type=int, default=6, help="Lookback months (default: 6)")
    p_forecast.add_argument("--streams", action="store_true", help="Include per-stream monthly breakdown")
    p_forecast.set_defaults(func=handle_forecast, command_name="biz.forecast")

    p_runway = biz_sub.add_parser("runway", parents=[format_parent], help="Burn rate and cash runway")
    p_runway.add_argument("--months", type=int, default=3, help="Lookback months (default: 3)")
    p_runway.set_defaults(func=handle_runway, command_name="biz.runway")

    p_seasonal = biz_sub.add_parser("seasonal", parents=[format_parent], help="Seasonal revenue pattern")
    p_seasonal.set_defaults(func=handle_seasonal, command_name="biz.seasonal")

    p_budget = biz_sub.add_parser("budget", help="Section-level business budgets")
    budget_sub = p_budget.add_subparsers(dest="biz_budget_action", required=True)

    p_set = budget_sub.add_parser("set", parents=[format_parent])
    p_set.add_argument(
        "--section",
        required=True,
        choices=list(_BIZ_BUDGET_SECTIONS),
        help="Expense P&L section (revenue excluded — budgets track spend)",
    )
    p_set.add_argument("--amount", required=True, type=float)
    p_set.add_argument("--period", default="monthly", choices=["monthly", "quarterly", "yearly"])
    p_set.add_argument("--from", dest="effective_from")
    p_set.set_defaults(func=handle_biz_budget_set, command_name="biz.budget_set")

    p_status = budget_sub.add_parser("status", parents=[format_parent])
    p_status.add_argument("--month")
    p_status.set_defaults(func=handle_biz_budget_status, command_name="biz.budget_status")


def _round_half_up(value: float | Decimal) -> int:
    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _dollars_to_cents(value: float | None) -> int | None:
    if value is None:
        return None
    return _round_half_up(float(value) * 100)


def _int_or_default(value: str | None, default: int = 0) -> int:
    if value is None or str(value).strip() == "":
        return default
    return int(str(value).strip())


def _year_param(params: dict[int, Any], tax_year: int) -> Any:
    if tax_year in params:
        return params[tax_year]
    if not params:
        raise ValueError("Missing tax parameters")
    years = sorted(params.keys())
    candidates = [year for year in years if year <= tax_year]
    if candidates:
        return params[candidates[-1]]
    return params[years[0]]


def _tax_assumption_lines() -> list[str]:
    lines = ["Assumptions and limitations:"]
    for item in _TAX_ASSUMPTIONS:
        lines.append(f"  - {item}")
    return lines


def _get_tax_config(conn: sqlite3.Connection, tax_year: int) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT config_key, config_value
          FROM tax_config
         WHERE tax_year = ?
        """,
        (tax_year,),
    ).fetchall()
    return {str(row["config_key"]): str(row["config_value"]) for row in rows}


def _set_tax_config(conn: sqlite3.Connection, tax_year: int, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO tax_config (tax_year, config_key, config_value, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(tax_year, config_key) DO UPDATE
            SET config_value = excluded.config_value,
                updated_at = datetime('now')
        """,
        (tax_year, key, value),
    )


def _compute_bracket_tax(
    income_cents: int,
    brackets: list[tuple[int, int | None, float]],
) -> int:
    taxable_cents = max(0, int(income_cents))
    if taxable_cents <= 0:
        return 0

    total_tax_cents = 0
    for lower_bound, upper_bound, rate in brackets:
        if taxable_cents <= lower_bound:
            continue
        top = taxable_cents if upper_bound is None else min(taxable_cents, upper_bound)
        if top <= lower_bound:
            continue
        total_tax_cents += _round_half_up((top - lower_bound) * rate)
        if upper_bound is not None and taxable_cents <= upper_bound:
            break
    return int(total_tax_cents)


def _parse_year_or_default(raw_year: str | None, *, field_name: str = "year") -> int:
    if raw_year is None or str(raw_year).strip() == "":
        return date.today().year
    year = str(raw_year).strip()
    if not _YEAR_RE.match(year):
        raise ValueError(f"{field_name} must be in YYYY format")
    return int(year)


def _mileage_rate_cents(conn: sqlite3.Connection, tax_year: int) -> int:
    fallback = int(_year_param(_MILEAGE_RATES_CENTS, tax_year))
    try:
        row = conn.execute(
            """
            SELECT rate_cents
              FROM mileage_rates
             WHERE tax_year = ?
            """,
            (tax_year,),
        ).fetchone()
    except sqlite3.Error:
        return fallback
    if row and row["rate_cents"] is not None:
        return int(row["rate_cents"])
    return fallback


def _mileage_totals(
    conn: sqlite3.Connection,
    *,
    tax_year: int,
    start: date | None = None,
    end: date | None = None,
    vehicle: str | None = None,
) -> dict[str, Any]:
    clauses = ["tax_year = ?"]
    params: list[Any] = [tax_year]
    if start is not None:
        clauses.append("trip_date >= ?")
        params.append(start.isoformat())
    if end is not None:
        clauses.append("trip_date <= ?")
        params.append(end.isoformat())
    normalized_vehicle = str(vehicle or "").strip()
    if normalized_vehicle:
        clauses.append("vehicle_name = ?")
        params.append(normalized_vehicle)

    row = conn.execute(
        f"""
        SELECT COUNT(*) AS trip_count,
               COALESCE(SUM(miles), 0.0) AS total_miles
          FROM mileage_log
         WHERE {' AND '.join(clauses)}
        """,
        tuple(params),
    ).fetchone()
    return {
        "trip_count": int(row["trip_count"] or 0),
        "total_miles": float(row["total_miles"] or 0.0),
    }


def _line_9_transaction_deduction(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
    tax_year: int,
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS net_cents,
               COALESCE(MAX(sm.deduction_pct), 1.0) AS deduction_pct,
               COUNT(*) AS txn_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND sm.line_number = '9'
           AND t.date >= ?
           AND t.date <= ?
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchone()
    net_cents = int(row["net_cents"] or 0)
    deduction_pct = float(row["deduction_pct"] or 1.0)
    actual_cents = abs(net_cents) if net_cents < 0 else 0
    deductible_cents = _round_half_up(actual_cents * deduction_pct) if net_cents < 0 else 0
    return {
        "actual_cents": int(actual_cents),
        "deductible_cents": int(max(0, deductible_cents)),
        "deduction_pct": float(deduction_pct),
        "txn_count": int(row["txn_count"] or 0),
    }


def _line_label_for_number(conn: sqlite3.Connection, *, tax_year: int, line_number: str, default: str) -> str:
    row = conn.execute(
        """
        SELECT schedule_c_line
          FROM schedule_c_map
         WHERE tax_year = ?
           AND line_number = ?
         ORDER BY rowid ASC
         LIMIT 1
        """,
        (tax_year, line_number),
    ).fetchone()
    if row is None:
        return default
    value = str(row["schedule_c_line"] or "").strip()
    return value or default


def _mileage_summary_payload(
    conn: sqlite3.Connection,
    *,
    tax_year: int,
    start: date | None = None,
    end: date | None = None,
    vehicle: str | None = None,
) -> dict[str, Any]:
    start_date = start or date(tax_year, 1, 1)
    end_date = end or date(tax_year, 12, 31)
    totals = _mileage_totals(
        conn,
        tax_year=tax_year,
        start=start_date,
        end=end_date,
        vehicle=vehicle,
    )
    rate_cents = _mileage_rate_cents(conn, tax_year)
    total_deduction_cents = _round_half_up(float(totals["total_miles"]) * rate_cents)
    line_9_transactions = _line_9_transaction_deduction(
        conn,
        start=start_date,
        end=end_date,
        tax_year=tax_year,
    )
    return {
        "tax_year": int(tax_year),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "vehicle": str(vehicle or "").strip() or None,
        "rate_cents": int(rate_cents),
        "trip_count": int(totals["trip_count"]),
        "total_miles": float(totals["total_miles"]),
        "total_deduction_cents": int(total_deduction_cents),
        "transaction_based_line_9_cents": int(line_9_transactions["deductible_cents"]),
        "transaction_based_line_9_txn_count": int(line_9_transactions["txn_count"]),
    }


def _contractor_rows_for_year(
    conn: sqlite3.Connection,
    *,
    tax_year: int,
    include_inactive: bool,
) -> list[dict[str, Any]]:
    where_clause = "" if include_inactive else "WHERE c.is_active = 1"
    rows = conn.execute(
        f"""
        SELECT c.id,
               c.name,
               c.tin_last4,
               c.entity_type,
               c.is_active,
               c.notes,
               COALESCE(SUM(CASE WHEN cp.tax_year = ? AND t.is_active = 1 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS total_paid_cents,
               COALESCE(SUM(CASE WHEN cp.tax_year = ? AND t.is_active = 1 AND cp.paid_via_card = 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS non_card_paid_cents,
               COALESCE(SUM(CASE WHEN cp.tax_year = ? AND t.is_active = 1 AND cp.paid_via_card = 1 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS card_paid_cents,
               COALESCE(SUM(CASE WHEN cp.tax_year = ? AND t.is_active = 1 THEN 1 ELSE 0 END), 0) AS payment_count
          FROM contractors c
          LEFT JOIN contractor_payments cp ON cp.contractor_id = c.id
          LEFT JOIN transactions t ON t.id = cp.transaction_id
          {where_clause}
         GROUP BY c.id, c.name, c.tin_last4, c.entity_type, c.is_active, c.notes
         ORDER BY c.name ASC
        """,
        (tax_year, tax_year, tax_year, tax_year),
    ).fetchall()

    contractor_rows: list[dict[str, Any]] = []
    for row in rows:
        non_card_paid_cents = int(row["non_card_paid_cents"] or 0)
        entity_type = str(row["entity_type"] or "individual")
        requires_1099 = non_card_paid_cents >= 60_000 and entity_type != "corporation"
        contractor_rows.append(
            {
                "id": str(row["id"]),
                "name": str(row["name"] or ""),
                "tin_last4": (str(row["tin_last4"]) if row["tin_last4"] is not None else None),
                "entity_type": entity_type,
                "is_active": int(row["is_active"] or 0),
                "notes": (str(row["notes"]) if row["notes"] is not None else None),
                "payment_count": int(row["payment_count"] or 0),
                "total_paid_cents": int(row["total_paid_cents"] or 0),
                "non_card_paid_cents": non_card_paid_cents,
                "card_paid_cents": int(row["card_paid_cents"] or 0),
                "requires_1099": bool(requires_1099),
            }
        )
    return contractor_rows


def _unlinked_contract_labor(conn: sqlite3.Connection, *, tax_year: int) -> dict[str, Any]:
    start = date(tax_year, 1, 1)
    end = date(tax_year, 12, 31)
    row = conn.execute(
        """
        SELECT COUNT(*) AS txn_count,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN ABS(t.amount_cents) * sm.deduction_pct ELSE 0 END), 0) AS deductible_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         LEFT JOIN contractor_payments cp
            ON cp.transaction_id = t.id
           AND cp.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND sm.line_number = '11'
           AND t.date >= ?
           AND t.date <= ?
           AND cp.transaction_id IS NULL
        """,
        (tax_year, tax_year, start.isoformat(), end.isoformat()),
    ).fetchone()
    return {
        "txn_count": int(row["txn_count"] or 0),
        "total_cents": int(_round_half_up(float(row["deductible_cents"] or 0))),
    }


def _contractor_summary_payload(
    conn: sqlite3.Connection,
    *,
    tax_year: int,
    include_inactive: bool,
) -> dict[str, Any]:
    contractors = _contractor_rows_for_year(
        conn,
        tax_year=tax_year,
        include_inactive=include_inactive,
    )
    unlinked = _unlinked_contract_labor(conn, tax_year=tax_year)
    totals = {
        "contractor_count": len(contractors),
        "payment_count": sum(int(row["payment_count"]) for row in contractors),
        "requires_1099_count": sum(1 for row in contractors if bool(row["requires_1099"])),
        "total_paid_cents": sum(int(row["total_paid_cents"]) for row in contractors),
        "total_non_card_paid_cents": sum(int(row["non_card_paid_cents"]) for row in contractors),
        "total_card_paid_cents": sum(int(row["card_paid_cents"]) for row in contractors),
    }
    return {
        "tax_year": int(tax_year),
        "contractors": contractors,
        "totals": totals,
        "unlinked_contract_labor": unlinked,
    }


def _has_home_split_rule_conflict() -> bool:
    try:
        split_rules = load_rules().split_rules
    except Exception:
        return False
    target_categories = {"rent", "utilities"}
    for rule in split_rules:
        match_category = (rule.match_category or "").strip().lower()
        business_category = (rule.business_category or "").strip().lower()
        if match_category in target_categories or business_category in target_categories:
            return True
    return False


def _compute_home_office(config: dict[str, str], line_31_before_home_office: int) -> dict[str, Any]:
    method = str(config.get("home_office_method", "") or "").strip().lower()
    if method == "simplified":
        if _has_home_split_rule_conflict():
            raise ValueError(
                "Simplified home office cannot be combined with Rent/Utilities split rules; disable split rules first."
            )
        office_sqft = max(0, _int_or_default(config.get("home_office_sqft"), 0))
        eligible_sqft = min(office_sqft, 300)
        tentative_deduction_cents = eligible_sqft * 500
        gross_income_cap_cents = max(0, int(line_31_before_home_office))
        deduction_cents = min(tentative_deduction_cents, gross_income_cap_cents)
        return {
            "method": "simplified",
            "office_sqft": office_sqft,
            "eligible_sqft": eligible_sqft,
            "gross_income_cap_cents": gross_income_cap_cents,
            "tentative_deduction_cents": tentative_deduction_cents,
            "deduction_cents": int(deduction_cents),
            "display": fmt_dollars(cents_to_dollars(int(deduction_cents))),
        }

    if method == "actual":
        return {
            "method": "actual",
            "office_sqft": max(0, _int_or_default(config.get("home_office_sqft"), 0)),
            "total_sqft": max(0, _int_or_default(config.get("home_total_sqft"), 0)),
            "deduction_cents": 0,
            "display": "See Lines 20b, 25",
        }

    return {
        "method": "not_configured",
        "office_sqft": max(0, _int_or_default(config.get("home_office_sqft"), 0)),
        "deduction_cents": 0,
        "display": "N/A (not yet configured)",
    }


def _compute_se_tax(line_31_cents: int, config: dict[str, str], tax_year: int) -> dict[str, Any]:
    filing_status = str(config.get("filing_status", "single") or "single").strip().lower()
    addl_medicare_threshold_cents = 20_000_000 if filing_status != "mfj" else 25_000_000

    if int(line_31_cents) < 40_000:
        return {
            "line_31_cents": int(line_31_cents),
            "se_taxable_cents": 0,
            "remaining_ss_base_cents": 0,
            "ss_taxable_cents": 0,
            "ss_tax_cents": 0,
            "medicare_tax_cents": 0,
            "additional_medicare_cents": 0,
            "total_se_cents": 0,
            "deductible_half_cents": 0,
            "is_below_floor": True,
        }

    params = _year_param(_SE_TAX_PARAMS, tax_year)
    se_taxable_cents = _round_half_up(int(line_31_cents) * 0.9235)
    w2_wages_cents = max(0, _int_or_default(config.get("w2_wages_cents"), 0))
    remaining_ss_base_cents = max(0, int(params["ss_wage_base_cents"]) - w2_wages_cents)
    ss_taxable_cents = min(se_taxable_cents, remaining_ss_base_cents)
    ss_tax_cents = _round_half_up(ss_taxable_cents * float(params["ss_rate"]))
    medicare_tax_cents = _round_half_up(se_taxable_cents * float(params["medicare_rate"]))
    additional_medicare_cents = _round_half_up(max(0, se_taxable_cents - addl_medicare_threshold_cents) * 0.009)
    total_se_cents = int(ss_tax_cents + medicare_tax_cents + additional_medicare_cents)
    deductible_half_cents = _round_half_up(total_se_cents / 2)

    return {
        "line_31_cents": int(line_31_cents),
        "se_taxable_cents": int(se_taxable_cents),
        "remaining_ss_base_cents": int(remaining_ss_base_cents),
        "ss_taxable_cents": int(ss_taxable_cents),
        "ss_tax_cents": int(ss_tax_cents),
        "medicare_tax_cents": int(medicare_tax_cents),
        "additional_medicare_cents": int(additional_medicare_cents),
        "total_se_cents": total_se_cents,
        "deductible_half_cents": int(deductible_half_cents),
        "is_below_floor": False,
    }


def _compute_health_insurance(
    config: dict[str, str],
    line_31_cents: int,
    deductible_half_se: int,
) -> dict[str, Any]:
    monthly_premium_cents = max(0, _int_or_default(config.get("health_insurance_monthly_cents"), 0))
    annual_premiums_cents = monthly_premium_cents * 12
    earned_income_cap_cents = max(0, int(line_31_cents) - int(deductible_half_se))
    deduction_cents = min(annual_premiums_cents, earned_income_cap_cents)
    return {
        "monthly_premium_cents": int(monthly_premium_cents),
        "annual_premiums_cents": int(annual_premiums_cents),
        "earned_income_cap_cents": int(earned_income_cap_cents),
        "deduction_cents": int(deduction_cents),
    }


def _compute_qbi(
    line_31_cents: int,
    deductible_half_se: int,
    health_insurance_deduction: int,
    agi: int,
    standard_deduction: int,
    filing_status: str,
    tax_year: int,
) -> dict[str, Any]:
    thresholds = _year_param(_QBI_THRESHOLDS, tax_year)
    normalized_filing_status = "mfj" if filing_status == "mfj" else "single"
    qbi_threshold_cents = int(thresholds[normalized_filing_status])
    qbi_cents = int(line_31_cents) - int(deductible_half_se) - int(health_insurance_deduction)
    tentative_qbi_deduction_cents = _round_half_up(max(0, qbi_cents) * 0.20)
    taxable_income_before_qbi_cents = max(0, int(agi) - int(standard_deduction))
    taxable_income_cap_cents = _round_half_up(taxable_income_before_qbi_cents * 0.20)
    qbi_deduction_cents = min(tentative_qbi_deduction_cents, taxable_income_cap_cents)
    warnings: list[str] = []
    if int(agi) > qbi_threshold_cents:
        warnings.append("Income exceeds QBI threshold; W-2 wage/UBIA limits may reduce deduction.")
    return {
        "qbi_cents": int(qbi_cents),
        "tentative_qbi_deduction_cents": int(tentative_qbi_deduction_cents),
        "taxable_income_before_qbi_cents": int(taxable_income_before_qbi_cents),
        "taxable_income_cap_cents": int(taxable_income_cap_cents),
        "qbi_deduction_cents": int(qbi_deduction_cents),
        "threshold_cents": int(qbi_threshold_cents),
        "is_above_threshold": bool(int(agi) > qbi_threshold_cents),
        "warnings": warnings,
    }


def _compute_federal_tax(agi: int, standard_deduction: int, qbi_deduction: int, tax_year: int) -> dict[str, Any]:
    _ = tax_year
    taxable_income_cents = max(0, int(agi) - int(standard_deduction) - int(qbi_deduction))
    tax_cents = _compute_bracket_tax(taxable_income_cents, _FED_BRACKETS_2025)
    return {
        "taxable_income_cents": int(taxable_income_cents),
        "tax_cents": int(tax_cents),
    }


def _compute_ny_tax(agi: int, filing_status: str, tax_year: int) -> dict[str, Any]:
    _ = tax_year
    normalized_filing_status = "mfj" if filing_status == "mfj" else "single"
    standard_deduction_cents = int(_NY_STANDARD_DEDUCTION_2025[normalized_filing_status])
    taxable_income_cents = max(0, int(agi) - standard_deduction_cents)
    tax_cents = _compute_bracket_tax(taxable_income_cents, _NY_BRACKETS_2025)
    warnings: list[str] = []
    if taxable_income_cents > 10_765_000:
        warnings.append("NY tax estimate is simplified for higher-income calculations.")
    return {
        "standard_deduction_cents": standard_deduction_cents,
        "taxable_income_cents": int(taxable_income_cents),
        "tax_cents": int(tax_cents),
        "warnings": warnings,
    }


def _compute_nyc_tax(agi: int, line_31_cents: int, filing_status: str, state: str, tax_year: int) -> dict[str, Any]:
    _ = (filing_status, tax_year)
    normalized_state = str(state or "").strip().upper()
    if normalized_state != "NY-NYC":
        return {
            "resident_taxable_income_cents": 0,
            "resident_tax_cents": 0,
            "ubt_tax_cents": 0,
            "total_nyc_tax_cents": 0,
        }

    resident_taxable_income_cents = max(0, int(agi))
    resident_tax_cents = _compute_bracket_tax(resident_taxable_income_cents, _NYC_BRACKETS_2025)
    ubt_tax_cents = 0
    if int(line_31_cents) > _NYC_UBT_THRESHOLD_CENTS:
        ubt_tax_cents = _round_half_up(int(line_31_cents) * _NYC_UBT_RATE)
    return {
        "resident_taxable_income_cents": int(resident_taxable_income_cents),
        "resident_tax_cents": int(resident_tax_cents),
        "ubt_tax_cents": int(ubt_tax_cents),
        "total_nyc_tax_cents": int(resident_tax_cents + ubt_tax_cents),
    }


def _compute_s_corp_analysis(
    line_31_cents: int,
    se_total: int,
    salary_pct: float = 0.60,
    salary_override: int | None = None,
) -> dict[str, Any]:
    profit_cents = max(0, int(line_31_cents))
    if salary_override is not None:
        salary_cents = max(0, min(profit_cents, int(salary_override)))
    else:
        salary_cents = _round_half_up(profit_cents * float(salary_pct))
        salary_cents = max(0, min(profit_cents, salary_cents))
    distribution_cents = max(0, profit_cents - salary_cents)
    payroll_tax_total_cents = _round_half_up(salary_cents * 0.153)
    net_savings_cents = int(se_total) - payroll_tax_total_cents
    low_profit_flag = profit_cents < 4_000_000
    notes: list[str] = []
    if low_profit_flag:
        notes.append("Net profit below $40k; S-Corp election is often not cost-effective.")
    notes.append("S-Corp estimate is advisory and excludes setup/admin overhead.")
    return {
        "profit_cents": int(profit_cents),
        "salary_cents": int(salary_cents),
        "distribution_cents": int(distribution_cents),
        "sole_prop_se_tax_cents": int(se_total),
        "s_corp_payroll_tax_total_cents": int(payroll_tax_total_cents),
        "net_savings_cents": int(net_savings_cents),
        "low_profit_flag": bool(low_profit_flag),
        "notes": notes,
    }


def _compute_full_tax_summary(
    conn: sqlite3.Connection,
    snapshot: dict[str, Any],
    config: dict[str, str],
    tax_year: int,
) -> dict[str, Any]:
    _ = conn
    line_31_cents = int(snapshot.get("line_31_net_profit_cents", 0))
    filing_status = str(config.get("filing_status", "single") or "single").strip().lower()
    filing_status = "mfj" if filing_status == "mfj" else "single"
    state = str(config.get("state", "") or "").strip().upper()
    standard_deduction_cents = int(_FED_STANDARD_DEDUCTION_2025[filing_status])
    salary_override_cents = _int_or_default(config.get("analysis_salary_override_cents"), 0)
    salary_override = salary_override_cents if salary_override_cents > 0 else None

    schedule_se = _compute_se_tax(line_31_cents, config, tax_year)
    health_insurance = _compute_health_insurance(
        config,
        line_31_cents,
        int(schedule_se["deductible_half_cents"]),
    )
    agi_cents = max(
        0,
        line_31_cents - int(schedule_se["deductible_half_cents"]) - int(health_insurance["deduction_cents"]),
    )
    qbi = _compute_qbi(
        line_31_cents,
        int(schedule_se["deductible_half_cents"]),
        int(health_insurance["deduction_cents"]),
        agi_cents,
        standard_deduction_cents,
        filing_status,
        tax_year,
    )
    federal = _compute_federal_tax(
        agi_cents,
        standard_deduction_cents,
        int(qbi["qbi_deduction_cents"]),
        tax_year,
    )
    ny_state = (
        _compute_ny_tax(agi_cents, filing_status, tax_year)
        if state in {"NY", "NY-NYC"}
        else {"standard_deduction_cents": 0, "taxable_income_cents": 0, "tax_cents": 0, "warnings": []}
    )
    nyc = _compute_nyc_tax(agi_cents, line_31_cents, filing_status, state, tax_year)
    s_corp = _compute_s_corp_analysis(
        line_31_cents,
        int(schedule_se["total_se_cents"]),
        salary_override=salary_override,
    )
    if state == "NY-NYC":
        s_corp.setdefault("notes", []).append(
            "NYC S-Corp analysis is approximate; compare GCT vs UBT with an accountant."
        )

    total_estimated_tax_cents = int(federal["tax_cents"]) + int(ny_state["tax_cents"]) + int(nyc["total_nyc_tax_cents"])
    total_estimated_tax_with_se_cents = total_estimated_tax_cents + int(schedule_se["total_se_cents"])
    quarterly_payment_cents = _round_half_up(total_estimated_tax_with_se_cents / 4)

    return {
        "line_31_net_profit_cents": int(line_31_cents),
        "filing_status": filing_status,
        "state": state,
        "standard_deduction_cents": int(standard_deduction_cents),
        "schedule_se": schedule_se,
        "health_insurance": health_insurance,
        "agi_cents": int(agi_cents),
        "qbi": qbi,
        "federal": federal,
        "ny_state": ny_state,
        "nyc": nyc,
        "s_corp": s_corp,
        "total_estimated_tax_cents": int(total_estimated_tax_with_se_cents),
        "quarterly_payment_cents": int(quarterly_payment_cents),
        "assumptions": list(_TAX_ASSUMPTIONS),
    }


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def _quarter_bounds(year: int, quarter: int) -> tuple[date, date]:
    start_month = ((quarter - 1) * 3) + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    end = date(year, end_month, calendar.monthrange(year, end_month)[1])
    return start, end


def _period_mode(args, default_mode: str) -> str:
    if getattr(args, "month", None):
        return "month"
    if getattr(args, "quarter", None):
        return "quarter"
    if getattr(args, "year", None):
        return "year"
    return default_mode


def _parse_period(
    args,
    *,
    default_mode: str = "month",
    default_year: int | None = None,
) -> tuple[date, date, str]:
    """Parse month/quarter/year into an inclusive date range and display label."""
    month = getattr(args, "month", None)
    quarter = getattr(args, "quarter", None)
    year = getattr(args, "year", None)
    supplied = [value for value in (month, quarter, year) if value is not None]
    if len(supplied) > 1:
        raise ValueError("Provide only one of --month, --quarter, or --year")

    if month:
        try:
            parsed = date.fromisoformat(f"{month}-01")
        except ValueError as exc:
            raise ValueError("month must be in YYYY-MM format") from exc
        start, end = _month_bounds(parsed.year, parsed.month)
        return start, end, start.strftime("%B %Y")

    if quarter:
        match = _QUARTER_RE.match(quarter)
        if not match:
            raise ValueError("quarter must be in YYYY-Q1..Q4 format")
        quarter_year = int(match.group(1))
        quarter_number = int(match.group(2))
        start, end = _quarter_bounds(quarter_year, quarter_number)
        return start, end, f"{quarter_year}-Q{quarter_number}"

    if year:
        if not _YEAR_RE.match(year):
            raise ValueError("year must be in YYYY format")
        parsed_year = int(year)
        return date(parsed_year, 1, 1), date(parsed_year, 12, 31), str(parsed_year)

    if default_mode == "year":
        resolved_year = int(default_year if default_year is not None else date.today().year)
        return date(resolved_year, 1, 1), date(resolved_year, 12, 31), str(resolved_year)

    today = date.today()
    start, end = _month_bounds(today.year, today.month)
    return start, end, start.strftime("%B %Y")


def _previous_period(args, *, default_mode: str = "month") -> tuple[date, date, str]:
    mode = _period_mode(args, default_mode)
    if mode == "month":
        current_start, _, _ = _parse_period(args, default_mode=default_mode)
        prev_end = current_start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
        return prev_start, prev_end, prev_start.strftime("%B %Y")
    if mode == "quarter":
        current_start, _, _ = _parse_period(args, default_mode=default_mode)
        prev_end = current_start - timedelta(days=1)
        prev_quarter = ((prev_end.month - 1) // 3) + 1
        prev_start, prev_end_exact = _quarter_bounds(prev_end.year, prev_quarter)
        return prev_start, prev_end_exact, f"{prev_end.year}-Q{prev_quarter}"
    if mode == "year":
        current_start, _, _ = _parse_period(args, default_mode=default_mode)
        prev_year = current_start.year - 1
        return date(prev_year, 1, 1), date(prev_year, 12, 31), str(prev_year)
    raise ValueError(f"Unsupported period mode: {mode}")


def _parse_month_or_current(month_value: str | None) -> tuple[date, date, str]:
    if month_value:
        try:
            parsed = date.fromisoformat(f"{month_value}-01")
        except ValueError as exc:
            raise ValueError("month must be in YYYY-MM format") from exc
    else:
        parsed = date.today().replace(day=1)
    start, end = _month_bounds(parsed.year, parsed.month)
    return start, end, f"{start.year:04d}-{start.month:02d}"


def _normalize_budget_to_monthly(amount_cents: int, period: str) -> int:
    months = _BIZ_BUDGET_PERIOD_MONTHS.get(str(period))
    if months is None:
        raise ValueError("period must be monthly, quarterly, or yearly")
    return int(int(amount_cents) / int(months))


def _line_number_key(line_number: str) -> tuple[int, str]:
    match = _LINE_NUMBER_RE.match(str(line_number).strip())
    if not match:
        return (10_000, str(line_number))
    return (int(match.group(1)), match.group(2).lower())


def _unclassified_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
          FROM transactions
         WHERE is_active = 1
           AND use_type IS NULL
        """
    ).fetchone()
    return int(row["cnt"] or 0)


def _fmt_expense(cents: int) -> str:
    return fmt_dollars(cents_to_dollars(abs(cents)))


def _fmt_cashflow_expense(cents: int) -> str:
    return f"({fmt_dollars(cents_to_dollars(abs(cents)))})"


def _pl_rows(conn: sqlite3.Connection, start: date, end: date) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT pm.pl_section,
               pm.display_order,
               c.name AS category_name,
               SUM(t.amount_cents) AS total_cents,
               COUNT(*) AS txn_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN pl_section_map pm ON pm.category_id = c.id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY pm.pl_section, pm.display_order, c.name
         ORDER BY pm.display_order ASC, ABS(SUM(t.amount_cents)) DESC, c.name ASC
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return [
        {
            "pl_section": str(row["pl_section"]),
            "display_order": int(row["display_order"]),
            "category_name": str(row["category_name"]),
            "total_cents": int(row["total_cents"] or 0),
            "txn_count": int(row["txn_count"] or 0),
        }
        for row in rows
    ]


def _pl_unmapped_rows(conn: sqlite3.Connection, start: date, end: date) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT COALESCE(c.name, 'Uncategorized') AS category_name,
               SUM(t.amount_cents) AS total_cents,
               COUNT(*) AS txn_count
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN pl_section_map pm ON pm.category_id = c.id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           AND pm.category_id IS NULL
         GROUP BY COALESCE(c.name, 'Uncategorized')
         ORDER BY ABS(SUM(t.amount_cents)) DESC, category_name ASC
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return [
        {
            "category_name": str(row["category_name"]),
            "total_cents": int(row["total_cents"] or 0),
            "txn_count": int(row["txn_count"] or 0),
        }
        for row in rows
    ]


def _build_pl_data(
    mapped_rows: list[dict[str, Any]],
    unmapped_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    sections: dict[str, list[dict[str, Any]]] = {}
    section_totals: dict[str, int] = {}
    section_order: dict[str, int] = {}
    for row in mapped_rows:
        section = str(row["pl_section"])
        sections.setdefault(section, []).append(row)
        section_totals[section] = section_totals.get(section, 0) + int(row["total_cents"])
        section_order[section] = min(section_order.get(section, int(row["display_order"])), int(row["display_order"]))

    gross_revenue_cents = int(section_totals.get("revenue", 0))
    cogs_cents = int(section_totals.get("cogs", 0))
    gross_profit_cents = gross_revenue_cents + cogs_cents
    total_opex_cents = sum(total for section, total in section_totals.items() if section in _OPEX_SECTIONS)
    net_income_cents = gross_profit_cents + total_opex_cents

    sorted_sections = sorted(
        sections.keys(),
        key=lambda section: (section_order.get(section, 9999), section),
    )
    return {
        "sections": {section: sections[section] for section in sorted_sections},
        "section_totals_cents": section_totals,
        "gross_revenue_cents": gross_revenue_cents,
        "cogs_cents": cogs_cents,
        "gross_profit_cents": gross_profit_cents,
        "total_opex_cents": total_opex_cents,
        "net_income_cents": net_income_cents,
        "unmapped": unmapped_rows,
        "unmapped_count": sum(int(row["txn_count"]) for row in unmapped_rows),
    }


def _build_pl_cli(
    *,
    label: str,
    data: dict[str, Any],
    compare: dict[str, Any] | None,
    unclassified_count: int,
) -> str:
    lines: list[str] = [f"INCOME STATEMENT - {label}", ""]
    sections = data.get("sections", {})
    revenue_rows = sections.get("revenue", [])
    cogs_rows = sections.get("cogs", [])
    opex_sections = [section for section in sections.keys() if section in _OPEX_SECTIONS]

    lines.append("Revenue")
    if revenue_rows:
        for row in revenue_rows:
            lines.append(
                f"  {row['category_name']:<34} {fmt_dollars(cents_to_dollars(int(row['total_cents']))):>14}"
            )
    else:
        lines.append("  (none)")
    lines.append("  " + "-" * 52)
    lines.append(
        f"  {'Gross Revenue':<34} {fmt_dollars(cents_to_dollars(int(data['gross_revenue_cents']))):>14}"
    )
    lines.append("")

    lines.append("Cost of Goods Sold")
    if cogs_rows:
        for row in cogs_rows:
            lines.append(f"  {row['category_name']:<34} {_fmt_expense(int(row['total_cents'])):>14}")
    else:
        lines.append("  (none)")
    lines.append("  " + "-" * 52)
    lines.append(f"  {'Gross Profit':<34} {fmt_dollars(cents_to_dollars(int(data['gross_profit_cents']))):>14}")
    lines.append("")

    lines.append("Operating Expenses")
    if opex_sections:
        for section in opex_sections:
            section_label = _SECTION_LABELS.get(section, section)
            lines.append(f"  {section_label}")
            for row in sections[section]:
                lines.append(f"    {row['category_name']:<32} {_fmt_expense(int(row['total_cents'])):>14}")
    else:
        lines.append("  (none)")
    lines.append("  " + "-" * 52)
    lines.append(f"  {'Total Operating Expenses':<34} {_fmt_expense(int(data['total_opex_cents'])):>14}")
    lines.append("")
    lines.append(f"NET INCOME{'':<30} {fmt_dollars(cents_to_dollars(int(data['net_income_cents']))):>14}")

    unmapped_rows = data.get("unmapped", [])
    if unmapped_rows:
        lines.append("")
        lines.append("Unmapped Business Transactions")
        for row in unmapped_rows:
            lines.append(
                f"  {row['category_name']:<34} {fmt_dollars(cents_to_dollars(int(row['total_cents']))):>14} ({int(row['txn_count'])} txns)"
            )

    if compare:
        lines.append("")
        lines.append(f"Comparison vs {compare['label']}")
        lines.append(f"  {'Section':<22} {'Current':>12} {'Prior':>12} {'Delta':>12}")
        lines.append("  " + "-" * 62)
        for row in compare.get("section_totals", []):
            lines.append(
                f"  {row['section_label']:<22} "
                f"{fmt_dollars(cents_to_dollars(int(row['current_cents']))):>12} "
                f"{fmt_dollars(cents_to_dollars(int(row['prior_cents']))):>12} "
                f"{fmt_dollars(cents_to_dollars(int(row['delta_cents']))):>12}"
            )

    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")
    return "\n".join(lines)


def handle_pl(args, conn: sqlite3.Connection) -> dict[str, Any]:
    start, end, label = _parse_period(args, default_mode="month")
    mapped_rows = _pl_rows(conn, start, end)
    unmapped_rows = _pl_unmapped_rows(conn, start, end)
    data = _build_pl_data(mapped_rows, unmapped_rows)
    unclassified_count = _unclassified_count(conn)

    compare_payload: dict[str, Any] | None = None
    if bool(getattr(args, "compare", False)):
        prev_start, prev_end, prev_label = _previous_period(args, default_mode="month")
        prev_data = _build_pl_data(
            _pl_rows(conn, prev_start, prev_end),
            _pl_unmapped_rows(conn, prev_start, prev_end),
        )
        section_keys = sorted(
            set(data["section_totals_cents"].keys()) | set(prev_data["section_totals_cents"].keys()),
            key=lambda key: (
                0 if key == "revenue" else 1 if key == "cogs" else 2,
                _SECTION_LABELS.get(key, key),
            ),
        )
        compare_rows = []
        for section in section_keys:
            current_cents = int(data["section_totals_cents"].get(section, 0))
            prior_cents = int(prev_data["section_totals_cents"].get(section, 0))
            compare_rows.append(
                {
                    "section": section,
                    "section_label": _SECTION_LABELS.get(section, section),
                    "current_cents": current_cents,
                    "prior_cents": prior_cents,
                    "delta_cents": current_cents - prior_cents,
                }
            )
        compare_payload = {
            "label": prev_label,
            "start_date": prev_start.isoformat(),
            "end_date": prev_end.isoformat(),
            "section_totals": compare_rows,
        }

    payload = {
        "period": {
            "label": label,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
        **data,
        "compare": compare_payload,
        "unclassified_count": unclassified_count,
    }
    return {
        "data": payload,
        "summary": {
            "period": label,
            "net_income_cents": int(data["net_income_cents"]),
            "unmapped_count": int(data["unmapped_count"]),
            "unclassified_count": unclassified_count,
        },
        "cli_report": _build_pl_cli(
            label=label,
            data=data,
            compare=compare_payload,
            unclassified_count=unclassified_count,
        ),
    }


def _schedule_c_unmapped_rows(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
    tax_year: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT COALESCE(c.name, 'Uncategorized') AS category_name,
               SUM(t.amount_cents) AS total_cents,
               COUNT(*) AS txn_count
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           AND (
               t.category_id IS NULL
               OR (COALESCE(c.is_income, 0) = 0 AND sm.category_id IS NULL)
           )
         GROUP BY COALESCE(c.name, 'Uncategorized')
         ORDER BY ABS(SUM(t.amount_cents)) DESC, category_name ASC
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchall()
    return [
        {
            "category_name": str(row["category_name"]),
            "total_cents": int(row["total_cents"] or 0),
            "txn_count": int(row["txn_count"] or 0),
        }
        for row in rows
    ]


def _schedule_c_snapshot(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
    tax_year: int,
    config: dict[str, str] | None = None,
) -> dict[str, Any]:
    line_1_row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND c.is_income = 1
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchone()
    line_1_cents = int(line_1_row["total_cents"] or 0)

    cogs_row = conn.execute(
        """
        SELECT COALESCE(SUM(t.amount_cents), 0) AS net_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND sm.line_number = '42'
           AND t.date >= ?
           AND t.date <= ?
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchone()
    cogs_net_cents = int(cogs_row["net_cents"] or 0)
    line_4_cogs_cents = max(-cogs_net_cents, 0)

    expense_rows = conn.execute(
        """
        SELECT sm.line_number,
               sm.schedule_c_line,
               sm.deduction_pct,
               SUM(t.amount_cents) AS net_cents,
               COUNT(*) AS txn_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND sm.line_number != '42'
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY sm.line_number, sm.schedule_c_line, sm.deduction_pct
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchall()

    line_items: list[dict[str, Any]] = []
    for row in expense_rows:
        net_cents = int(row["net_cents"] or 0)
        actual_cents = abs(net_cents)
        if net_cents > 0:
            actual_cents = 0
        deduction_pct = float(row["deduction_pct"] or 0.0)
        deductible_cents = _round_half_up(actual_cents * deduction_pct)
        if net_cents > 0:
            deductible_cents = 0
        line_items.append(
            {
                "line_number": str(row["line_number"]),
                "line_label": str(row["schedule_c_line"]),
                "deduction_pct": deduction_pct,
                "actual_cents": actual_cents,
                "deductible_cents": max(0, int(deductible_cents)),
                "txn_count": int(row["txn_count"] or 0),
            }
        )

    warnings: list[str] = []
    mileage_method = str((config or {}).get("mileage_method", "actual") or "actual").strip().lower()
    if mileage_method not in {"standard", "actual"}:
        mileage_method = "actual"
    mileage_summary: dict[str, Any] | None = None
    if mileage_method == "standard":
        mileage_summary = _mileage_summary_payload(
            conn,
            tax_year=tax_year,
            start=start,
            end=end,
        )
        line_9_cents = int(mileage_summary["total_deduction_cents"])
        line_9_trips = int(mileage_summary["trip_count"])
        if line_9_trips == 0:
            line_9_cents = 0
            warnings.append(f"Standard mileage method selected but no trips logged for {tax_year}.")

        line_9_label = _line_label_for_number(
            conn,
            tax_year=tax_year,
            line_number="9",
            default="Car and truck expenses",
        )
        line_9_item = next((item for item in line_items if str(item["line_number"]) == "9"), None)
        if line_9_item is None:
            line_items.append(
                {
                    "line_number": "9",
                    "line_label": line_9_label,
                    "deduction_pct": 1.0,
                    "actual_cents": int(line_9_cents),
                    "deductible_cents": int(line_9_cents),
                    "txn_count": int(line_9_trips),
                }
            )
        else:
            line_9_item["line_label"] = line_9_label
            line_9_item["deduction_pct"] = 1.0
            line_9_item["actual_cents"] = int(line_9_cents)
            line_9_item["deductible_cents"] = int(line_9_cents)
            line_9_item["txn_count"] = int(line_9_trips)

    line_items.sort(key=lambda item: _line_number_key(str(item["line_number"])))

    line_28_total_expenses_cents = sum(int(item["deductible_cents"]) for item in line_items)
    line_7_gross_income_cents = line_1_cents - line_4_cogs_cents
    line_31_before_home_office_cents = line_7_gross_income_cents - line_28_total_expenses_cents
    home_office = _compute_home_office(config or {}, line_31_before_home_office_cents)
    line_30_home_office_cents = int(home_office["deduction_cents"])
    line_31_net_profit_cents = line_31_before_home_office_cents - line_30_home_office_cents
    unmapped_rows = _schedule_c_unmapped_rows(conn, start=start, end=end, tax_year=tax_year)

    return {
        "line_1_gross_receipts_cents": line_1_cents,
        "line_4_cogs_cents": line_4_cogs_cents,
        "line_7_gross_income_cents": line_7_gross_income_cents,
        "line_items": line_items,
        "line_28_total_expenses_cents": line_28_total_expenses_cents,
        "line_30_home_office": str(home_office["display"]),
        "line_30_home_office_method": str(home_office["method"]),
        "line_30_home_office_cents": line_30_home_office_cents,
        "line_31_before_home_office_cents": int(line_31_before_home_office_cents),
        "line_31_net_profit_cents": line_31_net_profit_cents,
        "home_office": home_office,
        "mileage_method": mileage_method,
        "mileage_summary": mileage_summary,
        "warnings": warnings,
        "unmapped": unmapped_rows,
        "unmapped_count": sum(int(row["txn_count"]) for row in unmapped_rows),
    }


def _schedule_c_transaction_groups(
    conn: sqlite3.Connection,
    *,
    start: date,
    end: date,
    tax_year: int,
) -> dict[str, Any]:
    income_row = conn.execute(
        """
        SELECT COUNT(*) AS txn_count,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND c.is_income = 1
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchone()

    rows = conn.execute(
        """
        SELECT sm.line_number,
               sm.schedule_c_line,
               sm.deduction_pct,
               COUNT(*) AS txn_count,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN schedule_c_map sm
            ON sm.category_id = c.id
           AND sm.tax_year = ?
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY sm.line_number, sm.schedule_c_line, sm.deduction_pct
         ORDER BY sm.line_number ASC
        """,
        (tax_year, start.isoformat(), end.isoformat()),
    ).fetchall()
    line_groups: list[dict[str, Any]] = []
    for row in rows:
        line_number = str(row["line_number"])
        total_cents = int(row["total_cents"] or 0)
        deduction_pct = float(row["deduction_pct"] or 0.0)
        deductible_cents = 0
        if line_number == "42":
            deductible_cents = max(-total_cents, 0)
        elif total_cents < 0:
            deductible_cents = _round_half_up(abs(total_cents) * deduction_pct)
        line_groups.append(
            {
                "line_number": line_number,
                "line_label": str(row["schedule_c_line"]),
                "deduction_pct": deduction_pct,
                "txn_count": int(row["txn_count"] or 0),
                "total_cents": total_cents,
                "deductible_cents": int(max(0, deductible_cents)),
            }
        )

    line_groups.sort(key=lambda item: _line_number_key(item["line_number"]))
    unmapped = _schedule_c_unmapped_rows(conn, start=start, end=end, tax_year=tax_year)
    return {
        "income": {
            "line_number": "1",
            "line_label": "Gross receipts",
            "txn_count": int(income_row["txn_count"] or 0),
            "total_cents": int(income_row["total_cents"] or 0),
        },
        "line_groups": line_groups,
        "unmapped": unmapped,
    }


def _latest_tax_year(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(tax_year) AS tax_year FROM schedule_c_map").fetchone()
    if row and row["tax_year"] is not None:
        return int(row["tax_year"])
    return None


def _year_for_period_args(args) -> int | None:
    month = getattr(args, "month", None)
    quarter = getattr(args, "quarter", None)
    year = getattr(args, "year", None)
    if month:
        return int(str(month).split("-", 1)[0])
    if quarter:
        match = _QUARTER_RE.match(str(quarter))
        if match:
            return int(match.group(1))
    if year and _YEAR_RE.match(str(year)):
        return int(year)
    return None


def _build_tax_cli(
    *,
    tax_year: int,
    period_label: str,
    snapshot: dict[str, Any],
    quarterly_breakdown: list[dict[str, Any]],
) -> str:
    lines = [f"SCHEDULE C SUMMARY - Tax Year {tax_year} ({period_label})", "", "Part I: Income"]
    lines.append(
        f"  Line 1   {'Gross receipts':<34} {fmt_dollars(cents_to_dollars(int(snapshot['line_1_gross_receipts_cents']))):>14}"
    )
    lines.append(f"  Line 4   {'Cost of goods sold':<34} {_fmt_cashflow_expense(int(snapshot['line_4_cogs_cents'])):>14}")
    lines.append(
        f"  Line 7   {'Gross income':<34} {fmt_dollars(cents_to_dollars(int(snapshot['line_7_gross_income_cents']))):>14}"
    )
    lines.append("")
    lines.append("Part II: Expenses")
    for item in snapshot.get("line_items", []):
        line_label = str(item["line_label"])
        if float(item["deduction_pct"]) < 1.0:
            deduction_pct = int(round(float(item["deduction_pct"]) * 100))
            line_label = f"{line_label} ({deduction_pct}%)"
            value = (
                f"{fmt_dollars(cents_to_dollars(int(item['deductible_cents'])))} "
                f"of {fmt_dollars(cents_to_dollars(int(item['actual_cents'])))}"
            )
        else:
            value = fmt_dollars(cents_to_dollars(int(item["deductible_cents"])))
        lines.append(f"  Line {item['line_number']:<4} {line_label:<34} {value:>14}")

    lines.append("  " + "-" * 58)
    lines.append(
        f"  Line 28  {'Total expenses':<34} {fmt_dollars(cents_to_dollars(int(snapshot['line_28_total_expenses_cents']))):>14}"
    )
    lines.append(f"  Line 30  {'Home office deduction':<34} {snapshot['line_30_home_office']:>14}")
    lines.append(
        f"  Line 31  {'Net profit (loss)':<34} {fmt_dollars(cents_to_dollars(int(snapshot['line_31_net_profit_cents']))):>14}"
    )

    lines.append("")
    lines.append("Quarterly Breakdown")
    for row in quarterly_breakdown:
        lines.append(
            f"  Q{row['quarter']}: "
            f"{fmt_dollars(cents_to_dollars(int(row['income_cents'])))} income / "
            f"{fmt_dollars(cents_to_dollars(int(row['deductible_expense_cents'])))} expenses / "
            f"{fmt_dollars(cents_to_dollars(int(row['net_profit_cents'])))} net"
        )

    unmapped_rows = snapshot.get("unmapped", [])
    if unmapped_rows:
        lines.append("")
        lines.append("Unmapped Business Transactions")
        for row in unmapped_rows:
            lines.append(
                f"  {row['category_name']:<34} {fmt_dollars(cents_to_dollars(int(row['total_cents']))):>14} ({int(row['txn_count'])} txns)"
            )

    return "\n".join(lines)


def _build_tax_summary_lines(tax_summary: dict[str, Any]) -> list[str]:
    schedule_se = tax_summary["schedule_se"]
    health = tax_summary["health_insurance"]
    qbi = tax_summary["qbi"]
    federal = tax_summary["federal"]
    ny_state = tax_summary["ny_state"]
    nyc = tax_summary["nyc"]
    s_corp = tax_summary["s_corp"]

    lines = ["Tax Summary", ""]
    lines.append("Self-Employment Tax (Schedule SE)")
    lines.append(
        f"  SE taxable (92.35%):           {fmt_dollars(cents_to_dollars(int(schedule_se['se_taxable_cents']))):>14}"
    )
    lines.append(
        f"  Social Security (12.4%):       {fmt_dollars(cents_to_dollars(int(schedule_se['ss_tax_cents']))):>14}"
    )
    lines.append(
        f"  Medicare (2.9%):               {fmt_dollars(cents_to_dollars(int(schedule_se['medicare_tax_cents']))):>14}"
    )
    lines.append(
        f"  Additional Medicare:           {fmt_dollars(cents_to_dollars(int(schedule_se['additional_medicare_cents']))):>14}"
    )
    lines.append(
        f"  Total SE tax:                  {fmt_dollars(cents_to_dollars(int(schedule_se['total_se_cents']))):>14}"
    )
    lines.append(
        f"  Deductible half:               {fmt_dollars(cents_to_dollars(int(schedule_se['deductible_half_cents']))):>14}"
    )
    lines.append("")
    lines.append("Above-the-Line Deductions")
    lines.append(
        f"  Health insurance:              {fmt_dollars(cents_to_dollars(int(health['deduction_cents']))):>14}"
    )
    lines.append(
        f"  Deductible half SE tax:        {fmt_dollars(cents_to_dollars(int(schedule_se['deductible_half_cents']))):>14}"
    )
    lines.append("")
    lines.append("Qualified Business Income (Form 8995)")
    lines.append(
        f"  QBI deduction:                 {fmt_dollars(cents_to_dollars(int(qbi['qbi_deduction_cents']))):>14}"
    )
    lines.append("")
    lines.append("Estimated Tax Liability")
    lines.append(f"  Federal income tax:            {fmt_dollars(cents_to_dollars(int(federal['tax_cents']))):>14}")
    lines.append(f"  Self-employment tax:           {fmt_dollars(cents_to_dollars(int(schedule_se['total_se_cents']))):>14}")
    lines.append(f"  NY state income tax:           {fmt_dollars(cents_to_dollars(int(ny_state['tax_cents']))):>14}")
    lines.append(f"  NYC income + UBT:              {fmt_dollars(cents_to_dollars(int(nyc['total_nyc_tax_cents']))):>14}")
    lines.append("  " + "-" * 56)
    lines.append(
        f"  Total estimated tax:           {fmt_dollars(cents_to_dollars(int(tax_summary['total_estimated_tax_cents']))):>14}"
    )
    lines.append(
        f"  Quarterly payment:             {fmt_dollars(cents_to_dollars(int(tax_summary['quarterly_payment_cents']))):>14}"
    )
    lines.append("")
    lines.append(
        f"S-Corp advisory: estimated payroll-tax savings {fmt_dollars(cents_to_dollars(int(s_corp['net_savings_cents'])))}"
    )
    for note in s_corp.get("notes", []):
        lines.append(f"  - {note}")
    for warning in qbi.get("warnings", []):
        lines.append(f"  - {warning}")
    for warning in ny_state.get("warnings", []):
        lines.append(f"  - {warning}")
    return lines


def _build_tax_detail_lines(
    *,
    detail: str,
    snapshot: dict[str, Any],
    tax_summary: dict[str, Any],
    transaction_groups: dict[str, Any],
) -> list[str]:
    lines = [f"Detail: {detail}"]
    if detail == "form-8829":
        home_office = snapshot.get("home_office", {})
        lines.append("")
        lines.append(f"  Method: {home_office.get('method', 'not_configured')}")
        lines.append(
            f"  Line 31 before home office: {fmt_dollars(cents_to_dollars(int(snapshot.get('line_31_before_home_office_cents', 0))))}"
        )
        lines.append(
            f"  Home office deduction:      {fmt_dollars(cents_to_dollars(int(snapshot.get('line_30_home_office_cents', 0))))}"
        )
        lines.append(f"  Line 30 display:            {snapshot.get('line_30_home_office', '')}")
    elif detail == "schedule-se":
        se = tax_summary["schedule_se"]
        lines.append("")
        lines.append(f"  Line 31 profit:             {fmt_dollars(cents_to_dollars(int(se['line_31_cents'])))}")
        lines.append(f"  SE taxable (92.35%):        {fmt_dollars(cents_to_dollars(int(se['se_taxable_cents'])))}")
        lines.append(f"  SS taxable wages:           {fmt_dollars(cents_to_dollars(int(se['ss_taxable_cents'])))}")
        lines.append(f"  Social Security tax:        {fmt_dollars(cents_to_dollars(int(se['ss_tax_cents'])))}")
        lines.append(f"  Medicare tax:               {fmt_dollars(cents_to_dollars(int(se['medicare_tax_cents'])))}")
        lines.append(
            f"  Additional Medicare tax:    {fmt_dollars(cents_to_dollars(int(se['additional_medicare_cents'])))}"
        )
        lines.append(f"  Total SE tax:               {fmt_dollars(cents_to_dollars(int(se['total_se_cents'])))}")
        lines.append(f"  Deductible half:            {fmt_dollars(cents_to_dollars(int(se['deductible_half_cents'])))}")
    elif detail == "qbi":
        qbi = tax_summary["qbi"]
        lines.append("")
        lines.append(f"  QBI base amount:            {fmt_dollars(cents_to_dollars(int(qbi['qbi_cents'])))}")
        lines.append(
            f"  Tentative 20% deduction:    {fmt_dollars(cents_to_dollars(int(qbi['tentative_qbi_deduction_cents'])))}"
        )
        lines.append(
            f"  Taxable-income cap (20%):   {fmt_dollars(cents_to_dollars(int(qbi['taxable_income_cap_cents'])))}"
        )
        lines.append(f"  QBI deduction:              {fmt_dollars(cents_to_dollars(int(qbi['qbi_deduction_cents'])))}")
        lines.append(f"  Threshold:                  {fmt_dollars(cents_to_dollars(int(qbi['threshold_cents'])))}")
        for warning in qbi.get("warnings", []):
            lines.append(f"  - {warning}")
    elif detail == "health-insurance":
        health = tax_summary["health_insurance"]
        lines.append("")
        lines.append(
            f"  Monthly premium:            {fmt_dollars(cents_to_dollars(int(health['monthly_premium_cents'])))}"
        )
        lines.append(
            f"  Annual premiums:            {fmt_dollars(cents_to_dollars(int(health['annual_premiums_cents'])))}"
        )
        lines.append(
            f"  Earned-income cap:          {fmt_dollars(cents_to_dollars(int(health['earned_income_cap_cents'])))}"
        )
        lines.append(f"  Deduction allowed:          {fmt_dollars(cents_to_dollars(int(health['deduction_cents'])))}")
    elif detail == "s-corp":
        s_corp = tax_summary["s_corp"]
        lines.append("")
        lines.append(f"  Net profit:                 {fmt_dollars(cents_to_dollars(int(s_corp['profit_cents'])))}")
        lines.append(f"  Salary:                     {fmt_dollars(cents_to_dollars(int(s_corp['salary_cents'])))}")
        lines.append(
            f"  Distribution:               {fmt_dollars(cents_to_dollars(int(s_corp['distribution_cents'])))}"
        )
        lines.append(
            f"  Sole-prop SE tax:           {fmt_dollars(cents_to_dollars(int(s_corp['sole_prop_se_tax_cents'])))}"
        )
        lines.append(
            f"  S-Corp payroll taxes:       {fmt_dollars(cents_to_dollars(int(s_corp['s_corp_payroll_tax_total_cents'])))}"
        )
        lines.append(
            f"  Net payroll-tax savings:    {fmt_dollars(cents_to_dollars(int(s_corp['net_savings_cents'])))}"
        )
        for note in s_corp.get("notes", []):
            lines.append(f"  - {note}")
    elif detail == "transactions":
        income = transaction_groups["income"]
        lines.append("")
        lines.append(
            f"  Line {income['line_number']} {income['line_label']}: "
            f"{fmt_dollars(cents_to_dollars(int(income['total_cents'])))} ({int(income['txn_count'])} txns)"
        )
        for group in transaction_groups.get("line_groups", []):
            deductible = fmt_dollars(cents_to_dollars(int(group["deductible_cents"])))
            total = fmt_dollars(cents_to_dollars(int(group["total_cents"])))
            lines.append(
                f"  Line {group['line_number']:<4} {group['line_label']:<30} {deductible:>12} deductible "
                f"({total} net, {int(group['txn_count'])} txns)"
            )
        if transaction_groups.get("unmapped"):
            lines.append("")
            lines.append("  Unmapped")
            for row in transaction_groups["unmapped"]:
                lines.append(
                    f"    {row['category_name']}: {fmt_dollars(cents_to_dollars(int(row['total_cents'])))} "
                    f"({int(row['txn_count'])} txns)"
                )
    else:
        lines.append("")
        lines.append("  No detail available.")
    return lines


def handle_tax(args, conn: sqlite3.Connection) -> dict[str, Any]:
    explicit_year = _year_for_period_args(args)
    latest_tax_year = _latest_tax_year(conn)
    default_tax_year = latest_tax_year or date.today().year
    selected_tax_year = explicit_year if explicit_year is not None else default_tax_year

    start, end, label = _parse_period(
        args,
        default_mode="year",
        default_year=(selected_tax_year if explicit_year is None else None),
    )
    if explicit_year is None:
        selected_tax_year = start.year

    config = _get_tax_config(conn, selected_tax_year)
    salary_override_cents = _dollars_to_cents(getattr(args, "salary", None))
    summary_config = dict(config)
    if salary_override_cents is not None:
        summary_config["analysis_salary_override_cents"] = str(salary_override_cents)

    snapshot = _schedule_c_snapshot(conn, start=start, end=end, tax_year=selected_tax_year, config=config)
    quarterly_breakdown: list[dict[str, Any]] = []
    for quarter in (1, 2, 3, 4):
        q_start, q_end = _quarter_bounds(start.year, quarter)
        if q_end < start or q_start > end:
            continue
        range_start = max(q_start, start)
        range_end = min(q_end, end)
        quarter_snapshot = _schedule_c_snapshot(
            conn,
            start=range_start,
            end=range_end,
            tax_year=selected_tax_year,
            config=config,
        )
        quarterly_breakdown.append(
            {
                "quarter": quarter,
                "start_date": range_start.isoformat(),
                "end_date": range_end.isoformat(),
                "income_cents": int(quarter_snapshot["line_1_gross_receipts_cents"]),
                "cogs_cents": int(quarter_snapshot["line_4_cogs_cents"]),
                "deductible_expense_cents": int(quarter_snapshot["line_28_total_expenses_cents"]),
                "net_profit_cents": int(quarter_snapshot["line_31_net_profit_cents"]),
            }
        )

    detail = str(getattr(args, "detail", "") or "").strip().lower() or None
    transaction_groups = _schedule_c_transaction_groups(conn, start=start, end=end, tax_year=selected_tax_year)
    tax_summary = _compute_full_tax_summary(conn, snapshot, summary_config, selected_tax_year)
    unclassified_count = _unclassified_count(conn)

    detail_payloads = {
        "form-8829": snapshot.get("home_office", {}),
        "schedule-se": tax_summary["schedule_se"],
        "qbi": tax_summary["qbi"],
        "health-insurance": tax_summary["health_insurance"],
        "s-corp": tax_summary["s_corp"],
        "transactions": transaction_groups,
    }
    for group in transaction_groups.get("line_groups", []):
        line_num = str(group.get("line_number") or "").strip()
        if line_num:
            detail_payloads[line_num] = group
    detail_payload = detail_payloads.get(detail) if detail else None

    cli_lines = [
        _build_tax_cli(
            tax_year=selected_tax_year,
            period_label=label,
            snapshot=snapshot,
            quarterly_breakdown=quarterly_breakdown,
        )
    ]
    cli_lines.append("")
    if detail:
        cli_lines.extend(
            _build_tax_detail_lines(
                detail=detail,
                snapshot=snapshot,
                tax_summary=tax_summary,
                transaction_groups=transaction_groups,
            )
        )
    else:
        cli_lines.extend(_build_tax_summary_lines(tax_summary))
    for warning in snapshot.get("warnings", []):
        cli_lines.append(f"WARNING: {warning}")
    cli_lines.append("")
    cli_lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")
    cli_lines.extend(_tax_assumption_lines())

    payload = {
        "period": {
            "label": label,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        },
        "tax_year": selected_tax_year,
        "tax_config": config,
        **snapshot,
        "quarterly_breakdown": quarterly_breakdown,
        "tax_summary": tax_summary,
        "transaction_groups": transaction_groups,
        "detail_section": detail,
        "detail": detail_payload,
        "unclassified_count": unclassified_count,
    }
    return {
        "data": payload,
        "summary": {
            "tax_year": selected_tax_year,
            "line_31_net_profit_cents": int(snapshot["line_31_net_profit_cents"]),
            "line_28_total_expenses_cents": int(snapshot["line_28_total_expenses_cents"]),
            "total_estimated_tax_cents": int(tax_summary["total_estimated_tax_cents"]),
            "unmapped_count": int(snapshot["unmapped_count"]),
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(cli_lines),
    }


def _parse_est_quarter(est_quarter: str | None) -> tuple[int, int, str]:
    if est_quarter:
        match = _QUARTER_RE.match(est_quarter)
        if not match:
            raise ValueError("quarter must be in YYYY-Q1..Q4 format")
        year = int(match.group(1))
        quarter = int(match.group(2))
        return year, quarter, f"{year}-Q{quarter}"
    today = date.today()
    quarter = ((today.month - 1) // 3) + 1
    return today.year, quarter, f"{today.year}-Q{quarter}"


def _due_dates_for_year(year: int) -> list[str]:
    return [
        f"{year}-04-15",
        f"{year}-06-15",
        f"{year}-09-15",
        f"{year + 1}-01-15",
    ]


def handle_tax_setup(args, conn: sqlite3.Connection) -> dict[str, Any]:
    year = str(getattr(args, "year", "")).strip()
    if not _YEAR_RE.match(year):
        raise ValueError("year must be in YYYY format")
    tax_year = int(year)

    updates: dict[str, str] = {}
    method = getattr(args, "method", None)
    if method is not None:
        updates["home_office_method"] = str(method).strip().lower()

    sqft = getattr(args, "sqft", None)
    if sqft is not None:
        if int(sqft) < 0:
            raise ValueError("sqft must be >= 0")
        updates["home_office_sqft"] = str(int(sqft))

    total_sqft = getattr(args, "total_sqft", None)
    if total_sqft is not None:
        if int(total_sqft) <= 0:
            raise ValueError("total-sqft must be > 0")
        updates["home_total_sqft"] = str(int(total_sqft))

    filing_status = getattr(args, "filing_status", None)
    if filing_status is not None:
        updates["filing_status"] = str(filing_status).strip().lower()

    state = getattr(args, "state", None)
    if state is not None:
        normalized_state = str(state).strip().upper()
        if normalized_state in {"", "NONE", "FEDERAL"}:
            normalized_state = ""
        if normalized_state not in {"", "NY", "NY-NYC"}:
            raise ValueError("state must be NY, NY-NYC, or empty")
        updates["state"] = normalized_state

    health_monthly = _dollars_to_cents(getattr(args, "health_insurance_monthly", None))
    if health_monthly is not None:
        if health_monthly < 0:
            raise ValueError("health-insurance-monthly must be >= 0")
        updates["health_insurance_monthly_cents"] = str(health_monthly)

    w2_wages = _dollars_to_cents(getattr(args, "w2_wages", None))
    if w2_wages is not None:
        if w2_wages < 0:
            raise ValueError("w2-wages must be >= 0")
        updates["w2_wages_cents"] = str(w2_wages)

    mileage_method = getattr(args, "mileage_method", None)
    if mileage_method is not None:
        normalized_method = str(mileage_method).strip().lower()
        if normalized_method not in {"standard", "actual"}:
            raise ValueError("mileage-method must be standard or actual")
        updates["mileage_method"] = normalized_method

    for key, value in updates.items():
        _set_tax_config(conn, tax_year, key, value)
    conn.commit()

    config = _get_tax_config(conn, tax_year)
    unclassified_count = _unclassified_count(conn)
    lines = [f"TAX SETUP - {tax_year}"]
    if updates:
        lines.append("")
        lines.append("Updated fields:")
        for key in sorted(updates.keys()):
            lines.append(f"  {key}: {config.get(key, '')}")
    else:
        lines.append("")
        lines.append("No updates provided. Current config returned.")
    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")

    return {
        "data": {
            "tax_year": tax_year,
            "updated_keys": sorted(updates.keys()),
            "config": config,
            "unclassified_count": unclassified_count,
        },
        "summary": {
            "tax_year": tax_year,
            "updated_count": len(updates),
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(lines),
    }


def _build_tax_package_markdown(
    *,
    tax_year: int,
    period_label: str,
    snapshot: dict[str, Any],
    quarterly_breakdown: list[dict[str, Any]],
    tax_summary: dict[str, Any],
    transaction_groups: dict[str, Any],
    unclassified_count: int,
) -> str:
    lines = [
        _build_tax_cli(
            tax_year=tax_year,
            period_label=period_label,
            snapshot=snapshot,
            quarterly_breakdown=quarterly_breakdown,
        ),
        "",
    ]
    lines.extend(_build_tax_summary_lines(tax_summary))
    lines.append("")
    lines.extend(
        _build_tax_detail_lines(
            detail="transactions",
            snapshot=snapshot,
            tax_summary=tax_summary,
            transaction_groups=transaction_groups,
        )
    )
    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")
    lines.extend(_tax_assumption_lines())
    return "\n".join(lines)


def handle_tax_package(args, conn: sqlite3.Connection) -> dict[str, Any]:
    year = str(getattr(args, "year", "")).strip()
    if not _YEAR_RE.match(year):
        raise ValueError("year must be in YYYY format")
    tax_year = int(year)

    start = date(tax_year, 1, 1)
    end = date(tax_year, 12, 31)
    period_label = str(tax_year)
    config = _get_tax_config(conn, tax_year)
    salary_override_cents = _dollars_to_cents(getattr(args, "salary", None))
    summary_config = dict(config)
    if salary_override_cents is not None:
        summary_config["analysis_salary_override_cents"] = str(salary_override_cents)

    snapshot = _schedule_c_snapshot(conn, start=start, end=end, tax_year=tax_year, config=config)
    quarterly_breakdown: list[dict[str, Any]] = []
    for quarter in (1, 2, 3, 4):
        q_start, q_end = _quarter_bounds(tax_year, quarter)
        quarter_snapshot = _schedule_c_snapshot(conn, start=q_start, end=q_end, tax_year=tax_year, config=config)
        quarterly_breakdown.append(
            {
                "quarter": quarter,
                "start_date": q_start.isoformat(),
                "end_date": q_end.isoformat(),
                "income_cents": int(quarter_snapshot["line_1_gross_receipts_cents"]),
                "cogs_cents": int(quarter_snapshot["line_4_cogs_cents"]),
                "deductible_expense_cents": int(quarter_snapshot["line_28_total_expenses_cents"]),
                "net_profit_cents": int(quarter_snapshot["line_31_net_profit_cents"]),
            }
        )

    transaction_groups = _schedule_c_transaction_groups(conn, start=start, end=end, tax_year=tax_year)
    tax_summary = _compute_full_tax_summary(conn, snapshot, summary_config, tax_year)
    unclassified_count = _unclassified_count(conn)
    mileage_summary = _mileage_summary_payload(conn, tax_year=tax_year)
    contractor_summary = _contractor_summary_payload(conn, tax_year=tax_year, include_inactive=True)
    markdown_report = _build_tax_package_markdown(
        tax_year=tax_year,
        period_label=period_label,
        snapshot=snapshot,
        quarterly_breakdown=quarterly_breakdown,
        tax_summary=tax_summary,
        transaction_groups=transaction_groups,
        unclassified_count=unclassified_count,
    )

    payload = {
        "tax_year": tax_year,
        "period": {"label": period_label, "start_date": start.isoformat(), "end_date": end.isoformat()},
        "tax_config": config,
        "schedule_c": snapshot,
        "quarterly_breakdown": quarterly_breakdown,
        "tax_summary": tax_summary,
        "transaction_groups": transaction_groups,
        "unclassified_count": unclassified_count,
        "assumptions": list(_TAX_ASSUMPTIONS),
    }
    if int(mileage_summary["trip_count"]) > 0:
        payload["mileage_summary"] = mileage_summary
    if (
        int(contractor_summary["totals"]["payment_count"]) > 0
        or int(contractor_summary["unlinked_contract_labor"]["total_cents"]) > 0
    ):
        payload["contractor_summary"] = contractor_summary

    output = getattr(args, "output", None)
    cli_report = markdown_report
    output_format = "markdown"
    if output:
        output_path = Path(str(output)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.suffix.lower() == ".json":
            output_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
            output_format = "json"
        else:
            output_path.write_text(markdown_report, encoding="utf-8")
            output_format = "markdown"
        payload["output"] = str(output_path)
        payload["output_format"] = output_format
        cli_report = "\n".join(
            [
                f"Wrote tax package ({output_format}) to {output_path}",
                f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}",
                *_tax_assumption_lines(),
            ]
        )

    return {
        "data": payload,
        "summary": {
            "tax_year": tax_year,
            "line_31_net_profit_cents": int(snapshot["line_31_net_profit_cents"]),
            "total_estimated_tax_cents": int(tax_summary["total_estimated_tax_cents"]),
            "quarterly_payment_cents": int(tax_summary["quarterly_payment_cents"]),
            "unclassified_count": unclassified_count,
        },
        "cli_report": cli_report,
    }


def handle_estimated_tax(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rate_value = getattr(args, "rate", None)
    if rate_value is not None and float(rate_value) < 0:
        raise ValueError("rate must be >= 0")
    include_se = bool(getattr(args, "include_se", True))
    year, quarter, label = _parse_est_quarter(getattr(args, "est_quarter", None))
    year_override = getattr(args, "year", None)
    if year_override and not getattr(args, "est_quarter", None):
        year = int(year_override)
        quarter = 4
        label = str(year)
        ytd_end = date(year, 12, 31)
    else:
        _, ytd_end = _quarter_bounds(year, quarter)
    ytd_start = date(year, 1, 1)
    tax_year = year
    config = _get_tax_config(conn, tax_year)
    salary_override_cents = _dollars_to_cents(getattr(args, "salary", None))
    summary_config = dict(config)
    if salary_override_cents is not None:
        summary_config["analysis_salary_override_cents"] = str(salary_override_cents)

    ytd_snapshot = _schedule_c_snapshot(conn, start=ytd_start, end=ytd_end, tax_year=tax_year, config=config)
    ytd_net_profit_cents = int(ytd_snapshot["line_31_net_profit_cents"])

    annualized_profit_cents = _round_half_up(max(0, ytd_net_profit_cents) * (4 / quarter))
    uses_brackets = bool(config) and rate_value is None
    federal_cents = 0
    se_cents = 0
    state_cents = 0
    city_cents = 0
    effective_rate: float | None = None

    if annualized_profit_cents > 0 and uses_brackets:
        annual_snapshot = {"line_31_net_profit_cents": annualized_profit_cents}
        annual_summary = _compute_full_tax_summary(conn, annual_snapshot, summary_config, tax_year)
        federal_cents = int(annual_summary["federal"]["tax_cents"])
        se_cents = int(annual_summary["schedule_se"]["total_se_cents"]) if include_se else 0
        state_cents = int(annual_summary["ny_state"]["tax_cents"])
        city_cents = int(annual_summary["nyc"]["total_nyc_tax_cents"])
        estimated_annual_tax_cents = int(federal_cents + se_cents + state_cents + city_cents)
    else:
        effective_rate = float(rate_value) if rate_value is not None else 0.30
        estimated_annual_tax_cents = _round_half_up(annualized_profit_cents * effective_rate)

    estimated_quarterly_payment_cents = _round_half_up(estimated_annual_tax_cents / 4)
    unclassified_count = _unclassified_count(conn)

    cli_lines = [
        f"ESTIMATED TAX - {label}",
        "",
        f"YTD net profit (Line 31): {fmt_dollars(cents_to_dollars(ytd_net_profit_cents))}",
        f"Annualized net profit:    {fmt_dollars(cents_to_dollars(annualized_profit_cents))}",
    ]
    if uses_brackets:
        cli_lines.extend(
            [
                "",
                "Bracket-Based Components",
                f"Federal income tax:      {fmt_dollars(cents_to_dollars(federal_cents))}",
                f"Self-employment tax:     {fmt_dollars(cents_to_dollars(se_cents))}",
                f"NY state income tax:     {fmt_dollars(cents_to_dollars(state_cents))}",
                f"NYC income + UBT:        {fmt_dollars(cents_to_dollars(city_cents))}",
            ]
        )
    else:
        cli_lines.extend(
            [
                f"Tax rate:                 {float(effective_rate or 0) * 100:.2f}%",
            ]
        )
    cli_lines.extend(
        [
            f"Estimated annual tax:     {fmt_dollars(cents_to_dollars(estimated_annual_tax_cents))}",
            f"Quarterly payment:        {fmt_dollars(cents_to_dollars(estimated_quarterly_payment_cents))}",
            "",
            "IRS estimated tax due dates:",
        ]
    )
    for due in _due_dates_for_year(year):
        cli_lines.append(f"  {due}")
    cli_lines.append("")
    cli_lines.append("Note: Rough estimate only; prior payments and safe-harbor rules are not included.")
    cli_lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")
    cli_lines.extend(_tax_assumption_lines())

    payload = {
        "quarter": label,
        "year": year,
        "quarters_elapsed": quarter,
        "method": "bracket" if uses_brackets else "flat_rate",
        "rate": effective_rate,
        "include_se": include_se,
        "components_cents": {
            "federal_tax_cents": int(federal_cents),
            "se_tax_cents": int(se_cents),
            "state_tax_cents": int(state_cents),
            "city_tax_cents": int(city_cents),
        },
        "ytd_start_date": ytd_start.isoformat(),
        "ytd_end_date": ytd_end.isoformat(),
        "ytd_net_profit_cents": ytd_net_profit_cents,
        "annualized_profit_cents": annualized_profit_cents,
        "estimated_annual_tax_cents": estimated_annual_tax_cents,
        "estimated_quarterly_payment_cents": estimated_quarterly_payment_cents,
        "due_dates": _due_dates_for_year(year),
        "unclassified_count": unclassified_count,
    }
    return {
        "data": payload,
        "summary": {
            "quarter": label,
            "estimated_quarterly_payment_cents": estimated_quarterly_payment_cents,
            "estimated_annual_tax_cents": estimated_annual_tax_cents,
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(cli_lines),
    }


def handle_mileage_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    raw_trip_date = str(getattr(args, "date", "") or "").strip()
    try:
        trip_date = date.fromisoformat(raw_trip_date)
    except ValueError as exc:
        raise ValueError("date must be in YYYY-MM-DD format") from exc

    miles = float(getattr(args, "miles", 0))
    if miles <= 0:
        raise ValueError("miles must be > 0")

    destination = str(getattr(args, "destination", "") or "").strip()
    if not destination:
        raise ValueError("destination is required")

    purpose = str(getattr(args, "purpose", "") or "").strip()
    if not purpose:
        raise ValueError("purpose is required")

    vehicle = str(getattr(args, "vehicle", "primary") or "primary").strip() or "primary"
    notes_raw = getattr(args, "notes", None)
    notes = str(notes_raw).strip() if notes_raw is not None and str(notes_raw).strip() else None
    round_trip = 1 if bool(getattr(args, "round_trip", False)) else 0
    tax_year = int(trip_date.year)
    rate_cents = _mileage_rate_cents(conn, tax_year)
    deduction_cents = _round_half_up(miles * rate_cents)

    mileage_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO mileage_log (
            id, trip_date, miles, destination, business_purpose, vehicle_name,
            tax_year, round_trip, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            mileage_id,
            trip_date.isoformat(),
            miles,
            destination,
            purpose,
            vehicle,
            tax_year,
            round_trip,
            notes,
        ),
    )
    conn.commit()

    return {
        "data": {
            "id": mileage_id,
            "trip_date": trip_date.isoformat(),
            "miles": miles,
            "destination": destination,
            "purpose": purpose,
            "vehicle": vehicle,
            "round_trip": bool(round_trip),
            "tax_year": tax_year,
            "rate_cents": int(rate_cents),
            "deduction_cents": int(deduction_cents),
            "notes": notes,
        },
        "summary": {
            "tax_year": tax_year,
            "miles": miles,
            "deduction_cents": int(deduction_cents),
        },
        "cli_report": (
            f"Added mileage trip {trip_date.isoformat()} ({miles:.1f} miles) "
            f"- deduction {fmt_dollars(cents_to_dollars(int(deduction_cents)))}"
        ),
    }


def handle_mileage_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    tax_year = _parse_year_or_default(getattr(args, "year", None))
    vehicle_filter = str(getattr(args, "vehicle", "") or "").strip() or None
    limit = int(getattr(args, "limit", 50) or 50)
    if limit <= 0:
        raise ValueError("limit must be >= 1")

    clauses = ["tax_year = ?"]
    params: list[Any] = [tax_year]
    if vehicle_filter:
        clauses.append("vehicle_name = ?")
        params.append(vehicle_filter)
    params.append(limit)

    rows = conn.execute(
        f"""
        SELECT id,
               trip_date,
               miles,
               destination,
               business_purpose,
               vehicle_name,
               round_trip,
               notes,
               tax_year
          FROM mileage_log
         WHERE {' AND '.join(clauses)}
         ORDER BY trip_date DESC, created_at DESC
         LIMIT ?
        """,
        tuple(params),
    ).fetchall()

    rate_cache: dict[int, int] = {}
    trips: list[dict[str, Any]] = []
    for row in rows:
        row_tax_year = int(row["tax_year"] or tax_year)
        if row_tax_year not in rate_cache:
            rate_cache[row_tax_year] = _mileage_rate_cents(conn, row_tax_year)
        row_rate_cents = rate_cache[row_tax_year]
        miles = float(row["miles"] or 0.0)
        deduction_cents = _round_half_up(miles * row_rate_cents)
        trips.append(
            {
                "id": str(row["id"]),
                "trip_date": str(row["trip_date"]),
                "miles": miles,
                "destination": str(row["destination"] or ""),
                "purpose": str(row["business_purpose"] or ""),
                "vehicle": str(row["vehicle_name"] or ""),
                "round_trip": bool(int(row["round_trip"] or 0)),
                "notes": (str(row["notes"]) if row["notes"] is not None else None),
                "tax_year": row_tax_year,
                "rate_cents": int(row_rate_cents),
                "deduction_cents": int(deduction_cents),
            }
        )

    total_miles = float(sum(float(row["miles"]) for row in trips))
    total_deduction_cents = int(sum(int(row["deduction_cents"]) for row in trips))
    lines = [f"MILEAGE LOG - {tax_year}", ""]
    lines.append(f"  {'Date':<10} {'Miles':>8} {'Destination':<20} {'Purpose':<22} {'Deduction':>12}")
    lines.append("  " + "-" * 80)
    if trips:
        for row in trips:
            lines.append(
                f"  {row['trip_date']:<10} "
                f"{float(row['miles']):>8.1f} "
                f"{str(row['destination'])[:20]:<20} "
                f"{str(row['purpose'])[:22]:<22} "
                f"{fmt_dollars(cents_to_dollars(int(row['deduction_cents']))):>12}"
            )
    else:
        lines.append("  (none)")

    return {
        "data": {
            "tax_year": tax_year,
            "vehicle": vehicle_filter,
            "limit": limit,
            "trips": trips,
            "trip_count": len(trips),
            "total_miles": total_miles,
            "total_deduction_cents": total_deduction_cents,
        },
        "summary": {
            "tax_year": tax_year,
            "trip_count": len(trips),
            "total_miles": total_miles,
            "total_deduction_cents": total_deduction_cents,
        },
        "cli_report": "\n".join(lines),
    }


def handle_mileage_summary(args, conn: sqlite3.Connection) -> dict[str, Any]:
    tax_year = _parse_year_or_default(getattr(args, "year", None))
    summary = _mileage_summary_payload(conn, tax_year=tax_year)
    config = _get_tax_config(conn, tax_year)
    mileage_method = str(config.get("mileage_method", "actual") or "actual").strip().lower()
    if mileage_method not in {"standard", "actual"}:
        mileage_method = "actual"
    summary["mileage_method"] = mileage_method

    lines = [f"MILEAGE SUMMARY - {tax_year}", ""]
    lines.append(f"Method: {mileage_method}")
    lines.append(f"Trips logged:            {int(summary['trip_count'])}")
    lines.append(f"Total miles:             {float(summary['total_miles']):.1f}")
    lines.append(f"Rate:                    {int(summary['rate_cents'])} cents/mile")
    lines.append(
        f"Standard mileage:        {fmt_dollars(cents_to_dollars(int(summary['total_deduction_cents'])))}"
    )
    lines.append(
        f"Transaction-based Line 9 {fmt_dollars(cents_to_dollars(int(summary['transaction_based_line_9_cents'])))}"
    )

    return {
        "data": summary,
        "summary": {
            "tax_year": tax_year,
            "trip_count": int(summary["trip_count"]),
            "total_deduction_cents": int(summary["total_deduction_cents"]),
        },
        "cli_report": "\n".join(lines),
    }


def handle_contractor_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    name = str(getattr(args, "name", "") or "").strip()
    if not name:
        raise ValueError("name is required")

    tin_last4_raw = getattr(args, "tin_last4", None)
    tin_last4: str | None = None
    if tin_last4_raw is not None and str(tin_last4_raw).strip() != "":
        tin_last4 = str(tin_last4_raw).strip()
        if not _TIN_LAST4_RE.match(tin_last4):
            raise ValueError("tin-last4 must be exactly 4 digits")

    entity_type = str(getattr(args, "entity_type", "individual") or "individual").strip().lower()
    if entity_type not in _CONTRACTOR_ENTITY_TYPES:
        raise ValueError("entity-type must be one of: individual, llc, partnership, corporation")

    notes_raw = getattr(args, "notes", None)
    notes = str(notes_raw).strip() if notes_raw is not None and str(notes_raw).strip() else None

    contractor_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO contractors (id, name, tin_last4, entity_type, is_active, notes)
        VALUES (?, ?, ?, ?, 1, ?)
        """,
        (contractor_id, name, tin_last4, entity_type, notes),
    )
    conn.commit()

    return {
        "data": {
            "id": contractor_id,
            "name": name,
            "tin_last4": tin_last4,
            "entity_type": entity_type,
            "is_active": 1,
            "notes": notes,
        },
        "summary": {
            "id": contractor_id,
            "entity_type": entity_type,
        },
        "cli_report": f"Added contractor {name} ({entity_type})",
    }


def handle_contractor_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    tax_year = _parse_year_or_default(getattr(args, "year", None))
    include_inactive = bool(getattr(args, "include_inactive", False))
    contractors = _contractor_rows_for_year(
        conn,
        tax_year=tax_year,
        include_inactive=include_inactive,
    )

    lines = [f"CONTRACTORS - {tax_year}", ""]
    lines.append(f"  {'Name':<24} {'Type':<12} {'Paid YTD':>12} {'Txns':>6} {'1099':>6}")
    lines.append("  " + "-" * 66)
    if contractors:
        for row in contractors:
            lines.append(
                f"  {str(row['name'])[:24]:<24} "
                f"{str(row['entity_type'])[:12]:<12} "
                f"{fmt_dollars(cents_to_dollars(int(row['total_paid_cents']))):>12} "
                f"{int(row['payment_count']):>6} "
                f"{('Yes' if row['requires_1099'] else 'No'):>6}"
            )
    else:
        lines.append("  (none)")

    return {
        "data": {
            "tax_year": tax_year,
            "include_inactive": include_inactive,
            "contractors": contractors,
            "totals": {
                "contractor_count": len(contractors),
                "payment_count": sum(int(row["payment_count"]) for row in contractors),
                "requires_1099_count": sum(1 for row in contractors if bool(row["requires_1099"])),
                "total_paid_cents": sum(int(row["total_paid_cents"]) for row in contractors),
            },
        },
        "summary": {
            "tax_year": tax_year,
            "contractor_count": len(contractors),
            "requires_1099_count": sum(1 for row in contractors if bool(row["requires_1099"])),
        },
        "cli_report": "\n".join(lines),
    }


def handle_contractor_link(args, conn: sqlite3.Connection) -> dict[str, Any]:
    contractor_id = str(getattr(args, "contractor_id", "") or "").strip()
    if not contractor_id:
        raise ValueError("contractor-id is required")
    transaction_id = str(getattr(args, "transaction_id", "") or "").strip()
    if not transaction_id:
        raise ValueError("transaction-id is required")
    paid_via_card = 1 if bool(getattr(args, "paid_via_card", False)) else 0

    contractor = conn.execute(
        """
        SELECT id, name, is_active
          FROM contractors
         WHERE id = ?
         LIMIT 1
        """,
        (contractor_id,),
    ).fetchone()
    if contractor is None:
        raise ValueError("contractor not found")
    if int(contractor["is_active"] or 0) != 1:
        raise ValueError("contractor must be active")

    txn = conn.execute(
        """
        SELECT id, date, use_type, is_active
          FROM transactions
         WHERE id = ?
         LIMIT 1
        """,
        (transaction_id,),
    ).fetchone()
    if txn is None:
        raise ValueError("transaction not found")
    if int(txn["is_active"] or 0) != 1:
        raise ValueError("transaction must be active")
    if str(txn["use_type"] or "") != "Business":
        raise ValueError("transaction must have use_type=Business")

    try:
        txn_date = date.fromisoformat(str(txn["date"]))
    except ValueError:
        txn_date = date(int(str(txn["date"])[:4]), 1, 1)
    tax_year = int(txn_date.year)

    payment_id = uuid.uuid4().hex
    try:
        conn.execute(
            """
            INSERT INTO contractor_payments (id, contractor_id, transaction_id, tax_year, paid_via_card)
            VALUES (?, ?, ?, ?, ?)
            """,
            (payment_id, contractor_id, transaction_id, tax_year, paid_via_card),
        )
    except sqlite3.IntegrityError as exc:
        message = str(exc)
        if "contractor_payments.transaction_id" in message:
            raise ValueError("transaction is already linked to a contractor") from exc
        if "contractor_payments.contractor_id, contractor_payments.transaction_id" in message:
            raise ValueError("payment link already exists") from exc
        raise
    conn.commit()

    contractor_name = str(contractor["name"] or "")
    return {
        "data": {
            "id": payment_id,
            "contractor_id": contractor_id,
            "contractor_name": contractor_name,
            "transaction_id": transaction_id,
            "tax_year": tax_year,
            "paid_via_card": bool(paid_via_card),
        },
        "summary": {
            "tax_year": tax_year,
            "paid_via_card": bool(paid_via_card),
        },
        "cli_report": (
            f"Linked transaction {transaction_id} to contractor {contractor_name} "
            f"(paid_via_card={'yes' if paid_via_card else 'no'})"
        ),
    }


def handle_1099_report(args, conn: sqlite3.Connection) -> dict[str, Any]:
    year = str(getattr(args, "year", "") or "").strip()
    if not _YEAR_RE.match(year):
        raise ValueError("year must be in YYYY format")
    tax_year = int(year)

    summary_payload = _contractor_summary_payload(
        conn,
        tax_year=tax_year,
        include_inactive=True,
    )
    contractors = [row for row in summary_payload["contractors"] if int(row["payment_count"]) > 0]
    totals = {
        "contractor_count": len(contractors),
        "payment_count": sum(int(row["payment_count"]) for row in contractors),
        "requires_1099_count": sum(1 for row in contractors if bool(row["requires_1099"])),
        "total_paid_cents": sum(int(row["total_paid_cents"]) for row in contractors),
        "total_non_card_paid_cents": sum(int(row["non_card_paid_cents"]) for row in contractors),
        "total_card_paid_cents": sum(int(row["card_paid_cents"]) for row in contractors),
    }

    lines = [f"1099 REPORT - {tax_year}", ""]
    lines.append(f"  {'Contractor':<24} {'Type':<12} {'Non-card':>12} {'Card':>12} {'Txns':>6} {'1099':>6}")
    lines.append("  " + "-" * 82)
    if contractors:
        for row in contractors:
            lines.append(
                f"  {str(row['name'])[:24]:<24} "
                f"{str(row['entity_type'])[:12]:<12} "
                f"{fmt_dollars(cents_to_dollars(int(row['non_card_paid_cents']))):>12} "
                f"{fmt_dollars(cents_to_dollars(int(row['card_paid_cents']))):>12} "
                f"{int(row['payment_count']):>6} "
                f"{('Yes' if row['requires_1099'] else 'No'):>6}"
            )
    else:
        lines.append("  (none)")

    unlinked = summary_payload["unlinked_contract_labor"]
    if int(unlinked["total_cents"]) > 0:
        lines.append("")
        lines.append(
            "WARNING: Unlinked Contract Labor spend: "
            f"{fmt_dollars(cents_to_dollars(int(unlinked['total_cents'])))} "
            f"across {int(unlinked['txn_count'])} transaction(s)"
        )

    payload = {
        "tax_year": tax_year,
        "contractors": contractors,
        "totals": totals,
        "unlinked_contract_labor": unlinked,
    }
    return {
        "data": payload,
        "summary": {
            "tax_year": tax_year,
            "contractor_count": int(totals["contractor_count"]),
            "requires_1099_count": int(totals["requires_1099_count"]),
            "total_non_card_paid_cents": int(totals["total_non_card_paid_cents"]),
            "unlinked_contract_labor_cents": int(unlinked["total_cents"]),
        },
        "cli_report": "\n".join(lines),
    }


def _trend_arrow(slope_cents: float | None) -> str:
    if slope_cents is None:
        return "→"
    if slope_cents > 0:
        return "↑"
    if slope_cents < 0:
        return "↓"
    return "→"


def _fmt_slope(slope_cents: float | None) -> str:
    if slope_cents is None:
        return "n/a"
    rounded = _round_half_up(slope_cents)
    return f"{fmt_dollars(cents_to_dollars(rounded))}/mo"


def _fmt_projection(cents: int | None) -> str:
    if cents is None:
        return "n/a"
    return fmt_dollars(cents_to_dollars(int(cents)))


def handle_forecast(args, conn: sqlite3.Connection) -> dict[str, Any]:
    months = int(getattr(args, "months", 6))
    if months < 1:
        raise ValueError("months must be >= 1")

    show_streams = bool(getattr(args, "streams", False))
    trend_data = revenue_trend(conn, months=months)
    totals = list(trend_data.get("totals", []))
    stream_rows = list(trend_data.get("streams", []))
    unclassified_count = _unclassified_count(conn)

    lines = [f"REVENUE FORECAST - Last {months} month(s)", "", "Monthly Totals"]
    if totals:
        for row in totals:
            lines.append(
                f"  {row['month']:<12} {fmt_dollars(cents_to_dollars(int(row['cents']))):>14}"
            )
    else:
        lines.append("  (none)")

    lines.append("")
    lines.append(
        f"Trend: {_trend_arrow(trend_data.get('trend_slope_cents'))} {_fmt_slope(trend_data.get('trend_slope_cents'))}"
    )
    lines.append(f"Projected next month: {_fmt_projection(trend_data.get('projected_next_month_cents'))}")

    if show_streams:
        lines.append("")
        lines.append("Per-Stream Breakdown")
        if stream_rows and totals:
            month_labels = [str(row["month"]) for row in totals]
            header = (
                f"  {'Stream':<16}"
                + "".join(f"{month:>11}" for month in month_labels)
                + f" {'Trend':>8} {'Next':>12}"
            )
            lines.append(header)
            lines.append("  " + "-" * max(54, len(header) - 2))

            for stream in stream_rows:
                values = {str(item["month"]): int(item["cents"]) for item in stream.get("monthly_totals", [])}
                row_cells = "".join(
                    f"{fmt_dollars(cents_to_dollars(int(values.get(month, 0)))):>11}"
                    for month in month_labels
                )
                lines.append(
                    f"  {str(stream['name'])[:16]:<16}"
                    f"{row_cells}"
                    f" {_trend_arrow(stream.get('trend_slope_cents')):>8}"
                    f" {_fmt_projection(stream.get('projected_next_month_cents')):>12}"
                )
        else:
            lines.append("  (none)")

    sparse_months = [str(row["month"]) for row in totals[-2:] if int(row["cents"]) < 500]
    if sparse_months:
        lines.append(
            f"Note: Sparse data in {', '.join(sparse_months)} — revenue may be incomplete (sync lag or partial month)."
        )

    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")

    last_month_revenue = int(totals[-1]["cents"]) if totals else 0
    projected_next = trend_data.get("projected_next_month_cents")
    return {
        "data": {
            "months": months,
            "totals": totals,
            "trend_slope_cents": trend_data.get("trend_slope_cents"),
            "projected_next_month_cents": projected_next,
            "streams": stream_rows if show_streams else [],
            "stream_count": len(stream_rows),
            "unclassified_count": unclassified_count,
        },
        "summary": {
            "months": months,
            "last_month_revenue_cents": last_month_revenue,
            "projected_next_month_cents": projected_next,
            "stream_count": len(stream_rows),
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(lines),
    }


def handle_runway(args, conn: sqlite3.Connection) -> dict[str, Any]:
    months = int(getattr(args, "months", 3))
    if months < 1:
        raise ValueError("months must be >= 1")

    runway_data = forecasting_runway(conn, months=months)
    unclassified_count = _unclassified_count(conn)
    monthly_income_cents = int(runway_data.get("monthly_avg_income_cents", 0))
    monthly_expense_cents = int(runway_data.get("monthly_avg_expense_cents", 0))
    monthly_net_burn_cents = int(runway_data.get("monthly_net_burn_cents", 0))
    liquid_balance_cents = int(runway_data.get("liquid_balance_cents", 0))
    runway_months = runway_data.get("runway_months")
    runway_date = runway_data.get("runway_date")

    lines = [f"RUNWAY DASHBOARD - Last {months} month(s)", ""]
    lines.append(f"  {'Avg Monthly Income':<28} {fmt_dollars(cents_to_dollars(monthly_income_cents)):>14}")
    lines.append(f"  {'Avg Monthly Expenses':<28} {fmt_dollars(cents_to_dollars(monthly_expense_cents)):>14}")
    lines.append(f"  {'Monthly Net Burn':<28} {fmt_dollars(cents_to_dollars(monthly_net_burn_cents)):>14}")
    lines.append(f"  {'Liquid Cash Balance':<28} {fmt_dollars(cents_to_dollars(liquid_balance_cents)):>14}")
    lines.append("")
    if runway_months is None:
        lines.append("Runway: Infinite (profitable or break-even)")
    else:
        lines.append(f"Runway: {float(runway_months):.2f} months (through {runway_date})")

    lines.append("")
    lines.append("Expense Breakdown")
    by_section = list(runway_data.get("by_section", []))
    if by_section:
        for row in by_section:
            section_key = str(row["section"])
            section_label = _SECTION_LABELS.get(section_key, section_key)
            lines.append(
                f"  {section_label:<28} {fmt_dollars(cents_to_dollars(int(row['monthly_avg_cents']))):>14}"
            )
    else:
        lines.append("  (none)")

    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")

    return {
        "data": {
            "months": months,
            **runway_data,
            "unclassified_count": unclassified_count,
        },
        "summary": {
            "months": months,
            "liquid_balance_cents": liquid_balance_cents,
            "monthly_net_burn_cents": monthly_net_burn_cents,
            "runway_months": runway_months,
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(lines),
    }


def handle_seasonal(args, conn: sqlite3.Connection) -> dict[str, Any]:
    _ = args
    pattern = seasonal_pattern(conn)
    month_rows = list(pattern.get("months", []))
    history_months = int(
        pattern.get("history_months", sum(1 for row in month_rows if int(row.get("data_points", 0)) > 0))
    )
    unclassified_count = _unclassified_count(conn)

    max_avg = max((int(row.get("avg_revenue_cents", 0)) for row in month_rows), default=0)
    lines = ["SEASONAL REVENUE PATTERN", "", f"History months available: {history_months}"]
    if history_months < 12:
        lines.append("WARNING: Fewer than 12 historical months available; seasonality confidence is limited.")

    lines.append("")
    lines.append(f"  {'Month':<8} {'Avg Revenue':>14} {'Pts':>5} {'Conf':>7}  {'Bar'}")
    lines.append("  " + "-" * 60)
    for row in month_rows:
        avg_cents = int(row.get("avg_revenue_cents", 0))
        bar = ""
        if max_avg > 0 and avg_cents > 0:
            bar_width = max(1, int(round((avg_cents / max_avg) * 16)))
            bar = "#" * bar_width
        lines.append(
            f"  {str(row.get('month_name', ''))[:3]:<8} "
            f"{fmt_dollars(cents_to_dollars(avg_cents)):>14} "
            f"{int(row.get('data_points', 0)):>5} "
            f"{str(row.get('confidence', 'none')):>7}  {bar}"
        )

    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")

    best_month = max(month_rows, key=lambda row: int(row.get("avg_revenue_cents", 0)), default=None)
    return {
        "data": {
            **pattern,
            "history_months": history_months,
            "unclassified_count": unclassified_count,
        },
        "summary": {
            "history_months": history_months,
            "best_month": (str(best_month.get("month_name")) if best_month else None),
            "best_month_avg_revenue_cents": (int(best_month.get("avg_revenue_cents", 0)) if best_month else 0),
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(lines),
    }


def handle_cashflow(args, conn: sqlite3.Connection) -> dict[str, Any]:
    start, end, label = _parse_period(args, default_mode="month")
    income_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount_cents), 0) AS total_cents
          FROM transactions
         WHERE is_active = 1
           AND use_type = 'Business'
           AND is_payment = 0
           AND amount_cents > 0
           AND date >= ?
           AND date <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchone()
    expense_row = conn.execute(
        """
        SELECT COALESCE(SUM(amount_cents), 0) AS total_cents
          FROM transactions
         WHERE is_active = 1
           AND use_type = 'Business'
           AND is_payment = 0
           AND amount_cents < 0
           AND date >= ?
           AND date <= ?
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchone()
    business_account_rows = conn.execute(
        """
        SELECT id,
               institution_name,
               account_name,
               account_type,
               balance_current_cents
          FROM accounts
         WHERE is_active = 1
           AND is_business = 1
           AND id NOT IN (SELECT hash_account_id FROM account_aliases)
         ORDER BY institution_name ASC, account_name ASC
        """
    ).fetchall()

    income_cents = int(income_row["total_cents"] or 0)
    expense_cents = int(expense_row["total_cents"] or 0)
    net_operating_cash_flow_cents = income_cents + expense_cents
    accounts = [
        {
            "id": str(row["id"]),
            "institution_name": str(row["institution_name"] or ""),
            "account_name": str(row["account_name"] or ""),
            "account_type": str(row["account_type"] or ""),
            "balance_current_cents": int(row["balance_current_cents"] or 0),
        }
        for row in business_account_rows
    ]
    unclassified_count = _unclassified_count(conn)

    lines = [f"CASH FLOW - {label}", "", "Operating Activities"]
    lines.append(f"  {'Business income received':<34} {fmt_dollars(cents_to_dollars(income_cents)):>14}")
    lines.append(f"  {'Business expenses paid':<34} {_fmt_cashflow_expense(expense_cents):>14}")
    lines.append("  " + "-" * 52)
    lines.append(
        f"  {'Net Operating Cash Flow':<34} {fmt_dollars(cents_to_dollars(net_operating_cash_flow_cents)):>14}"
    )
    lines.append("")
    lines.append("Business Account Balances")
    if accounts:
        for account in accounts:
            account_label = f"{account['institution_name']} {account['account_name']}".strip()
            lines.append(
                f"  {account_label:<34} {fmt_dollars(cents_to_dollars(int(account['balance_current_cents']))):>14}"
            )
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append(f"WARNING: Unclassified transactions (NULL use_type): {unclassified_count}")

    return {
        "data": {
            "period": {
                "label": label,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
            },
            "business_income_cents": income_cents,
            "business_expense_cents": expense_cents,
            "net_operating_cash_flow_cents": net_operating_cash_flow_cents,
            "business_accounts": accounts,
            "unclassified_count": unclassified_count,
        },
        "summary": {
            "period": label,
            "account_count": len(accounts),
            "net_operating_cash_flow_cents": net_operating_cash_flow_cents,
            "unclassified_count": unclassified_count,
        },
        "cli_report": "\n".join(lines),
    }


def handle_biz_budget_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    section = str(getattr(args, "section", "") or "").strip().lower()
    if section not in _BIZ_BUDGET_SECTION_SET:
        allowed = ", ".join(_BIZ_BUDGET_SECTIONS)
        raise ValueError(f"section must be one of: {allowed}")

    amount_cents = _dollars_to_cents(getattr(args, "amount", None))
    if amount_cents is None:
        raise ValueError("amount is required")
    if amount_cents < 0:
        raise ValueError("amount must be >= 0")

    period = str(getattr(args, "period", "monthly") or "monthly").strip().lower()
    if period not in _BIZ_BUDGET_PERIOD_MONTHS:
        raise ValueError("period must be monthly, quarterly, or yearly")

    raw_effective_from = getattr(args, "effective_from", None)
    if raw_effective_from is None or str(raw_effective_from).strip() == "":
        effective_from_date = date.today().replace(day=1)
    else:
        try:
            effective_from_date = date.fromisoformat(str(raw_effective_from).strip())
        except ValueError as exc:
            raise ValueError("from must be in YYYY-MM-DD format") from exc
    effective_from = effective_from_date.isoformat()

    budget_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO biz_section_budgets (
            id, pl_section, amount_cents, period, effective_from
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (budget_id, section, amount_cents, period, effective_from),
    )
    conn.commit()

    section_label = _SECTION_LABELS.get(section, section)
    return {
        "data": {
            "id": budget_id,
            "pl_section": section,
            "section_label": section_label,
            "amount_cents": int(amount_cents),
            "period": period,
            "effective_from": effective_from,
        },
        "summary": {
            "pl_section": section,
            "amount_cents": int(amount_cents),
            "period": period,
        },
        "cli_report": (
            f"Set {section_label} budget to {fmt_dollars(cents_to_dollars(int(amount_cents)))} "
            f"per {period} effective {effective_from}"
        ),
    }


def handle_biz_budget_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    start, end, month_label = _parse_month_or_current(getattr(args, "month", None))
    placeholders = ", ".join("?" for _ in _BIZ_BUDGET_SECTIONS)

    actual_rows = conn.execute(
        f"""
        SELECT pm.pl_section, COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN pl_section_map pm ON pm.category_id = c.id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           AND pm.pl_section IN ({placeholders})
         GROUP BY pm.pl_section
        """,
        (start.isoformat(), end.isoformat(), *_BIZ_BUDGET_SECTIONS),
    ).fetchall()
    actual_by_section = {
        str(row["pl_section"]): max(0, -int(row["total_cents"] or 0))
        for row in actual_rows
    }

    budget_rows = conn.execute(
        f"""
        SELECT id,
               pl_section,
               amount_cents,
               period,
               effective_from,
               effective_to,
               created_at
          FROM biz_section_budgets
         WHERE pl_section IN ({placeholders})
           AND effective_from <= ?
           AND (effective_to IS NULL OR effective_to >= ?)
         ORDER BY pl_section ASC, effective_from DESC, created_at DESC, rowid DESC
        """,
        (*_BIZ_BUDGET_SECTIONS, end.isoformat(), start.isoformat()),
    ).fetchall()
    active_budget_by_section: dict[str, dict[str, Any]] = {}
    for row in budget_rows:
        section = str(row["pl_section"])
        if section in active_budget_by_section:
            continue
        active_budget_by_section[section] = {
            "id": str(row["id"]),
            "pl_section": section,
            "amount_cents": int(row["amount_cents"] or 0),
            "period": str(row["period"]),
            "effective_from": str(row["effective_from"]),
            "effective_to": (str(row["effective_to"]) if row["effective_to"] is not None else None),
            "created_at": str(row["created_at"] or ""),
        }

    section_order = {section: index for index, section in enumerate(_BIZ_BUDGET_SECTIONS)}
    sections = sorted(
        set(active_budget_by_section.keys()) | set(actual_by_section.keys()),
        key=lambda section: (section_order.get(section, 999), section),
    )

    status_rows: list[dict[str, Any]] = []
    for section in sections:
        budget = active_budget_by_section.get(section)
        budget_cents = int(budget["amount_cents"]) if budget is not None else None
        period = str(budget["period"]) if budget is not None else None
        monthly_budget_cents = (
            _normalize_budget_to_monthly(budget_cents, period) if budget_cents is not None and period is not None else None
        )
        actual_cents = int(actual_by_section.get(section, 0))

        remaining_cents: int | None
        pct_used: float | None
        if monthly_budget_cents is None or monthly_budget_cents == 0:
            remaining_cents = None
            pct_used = None
        else:
            remaining_cents = int(monthly_budget_cents - actual_cents)
            pct_used = (actual_cents / monthly_budget_cents) * 100.0

        status_rows.append(
            {
                "pl_section": section,
                "section_label": _SECTION_LABELS.get(section, section),
                "budget_cents": budget_cents,
                "period": period,
                "monthly_budget_cents": monthly_budget_cents,
                "effective_from": (str(budget["effective_from"]) if budget is not None else None),
                "actual_cents": actual_cents,
                "remaining_cents": remaining_cents,
                "pct_used": pct_used,
            }
        )

    lines = [
        f"BUSINESS BUDGET STATUS - {month_label}",
        "",
        f"  {'Section':<24} {'Budget':>11} {'Actual':>11} {'Remaining':>12} {'Used%':>7}",
        "  " + "-" * 71,
    ]
    if status_rows:
        for row in status_rows:
            budget_display = (
                fmt_dollars(cents_to_dollars(int(row["monthly_budget_cents"])))
                if row["monthly_budget_cents"] is not None
                else "—"
            )
            remaining_display = (
                fmt_dollars(cents_to_dollars(int(row["remaining_cents"])))
                if row["remaining_cents"] is not None
                else "—"
            )
            pct_display = f"{float(row['pct_used']):.1f}%" if row["pct_used"] is not None else "—"
            lines.append(
                f"  {str(row['section_label']):<24} "
                f"{budget_display:>11} "
                f"{fmt_dollars(cents_to_dollars(int(row['actual_cents']))):>11} "
                f"{remaining_display:>12} "
                f"{pct_display:>7}"
            )
    else:
        lines.append("  (none)")

    return {
        "data": {
            "month": month_label,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "rows": status_rows,
        },
        "summary": {
            "month": month_label,
            "section_count": len(status_rows),
            "with_budget_count": sum(1 for row in status_rows if row["monthly_budget_cents"] is not None),
            "total_actual_cents": sum(int(row["actual_cents"]) for row in status_rows),
        },
        "cli_report": "\n".join(lines),
    }
