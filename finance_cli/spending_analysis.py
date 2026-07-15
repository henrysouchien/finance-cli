"""Shared spending classification and category-average helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from .user_rules import load_rules

_DEFAULT_ESSENTIAL_CATEGORIES = frozenset({
    "Utilities",
    "Insurance",
    "Health & Wellness",
    "Rent",
    "Housing",
    "Childcare",
})

_EXCLUDED_CATEGORIES = frozenset({
    "Payments & Transfers",
    "Bank Charges & Fees",
    "Taxes",
    "Income",
    "Income: Salary",
    "Income: Business",
    "Income: Other",
})


def load_essential_categories(rules_path: Path | None = None) -> frozenset[str]:
    """Load essential categories from rules.yaml, falling back to defaults."""
    rules = load_rules(path=rules_path) if rules_path is not None else load_rules()
    if not rules.raw:
        return _DEFAULT_ESSENTIAL_CATEGORIES
    if "essential_categories" not in rules.raw:
        return _DEFAULT_ESSENTIAL_CATEGORIES
    configured = rules.raw["essential_categories"]
    if not isinstance(configured, list):
        return _DEFAULT_ESSENTIAL_CATEGORIES
    # Explicit empty list = user wants nothing essential (all discretionary)
    if not configured:
        return frozenset()
    cleaned = frozenset(str(c).strip() for c in configured if isinstance(c, str) and str(c).strip())
    # Non-empty list but zero valid strings (e.g., [123]) -> fall back to defaults
    if not cleaned:
        return _DEFAULT_ESSENTIAL_CATEGORIES
    return cleaned


def is_essential(category_name: str, essential: frozenset[str]) -> bool:
    normalized = (category_name or "").strip()
    return any(normalized.casefold() == e.casefold() for e in essential)


def is_excluded(category_name: str) -> bool:
    normalized = (category_name or "").strip()
    return any(normalized.casefold() == e.casefold() for e in _EXCLUDED_CATEGORIES)


@dataclass(frozen=True)
class CategorySpending:
    category_name: str
    parent_name: str
    avg_monthly_cents: int
    total_cents: int
    months_with_data: int
    classification: str


def _first_day_n_months_back(end_of_last_complete_month: date, months: int) -> date:
    start = end_of_last_complete_month.replace(day=1)
    for _ in range(int(months) - 1):
        start = (start - timedelta(days=1)).replace(day=1)
    return start


def category_spending_averages(
    conn: sqlite3.Connection,
    months: int = 3,
    as_of: date | None = None,
    rules_path: Path | None = None,
    use_type: str | None = None,
) -> list[CategorySpending]:
    """Average monthly spend per category across last N complete calendar months.

    ``use_type`` filters by the transactions.use_type column. Accepted values
    mirror CLI view semantics: ``"Personal"`` matches rows tagged Personal OR
    NULL (NULL-as-personal default), ``"Business"`` matches only Business
    rows, and ``None`` returns all rows regardless of classification. Values
    are case-insensitive; stored values are ``'Personal'`` / ``'Business'``.
    """
    months = int(months)
    if months < 1:
        raise ValueError("months must be >= 1")

    as_of = as_of or date.today()
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    start_of_window = _first_day_n_months_back(end_of_last_complete_month, months)
    essential = load_essential_categories(rules_path=rules_path)

    use_type_clause = ""
    params: list[str] = [start_of_window.isoformat(), end_of_last_complete_month.isoformat()]
    if use_type is not None:
        normalized = str(use_type).strip().lower()
        if normalized == "personal":
            use_type_clause = " AND (t.use_type = 'Personal' OR t.use_type IS NULL)"
        elif normalized == "business":
            use_type_clause = " AND t.use_type = 'Business'"
        else:
            raise ValueError(f"use_type must be 'Personal', 'Business', or None; got {use_type!r}")

    rows = conn.execute(
        f"""
        SELECT c.name,
               COALESCE(p.name, c.name) AS parent_name,
               c.is_income,
               ABS(SUM(t.amount_cents)) AS total_cents,
               COUNT(DISTINCT substr(t.date, 1, 7)) AS months_with_data
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND c.is_income = 0
           AND t.date >= ?
           AND t.date <= ?
           {use_type_clause}
         GROUP BY c.id
        HAVING total_cents > 0
         ORDER BY total_cents DESC
        """,
        tuple(params),
    ).fetchall()

    categories: list[CategorySpending] = []
    for row in rows:
        category_name = str(row["name"] or "")
        parent_name = str(row["parent_name"] or category_name)
        total_cents = int(row["total_cents"] or 0)
        months_with_data = int(row["months_with_data"] or 0)
        avg_monthly_cents = int(
            (
                Decimal(total_cents)
                / Decimal(months)
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )

        if is_excluded(category_name) or is_excluded(parent_name):
            classification = "excluded"
        elif is_essential(category_name, essential) or is_essential(parent_name, essential):
            classification = "essential"
        else:
            classification = "discretionary"

        categories.append(
            CategorySpending(
                category_name=category_name,
                parent_name=parent_name,
                avg_monthly_cents=avg_monthly_cents,
                total_cents=total_cents,
                months_with_data=months_with_data,
                classification=classification,
            )
        )

    categories.sort(key=lambda item: (-item.avg_monthly_cents, item.category_name.casefold()))
    return categories
