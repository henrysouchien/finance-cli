"""Shared spending classification and category-average helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP

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


def load_essential_categories() -> frozenset[str]:
    """Load essential categories from rules.yaml, falling back to defaults."""
    rules = load_rules()
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
) -> list[CategorySpending]:
    """Average monthly spend per category across last N complete calendar months."""
    months = int(months)
    if months < 1:
        raise ValueError("months must be >= 1")

    as_of = as_of or date.today()
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    start_of_window = _first_day_n_months_back(end_of_last_complete_month, months)
    essential = load_essential_categories()

    rows = conn.execute(
        """
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
         GROUP BY c.id
        HAVING total_cents > 0
         ORDER BY total_cents DESC
        """,
        (start_of_window.isoformat(), end_of_last_complete_month.isoformat()),
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
