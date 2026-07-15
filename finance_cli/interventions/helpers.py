from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import sqlite3
from typing import Optional


_VALID_VIEWS = frozenset({"personal", "business", "all"})


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _view_use_type_clause(view: str) -> str:
    """SQL WHERE-clause fragment that matches CLI view semantics.

    ``personal`` -> ``use_type = 'Personal' OR use_type IS NULL`` (NULL is
    treated as personal in CLI view defaults). ``business`` -> only Business
    rows. ``all`` -> no use_type filter.
    """
    if view == "personal":
        return "AND (use_type = 'Personal' OR use_type IS NULL)"
    if view == "business":
        return "AND use_type = 'Business'"
    return ""


def data_quality_gap_ratio(
    conn: sqlite3.Connection,
    *,
    view: str = "personal",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    """Categorization-gap ratio for a window + view.

    Counting semantics: union. A transaction is counted in the numerator if
    EITHER ``category_id IS NULL`` OR ``is_reviewed = 0``; adding the two
    counts separately would double-count rows that are both. Denominator is
    total active transactions in the window scoped to ``view``.

    Args:
        conn: SQLite connection.
        view: ``"personal"`` (default), ``"business"``, or ``"all"``.
        date_from: ISO date inclusive lower bound. Default = today - 90d.
        date_to: ISO date inclusive upper bound. Default = today.

    Returns:
        ``{gap_ratio: float, uncat_or_unreviewed_count: int, total_count:
        int, window_days: int, date_from, date_to, view}``. ``gap_ratio`` is
        0.0 when no transactions are in the window.

    Raises:
        ValueError if ``view`` is not in the allowed set or if
        ``date_from > date_to``.
    """
    if view not in _VALID_VIEWS:
        raise ValueError(f"view must be one of {sorted(_VALID_VIEWS)}; got {view!r}")

    end_date = _parse_iso_date(date_to) or date.today()
    start_date = _parse_iso_date(date_from) or (end_date - timedelta(days=90))
    if start_date > end_date:
        raise ValueError("date_from must be <= date_to")

    clause = _view_use_type_clause(view)
    total_row = conn.execute(
        f"""
        SELECT COUNT(*) AS total_count
          FROM transactions
         WHERE is_active = 1
           AND date >= ?
           AND date <= ?
           {clause}
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchone()
    total_count = int(total_row["total_count"] or 0) if total_row is not None else 0

    gap_row = conn.execute(
        f"""
        SELECT COUNT(*) AS gap_count
          FROM transactions
         WHERE is_active = 1
           AND date >= ?
           AND date <= ?
           AND (category_id IS NULL OR is_reviewed = 0)
           {clause}
        """,
        (start_date.isoformat(), end_date.isoformat()),
    ).fetchone()
    gap_count = int(gap_row["gap_count"] or 0) if gap_row is not None else 0

    gap_ratio = 0.0
    if total_count > 0:
        gap_ratio = float(gap_count) / float(total_count)

    return {
        "gap_ratio": gap_ratio,
        "uncat_or_unreviewed_count": gap_count,
        "total_count": total_count,
        "window_days": (end_date - start_date).days,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
        "view": view,
    }


def expense_filter_clause(
    *,
    txn_alias: str = "t",
    category_alias: str = "c",
    use_type: str | None = None,
) -> str:
    clauses = [
        f"{txn_alias}.amount_cents < 0",
        f"{txn_alias}.is_payment = 0",
        f"{txn_alias}.is_active = 1",
        f"{category_alias}.is_income = 0",
    ]
    if use_type == "Business":
        clauses.append(f"{txn_alias}.use_type = 'Business'")
    elif use_type == "Personal":
        clauses.append(f"({txn_alias}.use_type = 'Personal' OR {txn_alias}.use_type IS NULL)")
    return " AND ".join(clauses)


def bounded_whole_percent(numerator: int, denominator: int) -> int:
    """Return a display percent without exact all/none claims for partial shares."""
    try:
        num = int(numerator)
        den = int(denominator)
    except (TypeError, ValueError):
        return 0
    if den <= 0 or num <= 0:
        return 0
    if num >= den:
        return 100
    pct = int(
        (Decimal(num) * Decimal(100) / Decimal(den)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )
    return min(99, max(1, pct))


def _first_day_n_months_back(end_of_last_complete_month: date, months: int) -> date:
    start = end_of_last_complete_month.replace(day=1)
    for _ in range(int(months) - 1):
        start = (start - timedelta(days=1)).replace(day=1)
    return start


def trailing_avg_expenses_cents(
    conn: sqlite3.Connection,
    months: int,
    *,
    as_of: date | None = None,
    use_type: str | None = None,
) -> int:
    months = int(months)
    if months < 1:
        raise ValueError("months must be >= 1")
    as_of = as_of or date.today()
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    start_of_window = _first_day_n_months_back(end_of_last_complete_month, months)
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(ABS(t.amount_cents)), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE {expense_filter_clause(use_type=use_type)}
           AND t.date >= ?
           AND t.date <= ?
        """,
        (start_of_window.isoformat(), end_of_last_complete_month.isoformat()),
    ).fetchone()
    total_cents = int(row["total_cents"] or 0)
    return int((Decimal(total_cents) / Decimal(months)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def income_by_stream(
    conn: sqlite3.Connection,
    months: int,
    *,
    as_of: date | None = None,
) -> list[dict[str, int | str]]:
    months = int(months)
    if months < 1:
        raise ValueError("months must be >= 1")
    as_of = as_of or date.today()
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    start_of_window = _first_day_n_months_back(end_of_last_complete_month, months)
    rows = conn.execute(
        """
        SELECT substr(t.date, 1, 7) AS month,
               c.name AS stream,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE c.is_income = 1
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents > 0
           AND t.date >= ?
           AND t.date <= ?
         GROUP BY substr(t.date, 1, 7), c.name
         ORDER BY month, stream
        """,
        (start_of_window.isoformat(), end_of_last_complete_month.isoformat()),
    ).fetchall()
    return [
        {
            "month": str(row["month"]),
            "stream": str(row["stream"]),
            "total_cents": int(row["total_cents"] or 0),
        }
        for row in rows
    ]
