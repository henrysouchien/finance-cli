"""Business forecasting helpers."""

from __future__ import annotations

import calendar
import sqlite3
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .user_rules import load_rules


def _normalize_months(months: int) -> int:
    value = int(months)
    if value < 1:
        raise ValueError("months must be >= 1")
    return value


def _month_floor(day: date) -> date:
    return date(day.year, day.month, 1)


def _month_shift(month_start: date, delta_months: int) -> date:
    month_index = (month_start.year * 12) + (month_start.month - 1) + int(delta_months)
    year, month_zero = divmod(month_index, 12)
    return date(year, month_zero + 1, 1)


def _lookback_month_starts(months: int) -> list[date]:
    normalized = _normalize_months(months)
    current_month = _month_floor(date.today())
    first_month = _month_shift(current_month, -(normalized - 1))
    return [_month_shift(first_month, offset) for offset in range(normalized)]


def _lookback_bounds(months: int) -> tuple[str, str]:
    starts = _lookback_month_starts(months)
    start_date = starts[0].isoformat()
    end_date = _month_shift(starts[-1], 1).isoformat()
    return start_date, end_date


def _lookback_month_labels(months: int) -> list[str]:
    return [month_start.strftime("%Y-%m") for month_start in _lookback_month_starts(months)]


def _avg_cents(total_cents: int, periods: int) -> int:
    if periods <= 0:
        return 0
    return int((Decimal(total_cents) / Decimal(periods)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _normalized_revenue_streams() -> list[dict[str, Any]]:
    raw_streams = load_rules().revenue_streams
    if not isinstance(raw_streams, list):
        return []

    streams: list[dict[str, Any]] = []
    for stream in raw_streams:
        if not isinstance(stream, dict):
            continue

        name = str(stream.get("name") or "").strip()
        if not name:
            continue

        match = stream.get("match")
        if not isinstance(match, dict):
            continue

        source = str(match.get("source") or "").strip()
        source_category = str(match.get("source_category") or "").strip()

        keywords_raw = match.get("keywords") or []
        keywords: list[str] = []
        if isinstance(keywords_raw, list):
            for keyword in keywords_raw:
                cleaned = str(keyword).strip()
                if cleaned:
                    keywords.append(cleaned.upper())

        if not source and not source_category and not keywords:
            continue

        streams.append(
            {
                "name": name,
                "source": source,
                "source_category": source_category,
                "keywords": keywords,
            }
        )
    return streams


def _stream_case_expression() -> tuple[str, list[Any]]:
    streams = _normalized_revenue_streams()
    case_parts: list[str] = []
    params: list[Any] = []

    for stream in streams:
        name = str(stream["name"])
        source = str(stream.get("source") or "")
        source_category = str(stream.get("source_category") or "")
        keywords = [str(item) for item in stream.get("keywords") or []]

        if source and source_category:
            case_parts.append("WHEN t.source = ? AND t.source_category = ? THEN ?")
            params.extend([source, source_category, name])
        elif source:
            case_parts.append("WHEN t.source = ? THEN ?")
            params.extend([source, name])
        elif source_category:
            case_parts.append("WHEN t.source_category = ? THEN ?")
            params.extend([source_category, name])

        for keyword in keywords:
            case_parts.append("WHEN UPPER(COALESCE(t.description, '')) LIKE ? THEN ?")
            params.extend([f"%{keyword}%", name])

    if case_parts:
        return "CASE " + " ".join(case_parts) + " ELSE 'Other' END", params

    # Important for empty-config safety: use a constant expression, not CASE.
    return "'Other'", []


def _trend_slope_projection(values: list[int]) -> tuple[float | None, int | None]:
    n = len(values)
    if n < 2:
        return None, None

    sum_x = sum(range(n))
    sum_y = sum(values)
    sum_xy = sum(index * value for index, value in enumerate(values))
    sum_x2 = sum(index * index for index in range(n))

    denom = (n * sum_x2) - (sum_x ** 2)
    if denom == 0:
        return None, None

    slope = ((n * sum_xy) - (sum_x * sum_y)) / denom
    intercept = (sum_y - (slope * sum_x)) / n
    projection = max(0, int(round(intercept + (slope * n))))
    return slope, projection


def revenue_by_stream(conn: sqlite3.Connection, months: int = 6) -> list[dict[str, Any]]:
    """Return business revenue grouped by month and configured stream."""
    normalized_months = _normalize_months(months)
    start_date, end_date = _lookback_bounds(normalized_months)
    stream_expr, stream_params = _stream_case_expression()

    rows = conn.execute(
        f"""
        SELECT strftime('%Y-%m', t.date) AS month,
               {stream_expr} AS stream,
               COALESCE(SUM(t.amount_cents), 0) AS gross_cents,
               COUNT(*) AS txn_count
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN pl_section_map pm
            ON pm.category_id = c.id
           AND pm.pl_section = 'revenue'
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date < ?
         GROUP BY month, stream
         ORDER BY month ASC, stream ASC
        """,
        [*stream_params, start_date, end_date],
    ).fetchall()

    return [
        {
            "month": str(row["month"]),
            "stream": str(row["stream"]),
            "gross_cents": int(row["gross_cents"] or 0),
            "txn_count": int(row["txn_count"] or 0),
        }
        for row in rows
    ]


def revenue_trend(conn: sqlite3.Connection, months: int = 6) -> dict[str, Any]:
    """Return per-stream monthly totals and least-squares trend projections."""
    normalized_months = _normalize_months(months)
    month_labels = _lookback_month_labels(normalized_months)
    grouped_rows = revenue_by_stream(conn, months=normalized_months)

    per_stream: dict[str, dict[str, int]] = {}
    total_by_month: dict[str, int] = {month: 0 for month in month_labels}

    for row in grouped_rows:
        month = str(row["month"])
        if month not in total_by_month:
            continue
        stream = str(row["stream"])
        cents = int(row["gross_cents"])

        stream_months = per_stream.setdefault(stream, {key: 0 for key in month_labels})
        stream_months[month] += cents
        total_by_month[month] += cents

    stream_payload: list[dict[str, Any]] = []
    for stream_name in sorted(per_stream.keys()):
        monthly_values = [int(per_stream[stream_name][month]) for month in month_labels]
        slope, projection = _trend_slope_projection(monthly_values)
        stream_payload.append(
            {
                "name": stream_name,
                "monthly_totals": [
                    {"month": month, "cents": int(per_stream[stream_name][month])}
                    for month in month_labels
                ],
                "trend_slope_cents": slope,
                "projected_next_month_cents": projection,
            }
        )

    total_values = [int(total_by_month[month]) for month in month_labels]
    total_slope, total_projection = _trend_slope_projection(total_values)

    return {
        "streams": stream_payload,
        "totals": [{"month": month, "cents": int(total_by_month[month])} for month in month_labels],
        "trend_slope_cents": total_slope,
        "projected_next_month_cents": total_projection,
    }


def burn_rate(conn: sqlite3.Connection, months: int = 3) -> dict[str, Any]:
    """Return trailing monthly average business income, expense, and net burn."""
    normalized_months = _normalize_months(months)
    start_date, end_date = _lookback_bounds(normalized_months)

    rows = conn.execute(
        """
        SELECT strftime('%Y-%m', t.date) AS month,
               pm.pl_section AS section,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN pl_section_map pm ON pm.category_id = c.id
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date < ?
         GROUP BY month, section
        """,
        (start_date, end_date),
    ).fetchall()

    income_total_cents = 0
    section_expense_totals: dict[str, int] = {}

    for row in rows:
        section = str(row["section"])
        net_cents = int(row["total_cents"] or 0)
        if section == "revenue":
            income_total_cents += max(0, net_cents)
            continue

        section_expense_totals[section] = section_expense_totals.get(section, 0) + max(0, -net_cents)

    by_section = [
        {
            "section": section,
            "monthly_avg_cents": _avg_cents(total_cents, normalized_months),
        }
        for section, total_cents in section_expense_totals.items()
        if total_cents > 0
    ]
    by_section.sort(key=lambda row: (-int(row["monthly_avg_cents"]), str(row["section"])))

    monthly_avg_income_cents = _avg_cents(income_total_cents, normalized_months)
    monthly_avg_expense_cents = sum(int(item["monthly_avg_cents"]) for item in by_section)
    monthly_net_burn_cents = int(monthly_avg_expense_cents - monthly_avg_income_cents)

    return {
        "monthly_avg_expense_cents": int(monthly_avg_expense_cents),
        "monthly_avg_income_cents": int(monthly_avg_income_cents),
        "monthly_net_burn_cents": monthly_net_burn_cents,
        "by_section": by_section,
    }


def runway(conn: sqlite3.Connection, months: int = 3) -> dict[str, Any]:
    """Return business cash runway based on burn rate and liquid cash balances."""
    burn = burn_rate(conn, months=months)

    liquid_row = conn.execute(
        """
        SELECT COALESCE(SUM(balance_current_cents), 0) AS liquid_balance_cents
          FROM accounts
         WHERE is_active = 1
           AND is_business = 1
           AND account_type IN ('checking', 'savings')
           AND id NOT IN (SELECT hash_account_id FROM account_aliases)
        """
    ).fetchone()

    liquid_balance_cents = int(liquid_row["liquid_balance_cents"] or 0)
    monthly_net_burn_cents = int(burn["monthly_net_burn_cents"])

    runway_months: float | None = None
    runway_date: str | None = None
    if monthly_net_burn_cents > 0:
        raw_months = max(0.0, liquid_balance_cents / float(monthly_net_burn_cents))
        runway_months = round(raw_months, 2)
        runway_days = max(0, int(round(raw_months * 30)))
        runway_date = (date.today() + timedelta(days=runway_days)).isoformat()

    return {
        "liquid_balance_cents": liquid_balance_cents,
        "monthly_avg_expense_cents": int(burn["monthly_avg_expense_cents"]),
        "monthly_avg_income_cents": int(burn["monthly_avg_income_cents"]),
        "monthly_net_burn_cents": monthly_net_burn_cents,
        "by_section": list(burn["by_section"]),
        "runway_months": runway_months,
        "runway_date": runway_date,
    }


def seasonal_pattern(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return month-of-year average business revenue across all history."""
    rows = conn.execute(
        """
        SELECT strftime('%Y-%m', t.date) AS month_key,
               CAST(strftime('%m', t.date) AS INTEGER) AS month_number,
               COALESCE(SUM(t.amount_cents), 0) AS revenue_cents
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
          JOIN pl_section_map pm
            ON pm.category_id = c.id
           AND pm.pl_section = 'revenue'
         WHERE t.is_active = 1
           AND t.use_type = 'Business'
           AND t.is_payment = 0
         GROUP BY month_key, month_number
         ORDER BY month_key ASC
        """
    ).fetchall()

    monthly_values: dict[int, list[int]] = {month_number: [] for month_number in range(1, 13)}
    for row in rows:
        month_number = int(row["month_number"] or 0)
        if month_number < 1 or month_number > 12:
            continue
        monthly_values[month_number].append(int(row["revenue_cents"] or 0))

    month_rows: list[dict[str, Any]] = []
    for month_number in range(1, 13):
        samples = monthly_values[month_number]
        data_points = len(samples)
        avg_cents = _avg_cents(sum(samples), data_points) if data_points > 0 else 0
        confidence = "none"
        if data_points >= 3:
            confidence = "high"
        elif data_points >= 1:
            confidence = "low"

        month_rows.append(
            {
                "month_number": month_number,
                "month_name": calendar.month_name[month_number],
                "avg_revenue_cents": int(avg_cents),
                "data_points": data_points,
                "confidence": confidence,
            }
        )

    return {
        "months": month_rows,
        "history_months": len(rows),
    }
