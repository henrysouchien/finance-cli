"""Budget calculations, monthly status, and forecasting."""

from __future__ import annotations

import calendar
import sqlite3
import uuid
from dataclasses import dataclass
from decimal import Decimal
from datetime import date

from finance_cli.exceptions import NotFoundError, ValidationError

from .commands.common import use_type_filter
from .models import dollars_to_cents


@dataclass
class BudgetStatusRow:
    category_id: str
    category_name: str
    group_name: str
    use_type: str
    budget_cents: int
    actual_cents: int

    @property
    def remaining_cents(self) -> int:
        return self.budget_cents - abs(self.actual_cents)

    @property
    def utilization(self) -> float:
        if self.budget_cents <= 0:
            return 0.0
        return abs(self.actual_cents) / self.budget_cents


def _month_bounds(month_str: str) -> tuple[str, str]:
    year, month = month_str.split("-")
    y = int(year)
    m = int(month)
    last_day = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{last_day:02d}"


def _today_month() -> str:
    return date.today().strftime("%Y-%m")


def find_budget(
    conn: sqlite3.Connection,
    category_id: str,
    period: str,
    use_type: str = "Personal",
) -> dict | None:
    row = conn.execute(
        """
        SELECT id, category_id, period, amount_cents, effective_from, effective_to, use_type
          FROM budgets
         WHERE category_id = ?
           AND period = ?
           AND use_type = ?
           AND effective_to IS NULL
         ORDER BY date(effective_from) DESC, id DESC
         LIMIT 1
        """,
        (category_id, period, use_type),
    ).fetchone()
    return dict(row) if row else None


def update_budget(
    conn: sqlite3.Connection,
    budget_id: str,
    amount_dollars: str,
    dry_run: bool = False,
) -> str:
    existing = conn.execute(
        "SELECT id FROM budgets WHERE id = ?",
        (budget_id,),
    ).fetchone()
    if not existing:
        raise NotFoundError(f"Budget '{budget_id}' not found")

    if Decimal(str(amount_dollars)) < 0:
        raise ValidationError("Budget amount cannot be negative")
    cents = dollars_to_cents(amount_dollars)
    conn.execute(
        "UPDATE budgets SET amount_cents = ? WHERE id = ?",
        (cents, budget_id),
    )
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return budget_id


def reallocate_budget(
    conn: sqlite3.Connection,
    *,
    from_budget_id: str,
    to_budget_id: str,
    amount_dollars: str,
    dry_run: bool = False,
) -> dict[str, int | str]:
    if from_budget_id == to_budget_id:
        raise ValidationError("Cannot reallocate within the same budget")
    amount = Decimal(str(amount_dollars))
    if amount <= 0:
        raise ValidationError("Reallocation amount must be positive")
    transfer_cents = dollars_to_cents(amount_dollars)
    if transfer_cents <= 0:
        raise ValidationError("Reallocation amount must be at least $0.01")

    savepoint = f"budget_reallocate_{uuid.uuid4().hex}"
    conn.execute(f"SAVEPOINT {savepoint}")

    try:
        source_update = conn.execute(
            """
            UPDATE budgets
               SET amount_cents = amount_cents - ?
             WHERE id = ?
               AND amount_cents >= ?
            """,
            (transfer_cents, from_budget_id, transfer_cents),
        )
        if source_update.rowcount != 1:
            source = conn.execute(
                "SELECT amount_cents FROM budgets WHERE id = ?",
                (from_budget_id,),
            ).fetchone()
            if source is None:
                raise NotFoundError(f"Budget '{from_budget_id}' not found")
            raise ValidationError("Reallocation would make source budget negative")

        target_update = conn.execute(
            """
            UPDATE budgets
               SET amount_cents = amount_cents + ?
             WHERE id = ?
            """,
            (transfer_cents, to_budget_id),
        )
        if target_update.rowcount != 1:
            raise NotFoundError(f"Budget '{to_budget_id}' not found")

        rows = conn.execute(
            """
            SELECT id, amount_cents
              FROM budgets
             WHERE id IN (?, ?)
            """,
            (from_budget_id, to_budget_id),
        ).fetchall()
        by_id = {str(row["id"]): int(row["amount_cents"] or 0) for row in rows}
        from_after_cents = by_id[from_budget_id]
        to_after_cents = by_id[to_budget_id]
        from_before_cents = from_after_cents + transfer_cents
        to_before_cents = to_after_cents - transfer_cents

        if dry_run:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        raise

    return {
        "from_budget_id": from_budget_id,
        "to_budget_id": to_budget_id,
        "transfer_cents": transfer_cents,
        "from_before_cents": from_before_cents,
        "from_after_cents": from_after_cents,
        "to_before_cents": to_before_cents,
        "to_after_cents": to_after_cents,
    }


def delete_budget(conn: sqlite3.Connection, budget_id: str, dry_run: bool = False) -> str:
    existing = conn.execute(
        "SELECT id FROM budgets WHERE id = ?",
        (budget_id,),
    ).fetchone()
    if not existing:
        raise NotFoundError(f"Budget '{budget_id}' not found")

    conn.execute("DELETE FROM budgets WHERE id = ?", (budget_id,))
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return budget_id


def set_budget(
    conn: sqlite3.Connection,
    category_id: str,
    amount_dollars: str,
    period: str,
    use_type: str = "Personal",
    effective_from: str | None = None,
    dry_run: bool = False,
) -> str:
    if Decimal(str(amount_dollars)) < 0:
        raise ValidationError("Budget amount cannot be negative")
    cents = dollars_to_cents(amount_dollars)
    effective_from = effective_from or date.today().replace(day=1).isoformat()

    category_row = conn.execute(
        "SELECT name FROM categories WHERE id = ?",
        (category_id,),
    ).fetchone()
    child_count = conn.execute(
        "SELECT COUNT(*) AS child_count FROM categories WHERE parent_id = ?",
        (category_id,),
    ).fetchone()
    if category_row and int(child_count["child_count"]) > 0:
        raise ValidationError(
            f"Cannot set budget on parent category '{category_row['name']}' - "
            "set budgets on leaf categories instead."
        )

    existing = find_budget(
        conn,
        category_id=category_id,
        period=period,
        use_type=use_type,
    )
    if existing:
        conn.execute(
            "UPDATE budgets SET amount_cents = ? WHERE id = ?",
            (cents, existing["id"]),
        )
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return str(existing["id"])

    budget_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO budgets (id, category_id, period, amount_cents, effective_from, effective_to, use_type)
        VALUES (?, ?, ?, ?, ?, NULL, ?)
        """,
        (budget_id, category_id, period, cents, effective_from, use_type),
    )
    if dry_run:
        conn.rollback()
    else:
        conn.commit()
    return budget_id


def list_budgets(conn: sqlite3.Connection, view: str = "all") -> list[dict]:
    where_clause = ""
    params: list[str] = []
    if view == "personal":
        where_clause = "WHERE b.use_type = ?"
        params.append("Personal")
    elif view == "business":
        where_clause = "WHERE b.use_type = ?"
        params.append("Business")

    rows = conn.execute(
        f"""
        SELECT b.id, b.category_id, c.name AS category_name, b.period,
               b.amount_cents, b.effective_from, b.effective_to, b.use_type
          FROM budgets b
          JOIN categories c ON c.id = b.category_id
          {where_clause}
         ORDER BY b.period, c.name, b.effective_from DESC
        """,
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def monthly_budget_status(
    conn: sqlite3.Connection,
    month: str | None = None,
    view: str = "all",
) -> list[BudgetStatusRow]:
    month = month or _today_month()
    start, end = _month_bounds(month)

    budget_filter = ""
    params: list[str] = [end, start]
    if view == "personal":
        budget_filter = "AND b.use_type = ?"
        params.append("Personal")
    elif view == "business":
        budget_filter = "AND b.use_type = ?"
        params.append("Business")

    budget_rows = conn.execute(
        f"""
        SELECT b.category_id,
               c.name AS category_name,
               COALESCE(p.name, c.name, 'Uncategorized') AS group_name,
               b.amount_cents,
               b.use_type
          FROM budgets b
          JOIN categories c ON c.id = b.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE b.period = 'monthly'
           AND date(b.effective_from) <= date(?)
           AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(?)
           {budget_filter}
         ORDER BY ABS(b.amount_cents) DESC, c.name ASC
        """,
        tuple(params),
    ).fetchall()

    result: list[BudgetStatusRow] = []
    for budget in budget_rows:
        if budget["use_type"] == "Personal":
            use_type_clause = "AND (use_type = 'Personal' OR use_type IS NULL)"
        else:
            use_type_clause = "AND use_type = 'Business'"
        actual = conn.execute(
            f"""
            SELECT COALESCE(SUM(amount_cents), 0) AS actual_cents
             FROM transactions
             WHERE is_active = 1
               AND is_payment = 0
               AND date >= ?
               AND date <= ?
               AND category_id = ?
               AND amount_cents < 0
               {use_type_clause}
            """,
            (start, end, budget["category_id"]),
        ).fetchone()
        result.append(
            BudgetStatusRow(
                category_id=budget["category_id"],
                category_name=budget["category_name"],
                group_name=budget["group_name"],
                use_type=budget["use_type"],
                budget_cents=int(budget["amount_cents"]),
                actual_cents=int(actual["actual_cents"]),
            )
        )

    return result


def monthly_budget_forecast(
    conn: sqlite3.Connection,
    month: str | None = None,
    view: str = "all",
) -> list[dict]:
    month = month or _today_month()
    rows = monthly_budget_status(conn, month, view=view)
    year, month_num = [int(v) for v in month.split("-")]
    days_in_month = calendar.monthrange(year, month_num)[1]

    today = date.today()
    if today.year == year and today.month == month_num:
        elapsed = max(today.day, 1)
    else:
        elapsed = days_in_month

    forecast_rows: list[dict] = []
    for row in rows:
        spent = abs(row.actual_cents)
        projected = int(round((spent / elapsed) * days_in_month)) if elapsed else spent
        forecast_rows.append(
            {
                "category_id": row.category_id,
                "category_name": row.category_name,
                "group_name": row.group_name,
                "use_type": row.use_type,
                "budget_cents": row.budget_cents,
                "actual_cents": row.actual_cents,
                "forecast_cents": projected,
                "forecast_over_budget_cents": projected - row.budget_cents,
            }
        )
    return forecast_rows


def budget_alerts(
    conn: sqlite3.Connection,
    month: str | None = None,
    view: str = "all",
    warn_pct: float = 0.80,
    alert_pct: float = 1.00,
) -> dict:
    if not (0 < warn_pct < alert_pct):
        raise ValidationError("warn_pct and alert_pct must satisfy 0 < warn_pct < alert_pct")

    month = month or _today_month()
    year, month_num = [int(v) for v in month.split("-")]
    days_in_month = calendar.monthrange(year, month_num)[1]

    today = date.today()
    if today.year == year and today.month == month_num:
        days_elapsed = max(today.day, 1)
    else:
        days_elapsed = days_in_month
    days_remaining = max(days_in_month - days_elapsed, 0)

    rows = monthly_budget_forecast(conn, month=month, view=view)

    alerts: list[dict] = []
    ok_count = 0
    over_count = 0
    alert_count = 0
    warn_count = 0

    for row in rows:
        budget_cents = int(row["budget_cents"])
        if budget_cents <= 0:
            continue

        actual_cents = int(row["actual_cents"])
        spent_cents = abs(actual_cents)
        forecast_cents = int(row["forecast_cents"])

        utilization = spent_cents / budget_cents
        forecast_utilization = forecast_cents / budget_cents
        daily_run_rate_cents = int(round(spent_cents / days_elapsed)) if days_elapsed else spent_cents
        remaining_daily_budget_cents = (
            int(round((budget_cents - spent_cents) / days_remaining))
            if days_remaining > 0
            else 0
        )

        severity: str | None = None
        if spent_cents > budget_cents:
            severity = "over"
            over_count += 1
        elif forecast_utilization >= alert_pct:
            severity = "alert"
            alert_count += 1
        elif forecast_utilization >= warn_pct:
            severity = "warn"
            warn_count += 1
        else:
            ok_count += 1

        if not severity:
            continue

        alerts.append(
            {
                "category_id": row["category_id"],
                "category_name": row["category_name"],
                "group_name": row["group_name"],
                "use_type": row["use_type"],
                "budget_cents": budget_cents,
                "actual_cents": actual_cents,
                "forecast_cents": forecast_cents,
                "utilization": utilization,
                "forecast_utilization": forecast_utilization,
                "daily_run_rate_cents": daily_run_rate_cents,
                "remaining_daily_budget_cents": remaining_daily_budget_cents,
                "severity": severity,
            }
        )

    return {
        "month": month,
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "days_in_month": days_in_month,
        "low_confidence": days_elapsed < 3,
        "alerts": alerts,
        "ok_count": ok_count,
        "over_count": over_count,
        "alert_count": alert_count,
        "warn_count": warn_count,
    }


def suggest_budget_cuts(conn: sqlite3.Connection, target_dollars: str, view: str = "all") -> dict:
    target_cents = dollars_to_cents(target_dollars)

    month = _today_month()
    start, end = _month_bounds(month)
    view_clause = use_type_filter(view)
    rows = conn.execute(
        f"""
        SELECT COALESCE(p.name, c.name, 'Uncategorized') AS category_name,
               ABS(SUM(t.amount_cents)) AS spend_cents
         FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.date >= ?
           AND t.date <= ?
           {view_clause}
         GROUP BY category_name
         HAVING spend_cents > 0
         ORDER BY spend_cents DESC
        """,
        (start, end),
    ).fetchall()

    total_spend = sum(int(r["spend_cents"]) for r in rows)
    suggestions: list[dict] = []
    if total_spend <= 0:
        return {
            "target_cents": target_cents,
            "month": month,
            "suggestions": suggestions,
        }

    for row in rows:
        share = int(round(target_cents * (int(row["spend_cents"]) / total_spend)))
        if share <= 0:
            continue
        suggestions.append(
            {
                "category_name": row["category_name"] or "Uncategorized",
                "current_spend_cents": int(row["spend_cents"]),
                "suggested_cut_cents": share,
            }
        )

    allocated = sum(s["suggested_cut_cents"] for s in suggestions)
    if suggestions and allocated != target_cents:
        suggestions[0]["suggested_cut_cents"] += target_cents - allocated

    return {
        "target_cents": target_cents,
        "month": month,
        "suggestions": suggestions,
    }
