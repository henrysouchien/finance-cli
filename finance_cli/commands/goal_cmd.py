"""Goal tracking commands: set, list, status."""

from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from ..debt_calculator import load_debt_cards, project_interest
from ..models import cents_to_dollars, dollars_to_cents
from .common import fmt_dollars


VALID_METRICS = {"net_worth", "liquid_cash", "total_debt", "investments", "savings_rate"}
DOLLAR_METRICS = {"net_worth", "liquid_cash", "total_debt", "investments"}


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("goal", parents=[format_parent], help="Financial goal tracking")
    goal_sub = parser.add_subparsers(dest="goal_command", required=True)

    p_set = goal_sub.add_parser("set", parents=[format_parent], help="Set or update a financial goal")
    p_set.add_argument("--name", required=True, help="Goal name (unique)")
    p_set.add_argument("--target", required=True, type=float, help="Target value (dollars or percentage)")
    p_set.add_argument(
        "--metric", choices=sorted(VALID_METRICS), default="net_worth",
        help="Metric to track",
    )
    p_set.add_argument("--direction", choices=["up", "down"], default="up")
    p_set.add_argument("--deadline", default=None, help="Optional ISO date deadline")
    p_set.set_defaults(func=handle_set, command_name="goal.set")

    p_list = goal_sub.add_parser("list", parents=[format_parent], help="List active goals")
    p_list.set_defaults(func=handle_list, command_name="goal.list")

    p_status = goal_sub.add_parser("status", parents=[format_parent], help="Goal progress report")
    p_status.set_defaults(func=handle_status, command_name="goal.status")


def _get_metric_value(conn: sqlite3.Connection, metric: str) -> int | float:
    """Return the current value for a metric (cents for dollar metrics, float for pct)."""
    if metric in DOLLAR_METRICS:
        return _get_balance_metric(conn, metric)
    if metric == "savings_rate":
        return _get_savings_rate(conn)
    raise ValueError(f"Unknown metric: {metric}")


def _get_balance_metric(conn: sqlite3.Connection, metric: str) -> int:
    """Return balance metric in cents."""
    rows = conn.execute(
        """
        SELECT account_type,
               COALESCE(SUM(balance_current_cents), 0) AS total_cents
          FROM accounts a
         WHERE a.is_active = 1
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
         GROUP BY account_type
        """
    ).fetchall()

    liquid_cents = 0
    investment_cents = 0
    asset_cents = 0
    liability_cents = 0

    for row in rows:
        acct_type = str(row["account_type"] or "")
        total = int(row["total_cents"] or 0)
        if acct_type in {"checking", "savings"}:
            liquid_cents += total
            asset_cents += total
        elif acct_type == "investment":
            investment_cents += total
            asset_cents += total
        elif acct_type in {"credit_card", "loan"}:
            liability_cents += abs(total)

    if metric == "net_worth":
        return asset_cents - liability_cents
    if metric == "liquid_cash":
        return liquid_cents
    if metric == "total_debt":
        return liability_cents
    if metric == "investments":
        return investment_cents
    return 0


def _get_savings_rate(conn: sqlite3.Connection) -> float:
    """Return savings rate as a percentage (0-100)."""
    row = conn.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS expense_cents
          FROM transactions t
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= date('now', 'start of month', '-3 months')
           AND t.date < date('now', 'start of month')
        """
    ).fetchone()
    income = int(row["income_cents"])
    expense = int(row["expense_cents"])
    if income <= 0:
        return 0.0
    return round((income - expense) / income * 100, 2)


def handle_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Set or update a financial goal."""
    name = str(args.name).strip()
    target = float(args.target)
    metric = str(args.metric)
    direction = str(getattr(args, "direction", "up") or "up")
    deadline = getattr(args, "deadline", None)

    if metric not in VALID_METRICS:
        raise ValueError(f"metric must be one of: {', '.join(sorted(VALID_METRICS))}")
    if direction not in {"up", "down"}:
        raise ValueError("direction must be 'up' or 'down'")

    goal_id = uuid.uuid4().hex

    current_value = _get_metric_value(conn, metric)

    if metric in DOLLAR_METRICS:
        target_cents = dollars_to_cents(target)
        target_pct = None
        starting_cents = int(current_value)
        starting_pct = None
    else:
        target_cents = None
        target_pct = target
        starting_cents = None
        starting_pct = float(current_value)

    conn.execute(
        """INSERT OR REPLACE INTO goals
           (id, name, metric, target_cents, target_pct, starting_cents, starting_pct,
            direction, deadline, is_active, created_at, updated_at)
           VALUES (
               COALESCE((SELECT id FROM goals WHERE name = ?), ?),
               ?, ?, ?, ?, ?, ?, ?, ?, 1,
               COALESCE((SELECT created_at FROM goals WHERE name = ?), datetime('now')),
               datetime('now')
           )""",
        (name, goal_id, name, metric, target_cents, target_pct, starting_cents, starting_pct,
         direction, deadline, name),
    )
    conn.commit()

    goal = {
        "name": name,
        "metric": metric,
        "direction": direction,
        "deadline": deadline,
    }
    if metric in DOLLAR_METRICS:
        goal["target"] = cents_to_dollars(target_cents)
        goal["target_cents"] = target_cents
        goal["starting"] = cents_to_dollars(starting_cents)
        goal["starting_cents"] = starting_cents
    else:
        goal["target_pct"] = target_pct
        goal["starting_pct"] = starting_pct

    return {
        "data": {"goal": goal},
        "summary": {"name": name, "metric": metric},
        "cli_report": f"Goal '{name}' set: {metric} -> {target} ({direction})",
    }


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """List all active goals."""
    rows = conn.execute(
        """SELECT id, name, metric, target_cents, target_pct, starting_cents, starting_pct,
                  direction, deadline, created_at, updated_at
             FROM goals WHERE is_active = 1 ORDER BY created_at"""
    ).fetchall()

    goals: list[dict[str, Any]] = []
    for row in rows:
        metric = str(row["metric"])
        g: dict[str, Any] = {
            "id": row["id"],
            "name": row["name"],
            "metric": metric,
            "direction": row["direction"],
            "deadline": row["deadline"],
        }
        if metric in DOLLAR_METRICS:
            g["target"] = cents_to_dollars(int(row["target_cents"] or 0))
            g["target_cents"] = int(row["target_cents"] or 0)
            g["starting"] = cents_to_dollars(int(row["starting_cents"] or 0))
            g["starting_cents"] = int(row["starting_cents"] or 0)
            current = _get_balance_metric(conn, metric)
            g["current"] = cents_to_dollars(current)
            g["current_cents"] = current
        else:
            g["target_pct"] = row["target_pct"]
            g["starting_pct"] = row["starting_pct"]
            current = _get_savings_rate(conn)
            g["current_pct"] = current
        goals.append(g)

    if not goals:
        cli_report = "No active goals. Use 'goal set' to create one."
    else:
        lines = ["Financial Goals", "=" * 40, ""]
        for g in goals:
            if g["metric"] in DOLLAR_METRICS:
                lines.append(f"  {g['name']}: {fmt_dollars(g['current'])} -> {fmt_dollars(g['target'])} ({g['direction']})")
            else:
                lines.append(f"  {g['name']}: {g['current_pct']:.1f}% -> {g['target_pct']:.1f}% ({g['direction']})")
        cli_report = "\n".join(lines)

    return {
        "data": {"goals": goals},
        "summary": {"count": len(goals)},
        "cli_report": cli_report,
    }


def _compute_progress(current, starting, target, direction: str) -> float:
    """Compute progress percentage (0-100), clamped."""
    if direction == "down":
        span = starting - target
        if span == 0:
            return 100.0
        raw = (starting - current) / span * 100
    else:
        span = target - starting
        if span == 0:
            return 100.0
        raw = (current - starting) / span * 100
    return max(0.0, min(100.0, raw))


def _progress_bar(pct: float, width: int = 20) -> str:
    """Build a text progress bar."""
    filled = int(round(pct / 100 * width))
    filled = max(0, min(width, filled))
    return "\u2588" * filled + "\u2591" * (width - filled)


def _estimate_months_to_target(
    conn: sqlite3.Connection, metric: str, current, target, direction: str,
) -> int | None:
    """Estimate months to reach target. Returns None if not on track."""
    if metric == "total_debt" and direction == "down":
        # Use debt projection for credit cards
        cards = load_debt_cards(conn, include_zero_balance=False)
        if cards and int(target) == 0:
            proj = project_interest(cards, months=360, summary_only=True)
            for entry in proj.get("schedule", []):
                if int(entry["remaining_balance_cents"]) <= 0:
                    return int(entry["month"])
            return None  # Never pays off in 360 months

    # Generic monthly trend from 3-month average
    rows = conn.execute(
        """
        SELECT strftime('%Y-%m', t.date) AS month,
               COALESCE(SUM(CASE WHEN t.amount_cents > 0 THEN t.amount_cents ELSE 0 END), 0) AS income_cents,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN ABS(t.amount_cents) ELSE 0 END), 0) AS expense_cents
          FROM transactions t
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= date('now', 'start of month', '-3 months')
           AND t.date < date('now', 'start of month')
         GROUP BY strftime('%Y-%m', t.date)
        """
    ).fetchall()

    if not rows:
        return None

    total_income = sum(int(r["income_cents"]) for r in rows)
    total_expense = sum(int(r["expense_cents"]) for r in rows)
    n = len(rows)
    monthly_trend = (total_income - total_expense) / n if n else 0

    if direction == "up":
        gap = target - current
        if gap <= 0:
            return 0
        if monthly_trend <= 0:
            return None
        return max(1, int(gap / monthly_trend + 0.999))
    else:  # down
        gap = current - target
        if gap <= 0:
            return 0
        # For down goals, positive trend means we have surplus to pay down
        if monthly_trend <= 0:
            return None
        return max(1, int(gap / monthly_trend + 0.999))


def handle_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Show progress on all active goals."""
    rows = conn.execute(
        """SELECT id, name, metric, target_cents, target_pct, starting_cents, starting_pct,
                  direction, deadline
             FROM goals WHERE is_active = 1 ORDER BY created_at"""
    ).fetchall()

    goals: list[dict[str, Any]] = []
    lines = ["Financial Goals", "=" * 40]

    for row in rows:
        metric = str(row["metric"])
        direction = str(row["direction"])
        g: dict[str, Any] = {
            "name": row["name"],
            "metric": metric,
            "direction": direction,
            "deadline": row["deadline"],
        }

        if metric in DOLLAR_METRICS:
            current = _get_balance_metric(conn, metric)
            target = int(row["target_cents"] or 0)
            starting = int(row["starting_cents"] or 0)
            progress = _compute_progress(current, starting, target, direction)

            g["current_cents"] = current
            g["current"] = cents_to_dollars(current)
            g["target_cents"] = target
            g["target"] = cents_to_dollars(target)
            g["starting_cents"] = starting
            g["starting"] = cents_to_dollars(starting)
            g["progress_pct"] = round(progress, 1)

            est = _estimate_months_to_target(conn, metric, current, target, direction)
            g["estimated_months"] = est

            lines.append("")
            lines.append(f"{row['name']} ({fmt_dollars(cents_to_dollars(target))})")
            lines.append(
                f"  Current: {fmt_dollars(cents_to_dollars(current))} | "
                f"Target: {fmt_dollars(cents_to_dollars(target))} | "
                f"Progress: {progress:.0f}%"
            )
            lines.append(f"  {_progress_bar(progress)} {progress:.0f}%")
            if est is not None:
                lines.append(f"  Estimated: ~{est} months to target")
            else:
                lines.append("  Not on track")
        else:
            current = _get_savings_rate(conn)
            target_pct = float(row["target_pct"] or 0)
            starting_pct = float(row["starting_pct"] or 0)
            progress = _compute_progress(current, starting_pct, target_pct, direction)

            g["current_pct"] = current
            g["target_pct"] = target_pct
            g["starting_pct"] = starting_pct
            g["progress_pct"] = round(progress, 1)
            g["estimated_months"] = None  # Rate goals don't have time estimates

            lines.append("")
            lines.append(f"{row['name']} ({target_pct:.1f}%)")
            lines.append(
                f"  Current: {current:.1f}% | Target: {target_pct:.1f}% | Progress: {progress:.0f}%"
            )
            lines.append(f"  {_progress_bar(progress)} {progress:.0f}%")

        goals.append(g)

    if not goals:
        lines = ["No active goals. Use 'goal set' to create one."]

    return {
        "data": {"goals": goals},
        "summary": {"count": len(goals)},
        "cli_report": "\n".join(lines),
    }
