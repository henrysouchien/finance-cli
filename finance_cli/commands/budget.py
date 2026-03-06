"""Budget commands."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any

from ..budget_engine import (
    budget_alerts,
    delete_budget,
    find_budget,
    list_budgets,
    monthly_budget_forecast,
    monthly_budget_status,
    set_budget,
    suggest_budget_cuts,
    update_budget,
)
from ..models import cents_to_dollars
from .common import fmt_dollars, get_category_id_by_name


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("budget", parents=[format_parent], help="Budget commands")
    budget_sub = parser.add_subparsers(dest="budget_command", required=True)

    p_set = budget_sub.add_parser("set", parents=[format_parent], help="Set budget")
    p_set.add_argument("--category", required=True)
    p_set.add_argument("--amount", required=True)
    p_set.add_argument("--period", required=True, choices=["monthly", "weekly", "yearly"])
    p_set.add_argument("--view", choices=["personal", "business", "all"], default="personal")
    p_set.set_defaults(func=handle_set, command_name="budget.set")

    p_update = budget_sub.add_parser("update", parents=[format_parent], help="Update budget amount")
    p_update.add_argument("--id")
    p_update.add_argument("--category")
    p_update.add_argument("--amount", required=True)
    p_update.add_argument("--period", choices=["monthly", "weekly", "yearly"], default="monthly")
    p_update.add_argument("--view", choices=["personal", "business", "all"], default="personal")
    p_update.set_defaults(func=handle_update, command_name="budget.update")

    p_delete = budget_sub.add_parser("delete", parents=[format_parent], help="Delete budget")
    p_delete.add_argument("--id")
    p_delete.add_argument("--category")
    p_delete.add_argument("--period", choices=["monthly", "weekly", "yearly"], default="monthly")
    p_delete.add_argument("--view", choices=["personal", "business", "all"], default="personal")
    p_delete.set_defaults(func=handle_delete, command_name="budget.delete")

    p_list = budget_sub.add_parser("list", parents=[format_parent], help="List budgets")
    p_list.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_list.set_defaults(func=handle_list, command_name="budget.list")

    p_status = budget_sub.add_parser("status", parents=[format_parent], help="Budget status")
    p_status.add_argument("--month")
    p_status.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_status.set_defaults(func=handle_status, command_name="budget.status")

    p_forecast = budget_sub.add_parser("forecast", parents=[format_parent], help="Budget forecast")
    p_forecast.add_argument("--month")
    p_forecast.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_forecast.set_defaults(func=handle_forecast, command_name="budget.forecast")

    p_alerts = budget_sub.add_parser("alerts", parents=[format_parent], help="Budget run-rate alerts")
    p_alerts.add_argument("--month")
    p_alerts.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_alerts.set_defaults(func=handle_alerts, command_name="budget.alerts")

    p_suggest = budget_sub.add_parser("suggest", parents=[format_parent], help="Suggest budget cuts")
    p_suggest.add_argument("--goal", required=True, choices=["savings"])
    p_suggest.add_argument("--target", required=True)
    p_suggest.add_argument("--view", choices=["personal", "business", "all"], default="all")
    p_suggest.set_defaults(func=handle_suggest, command_name="budget.suggest")


def handle_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    category_id = get_category_id_by_name(conn, args.category)
    if not category_id:
        raise ValueError(f"Category '{args.category}' not found")

    view = getattr(args, "view", "personal")
    if view == "all":
        raise ValueError("budget set requires --view personal or --view business")
    use_type = "Business" if view == "business" else "Personal"
    existing = find_budget(conn, category_id=category_id, period=args.period, use_type=use_type)

    budget_id = set_budget(
        conn,
        category_id=category_id,
        amount_dollars=args.amount,
        period=args.period,
        use_type=use_type,
    )
    created = existing is None
    action = "created" if created else "updated"
    return {
        "data": {
            "budget_id": budget_id,
            "category": args.category,
            "period": args.period,
            "amount": float(args.amount),
            "use_type": use_type,
            "action": action,
            "created": created,
        },
        "summary": {"total_budgets": 1},
        "cli_report": f"{action.title()} {args.period} budget for {args.category} [{use_type}]",
    }


def _resolve_active_budget_by_category(
    args,
    conn: sqlite3.Connection,
    *,
    action: str,
) -> tuple[str, str, str, str]:
    category_name = getattr(args, "category", None)
    if not category_name:
        raise ValueError(f"budget {action} requires --id or --category")

    view = getattr(args, "view", "personal")
    if view == "all":
        raise ValueError(f"budget {action} requires --view personal or --view business")
    use_type = "Business" if view == "business" else "Personal"

    period = getattr(args, "period", "monthly")
    category_id = get_category_id_by_name(conn, category_name)
    if not category_id:
        raise ValueError(f"Category '{category_name}' not found")

    budget_row = find_budget(conn, category_id=category_id, period=period, use_type=use_type)
    if not budget_row:
        raise ValueError(f"no budget found for {category_name} ({use_type}, {period})")

    return str(budget_row["id"]), category_name, period, use_type


def handle_update(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if getattr(args, "id", None):
        budget_id = update_budget(conn, budget_id=args.id, amount_dollars=args.amount)
        return {
            "data": {
                "budget_id": budget_id,
                "amount": float(args.amount),
            },
            "summary": {"total_budgets": 1},
            "cli_report": f"Updated budget {budget_id}",
        }

    budget_id, category_name, period, use_type = _resolve_active_budget_by_category(
        args,
        conn,
        action="update",
    )
    update_budget(conn, budget_id=budget_id, amount_dollars=args.amount)
    return {
        "data": {
            "budget_id": budget_id,
            "category": category_name,
            "period": period,
            "amount": float(args.amount),
            "use_type": use_type,
        },
        "summary": {"total_budgets": 1},
        "cli_report": f"Updated {period} budget for {category_name} [{use_type}]",
    }


def handle_delete(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if getattr(args, "id", None):
        budget_id = delete_budget(conn, budget_id=args.id)
        return {
            "data": {
                "budget_id": budget_id,
                "deleted": True,
            },
            "summary": {"total_budgets": 1},
            "cli_report": f"Deleted budget {budget_id}",
        }

    budget_id, category_name, period, use_type = _resolve_active_budget_by_category(
        args,
        conn,
        action="delete",
    )
    delete_budget(conn, budget_id=budget_id)
    return {
        "data": {
            "budget_id": budget_id,
            "category": category_name,
            "period": period,
            "use_type": use_type,
            "deleted": True,
        },
        "summary": {"total_budgets": 1},
        "cli_report": f"Deleted {period} budget for {category_name} [{use_type}]",
    }


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    budgets = list_budgets(conn, view=getattr(args, "view", "all"))
    out = []
    for row in budgets:
        item = dict(row)
        item["amount"] = cents_to_dollars(int(row["amount_cents"]))
        item["use_type"] = row["use_type"]
        out.append(item)

    cli_report = (
        "\n".join(
            f"{b['category_name']} [{b['use_type']}] {b['period']} {b['amount']:.2f}"
            for b in out
        )
        if out
        else "No budgets"
    )
    return {
        "data": {"budgets": out},
        "summary": {"total_budgets": len(out)},
        "cli_report": cli_report,
    }


def handle_status(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = monthly_budget_status(conn, month=args.month, view=getattr(args, "view", "all"))
    data_rows = []
    for row in rows:
        data_rows.append(
            {
                "category_id": row.category_id,
                "category_name": row.category_name,
                "group_name": row.group_name,
                "use_type": row.use_type,
                "budget_cents": row.budget_cents,
                "actual_cents": row.actual_cents,
                "remaining_cents": row.remaining_cents,
                "budget": cents_to_dollars(row.budget_cents),
                "actual": cents_to_dollars(row.actual_cents),
                "remaining": cents_to_dollars(row.remaining_cents),
                "utilization": row.utilization,
            }
        )

    groups: dict[str, dict[str, int]] = {}
    group_children: dict[str, list[dict[str, Any]]] = {}
    for row in data_rows:
        group_name = str(row["group_name"])
        groups.setdefault(group_name, {"budget_cents": 0, "actual_cents": 0})
        groups[group_name]["budget_cents"] += int(row["budget_cents"])
        groups[group_name]["actual_cents"] += int(row["actual_cents"])
        group_children.setdefault(group_name, []).append(row)

    sorted_groups = sorted(
        groups.items(),
        key=lambda item: (-abs(item[1]["actual_cents"]), item[0]),
    )
    cli_lines: list[str] = []
    for group_name, totals in sorted_groups:
        children = group_children[group_name]
        if len(children) == 1 and children[0]["category_name"] == group_name:
            child = children[0]
            cli_lines.append(
                f"{child['category_name']} [{child['use_type']}]: spent={child['actual']:.2f} "
                f"budget={child['budget']:.2f} remaining={child['remaining']:.2f}"
            )
            continue
        budget_cents = int(totals["budget_cents"])
        actual_cents = int(totals["actual_cents"])
        remaining_cents = budget_cents - abs(actual_cents)
        cli_lines.append(
            f"{group_name}: spent={cents_to_dollars(actual_cents):.2f} "
            f"budget={cents_to_dollars(budget_cents):.2f} "
            f"remaining={cents_to_dollars(remaining_cents):.2f}"
        )
        for child in sorted(
            children,
            key=lambda r: (-abs(int(r["actual_cents"])), str(r["category_name"]), str(r["use_type"])),
        ):
            cli_lines.append(
                f"  {child['category_name']} [{child['use_type']}]: spent={child['actual']:.2f} "
                f"budget={child['budget']:.2f} remaining={child['remaining']:.2f}"
            )

    cli_report = "\n".join(cli_lines) if cli_lines else "No active monthly budgets"

    return {
        "data": {"month": args.month, "status": data_rows},
        "summary": {"total_budgets": len(data_rows)},
        "cli_report": cli_report,
    }


def handle_forecast(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = monthly_budget_forecast(conn, month=args.month, view=getattr(args, "view", "all"))
    out = []
    for row in rows:
        item = dict(row)
        item["budget"] = cents_to_dollars(item["budget_cents"])
        item["actual"] = cents_to_dollars(item["actual_cents"])
        item["forecast"] = cents_to_dollars(item["forecast_cents"])
        item["forecast_over_budget"] = cents_to_dollars(item["forecast_over_budget_cents"])
        out.append(item)

    groups: dict[str, dict[str, int]] = {}
    group_children: dict[str, list[dict[str, Any]]] = {}
    for row in out:
        group_name = str(row["group_name"])
        groups.setdefault(group_name, {"budget_cents": 0, "forecast_cents": 0})
        groups[group_name]["budget_cents"] += int(row["budget_cents"])
        groups[group_name]["forecast_cents"] += int(row["forecast_cents"])
        group_children.setdefault(group_name, []).append(row)

    sorted_groups = sorted(
        groups.items(),
        key=lambda item: (-abs(item[1]["forecast_cents"]), item[0]),
    )
    cli_lines: list[str] = []
    for group_name, totals in sorted_groups:
        budget_cents = int(totals["budget_cents"])
        forecast_cents = int(totals["forecast_cents"])
        cli_lines.append(
            f"{group_name}: forecast={cents_to_dollars(forecast_cents):.2f} "
            f"budget={cents_to_dollars(budget_cents):.2f}"
        )
        children = group_children[group_name]
        if len(children) == 1 and children[0]["category_name"] == group_name:
            continue
        for child in sorted(children, key=lambda r: (-abs(int(r["forecast_cents"])), str(r["category_name"]))):
            cli_lines.append(
                f"  {child['category_name']}: forecast={child['forecast']:.2f} "
                f"budget={child['budget']:.2f}"
            )

    cli_report = "\n".join(cli_lines) if cli_lines else "No active monthly budgets"

    return {
        "data": {"month": args.month, "forecast": out},
        "summary": {"total_budgets": len(out)},
        "cli_report": cli_report,
    }


def handle_alerts(args, conn: sqlite3.Connection) -> dict[str, Any]:
    result = budget_alerts(conn, month=args.month, view=getattr(args, "view", "all"))

    out_alerts: list[dict[str, Any]] = []
    for row in result["alerts"]:
        budget_cents = int(row["budget_cents"])
        actual_cents = int(row["actual_cents"])
        forecast_cents = int(row["forecast_cents"])
        out_alerts.append(
            {
                **row,
                "budget": cents_to_dollars(budget_cents),
                "actual": cents_to_dollars(abs(actual_cents)),
                "forecast": cents_to_dollars(forecast_cents),
                "daily_run_rate": cents_to_dollars(int(row["daily_run_rate_cents"])),
                "remaining_daily_budget": cents_to_dollars(int(row["remaining_daily_budget_cents"])),
            }
        )

    try:
        month_label = datetime.strptime(result["month"], "%Y-%m").strftime("%B %Y")
    except ValueError:
        month_label = str(result["month"])

    lines: list[str] = [
        (
            f"Budget Alerts - {month_label} "
            f"(day {result['days_elapsed']} of {result['days_in_month']}, "
            f"{result['days_remaining']} days remaining)"
        )
    ]
    if result.get("low_confidence"):
        lines.append("Run-rate confidence is low (day 1-2 of month).")

    over_rows = [row for row in out_alerts if row.get("severity") == "over"]
    alert_rows = [row for row in out_alerts if row.get("severity") == "alert"]
    warn_rows = [row for row in out_alerts if row.get("severity") == "warn"]

    if over_rows:
        lines.append("")
        lines.append("OVER BUDGET:")
        for row in over_rows:
            spent_cents = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            over_cents = max(spent_cents - budget_cents, 0)
            lines.append(
                f"  {row['category_name']} [{row['use_type']}]: "
                f"{fmt_dollars(cents_to_dollars(spent_cents))} spent / "
                f"{fmt_dollars(cents_to_dollars(budget_cents))} budget "
                f"({row['utilization'] * 100:.0f}%) - "
                f"{fmt_dollars(cents_to_dollars(over_cents))} over"
            )

    if alert_rows:
        lines.append("")
        lines.append("AT RISK (projected to exceed):")
        for row in alert_rows:
            spent_cents = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            forecast_cents = int(row["forecast_cents"])
            over_cents = max(forecast_cents - budget_cents, 0)
            lines.append(
                f"  {row['category_name']} [{row['use_type']}]: "
                f"{fmt_dollars(cents_to_dollars(spent_cents))} spent / "
                f"{fmt_dollars(cents_to_dollars(budget_cents))} budget - "
                f"on pace for {fmt_dollars(cents_to_dollars(forecast_cents))} "
                f"({fmt_dollars(cents_to_dollars(over_cents))} over)"
            )

    if warn_rows:
        lines.append("")
        lines.append("WARNING (>80% projected):")
        for row in warn_rows:
            spent_cents = abs(int(row["actual_cents"]))
            budget_cents = int(row["budget_cents"])
            forecast_cents = int(row["forecast_cents"])
            over_cents = max(forecast_cents - budget_cents, 0)
            lines.append(
                f"  {row['category_name']} [{row['use_type']}]: "
                f"{fmt_dollars(cents_to_dollars(spent_cents))} spent / "
                f"{fmt_dollars(cents_to_dollars(budget_cents))} budget - "
                f"on pace for {fmt_dollars(cents_to_dollars(forecast_cents))} "
                f"({fmt_dollars(cents_to_dollars(over_cents))} over)"
            )

    if not out_alerts and int(result["ok_count"]) == 0:
        lines.append("")
        lines.append("No active monthly budgets to evaluate")
    else:
        lines.append("")
        lines.append(f"{result['ok_count']} categories on track")

    return {
        "data": {
            "month": result["month"],
            "days_elapsed": result["days_elapsed"],
            "days_remaining": result["days_remaining"],
            "days_in_month": result["days_in_month"],
            "low_confidence": result["low_confidence"],
            "alerts": out_alerts,
            "ok_count": result["ok_count"],
            "over_count": result["over_count"],
            "alert_count": result["alert_count"],
            "warn_count": result["warn_count"],
        },
        "summary": {
            "total_alerts": len(out_alerts),
            "over_count": result["over_count"],
            "alert_count": result["alert_count"],
            "warn_count": result["warn_count"],
            "ok_count": result["ok_count"],
        },
        "cli_report": "\n".join(lines),
    }


def handle_suggest(args, conn: sqlite3.Connection) -> dict[str, Any]:
    suggestion = suggest_budget_cuts(conn, target_dollars=args.target, view=getattr(args, "view", "all"))
    out = {
        "goal": args.goal,
        "target_cents": suggestion["target_cents"],
        "target": cents_to_dollars(suggestion["target_cents"]),
        "month": suggestion["month"],
        "suggestions": [
            {
                **item,
                "current_spend": cents_to_dollars(item["current_spend_cents"]),
                "suggested_cut": cents_to_dollars(item["suggested_cut_cents"]),
            }
            for item in suggestion["suggestions"]
        ],
    }

    cli_report = "\n".join(
        f"{s['category_name']}: cut {s['suggested_cut']:.2f}"
        for s in out["suggestions"]
    ) if out["suggestions"] else "No spending data available"

    return {
        "data": out,
        "summary": {"total_suggestions": len(out["suggestions"])},
        "cli_report": cli_report,
    }
