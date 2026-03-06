"""Weekly category summary command."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from ..budget_engine import budget_alerts
from ..models import cents_to_dollars
from .common import fmt_dollars, use_type_filter


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("weekly", parents=[format_parent], help="Weekly summary")
    parser.add_argument("--week", help="ISO week format YYYY-Wnn")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--view", choices=["personal", "business", "all"], default="all")
    parser.set_defaults(func=handle_weekly, command_name="weekly")


def _parse_week(week_str: str | None) -> tuple[date, date]:
    if week_str:
        start = datetime.strptime(f"{week_str}-1", "%G-W%V-%u").date()
    else:
        today = date.today()
        start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start, end


def _category_totals(conn, start: date, end: date, view: str = "all") -> list[dict]:
    view_clause = use_type_filter(view)
    rows = conn.execute(
        """
        SELECT COALESCE(p.name, c.name, 'Uncategorized') AS group_name,
               COALESCE(c.name, 'Uncategorized') AS category_name,
               COALESCE(SUM(t.amount_cents), 0) AS total_cents
         FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
          LEFT JOIN categories p ON p.id = c.parent_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.date >= ?
           AND t.date <= ?
           {view_clause}
         GROUP BY group_name, category_name
         ORDER BY ABS(total_cents) DESC, category_name ASC
        """.format(view_clause=view_clause),
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    return [
        {
            "group_name": row["group_name"],
            "category_name": row["category_name"],
            "total_cents": int(row["total_cents"]),
            "total": cents_to_dollars(int(row["total_cents"])),
        }
        for row in rows
    ]


def _budget_alert_cli_line(row: dict[str, Any]) -> str:
    category_name = str(row.get("category_name") or row.get("group_name") or "Uncategorized")
    budget_dollars = fmt_dollars(cents_to_dollars(int(row.get("budget_cents", 0))))
    severity = str(row.get("severity") or "").lower()
    if severity == "over":
        spent_dollars = fmt_dollars(cents_to_dollars(abs(int(row.get("actual_cents", 0)))))
        return f"{category_name}: {spent_dollars}/{budget_dollars} (OVER)"

    forecast_dollars = fmt_dollars(cents_to_dollars(int(row.get("forecast_cents", 0))))
    if severity == "alert":
        label = "AT RISK"
    elif severity == "warn":
        label = "WARN"
    else:
        label = "ALERT"
    return f"{category_name}: on pace for {forecast_dollars}/{budget_dollars} ({label})"


def handle_weekly(args, conn) -> dict[str, Any]:
    start, end = _parse_week(args.week)
    view = getattr(args, "view", "all")
    current = _category_totals(conn, start, end, view=view)
    view_clause = use_type_filter(view)
    data_range_row = conn.execute(
        f"""
        SELECT MIN(t.date) AS earliest, MAX(t.date) AS latest
          FROM transactions t
         WHERE t.is_active = 1
           {view_clause}
        """
    ).fetchone()
    data_range = {
        "earliest": data_range_row["earliest"],
        "latest": data_range_row["latest"],
    }

    data = {
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "categories": current,
        "data_range": data_range,
    }

    weekly_alerts: list[dict[str, Any]] | None = None
    today = date.today()
    current_week_start = today - timedelta(days=today.weekday())
    if start == current_week_start:
        alert_result = budget_alerts(conn, month=today.strftime("%Y-%m"), view=view)
        weekly_alerts = [dict(row) for row in alert_result.get("alerts", [])]
        data["budget_alerts"] = weekly_alerts

    if args.compare:
        prev_start = start - timedelta(days=7)
        prev_end = end - timedelta(days=7)
        prev = _category_totals(conn, prev_start, prev_end, view=view)
        prev_map = {row["category_name"]: row["total_cents"] for row in prev}

        compared = []
        for row in current:
            previous = prev_map.get(row["category_name"], 0)
            delta_cents = row["total_cents"] - previous
            compared.append(
                {
                    **row,
                    "previous_total_cents": previous,
                    "previous_total": cents_to_dollars(previous),
                    "delta_cents": delta_cents,
                    "delta": cents_to_dollars(delta_cents),
                }
            )

        data["compare_week_start"] = prev_start.isoformat()
        data["compare_week_end"] = prev_end.isoformat()
        data["categories"] = compared

    total_cents = sum(row["total_cents"] for row in current)

    # Build rollup view: group totals with leaf detail, sorted by group total
    groups: dict[str, int] = {}
    group_children: dict[str, list[dict]] = {}
    group_deltas: dict[str, int] = {}
    for row in data["categories"]:
        gn = row["group_name"]
        groups[gn] = groups.get(gn, 0) + row["total_cents"]
        group_children.setdefault(gn, []).append(row)
        if args.compare:
            group_deltas[gn] = group_deltas.get(gn, 0) + row.get("delta_cents", 0)

    sorted_groups = sorted(groups.items(), key=lambda x: abs(x[1]), reverse=True)

    if sorted_groups:
        cli_lines: list[str] = []

        # Header line
        week_label = f"{start.strftime('%G')}-W{start.strftime('%V')}"
        date_range_str = f"{start.strftime('%b %d')} \u2013 {end.strftime('%b %d')}"
        total_dollars = cents_to_dollars(total_cents)
        if args.compare:
            prev_total_cents = sum(row["total_cents"] for row in prev)
            prev_total_dollars = cents_to_dollars(prev_total_cents)
            delta_total_cents = total_cents - prev_total_cents
            delta_total_dollars = cents_to_dollars(delta_total_cents)
            prev_week_label = f"W{(start - timedelta(days=7)).strftime('%V')}"
            cli_lines.append(
                f"{week_label} vs {prev_week_label} ({date_range_str}) \u2014 Total: {fmt_dollars(total_dollars)} "
                f"(prev: {fmt_dollars(prev_total_dollars)}, \u0394 {fmt_dollars(delta_total_dollars)})"
            )
        else:
            cli_lines.append(f"{week_label} ({date_range_str}) \u2014 Total: {fmt_dollars(total_dollars)}")
        cli_lines.append("")

        for gn, total in sorted_groups:
            group_line = f"{gn}: {fmt_dollars(cents_to_dollars(total))}"
            if args.compare and gn in group_deltas:
                group_line += f" (\u0394 {fmt_dollars(cents_to_dollars(group_deltas[gn]))})"
            cli_lines.append(group_line)
            children = group_children[gn]
            if len(children) == 1 and children[0]["category_name"] == gn:
                continue  # standalone category, no children to show
            for child in sorted(children, key=lambda r: abs(r["total_cents"]), reverse=True):
                child_line = f"  {child['category_name']}: {fmt_dollars(child['total'])}"
                if args.compare and "delta_cents" in child:
                    child_line += f" (\u0394 {fmt_dollars(cents_to_dollars(child['delta_cents']))})"
                cli_lines.append(child_line)
        cli_report = "\n".join(cli_lines)
    elif data_range["earliest"] and data_range["latest"]:
        cli_report = (
            f"No transactions for week {start.isoformat()} to {end.isoformat()} "
            f"(data range: {data_range['earliest']} to {data_range['latest']})"
        )
    else:
        cli_report = (
            f"No transactions for week {start.isoformat()} to {end.isoformat()} "
            "(data range: empty)"
        )

    if weekly_alerts:
        alert_lines = ["--- Budget Alerts ---"] + [_budget_alert_cli_line(row) for row in weekly_alerts]
        cli_report = f"{cli_report}\n\n" + "\n".join(alert_lines)

    return {
        "data": data,
        "summary": {
            "total_categories": len(data["categories"]),
            "total_amount": cents_to_dollars(total_cents),
        },
        "cli_report": cli_report,
    }
