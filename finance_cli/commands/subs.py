"""Subscription commands."""

from __future__ import annotations

import sqlite3
import uuid
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from ..debt_calculator import (
    MAX_SIM_MONTHS,
    compute_dashboard,
    load_debt_cards,
    project_interest,
    simulate_paydown,
)
from ..models import cents_to_dollars, dollars_to_cents
from ..subscriptions import (
    SUBSCRIPTION_EXCLUDED_KEYWORDS,
    _monthly_equivalent,
    detect_recurring_patterns,
    detect_subscriptions,
    subscription_burn,
)
from ..spending_analysis import (
    _DEFAULT_ESSENTIAL_CATEGORIES,
    is_essential as _is_essential,
    load_essential_categories as _load_essential_categories,
)
from .common import fmt_dollars
from .common import get_category_id_by_name


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("subs", parents=[format_parent], help="Subscription tracking")
    subs_sub = parser.add_subparsers(dest="subs_command", required=True)

    p_list = subs_sub.add_parser("list", parents=[format_parent], help="List subscriptions")
    p_list.add_argument("--all", dest="show_all", action="store_true", help="Include inactive subscriptions")
    p_list.set_defaults(func=handle_list, command_name="subs.list")

    p_detect = subs_sub.add_parser("detect", parents=[format_parent], help="Detect subscriptions")
    p_detect.set_defaults(func=handle_detect, command_name="subs.detect")

    p_recurring = subs_sub.add_parser("recurring", parents=[format_parent], help="List recurring patterns")
    p_recurring.set_defaults(func=handle_recurring, command_name="subs.recurring")

    p_add = subs_sub.add_parser("add", parents=[format_parent], help="Add subscription")
    p_add.add_argument("--vendor", required=True)
    p_add.add_argument("--amount", required=True)
    p_add.add_argument("--frequency", required=True, choices=["weekly", "biweekly", "monthly", "quarterly", "yearly"])
    p_add.add_argument("--category")
    p_add.add_argument("--use-type", choices=["Business", "Personal"])
    p_add.set_defaults(func=handle_add, command_name="subs.add")

    p_cancel = subs_sub.add_parser("cancel", parents=[format_parent], help="Cancel subscription")
    p_cancel.add_argument("id")
    p_cancel.set_defaults(func=handle_cancel, command_name="subs.cancel")

    p_total = subs_sub.add_parser("total", parents=[format_parent], help="Subscription burn totals")
    p_total.set_defaults(func=handle_total, command_name="subs.total")

    p_audit = subs_sub.add_parser("audit", parents=[format_parent], help="Audit subs vs debt payoff impact")
    p_audit.set_defaults(func=handle_audit, command_name="subs.audit")


_MONTHLY_MULTIPLIERS = {
    "weekly": 52 / 12,
    "biweekly": 26 / 12,
    "monthly": 1.0,
    "quarterly": 1 / 3,
    "yearly": 1 / 12,
}

def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT s.*, c.name AS category_name
          FROM subscriptions s
          LEFT JOIN categories c ON c.id = s.category_id
         ORDER BY s.is_active DESC, s.vendor_name ASC
        """
    ).fetchall()

    all_subs = []
    for row in rows:
        item = dict(row)
        item["amount"] = cents_to_dollars(int(item["amount_cents"]))
        freq = str(item.get("frequency") or "monthly")
        multiplier = _MONTHLY_MULTIPLIERS.get(freq, 1.0)
        item["monthly_amount"] = abs(item["amount"]) * multiplier
        all_subs.append(item)

    active_subs = [s for s in all_subs if int(s.get("is_active", 0))]
    inactive_subs = [s for s in all_subs if not int(s.get("is_active", 0))]

    if args.show_all:
        display_subs = all_subs
    else:
        display_subs = active_subs

    # Sort by monthly cost descending
    display_subs = sorted(display_subs, key=lambda s: s["monthly_amount"], reverse=True)

    total_monthly_burn = sum(s["monthly_amount"] for s in active_subs)

    if display_subs:
        header = f"{len(active_subs)} active subscription{'s' if len(active_subs) != 1 else ''} \u2014 {fmt_dollars(total_monthly_burn)}/mo"
        cli_lines = [header, ""]
        for s in display_subs:
            vendor = (s.get("vendor_name") or "")[:30].ljust(30)
            freq = (s.get("frequency") or "")[:10].ljust(10)
            amt = fmt_dollars(abs(s["amount"]))
            monthly = fmt_dollars(s["monthly_amount"])
            sub_type_tag = " [metered]" if s.get("sub_type") == "metered" else ""
            cli_lines.append(f"  {vendor} {freq} {amt:>10s}  {monthly:>10s}/mo{sub_type_tag}")
        if not args.show_all and inactive_subs:
            cli_lines.append("")
            cli_lines.append(f"({len(inactive_subs)} inactive hidden \u2014 use --all to show)")
        cli_report = "\n".join(cli_lines)
    else:
        cli_report = "No subscriptions"

    return {
        "data": {"subscriptions": all_subs},
        "summary": {"total_subscriptions": len(all_subs)},
        "cli_report": cli_report,
    }


def handle_detect(args, conn: sqlite3.Connection) -> dict[str, Any]:
    report = detect_subscriptions(conn)
    return {
        "data": report,
        "summary": {"total_detected": report["detected"]},
        "cli_report": (
            f"detected={report['detected']} inserted={report['inserted']} updated={report['updated']} "
            f"deactivated={report['deactivated']} recurring={report['recurring_patterns']} "
            f"recurring_txns={report['recurring_txns']}"
        ),
    }


def handle_recurring(args, conn: sqlite3.Connection) -> dict[str, Any]:
    patterns = detect_recurring_patterns(conn)
    category_rows = conn.execute("SELECT id, name FROM categories").fetchall()
    category_by_id = {str(row["id"]): str(row["name"]) for row in category_rows}

    recurring: list[dict[str, Any]] = []
    for pattern in patterns:
        is_subscription_eligible = (
            pattern.occurrence_count >= 3
            and pattern.amount_variance <= 0.15
            and not any(keyword in pattern.vendor_name.lower() for keyword in SUBSCRIPTION_EXCLUDED_KEYWORDS)
        )
        category_name = category_by_id.get(pattern.category_id, pattern.category_id)
        recurring.append(
            {
                "vendor_name": pattern.vendor_name,
                "account_id": pattern.account_id,
                "frequency": pattern.frequency,
                "median_amount_cents": pattern.median_amount_cents,
                "median_amount": cents_to_dollars(pattern.median_amount_cents),
                "amount_variance": pattern.amount_variance,
                "amount_variance_pct": round(pattern.amount_variance * 100, 2),
                "category_id": pattern.category_id,
                "category": category_name,
                "use_type": pattern.use_type,
                "next_expected": pattern.next_expected,
                "occurrence_count": pattern.occurrence_count,
                "transaction_count": len(pattern.transaction_ids),
                "subscription_eligible": is_subscription_eligible,
            }
        )

    cli_report = (
        "\n".join(
            (
                f"{row['vendor_name']} {row['frequency']} {row['median_amount']:.2f} "
                f"var={row['amount_variance_pct']:.1f}% n={row['occurrence_count']} "
                f"category={row['category'] or '-'} "
                f"{'subscription' if row['subscription_eligible'] else 'recurring'}"
            )
            for row in recurring
        )
        if recurring
        else "No recurring patterns"
    )
    return {
        "data": {"patterns": recurring},
        "summary": {"total_patterns": len(recurring)},
        "cli_report": cli_report,
    }


def handle_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    category_id = get_category_id_by_name(conn, args.category, required=True)
    sub_id = uuid.uuid4().hex

    conn.execute(
        """
        INSERT INTO subscriptions (
            id,
            vendor_name,
            category_id,
            amount_cents,
            frequency,
            next_expected,
            is_active,
            use_type,
            is_auto_detected
        ) VALUES (?, ?, ?, ?, ?, NULL, 1, ?, 0)
        """,
        (sub_id, args.vendor, category_id, dollars_to_cents(args.amount), args.frequency, args.use_type),
    )
    conn.commit()

    return {
        "data": {"subscription_id": sub_id},
        "summary": {"total_subscriptions": 1},
        "cli_report": f"Added subscription {args.vendor}",
    }


def handle_cancel(args, conn: sqlite3.Connection) -> dict[str, Any]:
    cursor = conn.execute(
        "UPDATE subscriptions SET is_active = 0 WHERE id = ?",
        (args.id,),
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Subscription {args.id} not found")

    return {
        "data": {"subscription_id": args.id, "is_active": False},
        "summary": {"total_subscriptions": 1},
        "cli_report": f"Canceled subscription {args.id}",
    }


def handle_total(args, conn: sqlite3.Connection) -> dict[str, Any]:
    totals = subscription_burn(conn)
    totals_out = {
        **totals,
        "monthly_burn": cents_to_dollars(totals["monthly_burn_cents"]),
        "yearly_burn": cents_to_dollars(totals["yearly_burn_cents"]),
    }
    return {
        "data": totals_out,
        "summary": {"total_subscriptions": totals["active_subscriptions"]},
        "cli_report": (
            f"Subscription Burn: {fmt_dollars(totals_out['monthly_burn'])}/mo "
            f"({fmt_dollars(totals_out['yearly_burn'])}/yr)\n"
            f"{totals['active_subscriptions']} active subscriptions"
        ),
    }


def handle_audit(args, conn: sqlite3.Connection) -> dict[str, Any]:
    essential_categories = _load_essential_categories()

    rows = conn.execute(
        """
        SELECT s.id,
               s.vendor_name,
               s.amount_cents,
               s.frequency,
               c.name AS category_name
          FROM subscriptions s
          LEFT JOIN categories c ON c.id = s.category_id
         WHERE s.is_active = 1
         ORDER BY s.vendor_name ASC
        """
    ).fetchall()

    subscriptions: list[dict[str, Any]] = []
    for row in rows:
        category_name = str(row["category_name"] or "")
        monthly_cents = _monthly_equivalent(
            abs(int(row["amount_cents"] or 0)),
            str(row["frequency"] or "monthly"),
        )
        classification = "essential" if _is_essential(category_name, essential_categories) else "discretionary"
        subscriptions.append(
            {
                "id": str(row["id"]),
                "vendor_name": str(row["vendor_name"] or ""),
                "monthly_cents": int(monthly_cents),
                "category": category_name,
                "classification": classification,
                "est_interest_saved_cents": 0,
                "est_months_saved": 0.0,
            }
        )

    subscriptions.sort(key=lambda item: (-int(item["monthly_cents"]), str(item["vendor_name"]).casefold()))

    essential_subs = [item for item in subscriptions if item["classification"] == "essential"]
    discretionary_subs = [item for item in subscriptions if item["classification"] == "discretionary"]

    essential_monthly_cents = sum(int(item["monthly_cents"]) for item in essential_subs)
    discretionary_monthly_cents = sum(int(item["monthly_cents"]) for item in discretionary_subs)
    total_monthly_cents = essential_monthly_cents + discretionary_monthly_cents

    zero_baseline = {
        "total_debt_cents": 0,
        "weighted_avg_apr": None,
        "monthly_minimums_cents": 0,
        "monthly_interest_cents": 0,
        "months_to_payoff": 0,
        "fully_paid_off": True,
        "total_interest_cents": 0,
        "apr_unknown_count": 0,
        "apr_unknown_balance_cents": 0,
    }

    cards = load_debt_cards(conn)
    if not cards:
        cli_lines = [
            "SUBSCRIPTION AUDIT",
            "==================",
            "",
            f"{len(subscriptions)} active subscriptions - {fmt_dollars(cents_to_dollars(total_monthly_cents))}/mo",
            f"  Essential:     {len(essential_subs)} subs   {fmt_dollars(cents_to_dollars(essential_monthly_cents))}/mo",
            (
                f"  Discretionary: {len(discretionary_subs)} subs   "
                f"{fmt_dollars(cents_to_dollars(discretionary_monthly_cents))}/mo"
            ),
            "",
            "DEBT CONTEXT",
            "  No active credit card debt balances.",
            "",
            "DISCRETIONARY SUBSCRIPTIONS (by monthly cost)",
            "  None" if not discretionary_subs else "",
            "",
            "SCENARIOS",
            "  No debt cards with balance > 0; scenarios unavailable.",
        ]
        if discretionary_subs:
            cli_lines[11] = "                                   Monthly  Est Interest Saved  Est Months Saved"
            for sub in discretionary_subs:
                vendor = str(sub["vendor_name"])[:32].ljust(32)
                monthly = f"{fmt_dollars(cents_to_dollars(int(sub['monthly_cents'])))}"
                interest_saved = fmt_dollars(cents_to_dollars(int(sub["est_interest_saved_cents"])))
                months_saved = f"{float(sub['est_months_saved']):.1f}"
                cli_lines.append(
                    f"  {vendor} {monthly:>8s}/mo {interest_saved:>18s} {months_saved:>17s}"
                )
        cli_report = "\n".join(line for line in cli_lines if line != "")

        return {
            "data": {
                "subscriptions": subscriptions,
                "essential_count": len(essential_subs),
                "essential_monthly_cents": essential_monthly_cents,
                "discretionary_count": len(discretionary_subs),
                "discretionary_monthly_cents": discretionary_monthly_cents,
                "scenarios": [],
                "baseline": zero_baseline,
            },
            "summary": {
                "total_subscriptions": len(subscriptions),
                "essential_count": len(essential_subs),
                "discretionary_count": len(discretionary_subs),
                "discretionary_monthly_cents": discretionary_monthly_cents,
            },
            "cli_report": cli_report,
        }

    debt_context = compute_dashboard(cards)
    baseline_projection = project_interest(cards, months=MAX_SIM_MONTHS)

    baseline_months_to_payoff: int | None = None
    baseline_fully_paid_off = False
    for month in baseline_projection.get("schedule", []):
        if int(month.get("remaining_balance_cents", 0)) <= 0:
            baseline_months_to_payoff = int(month.get("month", 0))
            baseline_fully_paid_off = True
            break

    baseline_total_interest_cents = int(baseline_projection.get("total_interest_cents", 0))
    baseline = {
        "total_debt_cents": int(debt_context.get("total_balance_cents", 0)),
        "weighted_avg_apr": debt_context.get("weighted_avg_apr"),
        "monthly_minimums_cents": int(debt_context.get("total_min_payment_cents", 0)),
        "monthly_interest_cents": int(debt_context.get("total_monthly_interest_cents", 0)),
        "months_to_payoff": baseline_months_to_payoff,
        "fully_paid_off": baseline_fully_paid_off,
        "total_interest_cents": baseline_total_interest_cents,
        "apr_unknown_count": int(baseline_projection.get("apr_unknown_count", 0)),
        "apr_unknown_balance_cents": int(baseline_projection.get("apr_unknown_balance_cents", 0)),
    }

    def _scenario(name: str, affected: list[dict[str, Any]]) -> dict[str, Any]:
        monthly_savings_cents = sum(int(item["monthly_cents"]) for item in affected)
        subs_affected = [str(item["vendor_name"]) for item in affected]

        if monthly_savings_cents <= 0 or not affected:
            months_to_payoff = baseline_months_to_payoff
            fully_paid_off = baseline_fully_paid_off
            total_interest_cents = baseline_total_interest_cents
            months_shaved = 0
            interest_saved_cents = 0
        else:
            sim = simulate_paydown(cards, extra_cents=monthly_savings_cents, strategy="avalanche")
            fully_paid_off = bool(sim.get("fully_paid_off"))
            months_to_payoff = int(sim.get("months_to_payoff", 0)) if fully_paid_off else None
            total_interest_cents = int(sim.get("total_interest_cents", baseline_total_interest_cents))
            baseline_months_for_math = baseline_months_to_payoff if baseline_months_to_payoff is not None else MAX_SIM_MONTHS
            scenario_months_for_math = months_to_payoff if months_to_payoff is not None else MAX_SIM_MONTHS
            months_shaved = int(baseline_months_for_math) - int(scenario_months_for_math)
            interest_saved_cents = baseline_total_interest_cents - total_interest_cents

        return {
            "name": name,
            "subs_affected": subs_affected,
            "monthly_savings_cents": int(monthly_savings_cents),
            "months_to_payoff": months_to_payoff,
            "fully_paid_off": fully_paid_off,
            "total_interest_cents": int(total_interest_cents),
            "months_shaved": int(months_shaved),
            "interest_saved_cents": int(interest_saved_cents),
        }

    scenarios = [
        _scenario("Cut all discretionary", discretionary_subs),
        _scenario("Cut top 3", discretionary_subs[:3]),
        _scenario(
            "Cut all discretionary over $50/mo",
            [item for item in discretionary_subs if int(item["monthly_cents"]) > 5000],
        ),
    ]

    all_discretionary_interest_saved_cents = int(scenarios[0]["interest_saved_cents"]) if scenarios else 0
    all_discretionary_months_saved = float(scenarios[0]["months_shaved"]) if scenarios else 0.0

    for item in discretionary_subs:
        sub_monthly_cents = int(item["monthly_cents"])
        if discretionary_monthly_cents <= 0:
            item["est_interest_saved_cents"] = 0
            item["est_months_saved"] = 0.0
            continue
        est_interest_saved_cents = int(
            (
                Decimal(sub_monthly_cents)
                / Decimal(discretionary_monthly_cents)
                * Decimal(all_discretionary_interest_saved_cents)
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        est_months_saved = round(
            sub_monthly_cents / discretionary_monthly_cents * all_discretionary_months_saved,
            1,
        )
        item["est_interest_saved_cents"] = est_interest_saved_cents
        item["est_months_saved"] = est_months_saved

    avg_apr = baseline.get("weighted_avg_apr")
    avg_apr_text = f"{float(avg_apr):.2f}%" if avg_apr is not None else "N/A"
    if baseline["fully_paid_off"]:
        min_only_text = f"~{baseline['months_to_payoff']} months"
    else:
        min_only_text = f">{MAX_SIM_MONTHS} months (not paid off in {MAX_SIM_MONTHS}-mo cap)"

    cli_lines = [
        "SUBSCRIPTION AUDIT",
        "==================",
        "",
        f"{len(subscriptions)} active subscriptions - {fmt_dollars(cents_to_dollars(total_monthly_cents))}/mo",
        f"  Essential:     {len(essential_subs)} subs   {fmt_dollars(cents_to_dollars(essential_monthly_cents))}/mo",
        (
            f"  Discretionary: {len(discretionary_subs)} subs   "
            f"{fmt_dollars(cents_to_dollars(discretionary_monthly_cents))}/mo"
        ),
        "",
        "DEBT CONTEXT",
        (
            f"  Total debt: {fmt_dollars(cents_to_dollars(int(baseline['total_debt_cents'])))}"
            f" | Avg APR: {avg_apr_text}"
        ),
        (
            f"  Minimums: {fmt_dollars(cents_to_dollars(int(baseline['monthly_minimums_cents'])))}"
            f"/mo | Interest: {fmt_dollars(cents_to_dollars(int(baseline['monthly_interest_cents'])))}"
            "/mo"
        ),
        (
            f"  Min-only payoff: {min_only_text}, "
            f"{fmt_dollars(cents_to_dollars(int(baseline['total_interest_cents'])))} interest"
        ),
    ]
    if not baseline_fully_paid_off:
        cli_lines.append(
            f"  Note: Balance never reaches zero within {MAX_SIM_MONTHS}-month cap; savings estimates are approximate."
        )
    cli_lines.extend(["", "DISCRETIONARY SUBSCRIPTIONS (by monthly cost)"])

    if discretionary_subs:
        cli_lines.append("                                   Monthly  Est Interest Saved  Est Months Saved")
        for sub in discretionary_subs:
            vendor = str(sub["vendor_name"])[:32].ljust(32)
            monthly = fmt_dollars(cents_to_dollars(int(sub["monthly_cents"])))
            interest_saved = fmt_dollars(cents_to_dollars(int(sub["est_interest_saved_cents"])))
            months_saved = f"{float(sub['est_months_saved']):.1f}"
            cli_lines.append(
                f"  {vendor} {monthly:>8s}/mo {interest_saved:>18s} {months_saved:>17s}"
            )
    else:
        cli_lines.append("  None")

    cli_lines.extend(["", "SCENARIOS"])
    for scenario in scenarios:
        savings_text = fmt_dollars(cents_to_dollars(int(scenario["monthly_savings_cents"])))
        subs_affected = list(scenario["subs_affected"])
        if scenario["name"] == "Cut all discretionary":
            cli_lines.append(
                f"  {scenario['name']} ({len(subs_affected)} subs, {savings_text}/mo freed):"
            )
        elif scenario["name"] == "Cut top 3":
            joined = ", ".join(subs_affected)
            if joined:
                cli_lines.append(f"  {scenario['name']} ({savings_text}/mo freed: {joined}):")
            else:
                cli_lines.append(f"  {scenario['name']} ({savings_text}/mo freed):")
        else:
            cli_lines.append(
                f"  {scenario['name']} ({len(subs_affected)} subs, {savings_text}/mo freed):"
            )

        if scenario["months_to_payoff"] is None:
            payoff_text = f">{MAX_SIM_MONTHS} months"
        else:
            payoff_text = f"{scenario['months_to_payoff']} months"
        cli_lines.append(
            f"    Payoff: {payoff_text} - saves {int(scenario['months_shaved'])} months and "
            f"{fmt_dollars(cents_to_dollars(int(scenario['interest_saved_cents'])))} in interest"
        )
        cli_lines.append("")

    return {
        "data": {
            "subscriptions": subscriptions,
            "essential_count": len(essential_subs),
            "essential_monthly_cents": essential_monthly_cents,
            "discretionary_count": len(discretionary_subs),
            "discretionary_monthly_cents": discretionary_monthly_cents,
            "scenarios": scenarios,
            "baseline": baseline,
        },
        "summary": {
            "total_subscriptions": len(subscriptions),
            "essential_count": len(essential_subs),
            "discretionary_count": len(discretionary_subs),
            "discretionary_monthly_cents": discretionary_monthly_cents,
        },
        "cli_report": "\n".join(cli_lines).rstrip(),
    }
