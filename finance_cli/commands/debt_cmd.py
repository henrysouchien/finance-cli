"""Debt dashboard, projection, and simulation commands."""

from __future__ import annotations

import argparse
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from ..debt_calculator import (
    MAX_SIM_MONTHS,
    compare_strategies,
    compute_dashboard,
    load_debt_cards,
    project_interest,
    simulate_paydown,
)
from ..models import cents_to_dollars, dollars_to_cents
from ..spending_analysis import category_spending_averages
from .common import fmt_dollars


def _positive_months(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--months must be an integer >= 1") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("--months must be >= 1")
    return parsed


def _valid_cut_pct(value: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--cut-pct must be an integer") from exc
    if parsed < 1 or parsed > 100:
        raise argparse.ArgumentTypeError("--cut-pct must be between 1 and 100")
    return parsed


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("debt", parents=[format_parent], help="Debt dashboard and payoff simulation")
    debt_sub = parser.add_subparsers(dest="debt_command", required=True)

    p_dashboard = debt_sub.add_parser("dashboard", parents=[format_parent], help="Show debt dashboard")
    p_dashboard.add_argument("--include-zero-balance", action="store_true")
    p_dashboard.add_argument("--sort", choices=["balance", "apr", "interest"], default="balance")
    p_dashboard.set_defaults(func=handle_dashboard, command_name="debt.dashboard")

    p_interest = debt_sub.add_parser("interest", parents=[format_parent], help="Project minimum-payment interest")
    p_interest.add_argument("--months", type=_positive_months, default=12)
    p_interest.set_defaults(func=handle_interest, command_name="debt.interest")

    p_simulate = debt_sub.add_parser("simulate", parents=[format_parent], help="Simulate debt payoff strategy")
    p_simulate.add_argument("--extra", required=True, type=float, help="Extra monthly payment in dollars")
    p_simulate.add_argument("--strategy", choices=["avalanche", "snowball", "compare"], default="compare")
    p_simulate.add_argument("--lump-sum", type=float, default=0, help="One-time lump sum payment in dollars")
    p_simulate.add_argument("--lump-month", type=int, default=1, help="Month to apply lump sum (default: 1)")
    p_simulate.set_defaults(func=handle_simulate, command_name="debt.simulate")

    p_impact = debt_sub.add_parser(
        "impact",
        parents=[format_parent],
        help="Model spending cuts -> debt payoff impact",
    )
    p_impact.add_argument(
        "--months",
        type=_positive_months,
        default=3,
        help="N-month lookback for average spending (default: 3)",
    )
    p_impact.add_argument(
        "--cut-pct",
        type=_valid_cut_pct,
        default=50,
        help="Discretionary cut percentage 1-100 (default: 50)",
    )
    p_impact.set_defaults(func=handle_impact, command_name="debt.impact")


def _format_apr(apr: float | None) -> str:
    return f"{apr:.2f}%" if apr is not None else "—"


def _format_utilization(utilization_pct: float | None) -> str:
    return f"{utilization_pct:.2f}%" if utilization_pct is not None else "—"


def _sort_dashboard_rows(rows: list[dict[str, Any]], sort_key: str) -> list[dict[str, Any]]:
    if sort_key == "balance":
        return sorted(rows, key=lambda row: (-int(row["balance_cents"]), str(row["card_id"])))
    if sort_key == "apr":
        return sorted(
            rows,
            key=lambda row: (
                row["apr"] is None,
                -float(row["apr"] or 0),
                -int(row["balance_cents"]),
                str(row["card_id"]),
            ),
        )
    if sort_key == "interest":
        return sorted(
            rows,
            key=lambda row: (
                row["apr"] is None,
                -int(row["monthly_interest_cents"]),
                -int(row["balance_cents"]),
                str(row["card_id"]),
            ),
        )
    raise ValueError("sort must be one of: balance, apr, interest")


def _build_dashboard_report(data: dict[str, Any]) -> str:
    rows = data.get("cards", [])
    if not rows:
        return "No active credit card debt accounts"

    lines = [
        f"{'Card':<28} {'Balance':>12} {'APR':>8} {'Min Payment':>12} {'Monthly Int':>12} {'Utilization':>11}",
        "-" * 91,
    ]
    for row in rows:
        label = str(row["label"])[:28]
        balance = fmt_dollars(cents_to_dollars(int(row["balance_cents"])))
        minimum = fmt_dollars(cents_to_dollars(int(row["min_payment_cents"])))
        interest = fmt_dollars(cents_to_dollars(int(row["monthly_interest_cents"])))
        lines.append(
            f"{label:<28} {balance:>12} {_format_apr(row['apr']):>8} {minimum:>12} {interest:>12} {_format_utilization(row['utilization_pct']):>11}"
        )

    lines.append("-" * 91)
    lines.append(
        f"{'TOTAL':<28} {fmt_dollars(cents_to_dollars(int(data['total_balance_cents']))):>12} {'':>8} "
        f"{fmt_dollars(cents_to_dollars(int(data['total_min_payment_cents']))):>12} "
        f"{fmt_dollars(cents_to_dollars(int(data['total_monthly_interest_cents']))):>12} {'':>11}"
    )
    weighted_avg = data.get("weighted_avg_apr")
    lines.append(f"Weighted Avg APR: {_format_apr(weighted_avg)}")

    apr_unknown_cards = list(data.get("apr_unknown_cards", []))
    if apr_unknown_cards:
        lines.append(f"Unknown APR cards: {', '.join(apr_unknown_cards)}")

    return "\n".join(lines)


def _build_interest_report(data: dict[str, Any]) -> str:
    schedule = data.get("schedule", [])
    if not schedule:
        return "No debt balances to project"

    lines = [
        f"Interest Projection ({data['months']} months, minimum payments only)",
        f"{'Month':>5} {'Interest':>12} {'Cumulative':>12} {'Remaining':>12}",
        "-" * 45,
    ]

    for month_row in schedule:
        lines.append(
            f"{int(month_row['month']):>5} "
            f"{fmt_dollars(cents_to_dollars(int(month_row['interest_cents']))):>12} "
            f"{fmt_dollars(cents_to_dollars(int(month_row['cumulative_interest_cents']))):>12} "
            f"{fmt_dollars(cents_to_dollars(int(month_row['remaining_balance_cents']))):>12}"
        )

    lines.append("-" * 45)
    lines.append(f"Total Interest: {fmt_dollars(cents_to_dollars(int(data['total_interest_cents'])))}")

    apr_unknown_count = int(data.get("apr_unknown_count", 0))
    if apr_unknown_count > 0:
        lines.append(
            "Warning: Some cards have unknown APR and accrue 0% in this projection "
            "(lower-bound estimate)."
        )

    return "\n".join(lines)


def _build_single_simulation_report(
    strategy: str,
    extra_cents: int,
    result: dict[str, Any],
    lump_sum_cents: int = 0,
    lump_sum_month: int = 1,
) -> str:
    if not result.get("schedule"):
        return "No debt balances to simulate"

    lines = [
        f"{strategy.capitalize()} simulation (extra {fmt_dollars(cents_to_dollars(extra_cents))}/mo)",
    ]
    if int(lump_sum_cents) > 0:
        lines.append(
            f"Lump Sum: {fmt_dollars(cents_to_dollars(lump_sum_cents))} at month {lump_sum_month}"
        )
    lines.extend([
        f"Months to Payoff: {int(result['months_to_payoff'])}",
        f"Total Interest: {fmt_dollars(cents_to_dollars(int(result['total_interest_cents'])))}",
        f"Total Paid: {fmt_dollars(cents_to_dollars(int(result['total_paid_cents'])))}",
        f"Fully Paid Off: {'yes' if bool(result['fully_paid_off']) else 'no'}",
    ])

    if not bool(result.get("fully_paid_off")):
        capped_cards = list(result.get("capped_cards", []))
        if capped_cards:
            lines.append(f"Cap Reached (360 months), remaining cards: {', '.join(capped_cards)}")

    assumptions = list(result.get("assumptions", []))
    if assumptions:
        lines.append(f"Assumptions: {', '.join(assumptions)}")

    return "\n".join(lines)


def _build_compare_report(
    extra_cents: int,
    data: dict[str, Any],
    lump_sum_cents: int = 0,
    lump_sum_month: int = 1,
) -> str:
    avalanche = data["avalanche"]
    snowball = data["snowball"]
    baseline = data["baseline"]
    savings = data["interest_savings_vs_baseline_cents"]

    lines = [
        f"Strategy comparison (extra {fmt_dollars(cents_to_dollars(extra_cents))}/mo)",
    ]
    if int(lump_sum_cents) > 0:
        lines.append(
            f"Lump Sum: {fmt_dollars(cents_to_dollars(lump_sum_cents))} at month {lump_sum_month}"
        )
    lines.extend([
        "",
        f"{'Strategy':<12} {'Months':>7} {'Interest':>12} {'Total Paid':>12} {'Paid Off':>8}",
        "-" * 60,
        f"{'Avalanche':<12} {int(avalanche['months_to_payoff']):>7} "
        f"{fmt_dollars(cents_to_dollars(int(avalanche['total_interest_cents']))):>12} "
        f"{fmt_dollars(cents_to_dollars(int(avalanche['total_paid_cents']))):>12} "
        f"{('yes' if avalanche['fully_paid_off'] else 'no'):>8}",
        f"{'Snowball':<12} {int(snowball['months_to_payoff']):>7} "
        f"{fmt_dollars(cents_to_dollars(int(snowball['total_interest_cents']))):>12} "
        f"{fmt_dollars(cents_to_dollars(int(snowball['total_paid_cents']))):>12} "
        f"{('yes' if snowball['fully_paid_off'] else 'no'):>8}",
        f"{'Min-Only':<12} {int(baseline['months']):>7} "
        f"{fmt_dollars(cents_to_dollars(int(baseline['total_interest_cents']))):>12} "
        f"{fmt_dollars(cents_to_dollars(int(baseline['total_paid_cents']))):>12} "
        f"{'n/a':>8}",
        "-" * 60,
        f"Avalanche Savings vs Baseline: {fmt_dollars(cents_to_dollars(int(savings['avalanche'])))}",
        f"Snowball Savings vs Baseline: {fmt_dollars(cents_to_dollars(int(savings['snowball'])))}",
    ])

    all_assumptions = sorted({*avalanche.get("assumptions", []), *snowball.get("assumptions", [])})
    if all_assumptions:
        lines.append(f"Assumptions: {', '.join(all_assumptions)}")

    return "\n".join(lines)


def handle_dashboard(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if args.sort not in {"balance", "apr", "interest"}:
        raise ValueError("sort must be one of: balance, apr, interest")

    cards = load_debt_cards(conn, include_zero_balance=bool(args.include_zero_balance))
    data = compute_dashboard(cards)
    data["cards"] = _sort_dashboard_rows(list(data.get("cards", [])), args.sort)

    return {
        "data": data,
        "summary": {
            "total_cards": len(data.get("cards", [])),
            "apr_unknown_count": len(data.get("apr_unknown_cards", [])),
        },
        "cli_report": _build_dashboard_report(data),
    }


def handle_interest(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if int(args.months) < 1:
        raise ValueError("months must be >= 1")

    cards = load_debt_cards(conn, include_zero_balance=False)
    data = project_interest(
        cards,
        months=int(args.months),
        summary_only=bool(getattr(args, "summary_only", False)),
    )

    return {
        "data": data,
        "summary": {
            "total_cards": len(cards),
            "months": int(args.months),
            "apr_unknown_count": int(data.get("apr_unknown_count", 0)),
        },
        "cli_report": _build_interest_report(data),
    }


def handle_simulate(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if float(args.extra) < 0:
        raise ValueError("extra must be >= 0")
    if args.strategy not in {"avalanche", "snowball", "compare"}:
        raise ValueError("strategy must be one of: avalanche, snowball, compare")

    cards = load_debt_cards(conn, include_zero_balance=False)
    extra_cents = dollars_to_cents(args.extra)
    summary_only = bool(getattr(args, "summary_only", False))

    lump_sum = float(getattr(args, "lump_sum", 0) or 0)
    lump_sum_cents = dollars_to_cents(lump_sum) if lump_sum > 0 else 0
    lump_sum_month = int(getattr(args, "lump_month", 1) or 1)

    if args.strategy == "compare":
        data = compare_strategies(
            cards, extra_cents=extra_cents, summary_only=summary_only,
            lump_sum_cents=lump_sum_cents, lump_sum_month=lump_sum_month,
        )
        cli_report = _build_compare_report(extra_cents, data, lump_sum_cents=lump_sum_cents, lump_sum_month=lump_sum_month)
        summary = {
            "total_cards": len(cards),
            "strategy": "compare",
            "baseline_months": int(data.get("baseline", {}).get("months", 0)),
        }
    else:
        data = simulate_paydown(
            cards,
            extra_cents=extra_cents,
            strategy=args.strategy,
            summary_only=summary_only,
            lump_sum_cents=lump_sum_cents,
            lump_sum_month=lump_sum_month,
        )
        cli_report = _build_single_simulation_report(
            args.strategy, extra_cents, data,
            lump_sum_cents=lump_sum_cents, lump_sum_month=lump_sum_month,
        )
        summary = {
            "total_cards": len(cards),
            "strategy": args.strategy,
            "months_to_payoff": int(data.get("months_to_payoff", 0)),
            "fully_paid_off": bool(data.get("fully_paid_off", False)),
        }

    if lump_sum_cents > 0:
        summary["lump_sum_cents"] = lump_sum_cents
        summary["lump_sum_month"] = lump_sum_month

    return {
        "data": data,
        "summary": summary,
        "cli_report": cli_report,
    }


def handle_impact(args, conn: sqlite3.Connection) -> dict[str, Any]:
    months = int(args.months)
    cut_pct = int(args.cut_pct)
    if months < 1:
        raise ValueError("months must be >= 1")
    if cut_pct < 1 or cut_pct > 100:
        raise ValueError("cut_pct must be between 1 and 100")

    category_rows = category_spending_averages(conn, months=months)

    categories: list[dict[str, Any]] = []
    for row in category_rows:
        if row.classification == "discretionary":
            cut_monthly_cents = int(
                (
                    Decimal(row.avg_monthly_cents)
                    * Decimal(cut_pct)
                    / Decimal("100")
                ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
        else:
            cut_monthly_cents = 0
        categories.append(
            {
                "category_name": row.category_name,
                "parent_name": row.parent_name,
                "avg_monthly_cents": int(row.avg_monthly_cents),
                "total_cents": int(row.total_cents),
                "months_with_data": int(row.months_with_data),
                "classification": row.classification,
                "cut_monthly_cents": cut_monthly_cents,
                "est_interest_saved_cents": 0,
                "est_months_saved": 0.0,
            }
        )

    essential_categories = [item for item in categories if item["classification"] == "essential"]
    discretionary_categories = [item for item in categories if item["classification"] == "discretionary"]
    excluded_categories = [item for item in categories if item["classification"] == "excluded"]

    essential_monthly_cents = sum(int(item["avg_monthly_cents"]) for item in essential_categories)
    discretionary_monthly_cents = sum(int(item["avg_monthly_cents"]) for item in discretionary_categories)
    excluded_monthly_cents = sum(int(item["avg_monthly_cents"]) for item in excluded_categories)

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
            "SPENDING IMPACT ANALYSIS",
            "========================",
            "",
            f"{months}-month average spending:",
            (
                f"  Essential:     {fmt_dollars(cents_to_dollars(essential_monthly_cents))}/mo "
                f"({len(essential_categories)} categories)"
            ),
            (
                f"  Discretionary: {fmt_dollars(cents_to_dollars(discretionary_monthly_cents))}/mo "
                f"({len(discretionary_categories)} categories)"
            ),
            (
                f"  Excluded:      {fmt_dollars(cents_to_dollars(excluded_monthly_cents))}/mo "
                f"({len(excluded_categories)} categories)"
            ),
            "",
            "DEBT CONTEXT",
            "  No active credit card debt balances.",
            "",
            f"DISCRETIONARY CATEGORIES ({months}-mo avg, by monthly spend)",
            "  None" if not discretionary_categories else "",
            "",
            "SCENARIOS",
            "  No debt cards with balance > 0; scenarios unavailable.",
        ]
        if discretionary_categories:
            cli_lines[12] = (
                "                              Avg/Mo   "
                f"{cut_pct}% Cut  Est Interest Saved  Est Months Saved"
            )
            for category in discretionary_categories:
                category_name = str(category["category_name"])[:28].ljust(28)
                avg_monthly = fmt_dollars(cents_to_dollars(int(category["avg_monthly_cents"])))
                cut_monthly = fmt_dollars(cents_to_dollars(int(category["cut_monthly_cents"])))
                interest_saved = fmt_dollars(cents_to_dollars(int(category["est_interest_saved_cents"])))
                months_saved = f"{float(category['est_months_saved']):.1f}"
                cli_lines.append(
                    f"  {category_name} {avg_monthly:>8s}/mo {cut_monthly:>8s}/mo "
                    f"{interest_saved:>18s} {months_saved:>17s}"
                )
        cli_report = "\n".join(line for line in cli_lines if line != "")

        return {
            "data": {
                "categories": categories,
                "essential_count": len(essential_categories),
                "essential_monthly_cents": essential_monthly_cents,
                "discretionary_count": len(discretionary_categories),
                "discretionary_monthly_cents": discretionary_monthly_cents,
                "excluded_count": len(excluded_categories),
                "excluded_monthly_cents": excluded_monthly_cents,
                "scenarios": [],
                "baseline": zero_baseline,
            },
            "summary": {
                "total_categories": len(categories),
                "essential_count": len(essential_categories),
                "discretionary_count": len(discretionary_categories),
                "discretionary_monthly_cents": discretionary_monthly_cents,
            },
            "cli_report": cli_report,
        }

    debt_context = compute_dashboard(cards)
    baseline_projection = project_interest(cards, months=MAX_SIM_MONTHS)

    baseline_months_to_payoff: int | None = None
    baseline_fully_paid_off = False
    for month_row in baseline_projection.get("schedule", []):
        if int(month_row.get("remaining_balance_cents", 0)) <= 0:
            baseline_months_to_payoff = int(month_row.get("month", 0))
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

    def _scenario(name: str, affected: list[dict[str, Any]], pct: int) -> dict[str, Any]:
        affected_total_cents = sum(int(item["avg_monthly_cents"]) for item in affected)
        monthly_savings_cents = int(
            (
                Decimal(affected_total_cents)
                * Decimal(pct)
                / Decimal("100")
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        categories_affected = [str(item["category_name"]) for item in affected]

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
            baseline_months_for_math = (
                baseline_months_to_payoff if baseline_months_to_payoff is not None else MAX_SIM_MONTHS
            )
            scenario_months_for_math = months_to_payoff if months_to_payoff is not None else MAX_SIM_MONTHS
            months_shaved = int(baseline_months_for_math) - int(scenario_months_for_math)
            interest_saved_cents = baseline_total_interest_cents - total_interest_cents

        return {
            "name": name,
            "cut_pct": int(pct),
            "categories_affected": categories_affected,
            "monthly_savings_cents": int(monthly_savings_cents),
            "months_to_payoff": months_to_payoff,
            "fully_paid_off": fully_paid_off,
            "total_interest_cents": int(total_interest_cents),
            "months_shaved": int(months_shaved),
            "interest_saved_cents": int(interest_saved_cents),
        }

    scenarios = [
        _scenario(f"Cut all discretionary by {cut_pct}%", discretionary_categories, cut_pct),
        _scenario(f"Cut top 3 by {cut_pct}%", discretionary_categories[:3], cut_pct),
    ]

    conservative_pct = min(25, cut_pct // 2)
    conservative_savings_cents = int(
        (
            Decimal(discretionary_monthly_cents)
            * Decimal(conservative_pct)
            / Decimal("100")
        ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    if conservative_pct < cut_pct and conservative_savings_cents > 0:
        scenarios.append(
            _scenario(
                f"Cut all discretionary by {conservative_pct}%",
                discretionary_categories,
                conservative_pct,
            )
        )

    all_discretionary_interest_saved_cents = int(scenarios[0]["interest_saved_cents"]) if scenarios else 0
    all_discretionary_months_saved = float(scenarios[0]["months_shaved"]) if scenarios else 0.0

    for category in discretionary_categories:
        category_monthly_cents = int(category["avg_monthly_cents"])
        if discretionary_monthly_cents <= 0:
            category["est_interest_saved_cents"] = 0
            category["est_months_saved"] = 0.0
            continue
        est_interest_saved_cents = int(
            (
                Decimal(category_monthly_cents)
                / Decimal(discretionary_monthly_cents)
                * Decimal(all_discretionary_interest_saved_cents)
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        est_months_saved = round(
            category_monthly_cents / discretionary_monthly_cents * all_discretionary_months_saved,
            1,
        )
        category["est_interest_saved_cents"] = est_interest_saved_cents
        category["est_months_saved"] = est_months_saved

    avg_apr = baseline.get("weighted_avg_apr")
    avg_apr_text = f"{float(avg_apr):.2f}%" if avg_apr is not None else "N/A"
    if baseline["fully_paid_off"]:
        min_only_text = f"~{baseline['months_to_payoff']} months"
    else:
        min_only_text = f">{MAX_SIM_MONTHS} months (not paid off in {MAX_SIM_MONTHS}-mo cap)"

    cli_lines = [
        "SPENDING IMPACT ANALYSIS",
        "========================",
        "",
        f"{months}-month average spending:",
        (
            f"  Essential:     {fmt_dollars(cents_to_dollars(essential_monthly_cents))}/mo "
            f"({len(essential_categories)} categories)"
        ),
        (
            f"  Discretionary: {fmt_dollars(cents_to_dollars(discretionary_monthly_cents))}/mo "
            f"({len(discretionary_categories)} categories)"
        ),
        (
            f"  Excluded:      {fmt_dollars(cents_to_dollars(excluded_monthly_cents))}/mo "
            f"({len(excluded_categories)} categories)"
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
    cli_lines.extend(["", f"DISCRETIONARY CATEGORIES ({months}-mo avg, by monthly spend)"])

    if discretionary_categories:
        cli_lines.append(
            "                              Avg/Mo   "
            f"{cut_pct}% Cut  Est Interest Saved  Est Months Saved"
        )
        for category in discretionary_categories:
            category_name = str(category["category_name"])[:28].ljust(28)
            avg_monthly = fmt_dollars(cents_to_dollars(int(category["avg_monthly_cents"])))
            cut_monthly = fmt_dollars(cents_to_dollars(int(category["cut_monthly_cents"])))
            interest_saved = fmt_dollars(cents_to_dollars(int(category["est_interest_saved_cents"])))
            months_saved = f"{float(category['est_months_saved']):.1f}"
            cli_lines.append(
                f"  {category_name} {avg_monthly:>8s}/mo {cut_monthly:>8s}/mo "
                f"{interest_saved:>18s} {months_saved:>17s}"
            )
    else:
        cli_lines.append("  None")

    cli_lines.extend(["", "SCENARIOS"])
    for scenario in scenarios:
        savings_text = fmt_dollars(cents_to_dollars(int(scenario["monthly_savings_cents"])))
        categories_affected = list(scenario["categories_affected"])
        if scenario["name"].startswith("Cut top 3"):
            joined = ", ".join(categories_affected)
            if joined:
                cli_lines.append(f"  {scenario['name']} ({savings_text}/mo freed: {joined}):")
            else:
                cli_lines.append(f"  {scenario['name']} ({savings_text}/mo freed):")
        else:
            cli_lines.append(
                f"  {scenario['name']} ({len(categories_affected)} categories, {savings_text}/mo freed):"
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
            "categories": categories,
            "essential_count": len(essential_categories),
            "essential_monthly_cents": essential_monthly_cents,
            "discretionary_count": len(discretionary_categories),
            "discretionary_monthly_cents": discretionary_monthly_cents,
            "excluded_count": len(excluded_categories),
            "excluded_monthly_cents": excluded_monthly_cents,
            "scenarios": scenarios,
            "baseline": baseline,
        },
        "summary": {
            "total_categories": len(categories),
            "essential_count": len(essential_categories),
            "discretionary_count": len(discretionary_categories),
            "discretionary_monthly_cents": discretionary_monthly_cents,
        },
        "cli_report": "\n".join(cli_lines).rstrip(),
    }
