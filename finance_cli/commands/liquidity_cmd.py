"""Liquidity command."""

from __future__ import annotations

from typing import Any

from ..liquidity import liquidity_snapshot
from ..models import cents_to_dollars
from .common import fmt_dollars


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("liquidity", parents=[format_parent], help="Liquidity snapshot")
    parser.add_argument("--forecast", type=int, default=90)
    parser.add_argument("--include-investments", action="store_true")
    parser.add_argument("--view", choices=["personal", "business", "all"], default="all")
    parser.set_defaults(func=handle_liquidity, command_name="liquidity")


def _build_liquidity_cli_report(out: dict, forecast_days: int) -> str:
    W = 22  # label width
    lines = [f"Liquidity ({forecast_days}-day window)", ""]
    lines.append(f"  {'Liquid Balance:':<{W}s} {fmt_dollars(out['liquid_balance']):>12s}")
    lines.append(f"  {'Credit Owed:':<{W}s} {fmt_dollars(out['credit_owed']):>12s}")
    lines.append("")
    lines.append(f"  {f'Income ({forecast_days}d):':<{W}s} {fmt_dollars(out['income_90d']):>12s}")
    lines.append(f"  {f'Expenses ({forecast_days}d):':<{W}s} {fmt_dollars(-abs(out['expense_90d'])):>12s}")
    lines.append(f"  {f'Net ({forecast_days}d):':<{W}s} {fmt_dollars(out['net_90d']):>12s}")
    lines.append("")
    lines.append(f"  {'Subscription Burn:':<{W}s} {fmt_dollars(-abs(out['subscriptions_monthly_burn'])):>12s}/mo")
    lines.append(f"  {'Projected Net:':<{W}s} {fmt_dollars(out['projected_net']):>12s}")
    return "\n".join(lines)


def handle_liquidity(args, conn) -> dict[str, Any]:
    """Render liquidity snapshot with both cents and human-dollar fields."""
    view = getattr(args, "view", "all")
    snap = liquidity_snapshot(
        conn,
        forecast_days=args.forecast,
        include_investments=args.include_investments,
        view=view,
    )
    out = {
        **snap,
        "income_90d": cents_to_dollars(snap["income_90d_cents"]),
        "expense_90d": cents_to_dollars(snap["expense_90d_cents"]),
        "net_90d": cents_to_dollars(snap["net_90d_cents"]),
        "subscriptions_monthly_burn": cents_to_dollars(snap["subscriptions_monthly_burn_cents"]),
        "recurring_monthly_net": cents_to_dollars(snap["recurring_monthly_net_cents"]),
        "projected_net": cents_to_dollars(snap["projected_net_cents"]),
        "liquid_balance": cents_to_dollars(snap["liquid_balance_cents"]),
        "credit_owed": cents_to_dollars(snap["credit_owed_cents"]),
        "upcoming_liability_payments": cents_to_dollars(snap["upcoming_liability_payments_cents"]),
    }

    return {
        "data": out,
        "summary": {"forecast_days": args.forecast, "projected_net": out["projected_net"]},
        "cli_report": _build_liquidity_cli_report(out, args.forecast),
    }
