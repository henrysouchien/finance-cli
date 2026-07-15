"""Debt dashboard, projection, and simulation commands."""

from __future__ import annotations

import argparse
import sqlite3
import uuid
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from ..analytics import log_event
from ..db import _connected_main_db_path
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


def _valid_apr_pct(value: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("--apr must be a percentage from 0 to 100") from exc
    if parsed < 0 or parsed > 100:
        raise argparse.ArgumentTypeError("--apr must be between 0 and 100")
    return parsed


def _valid_portion_type(value: str) -> str:
    normalized = str(value or "").strip()
    allowed = {
        "purchase",
        "installment",
        "balance_transfer",
        "cash_advance",
        "promotional",
        "fee",
        "other",
    }
    if normalized not in allowed:
        raise argparse.ArgumentTypeError(
            "--portion-type must be one of: " + ", ".join(sorted(allowed))
        )
    return normalized


def _positive_dollars_to_cents(value: Any, *, field_name: str) -> int:
    cents = dollars_to_cents(value)
    if cents <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return cents


def _nonnegative_dollars_to_cents(value: Any, *, field_name: str) -> int:
    cents = dollars_to_cents(value)
    if cents < 0:
        raise ValueError(f"{field_name} must be zero or greater")
    return cents


def _normalize_optional_date(value: Any, *, field_name: str) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty")
    try:
        return date.fromisoformat(normalized).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date: YYYY-MM-DD") from exc


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

    p_set_apr = debt_sub.add_parser(
        "set-apr",
        parents=[format_parent],
        help="Manually set purchase APR for a credit-card account",
    )
    p_set_apr.add_argument("--account", required=True, help="Credit-card account ID")
    p_set_apr.add_argument("--apr", required=True, type=_valid_apr_pct, help="Purchase APR percentage")
    p_set_apr.add_argument("--dry-run", action="store_true")
    p_set_apr.set_defaults(func=handle_set_apr, command_name="debt.set_apr")

    p_portion = debt_sub.add_parser(
        "portion",
        parents=[format_parent],
        help="Manage credit-card balance portions with separate APR terms",
    )
    portion_sub = p_portion.add_subparsers(dest="portion_command", required=True)

    p_portion_add = portion_sub.add_parser("add", parents=[format_parent], help="Add a debt balance portion")
    p_portion_add.add_argument("--account", required=True, help="Parent credit-card account ID")
    p_portion_add.add_argument("--label", required=True, help="Portion label, e.g. 'Amex Plan It'")
    p_portion_add.add_argument("--principal", required=True, type=float, help="Portion principal in dollars")
    p_portion_add.add_argument("--apr", required=True, type=_valid_apr_pct, help="Portion APR percentage")
    p_portion_add.add_argument("--monthly-payment", type=float, help="Optional portion monthly payment in dollars")
    p_portion_add.add_argument("--portion-type", type=_valid_portion_type, default="installment")
    p_portion_add.add_argument("--promo-end-date", help="Optional promotional/end date, YYYY-MM-DD")
    p_portion_add.add_argument(
        "--expected-payoff-date",
        dest="expected_payoff_date",
        help="Alias for --promo-end-date, YYYY-MM-DD",
    )
    p_portion_add.add_argument("--notes")
    p_portion_add.add_argument("--dry-run", action="store_true")
    p_portion_add.set_defaults(func=handle_portion_add, command_name="debt.portion.add")

    p_portion_list = portion_sub.add_parser("list", parents=[format_parent], help="List debt balance portions")
    p_portion_list.add_argument("--account", help="Filter by parent credit-card account ID")
    p_portion_list.add_argument("--include-inactive", action="store_true")
    p_portion_list.set_defaults(func=handle_portion_list, command_name="debt.portion.list")

    p_portion_update = portion_sub.add_parser("update", parents=[format_parent], help="Update a debt balance portion")
    p_portion_update.add_argument("portion_id")
    p_portion_update.add_argument("--label")
    p_portion_update.add_argument("--principal", type=float)
    p_portion_update.add_argument("--apr", type=_valid_apr_pct)
    p_portion_update.add_argument("--monthly-payment", type=float)
    p_portion_update.add_argument("--clear-monthly-payment", action="store_true")
    p_portion_update.add_argument("--portion-type", type=_valid_portion_type)
    p_portion_update.add_argument("--promo-end-date")
    p_portion_update.add_argument(
        "--expected-payoff-date",
        dest="expected_payoff_date",
        help="Alias for --promo-end-date, YYYY-MM-DD",
    )
    p_portion_update.add_argument("--clear-promo-end-date", action="store_true")
    p_portion_update.add_argument("--notes")
    p_portion_update.add_argument("--clear-notes", action="store_true")
    p_portion_update.add_argument("--dry-run", action="store_true")
    p_portion_update.set_defaults(func=handle_portion_update, command_name="debt.portion.update")

    p_portion_deactivate = portion_sub.add_parser(
        "deactivate",
        parents=[format_parent],
        help="Deactivate a debt balance portion",
    )
    p_portion_deactivate.add_argument("portion_id")
    p_portion_deactivate.add_argument("--dry-run", action="store_true")
    p_portion_deactivate.set_defaults(
        func=handle_portion_deactivate,
        command_name="debt.portion.deactivate",
    )


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
        return "No active debt accounts"

    lines = [
        f"{'Account':<28} {'Balance':>12} {'APR':>8} {'Min Payment':>12} {'Monthly Int':>12} {'Utilization':>11}",
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
        lines.append(f"Unknown APR accounts: {', '.join(apr_unknown_cards)}")

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
            "Warning: Some accounts have unknown APR and accrue 0% in this projection "
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
            lines.append(f"Cap Reached (360 months), remaining accounts: {', '.join(capped_cards)}")

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


def _card_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    ending = str(row["card_ending"] or "").strip()
    account = str(row["account_name"] or "").strip()
    if institution and ending:
        return f"{institution} {ending}"
    if institution and account:
        if account.lower().startswith(institution.lower()):
            return account
        return f"{institution} {account}"
    if institution:
        return institution
    if account:
        return account
    return str(row["id"])


def _load_credit_card_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, institution_name, account_name, card_ending, account_type, is_active
          FROM accounts
         WHERE id = ?
        """,
        (account_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"account not found: {account_id}")
    if str(row["account_type"] or "") != "credit_card":
        raise ValueError("debt set-apr requires a credit_card account")
    if not bool(row["is_active"]):
        raise ValueError("debt set-apr requires an active account")
    return row


def _load_portion_parent_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row:
    account = _load_credit_card_account(conn, account_id)
    alias_row = conn.execute(
        "SELECT 1 FROM account_aliases WHERE hash_account_id = ?",
        (account_id,),
    ).fetchone()
    if alias_row is not None:
        raise ValueError("debt portions require the canonical credit-card account, not an alias account")
    return account


def _load_debt_balance_portion(conn: sqlite3.Connection, portion_id: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT p.id,
               p.account_id,
               p.label,
               p.portion_type,
               p.principal_cents,
               p.apr_pct,
               p.monthly_payment_cents,
               p.start_date,
               p.promo_end_date,
               p.source,
               p.is_active,
               p.notes,
               p.created_at,
               p.updated_at,
               a.institution_name,
               a.account_name,
               a.card_ending,
               a.account_type,
               a.is_active AS account_is_active
          FROM debt_balance_portions p
          LEFT JOIN accounts a
            ON a.id = p.account_id
         WHERE p.id = ?
        """,
        (portion_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"debt balance portion not found: {portion_id}")
    return row


def _portion_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    if row["account_type"] is not None:
        institution = str(row["institution_name"] or "").strip()
        ending = str(row["card_ending"] or "").strip()
        account_name = str(row["account_name"] or "").strip()
        if institution and ending:
            account_label = f"{institution} {ending}"
        elif institution and account_name:
            account_label = (
                account_name
                if account_name.lower().startswith(institution.lower())
                else f"{institution} {account_name}"
            )
        else:
            account_label = institution or account_name or str(row["account_id"])
    else:
        account_label = str(row["account_id"])
    return {
        "id": str(row["id"]),
        "account_id": str(row["account_id"]),
        "account_label": account_label,
        "label": str(row["label"]),
        "portion_type": str(row["portion_type"]),
        "principal_cents": int(row["principal_cents"] or 0),
        "apr_pct": None if row["apr_pct"] is None else float(row["apr_pct"]),
        "monthly_payment_cents": (
            None
            if row["monthly_payment_cents"] is None
            else int(row["monthly_payment_cents"])
        ),
        "start_date": row["start_date"],
        "promo_end_date": row["promo_end_date"],
        "source": str(row["source"]),
        "is_active": bool(row["is_active"]),
        "notes": row["notes"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _format_cents(cents: int | None) -> str:
    if cents is None:
        return "—"
    return fmt_dollars(cents_to_dollars(int(cents)))


def _build_portion_add_report(data: dict[str, Any]) -> str:
    action = "Would add" if data.get("dry_run") else "Added"
    payment = data.get("monthly_payment_cents")
    payment_text = f", payment {_format_cents(payment)}/mo" if payment is not None else ""
    return (
        f"{action} debt balance portion {data['label']} on {data['account_label']}: "
        f"{_format_cents(int(data['principal_cents']))} at {_format_apr(data['apr_pct'])}"
        f"{payment_text}."
    )


def _build_portion_list_report(portions: list[dict[str, Any]]) -> str:
    if not portions:
        return "No debt balance portions found"

    lines = [
        f"{'ID':<12} {'Account':<24} {'Label':<24} {'Principal':>12} {'APR':>8} {'Payment':>12} {'Status':>8}",
        "-" * 108,
    ]
    for portion in portions:
        lines.append(
            f"{portion['id'][:12]:<12} "
            f"{str(portion['account_label'])[:24]:<24} "
            f"{str(portion['label'])[:24]:<24} "
            f"{_format_cents(int(portion['principal_cents'])):>12} "
            f"{_format_apr(portion['apr_pct']):>8} "
            f"{_format_cents(portion['monthly_payment_cents']):>12} "
            f"{('active' if portion['is_active'] else 'inactive'):>8}"
        )
    return "\n".join(lines)


def _build_portion_update_report(data: dict[str, Any]) -> str:
    if data.get("no_changes"):
        return f"No changes needed for debt balance portion {data['id']}"
    action = "Would update" if data.get("dry_run") else "Updated"
    fields = ", ".join(sorted(data.get("changes", {})))
    return f"{action} debt balance portion {data['id']} ({fields})."


def _build_portion_deactivate_report(data: dict[str, Any]) -> str:
    if data.get("no_changes"):
        return f"Debt balance portion {data['id']} is already inactive"
    action = "Would deactivate" if data.get("dry_run") else "Deactivated"
    return f"{action} debt balance portion {data['id']}."


def _build_set_apr_report(data: dict[str, Any]) -> str:
    prior = data.get("previous_apr")
    prior_text = _format_apr(float(prior)) if prior is not None else "unknown"
    action = "Would set" if data.get("dry_run") else "Set"
    suffix = " (new liability row)" if data.get("created") else ""
    return (
        f"{action} purchase APR for {data['account_label']} to "
        f"{_format_apr(float(data['apr']))}; previous APR: {prior_text}{suffix}."
    )


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
    log_event(_connected_main_db_path(conn), "feature.debt_simulated")

    return {
        "data": data,
        "summary": summary,
        "cli_report": cli_report,
    }


def handle_set_apr(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(args.account).strip()
    if not account_id:
        raise ValueError("account is required")
    apr = _valid_apr_pct(str(args.apr))
    dry_run = bool(getattr(args, "dry_run", False))

    account = _load_credit_card_account(conn, account_id)
    existing = conn.execute(
        """
        SELECT id, apr_purchase
          FROM liabilities
         WHERE account_id = ?
           AND liability_type = 'credit'
        """,
        (account_id,),
    ).fetchone()
    liability_id = str(existing["id"]) if existing else uuid.uuid4().hex
    previous_apr = None if existing is None else existing["apr_purchase"]
    created = existing is None

    if not dry_run:
        conn.execute(
            """
            INSERT INTO liabilities (
                id, account_id, liability_type, is_active, apr_purchase, updated_at
            ) VALUES (?, ?, 'credit', 1, ?, datetime('now'))
            ON CONFLICT(account_id, liability_type) DO UPDATE SET
                is_active = 1,
                apr_purchase = excluded.apr_purchase,
                updated_at = datetime('now')
            """,
            (liability_id, account_id, apr),
        )
        conn.commit()

    data = {
        "account_id": account_id,
        "account_label": _card_label(account),
        "liability_id": liability_id,
        "apr": apr,
        "previous_apr": previous_apr,
        "created": created,
        "dry_run": dry_run,
    }
    return {
        "data": data,
        "summary": {
            "account_id": account_id,
            "apr": apr,
            "dry_run": dry_run,
        },
        "cli_report": _build_set_apr_report(data),
    }


def handle_portion_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = str(args.account).strip()
    if not account_id:
        raise ValueError("account is required")
    label = str(args.label or "").strip()
    if not label:
        raise ValueError("label is required")

    account = _load_portion_parent_account(conn, account_id)
    principal_cents = _positive_dollars_to_cents(args.principal, field_name="principal")
    apr = _valid_apr_pct(str(args.apr))
    monthly_payment = getattr(args, "monthly_payment", None)
    monthly_payment_cents = (
        None
        if monthly_payment is None
        else _nonnegative_dollars_to_cents(monthly_payment, field_name="monthly_payment")
    )
    portion_type = _valid_portion_type(str(getattr(args, "portion_type", "installment")))
    promo_end_date = getattr(args, "promo_end_date", None)
    expected_payoff_date = getattr(args, "expected_payoff_date", None)
    if promo_end_date and expected_payoff_date:
        raise ValueError("use --promo-end-date or --expected-payoff-date, not both")
    normalized_promo_end_date = _normalize_optional_date(
        promo_end_date or expected_payoff_date,
        field_name="promo_end_date",
    )
    notes = getattr(args, "notes", None)
    normalized_notes = None if notes is None else str(notes).strip() or None
    dry_run = bool(getattr(args, "dry_run", False))
    portion_id = uuid.uuid4().hex

    if not dry_run:
        conn.execute(
            """
            INSERT INTO debt_balance_portions (
                id, account_id, label, portion_type, principal_cents, apr_pct,
                monthly_payment_cents, promo_end_date, source, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?)
            """,
            (
                portion_id,
                account_id,
                label,
                portion_type,
                principal_cents,
                apr,
                monthly_payment_cents,
                normalized_promo_end_date,
                normalized_notes,
            ),
        )
        conn.commit()

    data = {
        "id": portion_id,
        "account_id": account_id,
        "account_label": _card_label(account),
        "label": label,
        "portion_type": portion_type,
        "principal_cents": principal_cents,
        "apr_pct": apr,
        "monthly_payment_cents": monthly_payment_cents,
        "promo_end_date": normalized_promo_end_date,
        "source": "manual",
        "notes": normalized_notes,
        "dry_run": dry_run,
    }
    return {
        "data": data,
        "summary": {
            "portion_id": portion_id,
            "account_id": account_id,
            "principal_cents": principal_cents,
            "apr_pct": apr,
            "dry_run": dry_run,
        },
        "cli_report": _build_portion_add_report(data),
    }


def handle_portion_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    account_id = getattr(args, "account", None)
    include_inactive = bool(getattr(args, "include_inactive", False))
    where = []
    params: list[Any] = []

    if account_id is not None:
        account_id = str(account_id).strip()
        if not account_id:
            raise ValueError("account cannot be empty")
        _load_portion_parent_account(conn, account_id)
        where.append("p.account_id = ?")
        params.append(account_id)

    if not include_inactive:
        where.append("p.is_active = 1")

    where_clause = "WHERE " + " AND ".join(where) if where else ""
    rows = conn.execute(
        f"""
        SELECT p.id,
               p.account_id,
               p.label,
               p.portion_type,
               p.principal_cents,
               p.apr_pct,
               p.monthly_payment_cents,
               p.start_date,
               p.promo_end_date,
               p.source,
               p.is_active,
               p.notes,
               p.created_at,
               p.updated_at,
               a.institution_name,
               a.account_name,
               a.card_ending,
               a.account_type,
               a.is_active AS account_is_active
          FROM debt_balance_portions p
          LEFT JOIN accounts a
            ON a.id = p.account_id
        {where_clause}
         ORDER BY p.is_active DESC, a.institution_name, a.account_name, p.created_at, p.id
        """,
        tuple(params),
    ).fetchall()

    portions = [_portion_row_to_dict(row) for row in rows]
    return {
        "data": {"portions": portions, "total_count": len(portions)},
        "summary": {
            "total_count": len(portions),
            "active_count": sum(1 for portion in portions if portion["is_active"]),
            "include_inactive": include_inactive,
        },
        "cli_report": _build_portion_list_report(portions),
    }


def handle_portion_update(args, conn: sqlite3.Connection) -> dict[str, Any]:
    portion_id = str(args.portion_id).strip()
    if not portion_id:
        raise ValueError("portion_id is required")
    existing = _load_debt_balance_portion(conn, portion_id)
    dry_run = bool(getattr(args, "dry_run", False))

    if getattr(args, "monthly_payment", None) is not None and bool(
        getattr(args, "clear_monthly_payment", False)
    ):
        raise ValueError("use --monthly-payment or --clear-monthly-payment, not both")
    promo_end_date = getattr(args, "promo_end_date", None)
    expected_payoff_date = getattr(args, "expected_payoff_date", None)
    if promo_end_date and expected_payoff_date:
        raise ValueError("use --promo-end-date or --expected-payoff-date, not both")
    if (promo_end_date is not None or expected_payoff_date is not None) and bool(
        getattr(args, "clear_promo_end_date", False)
    ):
        raise ValueError("use --promo-end-date or --clear-promo-end-date, not both")
    if getattr(args, "notes", None) is not None and bool(getattr(args, "clear_notes", False)):
        raise ValueError("use --notes or --clear-notes, not both")

    updates: list[tuple[str, Any]] = []
    changes: dict[str, dict[str, Any]] = {}
    field_requested = False

    def add_change(column: str, new_value: Any, *, public_name: str | None = None) -> None:
        old_value = existing[column]
        if old_value == new_value:
            return
        updates.append((column, new_value))
        changes[public_name or column] = {"old": old_value, "new": new_value}

    if getattr(args, "label", None) is not None:
        field_requested = True
        label = str(args.label).strip()
        if not label:
            raise ValueError("label cannot be empty")
        add_change("label", label)

    if getattr(args, "principal", None) is not None:
        field_requested = True
        principal_cents = _positive_dollars_to_cents(args.principal, field_name="principal")
        add_change("principal_cents", principal_cents, public_name="principal")

    if getattr(args, "apr", None) is not None:
        field_requested = True
        add_change("apr_pct", _valid_apr_pct(str(args.apr)), public_name="apr")

    if bool(getattr(args, "clear_monthly_payment", False)):
        field_requested = True
        add_change("monthly_payment_cents", None, public_name="monthly_payment")
    elif getattr(args, "monthly_payment", None) is not None:
        field_requested = True
        monthly_payment_cents = _nonnegative_dollars_to_cents(
            args.monthly_payment,
            field_name="monthly_payment",
        )
        add_change(
            "monthly_payment_cents",
            monthly_payment_cents,
            public_name="monthly_payment",
        )

    if getattr(args, "portion_type", None) is not None:
        field_requested = True
        add_change("portion_type", _valid_portion_type(str(args.portion_type)))

    if bool(getattr(args, "clear_promo_end_date", False)):
        field_requested = True
        add_change("promo_end_date", None)
    elif promo_end_date is not None or expected_payoff_date is not None:
        field_requested = True
        add_change(
            "promo_end_date",
            _normalize_optional_date(
                promo_end_date or expected_payoff_date,
                field_name="promo_end_date",
            ),
        )

    if bool(getattr(args, "clear_notes", False)):
        field_requested = True
        add_change("notes", None)
    elif getattr(args, "notes", None) is not None:
        field_requested = True
        notes = str(args.notes).strip()
        if not notes:
            raise ValueError("notes cannot be empty")
        add_change("notes", notes)

    if not field_requested:
        raise ValueError("debt portion update requires at least one field to change")

    base_data = _portion_row_to_dict(existing)
    if not updates:
        data = {
            **base_data,
            "changes": {},
            "no_changes": True,
            "dry_run": dry_run,
        }
        return {
            "data": data,
            "summary": {"portion_id": portion_id, "fields_changed": 0, "dry_run": dry_run},
            "cli_report": _build_portion_update_report(data),
        }

    if not dry_run:
        set_clause = ", ".join(f"{column} = ?" for column, _value in updates)
        conn.execute(
            f"""
            UPDATE debt_balance_portions
               SET {set_clause},
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            [value for _column, value in updates] + [portion_id],
        )
        conn.commit()

    data = {
        **base_data,
        "changes": changes,
        "no_changes": False,
        "dry_run": dry_run,
    }
    for column, value in updates:
        public_name = {
            "principal_cents": "principal_cents",
            "apr_pct": "apr_pct",
            "monthly_payment_cents": "monthly_payment_cents",
        }.get(column, column)
        data[public_name] = value
    return {
        "data": data,
        "summary": {
            "portion_id": portion_id,
            "fields_changed": len(changes),
            "dry_run": dry_run,
        },
        "cli_report": _build_portion_update_report(data),
    }


def handle_portion_deactivate(args, conn: sqlite3.Connection) -> dict[str, Any]:
    portion_id = str(args.portion_id).strip()
    if not portion_id:
        raise ValueError("portion_id is required")
    existing = _load_debt_balance_portion(conn, portion_id)
    dry_run = bool(getattr(args, "dry_run", False))
    no_changes = not bool(existing["is_active"])

    if not dry_run and not no_changes:
        conn.execute(
            """
            UPDATE debt_balance_portions
               SET is_active = 0,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            (portion_id,),
        )
        conn.commit()

    data = {
        **_portion_row_to_dict(existing),
        "is_active": False if not dry_run and not no_changes else bool(existing["is_active"]),
        "no_changes": no_changes,
        "dry_run": dry_run,
    }
    return {
        "data": data,
        "summary": {"portion_id": portion_id, "dry_run": dry_run, "no_changes": no_changes},
        "cli_report": _build_portion_deactivate_report(data),
    }


def handle_impact(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    months = int(args.months)
    cut_pct = int(args.cut_pct)
    if months < 1:
        raise ValueError("months must be >= 1")
    if cut_pct < 1 or cut_pct > 100:
        raise ValueError("cut_pct must be between 1 and 100")

    category_rows = category_spending_averages(conn, months=months, rules_path=rules_path)

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
            "  No active debt balances.",
            "",
            f"DISCRETIONARY CATEGORIES ({months}-mo avg, by monthly spend)",
            "  None" if not discretionary_categories else "",
            "",
            "SCENARIOS",
            "  No debt accounts with balance > 0; scenarios unavailable.",
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
