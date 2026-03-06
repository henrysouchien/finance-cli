"""Debt dashboard, projection, and paydown simulation helpers."""

from __future__ import annotations

import dataclasses
import sqlite3
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any

from .institution_names import canonicalize


MAX_SIM_MONTHS = 360


@dataclasses.dataclass
class DebtCard:
    """Debt account state used for dashboarding and simulations."""

    card_id: str
    label: str
    balance_cents: int
    apr: float | None
    min_payment_cents: int
    limit_cents: int | None = None


def monthly_interest_cents(balance_cents: int, apr: float) -> int:
    """Calculate monthly interest from balance and APR using ROUND_HALF_UP."""
    digits = len(str(abs(int(balance_cents)))) if balance_cents else 1
    with localcontext() as ctx:
        ctx.prec = max(28, digits + 16)
        return int(
            (
                Decimal(balance_cents)
                * Decimal(str(apr))
                / Decimal("100")
                / Decimal("12")
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )


def _clone_cards(cards: list[DebtCard]) -> list[DebtCard]:
    """Clone cards to guarantee mutation safety for callers."""
    cloned: list[DebtCard] = []
    for card in cards:
        cloned.append(
            dataclasses.replace(
                card,
                balance_cents=max(0, int(card.balance_cents)),
                min_payment_cents=max(0, int(card.min_payment_cents)),
                apr=None if card.apr is None else float(card.apr),
                limit_cents=None if card.limit_cents is None else int(card.limit_cents),
            )
        )
    return cloned


def compute_dashboard(cards: list[DebtCard]) -> dict[str, Any]:
    """Return per-card debt breakdown and portfolio totals."""
    cards_state = _clone_cards(cards)
    sorted_cards = sorted(cards_state, key=lambda card: (-card.balance_cents, card.card_id))

    rows: list[dict[str, Any]] = []
    apr_unknown_cards: list[str] = []

    total_balance_cents = 0
    total_min_payment_cents = 0
    total_monthly_interest_cents = 0

    weighted_apr_numerator = Decimal("0")
    weighted_apr_balance = 0

    for card in sorted_cards:
        balance_cents = int(card.balance_cents)
        min_payment_cents = int(card.min_payment_cents)
        monthly_interest = monthly_interest_cents(balance_cents, card.apr) if card.apr is not None else 0

        if card.limit_cents is None or int(card.limit_cents) <= 0:
            utilization_pct = None
        else:
            utilization_pct = round((balance_cents / int(card.limit_cents)) * 100, 2)

        rows.append(
            {
                "card_id": card.card_id,
                "label": card.label,
                "balance_cents": balance_cents,
                "apr": card.apr,
                "min_payment_cents": min_payment_cents,
                "monthly_interest_cents": monthly_interest,
                "limit_cents": card.limit_cents,
                "utilization_pct": utilization_pct,
            }
        )

        total_balance_cents += balance_cents
        total_min_payment_cents += min_payment_cents
        total_monthly_interest_cents += monthly_interest

        if card.apr is None:
            apr_unknown_cards.append(card.label)
        else:
            weighted_apr_numerator += Decimal(balance_cents) * Decimal(str(card.apr))
            weighted_apr_balance += balance_cents

    weighted_avg_apr = (
        float(weighted_apr_numerator / Decimal(weighted_apr_balance))
        if weighted_apr_balance > 0
        else None
    )

    return {
        "cards": rows,
        "total_balance_cents": total_balance_cents,
        "total_min_payment_cents": total_min_payment_cents,
        "total_monthly_interest_cents": total_monthly_interest_cents,
        "weighted_avg_apr": weighted_avg_apr,
        "apr_unknown_cards": apr_unknown_cards,
    }


def project_interest(cards: list[DebtCard], months: int = 12, summary_only: bool = False) -> dict[str, Any]:
    """Project interest and balances when only minimum payments are made."""
    if int(months) < 1:
        raise ValueError("months must be >= 1")

    cards_state = _clone_cards(cards)
    apr_unknown_cards = [card for card in cards_state if card.apr is None and card.balance_cents > 0]
    apr_unknown_balance_cents = sum(card.balance_cents for card in apr_unknown_cards)

    total_interest_cents = 0
    total_paid_cents = 0
    schedule: list[dict[str, Any]] = []

    for month in range(1, int(months) + 1):
        month_interest_cents = 0
        month_paid_cents = 0
        card_rows: list[dict[str, Any]] = []

        for card in cards_state:
            start_balance = int(card.balance_cents)
            if start_balance <= 0:
                card_rows.append(
                    {
                        "card_id": card.card_id,
                        "label": card.label,
                        "start_balance_cents": start_balance,
                        "interest_cents": 0,
                        "payment_cents": 0,
                        "end_balance_cents": 0,
                        "apr": card.apr,
                    }
                )
                continue

            interest_cents = monthly_interest_cents(start_balance, card.apr) if card.apr is not None else 0
            balance_with_interest = start_balance + interest_cents
            payment_cents = min(int(card.min_payment_cents), balance_with_interest)
            end_balance = max(0, balance_with_interest - payment_cents)

            card.balance_cents = end_balance

            month_interest_cents += interest_cents
            month_paid_cents += payment_cents
            card_rows.append(
                {
                    "card_id": card.card_id,
                    "label": card.label,
                    "start_balance_cents": start_balance,
                    "interest_cents": interest_cents,
                    "payment_cents": payment_cents,
                    "end_balance_cents": end_balance,
                    "apr": card.apr,
                }
            )

        total_interest_cents += month_interest_cents
        total_paid_cents += month_paid_cents

        entry = {
            "month": month,
            "interest_cents": month_interest_cents,
            "payment_cents": month_paid_cents,
            "cumulative_interest_cents": total_interest_cents,
            "remaining_balance_cents": sum(card.balance_cents for card in cards_state),
        }
        if not summary_only:
            entry["cards"] = card_rows
        schedule.append(entry)

    return {
        "months": int(months),
        "schedule": schedule,
        "total_interest_cents": total_interest_cents,
        "total_paid_cents": total_paid_cents,
        "final_balance_cents": sum(card.balance_cents for card in cards_state),
        "apr_unknown_count": len(apr_unknown_cards),
        "apr_unknown_balance_cents": apr_unknown_balance_cents,
    }


def _ranked_order(cards: list[DebtCard], strategy: str) -> list[DebtCard]:
    if strategy == "avalanche":
        return sorted(cards, key=lambda card: (-float(card.apr or 0), -card.balance_cents, card.card_id))
    return sorted(cards, key=lambda card: (card.balance_cents, -float(card.apr or 0), card.card_id))


def _unranked_order(cards: list[DebtCard]) -> list[DebtCard]:
    return sorted(cards, key=lambda card: (card.balance_cents, card.card_id))


def simulate_paydown(
    cards: list[DebtCard],
    extra_cents: int,
    strategy: str,
    summary_only: bool = False,
    lump_sum_cents: int = 0,
    lump_sum_month: int = 1,
) -> dict[str, Any]:
    """Simulate debt payoff with avalanche/snowball strategy and monthly extra payment.

    Args:
        lump_sum_cents: One-time extra payment in cents applied at *lump_sum_month*.
        lump_sum_month: Month number (1-based) when the lump sum is applied.
    """
    if int(extra_cents) < 0:
        raise ValueError("extra_cents must be >= 0")
    if strategy not in {"avalanche", "snowball"}:
        raise ValueError("strategy must be one of: avalanche, snowball")
    if int(lump_sum_cents) < 0:
        raise ValueError("lump_sum_cents must be >= 0")
    if int(lump_sum_month) < 1:
        raise ValueError("lump_sum_month must be >= 1")

    cards_state = _clone_cards(cards)
    starting_active = [card for card in cards_state if card.balance_cents > 0]

    if not starting_active:
        return {
            "months_to_payoff": 0,
            "total_interest_cents": 0,
            "total_paid_cents": 0,
            "fully_paid_off": True,
            "capped_cards": [],
            "unranked_cards": [],
            "assumptions": [],
            "unranked_received_extra": False,
            "schedule": [],
            "payoff_order": [],
        }

    base_extra = int(extra_cents)
    total_interest_cents = 0
    total_paid_cents = 0
    schedule: list[dict[str, Any]] = []
    payoff_order: list[str] = []
    paid_ids: set[str] = set()
    unranked_received_extra = False

    unranked_cards = sorted(
        [card.label for card in starting_active if card.apr is None],
    )
    all_unranked = bool(starting_active) and all(card.apr is None for card in starting_active)
    if all_unranked:
        assumptions = ["all_apr_unknown_zero_interest"]
    elif unranked_cards:
        assumptions = ["unknown_apr_zero_interest_optimistic"]
    else:
        assumptions = []

    months = 0
    while months < MAX_SIM_MONTHS:
        active_cards = [card for card in cards_state if card.balance_cents > 0]
        if not active_cards:
            break

        months += 1
        month_interest_cents = 0
        month_min_payment_cents = 0
        month_extra_payment_cents = 0
        month_card_rows: list[dict[str, Any]] = []
        month_paid_off_labels: list[str] = []

        month_start_extra = base_extra
        lump_applied_this_month = 0
        if int(lump_sum_cents) > 0 and months == int(lump_sum_month):
            month_start_extra += int(lump_sum_cents)
            lump_applied_this_month = int(lump_sum_cents)
        month_min_budget = sum(card.min_payment_cents for card in active_cards)
        extra_pool = month_start_extra

        for card in active_cards:
            start_balance = int(card.balance_cents)
            interest_cents = monthly_interest_cents(start_balance, card.apr) if card.apr is not None else 0
            card.balance_cents = start_balance + interest_cents
            month_interest_cents += interest_cents

            min_payment = min(int(card.min_payment_cents), card.balance_cents)
            card.balance_cents -= min_payment
            month_min_payment_cents += min_payment

            unused_min = int(card.min_payment_cents) - min_payment
            if unused_min > 0:
                extra_pool += unused_min

            month_card_rows.append(
                {
                    "card_id": card.card_id,
                    "label": card.label,
                    "apr": card.apr,
                    "start_balance_cents": start_balance,
                    "interest_cents": interest_cents,
                    "min_payment_cents": min_payment,
                    "extra_payment_cents": 0,
                    "end_balance_cents": card.balance_cents,
                }
            )

        active_after_min = [card for card in active_cards if card.balance_cents > 0]
        ranked_targets = [card for card in active_after_min if card.apr is not None]

        if ranked_targets:
            for target in _ranked_order(ranked_targets, strategy):
                if extra_pool <= 0:
                    break
                applied = min(extra_pool, target.balance_cents)
                if applied <= 0:
                    continue
                target.balance_cents -= applied
                extra_pool -= applied
                month_extra_payment_cents += applied
                for row in month_card_rows:
                    if row["card_id"] == target.card_id:
                        row["extra_payment_cents"] += applied
                        row["end_balance_cents"] = target.balance_cents
                        break

        ranked_remaining = [card for card in ranked_targets if card.balance_cents > 0]
        if extra_pool > 0 and (not ranked_targets or not ranked_remaining):
            spill_targets = [card for card in active_after_min if card.apr is None and card.balance_cents > 0]
            for target in _unranked_order(spill_targets):
                if extra_pool <= 0:
                    break
                applied = min(extra_pool, target.balance_cents)
                if applied <= 0:
                    continue
                target.balance_cents -= applied
                extra_pool -= applied
                month_extra_payment_cents += applied
                unranked_received_extra = True
                for row in month_card_rows:
                    if row["card_id"] == target.card_id:
                        row["extra_payment_cents"] += applied
                        row["end_balance_cents"] = target.balance_cents
                        break

        paid_this_month = [card for card in active_cards if card.balance_cents <= 0 and card.card_id not in paid_ids]
        for card in sorted(paid_this_month, key=lambda entry: entry.card_id):
            paid_ids.add(card.card_id)
            month_paid_off_labels.append(card.label)
            payoff_order.append(card.label)
            base_extra += int(card.min_payment_cents)

        total_interest_cents += month_interest_cents
        month_total_payment_cents = month_min_payment_cents + month_extra_payment_cents
        total_paid_cents += month_total_payment_cents

        entry = {
            "month": months,
            "interest_cents": month_interest_cents,
            "min_payment_cents": month_min_payment_cents,
            "extra_payment_cents": month_extra_payment_cents,
            "total_payment_cents": month_total_payment_cents,
            "payment_budget_cents": month_min_budget + month_start_extra,
            "remaining_balance_cents": sum(card.balance_cents for card in cards_state if card.balance_cents > 0),
            "base_extra_cents": base_extra,
            "lump_sum_applied_cents": lump_applied_this_month,
        }
        if summary_only:
            entry["paid_off_count"] = len(month_paid_off_labels)
        else:
            entry["paid_off_cards"] = month_paid_off_labels
            entry["cards"] = month_card_rows
        schedule.append(entry)

    remaining_cards = [card for card in cards_state if card.balance_cents > 0]
    fully_paid_off = len(remaining_cards) == 0

    return {
        "months_to_payoff": months,
        "total_interest_cents": total_interest_cents,
        "total_paid_cents": total_paid_cents,
        "fully_paid_off": fully_paid_off,
        "capped_cards": [] if fully_paid_off else sorted(card.label for card in remaining_cards),
        "unranked_cards": unranked_cards,
        "assumptions": assumptions,
        "unranked_received_extra": unranked_received_extra,
        "schedule": schedule,
        "payoff_order": payoff_order,
    }


def compare_strategies(
    cards: list[DebtCard],
    extra_cents: int,
    summary_only: bool = False,
    lump_sum_cents: int = 0,
    lump_sum_month: int = 1,
) -> dict[str, Any]:
    """Compare avalanche and snowball against minimum-only baseline."""
    active_cards = [card for card in cards if int(card.balance_cents) > 0]
    if not active_cards:
        zero_result = {
            "months_to_payoff": 0,
            "total_interest_cents": 0,
            "total_paid_cents": 0,
            "fully_paid_off": True,
            "capped_cards": [],
            "unranked_cards": [],
            "assumptions": [],
            "unranked_received_extra": False,
            "schedule": [],
            "payoff_order": [],
        }
        baseline = {
            "months": 0,
            "schedule": [],
            "total_interest_cents": 0,
            "total_paid_cents": 0,
            "final_balance_cents": 0,
            "apr_unknown_count": 0,
            "apr_unknown_balance_cents": 0,
        }
        return {
            "avalanche": zero_result,
            "snowball": zero_result,
            "baseline": baseline,
            "interest_savings_vs_baseline_cents": {
                "avalanche": 0,
                "snowball": 0,
            },
        }

    avalanche = simulate_paydown(
        cards, extra_cents=extra_cents, strategy="avalanche", summary_only=summary_only,
        lump_sum_cents=lump_sum_cents, lump_sum_month=lump_sum_month,
    )
    snowball = simulate_paydown(
        cards, extra_cents=extra_cents, strategy="snowball", summary_only=summary_only,
        lump_sum_cents=lump_sum_cents, lump_sum_month=lump_sum_month,
    )

    horizon_months = max(int(avalanche["months_to_payoff"]), int(snowball["months_to_payoff"]))
    if horizon_months < 1:
        horizon_months = 1

    baseline = project_interest(cards, months=horizon_months, summary_only=summary_only)

    return {
        "avalanche": avalanche,
        "snowball": snowball,
        "baseline": baseline,
        "interest_savings_vs_baseline_cents": {
            "avalanche": int(baseline["total_interest_cents"]) - int(avalanche["total_interest_cents"]),
            "snowball": int(baseline["total_interest_cents"]) - int(snowball["total_interest_cents"]),
        },
    }


def _build_label(institution_name: str | None, card_ending: str | None, account_name: str | None, account_id: str) -> str:
    institution = canonicalize(str(institution_name or ""))
    ending = str(card_ending or "").strip()
    account = str(account_name or "").strip()

    if institution and ending:
        return f"{institution} {ending}"
    if institution and account:
        return f"{institution} {account}"
    if institution:
        return institution
    if account:
        return account
    return account_id


def load_debt_cards(conn: sqlite3.Connection, include_zero_balance: bool = False) -> list[DebtCard]:
    """Load active credit-card debt with liability metadata from the database."""
    rows = conn.execute(
        """
        SELECT a.id,
               a.institution_name,
               a.card_ending,
               a.account_name,
               a.balance_current_cents,
               a.balance_limit_cents,
               l.apr_purchase,
               COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents, 0) AS min_payment_cents
          FROM accounts a
          LEFT JOIN liabilities l
            ON l.account_id = a.id
           AND l.is_active = 1
           AND l.liability_type = 'credit'
         WHERE a.account_type = 'credit_card'
           AND a.is_active = 1
           AND a.balance_current_cents IS NOT NULL
           AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)
         ORDER BY ABS(a.balance_current_cents) DESC
        """
    ).fetchall()

    cards: list[DebtCard] = []
    for row in rows:
        balance_cents = abs(int(row["balance_current_cents"] or 0))
        if not include_zero_balance and balance_cents <= 0:
            continue

        limit_cents = row["balance_limit_cents"]
        cards.append(
            DebtCard(
                card_id=str(row["id"]),
                label=_build_label(
                    row["institution_name"],
                    row["card_ending"],
                    row["account_name"],
                    str(row["id"]),
                ),
                balance_cents=balance_cents,
                apr=None if row["apr_purchase"] is None else float(row["apr_purchase"]),
                min_payment_cents=int(row["min_payment_cents"] or 0),
                limit_cents=None if limit_cents is None else int(limit_cents),
            )
        )

    return cards
