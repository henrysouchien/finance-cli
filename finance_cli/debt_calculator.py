"""Debt dashboard, projection, and paydown simulation helpers."""

from __future__ import annotations

import dataclasses
import sqlite3
from decimal import Decimal, ROUND_HALF_UP, localcontext
from typing import Any

from .institution_names import canonicalize


MAX_SIM_MONTHS = 360
_MIN_PAYMENT_FLOOR_CENTS = 2_500


@dataclasses.dataclass
class DebtCard:
    """Debt account state used for dashboarding and simulations."""

    card_id: str
    label: str
    balance_cents: int
    apr: float | None
    min_payment_cents: int
    limit_cents: int | None = None
    intro_apr_end_date: str | None = None
    parent_account_id: str | None = None
    portion_id: str | None = None
    portion_type: str | None = None
    portion_over_allocated_cents: int = 0


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


def _estimate_min_payment(balance_cents: int, apr: float | None) -> int:
    """Estimate a card minimum payment when the source data is missing or unusable."""
    if balance_cents <= 0:
        return 0

    interest_cents = monthly_interest_cents(balance_cents, apr) if apr is not None else 0
    principal_cents = int(
        (Decimal(balance_cents) * Decimal("0.01")).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    )
    estimated_cents = principal_cents + interest_cents
    return max(estimated_cents, min(_MIN_PAYMENT_FLOOR_CENTS, balance_cents))


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
                portion_over_allocated_cents=max(
                    0,
                    int(card.portion_over_allocated_cents),
                ),
            )
        )
    return cloned


def compute_dashboard(cards: list[DebtCard]) -> dict[str, Any]:
    """Return per-card debt breakdown and portfolio totals."""
    cards_state = _clone_cards(cards)
    sorted_cards = sorted(cards_state, key=lambda card: (-card.balance_cents, card.card_id))

    rows: list[dict[str, Any]] = []
    apr_unknown_cards: list[str] = []
    portion_over_allocations: list[dict[str, Any]] = []

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
                "intro_apr_end_date": card.intro_apr_end_date,
                "parent_account_id": card.parent_account_id,
                "portion_id": card.portion_id,
                "portion_type": card.portion_type,
                "portion_over_allocated_cents": card.portion_over_allocated_cents,
            }
        )
        if card.portion_over_allocated_cents > 0:
            portion_over_allocations.append(
                {
                    "card_id": card.card_id,
                    "label": card.label,
                    "parent_account_id": card.parent_account_id,
                    "portion_id": card.portion_id,
                    "over_allocated_cents": card.portion_over_allocated_cents,
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
        "portion_over_allocations": portion_over_allocations,
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
        if account.lower().startswith(institution.lower()):
            return account
        return f"{institution} {account}"
    if institution:
        return institution
    if account:
        return account
    return account_id


def _build_portion_label(base_label: str, portion_label: str | None, portion_type: str | None) -> str:
    label = str(portion_label or "").strip()
    if not label:
        label = str(portion_type or "portion").replace("_", " ").title()
    return f"{base_label}: {label}"


def _load_active_balance_portions(
    conn: sqlite3.Connection,
    account_ids: list[str],
) -> dict[str, list[sqlite3.Row]]:
    if not account_ids:
        return {}

    placeholders = ",".join("?" for _ in account_ids)
    rows = conn.execute(
        f"""
        SELECT id,
               account_id,
               label,
               portion_type,
               principal_cents,
               apr_pct,
               monthly_payment_cents,
               promo_end_date,
               created_at
          FROM debt_balance_portions
         WHERE is_active = 1
           AND account_id IN ({placeholders})
         ORDER BY account_id, created_at, id
        """,
        tuple(account_ids),
    ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["account_id"]), []).append(row)
    return grouped


def _assign_segment_min_payments(
    segments: list[dict[str, Any]],
    parent_min_payment_cents: int | None,
) -> None:
    missing_indices: list[int] = []
    explicit_total = 0

    for index, segment in enumerate(segments):
        raw_payment = segment.get("raw_min_payment_cents")
        if raw_payment is not None and int(raw_payment) > 0:
            payment_cents = int(raw_payment)
            segment["min_payment_cents"] = payment_cents
            explicit_total += payment_cents
        else:
            missing_indices.append(index)

    if not missing_indices:
        return

    if parent_min_payment_cents is None:
        for index in missing_indices:
            segment = segments[index]
            segment["min_payment_cents"] = _estimate_min_payment(
                int(segment["balance_cents"]),
                segment["apr"],
            )
        return

    remaining_min_cents = max(0, int(parent_min_payment_cents) - explicit_total)
    total_missing_balance = sum(
        int(segments[index]["balance_cents"]) for index in missing_indices
    )
    allocated_cents = 0
    for position, index in enumerate(missing_indices):
        segment = segments[index]
        if position == len(missing_indices) - 1:
            payment_cents = remaining_min_cents - allocated_cents
        elif total_missing_balance > 0:
            payment_cents = int(
                (
                    Decimal(remaining_min_cents)
                    * Decimal(int(segment["balance_cents"]))
                    / Decimal(total_missing_balance)
                ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
            payment_cents = min(max(payment_cents, 0), remaining_min_cents - allocated_cents)
        else:
            payment_cents = 0
        segment["min_payment_cents"] = payment_cents
        allocated_cents += payment_cents


def _debt_cards_for_credit_account(
    row: sqlite3.Row,
    portions: list[sqlite3.Row],
    *,
    include_zero_balance: bool,
) -> list[DebtCard]:
    account_id = str(row["id"])
    balance_cents = abs(int(row["balance_current_cents"] or 0))
    if not include_zero_balance and balance_cents <= 0:
        return []

    limit_cents = row["balance_limit_cents"]
    parent_apr = None if row["apr_purchase"] is None else float(row["apr_purchase"])
    raw_min = row["raw_min_payment_cents"]
    parent_min_payment_cents = int(raw_min) if raw_min is not None and int(raw_min) > 0 else None
    base_label = _build_label(
        row["institution_name"],
        row["card_ending"],
        row["account_name"],
        account_id,
    )

    if not portions:
        min_payment_cents = (
            parent_min_payment_cents
            if parent_min_payment_cents is not None
            else _estimate_min_payment(balance_cents, parent_apr)
        )
        return [
            DebtCard(
                card_id=account_id,
                label=base_label,
                balance_cents=balance_cents,
                apr=parent_apr,
                min_payment_cents=min_payment_cents,
                limit_cents=None if limit_cents is None else int(limit_cents),
                intro_apr_end_date=row["intro_apr_end_date"],
            )
        ]

    total_declared_cents = 0
    remaining_cents = balance_cents
    segments: list[dict[str, Any]] = []

    for portion in portions:
        declared_cents = max(0, int(portion["principal_cents"] or 0))
        total_declared_cents += declared_cents
        allocated_cents = min(declared_cents, remaining_cents)
        remaining_cents -= allocated_cents

        if not include_zero_balance and allocated_cents <= 0:
            continue

        portion_id = str(portion["id"])
        portion_type = str(portion["portion_type"] or "other")
        segments.append(
            {
                "card_id": f"{account_id}:{portion_id}",
                "label": _build_portion_label(base_label, portion["label"], portion_type),
                "balance_cents": allocated_cents,
                "apr": None if portion["apr_pct"] is None else float(portion["apr_pct"]),
                "raw_min_payment_cents": portion["monthly_payment_cents"],
                "limit_cents": None,
                "intro_apr_end_date": portion["promo_end_date"],
                "parent_account_id": account_id,
                "portion_id": portion_id,
                "portion_type": portion_type,
                "portion_over_allocated_cents": 0,
            }
        )

    if remaining_cents > 0 or (include_zero_balance and not segments):
        segments.append(
            {
                "card_id": account_id,
                "label": base_label if not segments else f"{base_label}: Revolving Balance",
                "balance_cents": remaining_cents,
                "apr": parent_apr,
                "raw_min_payment_cents": None,
                "limit_cents": None if limit_cents is None else int(limit_cents),
                "intro_apr_end_date": row["intro_apr_end_date"],
                "parent_account_id": account_id,
                "portion_id": None,
                "portion_type": "purchase_residual",
                "portion_over_allocated_cents": 0,
            }
        )

    if not segments:
        return []

    over_allocated_cents = max(0, total_declared_cents - balance_cents)
    if over_allocated_cents > 0:
        for segment in reversed(segments):
            if segment["portion_id"] is not None:
                segment["portion_over_allocated_cents"] = over_allocated_cents
                break
        else:
            segments[-1]["portion_over_allocated_cents"] = over_allocated_cents

    _assign_segment_min_payments(segments, parent_min_payment_cents)

    return [
        DebtCard(
            card_id=str(segment["card_id"]),
            label=str(segment["label"]),
            balance_cents=int(segment["balance_cents"]),
            apr=segment["apr"],
            min_payment_cents=int(segment["min_payment_cents"]),
            limit_cents=segment["limit_cents"],
            intro_apr_end_date=segment["intro_apr_end_date"],
            parent_account_id=segment["parent_account_id"],
            portion_id=segment["portion_id"],
            portion_type=segment["portion_type"],
            portion_over_allocated_cents=int(segment["portion_over_allocated_cents"]),
        )
        for segment in segments
    ]


def load_debt_cards(conn: sqlite3.Connection, include_zero_balance: bool = False) -> list[DebtCard]:
    """Load active debt accounts with liability metadata from the database."""
    rows = conn.execute(
        """
        SELECT a.id,
               a.institution_name,
               a.card_ending,
               a.account_name,
               a.balance_current_cents,
               a.balance_limit_cents,
               l.apr_purchase,
               l.intro_apr_end_date,
               COALESCE(l.minimum_payment_cents, l.next_monthly_payment_cents) AS raw_min_payment_cents
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

    account_ids = [str(row["id"]) for row in rows]
    portions_by_account = _load_active_balance_portions(conn, account_ids)

    cards: list[DebtCard] = []
    for row in rows:
        cards.extend(
            _debt_cards_for_credit_account(
                row,
                portions_by_account.get(str(row["id"]), []),
                include_zero_balance=include_zero_balance,
            )
        )

    loan_rows = conn.execute(
        """
        SELECT id, creditor_name, current_balance_cents,
               interest_rate_pct, monthly_payment_cents
          FROM manual_loans
         WHERE is_active = 1
           AND current_balance_cents > 0
         ORDER BY current_balance_cents DESC
        """
    ).fetchall()

    for row in loan_rows:
        balance_cents = int(row["current_balance_cents"])
        if not include_zero_balance and balance_cents <= 0:
            continue
        apr = float(row["interest_rate_pct"])
        cards.append(
            DebtCard(
                card_id=str(row["id"]),
                label=f"Loan: {row['creditor_name']}",
                balance_cents=balance_cents,
                apr=apr,
                min_payment_cents=int(row["monthly_payment_cents"] or 0),
                limit_cents=None,
            )
        )

    return cards
