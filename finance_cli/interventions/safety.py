from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
import json
import re
import sqlite3

from ..categorizer import normalize_description
from ..commands.common import fmt_dollars
from ..models import cents_to_dollars, normalize_date
from ..transaction_heuristics import is_possible_multi_passenger_travel_group
from .context import InterventionContext
from .registry import (
    CFPDomain,
    CFPProcessStep,
    Intervention,
    InterventionAction,
    Move,
    Priority,
    register_pattern,
)


_S1_DUPLICATE_WINDOW_DAYS = 7
_S1_RECENT_LOOKBACK_DAYS = 45
_S1_MIN_AMOUNT_CENTS = 1_000
_S2_RECENT_LOOKBACK_DAYS = 30
_S2_MIN_AMOUNT_CENTS = 20_000
_S2_DESCRIPTOR_TOKEN_MIN_DIGITS = 4
_S2_NON_MERCHANT_SOURCE_CATEGORY_PREFIXES = ("TRANSFER_IN", "TRANSFER_OUT", "LOAN_PAYMENTS")
_S2_BARE_CHECK_DESCRIPTOR_RE = re.compile(r"^check(?:\s*(?:#|no\.?|number)?\s*\d+)?$", re.IGNORECASE)


@dataclass(frozen=True)
class _DuplicateChargeCandidate:
    primary_id: str
    duplicate_id: str
    account_id: str
    account_label: str
    vendor: str
    amount_cents: int
    primary_date: date
    duplicate_date: date


@dataclass(frozen=True)
class _UnfamiliarVendorChargeCandidate:
    transaction_id: str
    account_label: str
    vendor: str
    amount_cents: int
    txn_date: date


def _parse_transaction_date(value: object) -> date | None:
    raw = normalize_date(str(value or "").strip())
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _has_existing_duplicate_workflow(
    conn: sqlite3.Connection,
    *,
    first_id: str,
    second_id: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM transaction_dispute_workflows
         WHERE dispute_reason = 'duplicate_charge'
           AND (
                (transaction_id = ? AND duplicate_transaction_id = ?)
                OR (transaction_id = ? AND duplicate_transaction_id = ?)
           )
         LIMIT 1
        """,
        (first_id, second_id, second_id, first_id),
    ).fetchone()
    return row is not None


def _account_label(row: sqlite3.Row) -> str:
    institution = str(row["institution_name"] or "").strip()
    account_name = str(row["account_name"] or "").strip()
    if institution and account_name:
        return f"{institution} {account_name}"
    return account_name or institution or "account"


def _find_s1_candidate(
    conn: sqlite3.Connection,
    *,
    as_of: date,
) -> _DuplicateChargeCandidate | None:
    lookback_start = as_of - timedelta(days=_S1_RECENT_LOOKBACK_DAYS)
    candidate_fetch_start = lookback_start - timedelta(days=_S1_DUPLICATE_WINDOW_DAYS)
    rows = conn.execute(
        """
        SELECT t.id, t.account_id, t.date, t.description, t.amount_cents,
               a.institution_name, a.account_name
          FROM transactions t
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.is_recurring = 0
           AND t.amount_cents < 0
           AND ABS(t.amount_cents) >= ?
           AND t.account_id IS NOT NULL
           AND t.parent_transaction_id IS NULL
           AND t.date >= ?
           AND t.date <= ?
         ORDER BY ABS(t.amount_cents) DESC, t.date DESC, t.id
        """,
        (
            _S1_MIN_AMOUNT_CENTS,
            candidate_fetch_start.isoformat(),
            as_of.isoformat(),
        ),
    ).fetchall()

    grouped: dict[tuple[str, int, str], list[tuple[sqlite3.Row, date]]] = {}
    for row in rows:
        txn_date = _parse_transaction_date(row["date"])
        if txn_date is None:
            continue
        vendor_key = normalize_description(str(row["description"] or ""))
        if len(vendor_key) < 3:
            continue
        key = (str(row["account_id"]), abs(int(row["amount_cents"] or 0)), vendor_key)
        grouped.setdefault(key, []).append((row, txn_date))

    candidates: list[_DuplicateChargeCandidate] = []
    for (_account_id, amount_cents, vendor_key), group in grouped.items():
        if len(group) < 2:
            continue
        date_counts = Counter(txn_date for _row, txn_date in group)
        ordered = sorted(
            group,
            key=lambda item: (item[1], str(item[0]["id"])),
            reverse=True,
        )
        for primary_index, (primary, primary_date) in enumerate(ordered):
            if primary_date < lookback_start:
                continue
            for duplicate, duplicate_date in ordered[primary_index + 1 :]:
                days_apart = abs((primary_date - duplicate_date).days)
                if days_apart > _S1_DUPLICATE_WINDOW_DAYS:
                    continue
                if (
                    primary_date == duplicate_date
                    and is_possible_multi_passenger_travel_group(date_counts[primary_date], vendor_key)
                ):
                    continue
                primary_id = str(primary["id"])
                duplicate_id = str(duplicate["id"])
                if _has_existing_duplicate_workflow(
                    conn,
                    first_id=primary_id,
                    second_id=duplicate_id,
                ):
                    continue
                candidates.append(
                    _DuplicateChargeCandidate(
                        primary_id=primary_id,
                        duplicate_id=duplicate_id,
                        account_id=str(primary["account_id"]),
                        account_label=_account_label(primary),
                        vendor=str(primary["description"] or vendor_key),
                        amount_cents=amount_cents,
                        primary_date=primary_date,
                        duplicate_date=duplicate_date,
                    )
                )
                break

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.amount_cents,
            item.primary_date,
            item.duplicate_date,
            item.vendor,
            item.primary_id,
        ),
    )


def _safe_raw_plaid_payload(value: object) -> dict[str, object]:
    if not value:
        return {}
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _plaid_merchant_entity_id(value: object) -> str | None:
    payload = _safe_raw_plaid_payload(value)
    merchant_entity_id = str(payload.get("merchant_entity_id") or "").strip()
    return merchant_entity_id or None


def _display_vendor(row: sqlite3.Row, normalized_vendor: str) -> str:
    payload = _safe_raw_plaid_payload(row["raw_plaid_json"])
    merchant_name = str(payload.get("merchant_name") or "").strip()
    if merchant_name:
        return merchant_name
    description = str(row["description"] or "").strip()
    return description or normalized_vendor


def _s2_vendor_history_key(normalized_vendor: str) -> str:
    tokens = normalized_vendor.split()
    without_numeric = [
        token
        for token in tokens
        if not (token.isdigit() and len(token) >= _S2_DESCRIPTOR_TOKEN_MIN_DIGITS)
    ]
    stripped = " ".join(without_numeric).strip()
    return stripped if len(stripped) >= 6 else normalized_vendor


def _is_s2_non_merchant_descriptor(row: sqlite3.Row, normalized_vendor: str) -> bool:
    source_category = str(row["source_category"] or "").strip().upper()
    if source_category.startswith(_S2_NON_MERCHANT_SOURCE_CATEGORY_PREFIXES):
        return True
    return bool(_S2_BARE_CHECK_DESCRIPTOR_RE.fullmatch(normalized_vendor))


def _is_s2_user_confirmed(conn: sqlite3.Connection, row: sqlite3.Row) -> bool:
    if int(row["is_reviewed"] or 0):
        return True
    category_source = str(row["category_source"] or "")
    if category_source == "user":
        return True
    if category_source != "vendor_memory":
        return False
    rule_id = str(row["category_rule_id"] or "").strip()
    if not rule_id:
        return False
    rule = conn.execute(
        """
        SELECT is_confirmed
          FROM vendor_memory
         WHERE id = ?
           AND is_enabled = 1
        """,
        (rule_id,),
    ).fetchone()
    return bool(rule and int(rule["is_confirmed"] or 0))


def _has_enabled_vendor_memory(
    conn: sqlite3.Connection,
    *,
    normalized_vendor: str,
    use_type: str | None,
) -> bool:
    if not normalized_vendor:
        return False

    params: list[object] = [normalized_vendor]
    use_type_clause = ""
    if use_type in {"Business", "Personal"}:
        use_type_clause = "AND use_type IN (?, 'Any')"
        params.append(use_type)

    row = conn.execute(
        f"""
        SELECT 1
          FROM vendor_memory
         WHERE is_enabled = 1
           AND is_confirmed = 1
           AND ? LIKE description_pattern || '%'
           {use_type_clause}
         LIMIT 1
        """,
        params,
    ).fetchone()
    return row is not None


def _has_prior_charge_from_vendor(
    conn: sqlite3.Connection,
    *,
    transaction_id: str,
    txn_date: date,
    normalized_vendor: str,
    merchant_entity_id: str | None,
) -> bool:
    history_key = _s2_vendor_history_key(normalized_vendor)
    params: list[object] = [transaction_id, txn_date.isoformat()]

    rows = conn.execute(
        """
        SELECT t.description, t.raw_plaid_json
          FROM transactions t
         WHERE t.id <> ?
           AND t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents < 0
           AND t.date < ?
        """,
        params,
    ).fetchall()
    for row in rows:
        prior_vendor = normalize_description(str(row["description"] or ""))
        if prior_vendor == normalized_vendor or _s2_vendor_history_key(prior_vendor) == history_key:
            return True
        if merchant_entity_id and _plaid_merchant_entity_id(row["raw_plaid_json"]) == merchant_entity_id:
            return True
    return False


def _find_s2_candidate(
    conn: sqlite3.Connection,
    *,
    as_of: date,
) -> _UnfamiliarVendorChargeCandidate | None:
    lookback_start = as_of - timedelta(days=_S2_RECENT_LOOKBACK_DAYS)
    rows = conn.execute(
        """
        SELECT t.id, t.account_id, t.date, t.description, t.amount_cents,
               t.category_source, t.category_rule_id, t.use_type, t.is_reviewed, t.raw_plaid_json,
               t.source_category,
               a.institution_name, a.account_name
          FROM transactions t
          LEFT JOIN accounts a ON a.id = t.account_id
         WHERE t.is_active = 1
           AND t.is_payment = 0
           AND t.amount_cents <= ?
           AND t.parent_transaction_id IS NULL
           AND t.date >= ?
           AND t.date <= ?
         ORDER BY ABS(t.amount_cents) DESC, t.date DESC, t.id
        """,
        (
            -_S2_MIN_AMOUNT_CENTS,
            lookback_start.isoformat(),
            as_of.isoformat(),
        ),
    ).fetchall()

    candidates: list[_UnfamiliarVendorChargeCandidate] = []
    for row in rows:
        if _is_s2_user_confirmed(conn, row):
            continue
        txn_date = _parse_transaction_date(row["date"])
        if txn_date is None:
            continue
        normalized_vendor = normalize_description(str(row["description"] or ""))
        if len(normalized_vendor) < 3:
            continue
        if _is_s2_non_merchant_descriptor(row, normalized_vendor):
            continue
        use_type = None if row["use_type"] is None else str(row["use_type"])
        if _has_enabled_vendor_memory(conn, normalized_vendor=normalized_vendor, use_type=use_type):
            continue
        merchant_entity_id = _plaid_merchant_entity_id(row["raw_plaid_json"])
        if _has_prior_charge_from_vendor(
            conn,
            transaction_id=str(row["id"]),
            txn_date=txn_date,
            normalized_vendor=normalized_vendor,
            merchant_entity_id=merchant_entity_id,
        ):
            continue
        candidates.append(
            _UnfamiliarVendorChargeCandidate(
                transaction_id=str(row["id"]),
                account_label=_account_label(row),
                vendor=_display_vendor(row, normalized_vendor),
                amount_cents=abs(int(row["amount_cents"] or 0)),
                txn_date=txn_date,
            )
        )

    if not candidates:
        return None
    return max(
        candidates,
        key=lambda item: (
            item.amount_cents,
            item.txn_date,
            item.vendor,
            item.transaction_id,
        ),
    )


@register_pattern(
    id="S-1",
    move=Move.PATTERN_CATCH,
    tiers=(1,),
    priority=Priority.HIGH,
    cooldown=timedelta(days=14),
    tool="txn_dispute_workflow",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
    cfp_steps=(CFPProcessStep.ANALYZE, CFPProcessStep.IMPLEMENT),
)
def evaluate_s1_duplicate_charge(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_s1_candidate(conn, as_of=ctx.now.date())
    if candidate is None:
        return None

    amount = fmt_dollars(cents_to_dollars(candidate.amount_cents))
    earlier_date, later_date = sorted((candidate.duplicate_date, candidate.primary_date))
    return Intervention(
        pattern_id="S-1",
        move=Move.PATTERN_CATCH,
        tiers=(1,),
        priority=Priority.HIGH,
        headline=(
            f"Possible duplicate: {candidate.vendor} charged {amount} on "
            f"{earlier_date.isoformat()} and {later_date.isoformat()}. "
            "Worth disputing if it's real."
        ),
        detail_bullets=(
            f"Account: {candidate.account_label}",
            f"Transactions: {candidate.duplicate_id} and {candidate.primary_id}",
            "This prepares a dispute workflow only; it does not file with the issuer.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Prepare dispute workflow",
            tool="txn_dispute_workflow",
            params={
                "transaction_id": candidate.primary_id,
                "duplicate_transaction_id": candidate.duplicate_id,
                "dispute_reason": "duplicate_charge",
                "note": (
                    f"Possible duplicate {amount} charge at {candidate.vendor} "
                    f"on {earlier_date.isoformat()} and {later_date.isoformat()}."
                ),
            },
            build_stub=False,
        ),
        dollar_impact_cents=candidate.amount_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("S-1"),
    )


@register_pattern(
    id="S-2",
    move=Move.WARN,
    tiers=(1,),
    tool="txn_explain",
    cfp_domains=(CFPDomain.GENERAL_PRINCIPLES, CFPDomain.RISK_INSURANCE),
    cfp_steps=(CFPProcessStep.IDENTIFY, CFPProcessStep.ANALYZE, CFPProcessStep.PRESENT),
)
def evaluate_s2_unfamiliar_vendor_large_charge(
    conn: sqlite3.Connection,
    ctx: InterventionContext,
) -> Intervention | None:
    candidate = _find_s2_candidate(conn, as_of=ctx.now.date())
    if candidate is None:
        return None

    amount = fmt_dollars(cents_to_dollars(candidate.amount_cents))
    return Intervention(
        pattern_id="S-2",
        move=Move.WARN,
        tiers=(1,),
        priority=Priority.MEDIUM,
        headline=(
            f"{amount} to {candidate.vendor} on {candidate.txn_date.isoformat()} - "
            "first charge from this merchant in your transaction history. Worth confirming it's legit."
        ),
        detail_bullets=(
            f"Account: {candidate.account_label}",
            "No enabled vendor-memory rule or prior matching charge history matched this merchant.",
            "Reviewing the transaction explains how it was categorized before you take action.",
        ),
        tier4_ladder=None,
        tier4_is_fallback=False,
        action=InterventionAction(
            label="Explain transaction",
            tool="txn_explain",
            params={"id": candidate.transaction_id},
            build_stub=False,
        ),
        dollar_impact_cents=candidate.amount_cents,
        goal_link=None,
        log_id=None,
        fired_at=ctx.now,
        last_fired_at=ctx.recent_fires.get("S-2"),
    )
