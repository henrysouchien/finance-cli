"""Vendor-memory based categorization engine."""

from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from .user_rules import (
    UserRules,
    get_category_override,
    get_split_rule,
    load_rules,
    match_keyword_rule,
    match_payment_exclusion,
    match_payment_keyword,
    resolve_category_alias,
)

ID_TOKEN_RE = re.compile(r"\b(?:id|ref|txn|confirmation|conf|trace|auth)[:#\-\s]*[a-z0-9]{4,}\b", re.IGNORECASE)
DATE_TOKEN_RE = re.compile(r"\b\d{1,4}[/-]\d{1,2}[/-]\d{1,4}\b")
LONG_NUMBER_RE = re.compile(r"\b\d{5,}\b")
WHITESPACE_RE = re.compile(r"\s+")

PFC_PRIMARY_MAP: dict[str, str | None] = {
    "INCOME": "Income: Other",
    "TRANSFER_IN": None,
    "TRANSFER_OUT": None,
    "LOAN_PAYMENTS": "Bank Charges & Fees",
    "BANK_FEES": "Bank Charges & Fees",
    "ENTERTAINMENT": "Entertainment",
    "FOOD_AND_DRINK": "Dining",
    "GENERAL_MERCHANDISE": "Shopping",
    "HOME_IMPROVEMENT": "Home Improvement",
    "MEDICAL": "Health & Wellness",
    "PERSONAL_CARE": "Personal Expense",
    "GENERAL_SERVICES": "Professional Fees",
    "GOVERNMENT_AND_NON_PROFIT": "Donations",
    "TRANSPORTATION": "Transportation",
    "TRAVEL": "Travel",
    "RENT_AND_UTILITIES": "Utilities",
}


@dataclass(frozen=True)
class MatchResult:
    category_id: str | None
    category_source: str | None
    category_confidence: float | None
    category_rule_id: str | None
    matched_rule_id: str | None = None
    is_payment: bool = False
    resolved_use_type: str | None = None


def map_plaid_pfc_to_category(personal_finance_category: dict | None) -> tuple[str | None, bool]:
    """Return (local_category_name, is_payment_flag) for Plaid PFC objects."""
    if not personal_finance_category:
        return None, False

    primary = str(personal_finance_category.get("primary") or "").strip().upper()
    detailed = str(personal_finance_category.get("detailed") or "").strip().upper()

    if primary in {"TRANSFER_IN", "TRANSFER_OUT"}:
        return None, True

    if detailed == "INCOME_WAGES":
        return "Income: Salary", False
    if detailed == "INCOME_OTHER_INCOME":
        return "Income: Other", False
    if detailed.startswith("FOOD_AND_DRINK_GROCERIES"):
        return "Groceries", False
    if detailed.startswith("FOOD_AND_DRINK_COFFEE"):
        return "Coffee", False
    if detailed.startswith("FOOD_AND_DRINK_"):
        return "Dining", False
    if detailed.startswith("RENT_AND_UTILITIES_RENT"):
        return "Rent", False
    if detailed.startswith("RENT_AND_UTILITIES_"):
        return "Utilities", False
    if detailed.startswith("GOVERNMENT_AND_NON_PROFIT_TAX_PAYMENT"):
        return "Taxes", False
    if detailed.startswith("GOVERNMENT_AND_NON_PROFIT_DONATIONS"):
        return "Donations", False
    if detailed.startswith("GENERAL_SERVICES_INSURANCE"):
        return "Insurance", False
    if detailed.startswith("GENERAL_SERVICES_EDUCATION"):
        return "Professional Fees", False
    if detailed.startswith("GENERAL_SERVICES_CHILDCARE"):
        return "Childcare", False
    if detailed.startswith("TRANSPORTATION_GAS"):
        return "Transportation", False
    if detailed.startswith("LOAN_PAYMENTS_CREDIT_CARD_PAYMENT"):
        return None, True
    if detailed.startswith("LOAN_PAYMENTS_MORTGAGE_PAYMENT"):
        return "Rent", False

    mapped = PFC_PRIMARY_MAP.get(primary)
    if mapped is None:
        return None, primary in {"LOAN_PAYMENTS", "TRANSFER_IN", "TRANSFER_OUT"}
    return mapped, False


def lookup_category_mapping(
    conn: sqlite3.Connection,
    source_category: str,
    source: str | None,
) -> str | None:
    """Look up canonical category_id for a source category via category_mappings."""
    normalized = source_category.strip().lower()
    if not normalized:
        return None

    row = conn.execute(
        """
        SELECT category_id
          FROM category_mappings
         WHERE lower(source_category) = ?
           AND source = ?
           AND is_enabled = 1
        """,
        (normalized, source),
    ).fetchone()
    if row and row["category_id"]:
        conn.execute(
            """
            UPDATE category_mappings
               SET match_count = match_count + 1,
                   last_matched = datetime('now')
             WHERE lower(source_category) = ?
               AND source = ?
            """,
            (normalized, source),
        )
        return str(row["category_id"])

    row = conn.execute(
        """
        SELECT category_id
          FROM category_mappings
         WHERE lower(source_category) = ?
           AND source IS NULL
           AND is_enabled = 1
        """,
        (normalized,),
    ).fetchone()
    if row and row["category_id"]:
        conn.execute(
            """
            UPDATE category_mappings
               SET match_count = match_count + 1,
                   last_matched = datetime('now')
             WHERE lower(source_category) = ?
               AND source IS NULL
            """,
            (normalized,),
        )
        return str(row["category_id"])

    return None


def normalize_description(description: str) -> str:
    text = description.strip().lower()
    text = ID_TOKEN_RE.sub(" ", text)
    text = DATE_TOKEN_RE.sub(" ", text)
    text = LONG_NUMBER_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def _fetch_rules(
    conn: sqlite3.Connection,
    normalized: str,
    use_type: str | None,
    exact_only: bool,
) -> list[sqlite3.Row]:
    params: list[object] = [normalized]
    where = ["is_enabled = 1"]

    if exact_only:
        where.append("description_pattern = ?")
    else:
        where.append("? LIKE description_pattern || '%' ")

    if use_type in {"Business", "Personal"}:
        where.append("use_type IN (?, 'Any')")
        params.append(use_type)
    else:
        where.append("use_type = 'Any'")

    query = f"""
        SELECT id, description_pattern, category_id, use_type, confidence, priority, match_count, is_confirmed
        FROM vendor_memory
        WHERE {' AND '.join(where)}
    """
    rows = conn.execute(query, tuple(params)).fetchall()

    if use_type in {"Business", "Personal"}:
        exact_use = [r for r in rows if r["use_type"] == use_type]
        if exact_use:
            return exact_use
    return rows


def _resolve_prefix_match(
    rows: list[sqlite3.Row],
    normalized: str,
    conn: sqlite3.Connection | None = None,
) -> MatchResult | None:
    if not rows:
        return None

    max_len = max(len(row["description_pattern"] or "") for row in rows)
    scoped = [row for row in rows if len(row["description_pattern"] or "") == max_len and normalized.startswith(row["description_pattern"])]
    if not scoped:
        return None

    max_priority = max(int(row["priority"] or 0) for row in scoped)
    scoped = [row for row in scoped if int(row["priority"] or 0) == max_priority]

    max_conf = max(float(row["confidence"] or 0.0) for row in scoped)
    scoped = [row for row in scoped if float(row["confidence"] or 0.0) == max_conf]

    max_match_count = max(int(row["match_count"] or 0) for row in scoped)
    scoped = [row for row in scoped if int(row["match_count"] or 0) == max_match_count]

    if len(scoped) > 1:
        return MatchResult(
            category_id=None,
            category_source="ambiguous",
            category_confidence=None,
            category_rule_id=None,
            matched_rule_id=None,
        )

    winner = scoped[0]
    confidence = 0.8
    if isinstance(winner, dict):
        is_confirmed = winner.get("is_confirmed", 1)
    else:
        try:
            is_confirmed = winner["is_confirmed"]
        except Exception:
            is_confirmed = 1

    if int(is_confirmed or 0) == 0:
        confidence = min(confidence, 0.7)

    resolved_use_type = str(winner["use_type"] or "").strip()
    if resolved_use_type not in {"Business", "Personal"}:
        resolved_use_type = None

    return MatchResult(
        category_id=winner["category_id"],
        category_source="auto_prefix",
        category_confidence=confidence,
        category_rule_id=winner["id"],
        matched_rule_id=winner["id"],
        is_payment=_is_payments_category(conn, winner["category_id"]),
        resolved_use_type=resolved_use_type,
    )


def _category_id_for_name(conn: sqlite3.Connection, category_name: str | None, rules: UserRules) -> str | None:
    if not category_name:
        return None

    canonical_name = resolve_category_alias(category_name, rules)
    if canonical_name is None:
        return None
    row = conn.execute(
        "SELECT id FROM categories WHERE lower(name) = lower(?)",
        (canonical_name,),
    ).fetchone()
    if row:
        return str(row["id"])
    return None


def _is_payments_category(conn: sqlite3.Connection | None, category_id: str | None) -> bool:
    """Return True when category_id refers to 'Payments & Transfers'."""
    if conn is None or not category_id:
        return False
    row = conn.execute("SELECT name FROM categories WHERE id = ?", (category_id,)).fetchone()
    return row is not None and str(row["name"]).lower() == "payments & transfers"


def _apply_category_override(
    conn: sqlite3.Connection,
    txn_id: str,
    category_id: str | None,
    category_source: str | None,
    rules: UserRules,
) -> None:
    if not category_id:
        return

    category_row = conn.execute(
        "SELECT name FROM categories WHERE id = ?",
        (category_id,),
    ).fetchone()
    if not category_row:
        return

    forced_use_type = get_category_override(
        str(category_row["name"]),
        category_source or "",
        rules,
    )
    if forced_use_type not in {"Business", "Personal"}:
        return

    conn.execute(
        """
        UPDATE transactions
           SET use_type = ?,
               updated_at = datetime('now')
         WHERE id = ?
           AND use_type IS NULL
        """,
        (forced_use_type, txn_id),
    )


def _apply_split_rule(
    conn: sqlite3.Connection,
    txn_id: str,
    rules: UserRules,
) -> None:
    txn = conn.execute(
        """
        SELECT t.id, t.account_id, t.date, t.description, t.amount_cents, t.category_id,
               t.category_source, t.is_payment, t.is_recurring, t.is_reviewed,
               t.project_id, t.notes, t.source, t.raw_plaid_json, t.is_active,
               t.parent_transaction_id, t.split_group_id, c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.id = ?
        """,
        (txn_id,),
    ).fetchone()
    if not txn:
        return
    if int(txn["is_active"] or 0) != 1:
        return
    if txn["parent_transaction_id"]:
        return
    if txn["split_group_id"]:
        return

    existing_children = conn.execute(
        "SELECT 1 FROM transactions WHERE parent_transaction_id = ? LIMIT 1",
        (txn_id,),
    ).fetchone()
    if existing_children:
        return

    split_rule = get_split_rule(
        str(txn["description"] or ""),
        str(txn["category_name"] or ""),
        rules,
    )
    if not split_rule:
        return

    business_category_id = _category_id_for_name(conn, split_rule.business_category, rules)
    personal_category_id = _category_id_for_name(conn, split_rule.personal_category, rules)
    if not business_category_id or not personal_category_id:
        return

    amount_cents = int(txn["amount_cents"] or 0)
    business_amount_cents = int(round(amount_cents * (split_rule.business_pct / 100.0)))
    personal_amount_cents = amount_cents - business_amount_cents
    split_group_id = uuid.uuid4().hex
    business_pct = split_rule.business_pct / 100.0
    personal_pct = 1.0 - business_pct

    children = [
        ("Business", business_amount_cents, business_category_id, business_pct, "business"),
        ("Personal", personal_amount_cents, personal_category_id, personal_pct, "personal"),
    ]
    for use_type, child_amount_cents, child_category_id, child_pct, side in children:
        conn.execute(
            """
            INSERT INTO transactions (
                id,
                account_id,
                dedupe_key,
                date,
                description,
                amount_cents,
                category_id,
                category_source,
                category_confidence,
                category_rule_id,
                use_type,
                is_payment,
                is_recurring,
                is_reviewed,
                is_active,
                project_id,
                notes,
                source,
                raw_plaid_json,
                split_group_id,
                parent_transaction_id,
                split_pct,
                split_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'keyword_rule', 0.9, NULL, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                txn["account_id"],
                f"split:{txn_id}:{split_group_id}:{side}",
                txn["date"],
                txn["description"],
                child_amount_cents,
                child_category_id,
                use_type,
                int(txn["is_payment"] or 0),
                int(txn["is_recurring"] or 0),
                int(txn["is_reviewed"] or 0),
                txn["project_id"],
                txn["notes"],
                txn["source"],
                txn["raw_plaid_json"],
                split_group_id,
                txn_id,
                child_pct,
                split_rule.note,
            ),
        )

    conn.execute(
        """
        UPDATE transactions
           SET is_active = 0,
               split_group_id = ?,
               split_note = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (split_group_id, split_rule.note, txn_id),
    )


def match_transaction(
    conn: sqlite3.Connection,
    description: str,
    use_type: str | None = None,
    plaid_category: str | None = None,
    source_category: str | None = None,
    is_payment: bool = False,
) -> MatchResult | None:
    rules = load_rules()

    # Priority 1: Payment keywords from rules.yaml (user-configured, highest priority)
    if match_payment_keyword(description, rules):
        payments_category_id = _category_id_for_name(conn, "Payments & Transfers", rules)
        if payments_category_id:
            return MatchResult(
                category_id=payments_category_id,
                category_source="keyword_rule",
                category_confidence=0.9,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=True,
            )

    # Priority 2: Payment exclusions suppress Plaid is_payment flag
    payment_excluded = False
    if is_payment and match_payment_exclusion(description, rules):
        is_payment = False
        payment_excluded = True

    normalized = normalize_description(description)
    if not normalized:
        # No normalizable description — honor Plaid is_payment if still set.
        if is_payment:
            payments_category_id = _category_id_for_name(conn, "Payments & Transfers", rules)
            if payments_category_id:
                return MatchResult(
                    category_id=payments_category_id,
                    category_source="keyword_rule",
                    category_confidence=0.9,
                    category_rule_id=None,
                    matched_rule_id=None,
                    is_payment=True,
                )
        return None

    # Priority 3: Vendor memory (exact match) — overrides Plaid is_payment.
    exact_rows = _fetch_rules(conn, normalized=normalized, use_type=use_type, exact_only=True)
    if exact_rows:
        winner = sorted(
            exact_rows,
            key=lambda row: (
                0 if row["use_type"] == use_type else 1,
                -int(row["priority"] or 0),
                -float(row["confidence"] or 0.0),
                -int(row["match_count"] or 0),
                row["id"],
            ),
        )[0]
        confidence = 1.0
        if int(winner["is_confirmed"] or 0) == 0:
            confidence = min(confidence, 0.7)
        return MatchResult(
            category_id=winner["category_id"],
            category_source="vendor_memory",
            category_confidence=confidence,
            category_rule_id=winner["id"],
            matched_rule_id=winner["id"],
            is_payment=_is_payments_category(conn, winner["category_id"]),
            resolved_use_type=(
                str(winner["use_type"]).strip()
                if str(winner["use_type"]).strip() in {"Business", "Personal"}
                else None
            ),
        )

    # Priority 4: Vendor memory (prefix match).
    prefix_rows = _fetch_rules(conn, normalized=normalized, use_type=use_type, exact_only=False)
    prefix = _resolve_prefix_match(prefix_rows, normalized, conn=conn)
    if prefix:
        return prefix

    # Priority 5: Plaid is_payment flag (demoted below vendor memory).
    if is_payment:
        payments_category_id = _category_id_for_name(conn, "Payments & Transfers", rules)
        if payments_category_id:
            return MatchResult(
                category_id=payments_category_id,
                category_source="keyword_rule",
                category_confidence=0.9,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=True,
            )

    # Priority 6+: keyword rules, category mappings, and plaid PFC fallback.
    keyword_match = match_keyword_rule(description, rules)
    if keyword_match:
        category_id = _category_id_for_name(conn, keyword_match.category, rules)
        if category_id:
            return MatchResult(
                category_id=category_id,
                category_source="keyword_rule",
                category_confidence=0.9,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=_is_payments_category(conn, category_id),
                resolved_use_type=keyword_match.use_type,
            )

    if source_category:
        mapping_id = lookup_category_mapping(conn, source_category, source=None)
        if mapping_id:
            return MatchResult(
                category_id=mapping_id,
                category_source="category_mapping",
                category_confidence=0.8,
                category_rule_id=None,
                matched_rule_id=None,
                is_payment=_is_payments_category(conn, mapping_id),
            )

    if plaid_category:
        plaid_category_id = _category_id_for_name(conn, plaid_category, rules)
        return MatchResult(
            category_id=plaid_category_id,
            category_source="plaid",
            category_confidence=0.5,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=_is_payments_category(conn, plaid_category_id),
        )

    if payment_excluded:
        # Sentinel result: explicit non-payment without category assignment.
        return MatchResult(
            category_id=None,
            category_source=None,
            category_confidence=None,
            category_rule_id=None,
            matched_rule_id=None,
            is_payment=False,
        )

    return None


def apply_match(
    conn: sqlite3.Connection,
    txn_id: str,
    result: MatchResult,
    dry_run: bool = False,
) -> bool:
    row = conn.execute(
        "SELECT id, use_type FROM transactions WHERE id = ?",
        (txn_id,),
    ).fetchone()
    if not row:
        return False

    if dry_run:
        return True

    rules = load_rules()

    resolved_use_type = None
    if row["use_type"] is None and result.resolved_use_type in {"Business", "Personal"}:
        resolved_use_type = result.resolved_use_type
    is_payment_val = None if result.category_source in ("ambiguous", None) else int(result.is_payment)

    conn.execute(
        """
        UPDATE transactions
           SET category_id = ?,
               category_source = ?,
               category_confidence = ?,
               category_rule_id = ?,
               use_type = CASE
                            WHEN use_type IS NULL AND ? IS NOT NULL THEN ?
                            ELSE use_type
                          END,
               is_payment = CASE WHEN ? IS NOT NULL THEN ? ELSE is_payment END,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (
            result.category_id,
            result.category_source,
            result.category_confidence,
            result.category_rule_id,
            resolved_use_type,
            resolved_use_type,
            is_payment_val,
            is_payment_val,
            txn_id,
        ),
    )

    if result.matched_rule_id:
        conn.execute(
            """
            UPDATE vendor_memory
               SET match_count = COALESCE(match_count, 0) + 1,
                   last_matched = ?
             WHERE id = ?
            """,
            (
                datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
                result.matched_rule_id,
            ),
        )

    _apply_category_override(conn, txn_id, result.category_id, result.category_source, rules)
    _apply_split_rule(conn, txn_id, rules)

    conn.commit()
    return True
