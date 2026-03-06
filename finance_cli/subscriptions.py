"""Subscription detection and rollup helpers."""

from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import median

from .categorizer import normalize_description

FREQ_TO_DAYS = {
    "weekly": 7,
    "biweekly": 14,
    "monthly": 30,
    "quarterly": 91,
    "yearly": 365,
}

SUBSCRIPTION_EXCLUDED_KEYWORDS: tuple[str, ...] = (
    "plan fee",
    "interest charge",
    "purchase interest",
    "total interest",
    "late fee",
    "annual fee",
    "finance charge",
    "minimum interest",
    "autopay",
    "auto pymt",
    "auto pmt",
    "pymt received",
    "payment received",
    "ach pmt",
    "scheduled transfer",
    "credit crd",
    "creditcard",
    "barclaycard",
    "credit card",
    "health & harmony",
    "juice generation",
    "players theat",
)

TRANSACTION_EXCLUDED_PATTERNS: tuple[str, ...] = (
    "plan fee",
)

# Vendor tokens too generic for metered subscription grouping
METERED_EXCLUDED_TOKENS: frozenset[str] = frozenset({
    "check", "payment", "transfer", "deposit", "withdrawal",
    "fee", "interest", "credit", "debit", "refund",
})

# Categories that are recurring spending patterns but never subscriptions.
# These pass variance filters (fixed fares, consistent grocery runs) but
# are not fixed-price service subscriptions. This is an intentional
# precision-over-recall tradeoff: false positives are costlier than
# occasionally missing a real subscription that can be manually added.
SUBSCRIPTION_EXCLUDED_CATEGORIES: frozenset[str] = frozenset({
    "Payments & Transfers",
    "Transportation",
    "Transit",
    "Groceries",
    "Dining",
    "Coffee",
    "Shopping",
    "Personal Expense",
})
STALENESS_THRESHOLDS = {
    "weekly": 21,
    "biweekly": 42,
    "monthly": 60,
    "quarterly": 120,
    "yearly": 400,
}
MIN_SUBSCRIPTION_INTERVAL_DAYS = 5

TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
_ADJACENT_FREQUENCIES: set[frozenset[str]] = {
    frozenset({"weekly", "biweekly"}),
    frozenset({"biweekly", "monthly"}),
}
_KNOWN_STATES_RE = (
    r"(?:al|ak|az|ar|ca|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|"
    r"ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|"
    r"wa|wv|wi|wy|dc|ab|bc|mb|nb|nl|ns|nt|nu|on|pe|qc|sk|yt)"
)


@dataclass
class RecurringPattern:
    vendor_name: str
    account_id: str | None
    frequency: str
    median_amount_cents: int
    amount_variance: float
    category_id: str | None
    use_type: str | None
    next_expected: str
    transaction_ids: list[str]
    occurrence_count: int


@dataclass
class _ClusterGroup:
    description: str
    clustering_description: str
    account_id: str | None
    transactions: list[sqlite3.Row]
    dominant_category_id: str | None
    inferred_frequency: str | None


def _normalize_for_clustering(description: str) -> str:
    """Aggressive normalization used only by clustering merge predicates."""
    base = normalize_description(description)
    text = base
    text = re.sub(r"https?://|www\.", "", text)
    text = re.sub(r"\.(com|org|net|io|co)\b", "", text)
    text = re.sub(r"[,.*\/]+", " ", text)
    text = re.sub(r"\b\d{3}[- ]?\d{3}[- ]?\d{4}\b", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Trim trailing city/state noise while protecting short suffixes like "tv"/"ai".
    text = re.sub(r"\s+\w+\s+\w+\s+" + _KNOWN_STATES_RE + r"(?:\s+usa?)?\s*$", "", text)
    text = re.sub(r"\s+\w{5,}\s+" + _KNOWN_STATES_RE + r"(?:\s+usa?)?\s*$", "", text)
    text = re.sub(r"\s+" + _KNOWN_STATES_RE + r"(?:\s+usa?)?\s*$", "", text)
    text = text.strip()
    return text or base


def _infer_frequency(interval_days: float) -> str | None:
    best = min(FREQ_TO_DAYS.items(), key=lambda item: abs(item[1] - interval_days))
    if abs(best[1] - interval_days) > 8:
        return None
    return best[0]


def _monthly_equivalent(amount_cents: int, frequency: str) -> int:
    if frequency == "weekly":
        return int(round(amount_cents * 52 / 12))
    if frequency == "biweekly":
        return int(round(amount_cents * 26 / 12))
    if frequency == "monthly":
        return amount_cents
    if frequency == "quarterly":
        return int(round(amount_cents / 3))
    if frequency == "yearly":
        return int(round(amount_cents / 12))
    return amount_cents


def _dominant_non_null(values: list[str | None]) -> str | None:
    non_null = [value for value in values if value]
    if not non_null:
        return None
    return Counter(non_null).most_common(1)[0][0]


def _infer_frequency_from_transactions(txns: list[sqlite3.Row]) -> str | None:
    if len(txns) < 2:
        return None
    dates = sorted(date.fromisoformat(str(txn["date"])) for txn in txns)
    intervals = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates)) if (dates[i] - dates[i - 1]).days > 0]
    intervals = [interval for interval in intervals if interval >= MIN_SUBSCRIPTION_INTERVAL_DAYS]
    if not intervals:
        return None
    return _infer_frequency(float(median(intervals)))


def _build_cluster_group(description: str, account_id: str | None, txns: list[sqlite3.Row]) -> _ClusterGroup:
    sorted_txns = sorted(txns, key=lambda txn: str(txn["date"]))
    return _ClusterGroup(
        description=description,
        clustering_description=_normalize_for_clustering(description),
        account_id=account_id,
        transactions=sorted_txns,
        dominant_category_id=_dominant_non_null([txn["category_id"] for txn in sorted_txns]),
        inferred_frequency=_infer_frequency_from_transactions(sorted_txns),
    )


def _jaccard_similarity(desc_a: str, desc_b: str) -> float:
    tokens_a = set(TOKEN_RE.findall(desc_a))
    tokens_b = set(TOKEN_RE.findall(desc_b))
    if not tokens_a or not tokens_b:
        return 0.0
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    return len(tokens_a & tokens_b) / len(union)


def _first_significant_token(description: str) -> str | None:
    match = TOKEN_RE.search(description)
    if not match:
        return None
    token = match.group(0)
    return token if len(token) >= 3 else None


def _can_merge_groups(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    category_a = group_a.dominant_category_id
    category_b = group_b.dominant_category_id
    return bool(category_a and category_b and category_a == category_b)


def _canonical_pair(description_a: str, description_b: str) -> tuple[str, str]:
    if (len(description_a), description_a) <= (len(description_b), description_b):
        return description_a, description_b
    return description_b, description_a


def _prefix_merge_match(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    shorter, longer = _canonical_pair(group_a.clustering_description, group_b.clustering_description)
    return len(shorter) >= 6 and longer.startswith(shorter)


def _token_merge_match(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    return _jaccard_similarity(group_a.clustering_description, group_b.clustering_description) >= 0.6


def _high_jaccard_merge_match(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    return _jaccard_similarity(group_a.clustering_description, group_b.clustering_description) >= 0.8


def _first_token_merge_match(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    token_a = _first_significant_token(group_a.clustering_description)
    token_b = _first_significant_token(group_b.clustering_description)
    if not token_a or not token_b or token_a != token_b:
        return False
    if not group_a.inferred_frequency and not group_b.inferred_frequency:
        return False

    amounts_a = [abs(int(t["amount_cents"])) for t in group_a.transactions]
    amounts_b = [abs(int(t["amount_cents"])) for t in group_b.transactions]
    if not amounts_a or not amounts_b:
        return False
    med_a = float(median(amounts_a))
    med_b = float(median(amounts_b))
    if med_a <= 0 or med_b <= 0:
        return False
    ratio = max(med_a, med_b) / min(med_a, med_b)

    if group_a.inferred_frequency and group_b.inferred_frequency:
        if group_a.inferred_frequency != group_b.inferred_frequency:
            if frozenset({group_a.inferred_frequency, group_b.inferred_frequency}) not in _ADJACENT_FREQUENCIES:
                return False
            return ratio <= 1.5

    return ratio <= 2.0


def _merge_pass(
    account_id: str | None,
    groups: dict[str, _ClusterGroup],
    predicate,
    merge_gate=None,
) -> dict[str, _ClusterGroup]:
    if merge_gate is None:
        merge_gate = _can_merge_groups
    merged = dict(groups)
    changed = True
    while changed:
        changed = False
        descriptions = sorted(merged.keys(), key=lambda value: (len(value), value))
        for index, desc_a in enumerate(descriptions):
            if desc_a not in merged:
                continue
            for desc_b in descriptions[index + 1 :]:
                if desc_b not in merged:
                    continue

                group_a = merged[desc_a]
                group_b = merged[desc_b]
                if not merge_gate(group_a, group_b):
                    continue
                if not predicate(group_a, group_b):
                    continue

                canonical_desc, source_desc = _canonical_pair(desc_a, desc_b)
                canonical_group = merged[canonical_desc]
                source_group = merged[source_desc]
                merged[canonical_desc] = _build_cluster_group(
                    canonical_desc,
                    account_id,
                    canonical_group.transactions + source_group.transactions,
                )
                del merged[source_desc]
                changed = True
                break
            if changed:
                break
    return merged


def _can_merge_groups_relaxed(group_a: _ClusterGroup, group_b: _ClusterGroup) -> bool:
    """Allow different categories when amounts are still highly similar.

    Same vendor on different import sources often gets different categories
    (e.g., Spotify → Entertainment from Plaid, Personal Expense from CSV).
    But "Amazon" (shopping $27) vs "Amazon Prime Video" ($1.88) are different services.
    """
    # Same category → always OK
    if (group_a.dominant_category_id and group_b.dominant_category_id
            and group_a.dominant_category_id == group_b.dominant_category_id):
        return True
    # Different/null categories → check amount similarity (within 50% of each other)
    amounts_a = [abs(int(t["amount_cents"])) for t in group_a.transactions]
    amounts_b = [abs(int(t["amount_cents"])) for t in group_b.transactions]
    if not amounts_a or not amounts_b:
        return False
    med_a = float(median(amounts_a))
    med_b = float(median(amounts_b))
    if med_a <= 0 or med_b <= 0:
        return False
    ratio = max(med_a, med_b) / min(med_a, med_b)
    return ratio <= 1.5


def _cluster_vendor_groups(
    groups: dict[tuple[str, str | None], list[sqlite3.Row]],
) -> dict[tuple[str, str | None], list[sqlite3.Row]]:
    grouped_by_account: dict[str | None, dict[str, _ClusterGroup]] = defaultdict(dict)
    for (description, account_id), txns in groups.items():
        grouped_by_account[account_id][description] = _build_cluster_group(description, account_id, txns)

    clustered: dict[tuple[str, str | None], list[sqlite3.Row]] = {}
    for account_id, account_groups in grouped_by_account.items():
        # Prefix pass: high confidence, relax category gate (same vendor, different categorization)
        merged = _merge_pass(account_id, account_groups, _prefix_merge_match, _can_merge_groups_relaxed)
        # Token pass: lower confidence, keep strict category gate.
        merged = _merge_pass(account_id, merged, _token_merge_match)
        # High Jaccard pass: allow cross-category matches only when similarity is very high.
        merged = _merge_pass(account_id, merged, _high_jaccard_merge_match, _can_merge_groups_relaxed)
        # First-token pass: relax category gate but enforce frequency/amount constraints.
        merged = _merge_pass(account_id, merged, _first_token_merge_match, _can_merge_groups_relaxed)
        for description, group in merged.items():
            clustered[(description, account_id)] = group.transactions
    return clustered


def _apply_recurring_flags(conn: sqlite3.Connection, recurring_transaction_ids: set[str]) -> None:
    conn.execute(
        """
        UPDATE transactions
           SET is_recurring = 0
         WHERE is_active = 1
           AND amount_cents < 0
           AND is_payment = 0
        """
    )

    if not recurring_transaction_ids:
        return

    recurring_ids = sorted(recurring_transaction_ids)
    chunk_size = 800
    for offset in range(0, len(recurring_ids), chunk_size):
        chunk = recurring_ids[offset : offset + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        conn.execute(
            f"""
            UPDATE transactions
               SET is_recurring = 1
             WHERE id IN ({placeholders})
            """,
            tuple(chunk),
        )


def _is_excluded_subscription_keyword(vendor_name: str) -> bool:
    lowered = vendor_name.lower()
    return any(keyword in lowered for keyword in SUBSCRIPTION_EXCLUDED_KEYWORDS)


def detect_recurring_patterns(conn: sqlite3.Connection) -> list[RecurringPattern]:
    rows = conn.execute(
        """
        SELECT id, account_id, date, description, amount_cents, category_id, use_type
          FROM transactions
         WHERE is_active = 1
           AND amount_cents < 0
           AND is_payment = 0
        ORDER BY date ASC
        """
    ).fetchall()

    groups: dict[tuple[str, str | None], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        normalized = normalize_description(row["description"])
        if not normalized:
            continue
        lowered = normalized.lower()
        if any(pattern in lowered for pattern in TRANSACTION_EXCLUDED_PATTERNS):
            continue
        acct = row["account_id"]
        groups[(normalized, acct)].append(row)

    clustered_groups = _cluster_vendor_groups(groups)
    patterns: list[RecurringPattern] = []
    recurring_transaction_ids: set[str] = set()

    for (pattern, account_id), txns in clustered_groups.items():
        if len(txns) < 2:
            continue

        dates = [date.fromisoformat(str(t["date"])) for t in txns]
        intervals = [(dates[i] - dates[i - 1]).days for i in range(1, len(dates)) if (dates[i] - dates[i - 1]).days > 0]
        intervals = [interval for interval in intervals if interval >= MIN_SUBSCRIPTION_INTERVAL_DAYS]
        if not intervals:
            continue

        median_interval = float(median(intervals))
        frequency = _infer_frequency(median_interval)
        if not frequency:
            continue

        amounts = [abs(int(t["amount_cents"])) for t in txns]
        med_amount = float(median(amounts))
        if med_amount <= 0:
            continue

        mad = float(median(abs(amount - med_amount) for amount in amounts)) / med_amount
        if mad > 0.50:
            continue

        last_date = max(dates)
        next_expected = (last_date + timedelta(days=FREQ_TO_DAYS[frequency])).isoformat()

        category_id = _dominant_non_null([t["category_id"] for t in txns])
        use_type = _dominant_non_null([t["use_type"] for t in txns])

        transaction_ids = [str(t["id"]) for t in txns]
        recurring_transaction_ids.update(transaction_ids)
        patterns.append(
            RecurringPattern(
                vendor_name=pattern.title()[:80],
                account_id=account_id,
                frequency=frequency,
                median_amount_cents=int(round(med_amount)),
                amount_variance=mad,
                category_id=category_id,
                use_type=use_type,
                next_expected=next_expected,
                transaction_ids=transaction_ids,
                occurrence_count=len(txns),
            )
        )

    _apply_recurring_flags(conn, recurring_transaction_ids)
    conn.commit()
    patterns.sort(key=lambda item: (item.vendor_name.lower(), item.account_id or "", item.frequency))
    return patterns


def _build_category_name_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Return {category_id: category_name} for all categories."""
    rows = conn.execute("SELECT id, name FROM categories").fetchall()
    return {row["id"]: row["name"] for row in rows}


def _detect_metered_subscriptions(
    conn: sqlite3.Connection,
    fixed_vendor_names: set[str],
) -> list[RecurringPattern]:
    cat_names = _build_category_name_map(conn)
    fixed_vendor_tokens = {
        token
        for token in (
            _first_significant_token(normalize_description(vendor_name))
            for vendor_name in fixed_vendor_names
        )
        if token
    }

    rows = conn.execute(
        """
        SELECT id, account_id, date, description, amount_cents, category_id, use_type
          FROM transactions
         WHERE is_active = 1
           AND amount_cents < 0
           AND is_payment = 0
        ORDER BY date ASC
        """
    ).fetchall()

    groups: dict[tuple[str, str | None], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        normalized = normalize_description(row["description"])
        if not normalized:
            continue
        lowered = normalized.lower()
        if any(pattern in lowered for pattern in TRANSACTION_EXCLUDED_PATTERNS):
            continue
        if _is_excluded_subscription_keyword(normalized):
            continue

        token = _first_significant_token(normalized)
        if not token or len(token) < 4 or token in fixed_vendor_tokens or token in METERED_EXCLUDED_TOKENS:
            continue

        if _is_excluded_subscription_keyword(token):
            continue

        category_name = cat_names.get(row["category_id"], "")
        if category_name in SUBSCRIPTION_EXCLUDED_CATEGORIES:
            continue

        groups[(token, row["account_id"])].append(row)

    metered_patterns: list[RecurringPattern] = []
    for (token, account_id), txns in groups.items():
        monthly_totals: dict[str, int] = defaultdict(int)
        for txn in txns:
            month = str(txn["date"])[:7]
            monthly_totals[month] += abs(int(txn["amount_cents"]))

        if len(monthly_totals) < 3:
            continue

        monthly_amounts = list(monthly_totals.values())
        median_monthly = float(median(monthly_amounts))
        if median_monthly <= 0:
            continue

        mad = float(median(abs(amount - median_monthly) for amount in monthly_amounts)) / median_monthly
        if mad > 0.50:
            continue

        vendor_name = token.title()[:80]
        if _is_excluded_subscription_keyword(vendor_name):
            continue

        dates = [date.fromisoformat(str(txn["date"])) for txn in txns]
        next_expected = (max(dates) + timedelta(days=FREQ_TO_DAYS["monthly"])).isoformat()
        metered_patterns.append(
            RecurringPattern(
                vendor_name=vendor_name,
                account_id=account_id,
                frequency="monthly",
                median_amount_cents=int(round(median_monthly)),
                amount_variance=mad,
                category_id=_dominant_non_null([txn["category_id"] for txn in txns]),
                use_type=_dominant_non_null([txn["use_type"] for txn in txns]),
                next_expected=next_expected,
                transaction_ids=[str(txn["id"]) for txn in txns],
                occurrence_count=len(monthly_totals),
            )
        )

    metered_patterns.sort(key=lambda item: (item.vendor_name.lower(), item.account_id or "", item.frequency))
    return metered_patterns


def _deactivate_stale_subscriptions(conn: sqlite3.Connection) -> int:
    deactivated = 0
    today = date.today()
    auto_rows = conn.execute(
        """
        SELECT id, vendor_name, frequency, account_id
          FROM subscriptions
         WHERE is_active = 1
           AND is_auto_detected = 1
        """
    ).fetchall()

    for row in auto_rows:
        normalized_vendor = normalize_description(str(row["vendor_name"] or ""))
        token = _first_significant_token(normalized_vendor)
        if not token:
            continue

        frequency = str(row["frequency"] or "monthly")
        staleness_days = STALENESS_THRESHOLDS.get(frequency, STALENESS_THRESHOLDS["monthly"])
        cutoff = (today - timedelta(days=staleness_days)).isoformat()

        # Build exclusion clause for PLAN FEE transactions so they don't
        # keep stale subscriptions looking fresh
        exclusion_clauses = " ".join(
            f"AND lower(description) NOT LIKE '%{pat}%'"
            for pat in TRANSACTION_EXCLUDED_PATTERNS
        )

        if row["account_id"]:
            recent_match = conn.execute(
                f"""
                SELECT 1
                  FROM transactions
                 WHERE is_active = 1
                   AND amount_cents < 0
                   AND is_payment = 0
                   AND account_id = ?
                   AND date >= ?
                   AND lower(description) LIKE ?
                   {exclusion_clauses}
                 LIMIT 1
                """,
                (row["account_id"], cutoff, f"%{token}%"),
            ).fetchone()
        else:
            recent_match = conn.execute(
                f"""
                SELECT 1
                  FROM transactions
                 WHERE is_active = 1
                   AND amount_cents < 0
                   AND is_payment = 0
                   AND date >= ?
                   AND lower(description) LIKE ?
                   {exclusion_clauses}
                 LIMIT 1
                """,
                (cutoff, f"%{token}%"),
            ).fetchone()

        if recent_match:
            continue

        conn.execute("UPDATE subscriptions SET is_active = 0 WHERE id = ?", (row["id"],))
        deactivated += 1

    return deactivated


def detect_subscriptions(conn: sqlite3.Connection) -> dict[str, int]:
    recurring_patterns = detect_recurring_patterns(conn)
    cat_names = _build_category_name_map(conn)
    fixed_patterns = [
        pattern
        for pattern in recurring_patterns
        if pattern.occurrence_count >= 3
        and pattern.amount_variance <= 0.15
        and not _is_excluded_subscription_keyword(pattern.vendor_name)
        and cat_names.get(pattern.category_id, "") not in SUBSCRIPTION_EXCLUDED_CATEGORIES
    ]
    deduped_patterns: list[RecurringPattern] = []
    seen_keys: dict[tuple[str, str, int], int] = {}
    for pattern in fixed_patterns:
        amount_band = int(round(abs(pattern.median_amount_cents) / 500.0) * 500)
        key = (pattern.vendor_name.lower(), pattern.frequency, amount_band)
        existing_index = seen_keys.get(key)
        if existing_index is None:
            seen_keys[key] = len(deduped_patterns)
            deduped_patterns.append(pattern)
            continue
        if pattern.occurrence_count > deduped_patterns[existing_index].occurrence_count:
            deduped_patterns[existing_index] = pattern
    fixed_patterns = deduped_patterns

    fixed_vendor_names = {pattern.vendor_name for pattern in fixed_patterns}
    metered_patterns = _detect_metered_subscriptions(conn, fixed_vendor_names)
    subscription_patterns: list[tuple[RecurringPattern, str]] = (
        [(pattern, "fixed") for pattern in fixed_patterns]
        + [(pattern, "metered") for pattern in metered_patterns]
    )

    inserted = 0
    updated = 0
    metered_inserted = 0
    metered_updated = 0
    detected_keys: set[tuple[str, str, str | None]] = set()

    for pattern, sub_type in subscription_patterns:
        detected_keys.add((pattern.vendor_name, pattern.frequency, pattern.account_id))

        existing = conn.execute(
            """
            SELECT id
              FROM subscriptions
             WHERE vendor_name = ?
               AND frequency = ?
               AND (
                    account_id = ?
                    OR (account_id IS NULL AND ? IS NULL)
               )
               AND is_auto_detected = 1
            """,
            (pattern.vendor_name, pattern.frequency, pattern.account_id, pattern.account_id),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE subscriptions
                   SET amount_cents = ?,
                       category_id = ?,
                       next_expected = ?,
                       use_type = ?,
                       sub_type = ?,
                       is_active = 1
                 WHERE id = ?
                """,
                (
                    pattern.median_amount_cents,
                    pattern.category_id,
                    pattern.next_expected,
                    pattern.use_type,
                    sub_type,
                    existing["id"],
                ),
            )
            updated += 1
            if sub_type == "metered":
                metered_updated += 1
        else:
            conn.execute(
                """
                INSERT INTO subscriptions (
                    id,
                    vendor_name,
                    category_id,
                    amount_cents,
                    frequency,
                    next_expected,
                    account_id,
                    is_active,
                    use_type,
                    sub_type,
                    is_auto_detected
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 1)
                """,
                (
                    uuid.uuid4().hex,
                    pattern.vendor_name,
                    pattern.category_id,
                    pattern.median_amount_cents,
                    pattern.frequency,
                    pattern.next_expected,
                    pattern.account_id,
                    pattern.use_type,
                    sub_type,
                ),
            )
            inserted += 1
            if sub_type == "metered":
                metered_inserted += 1

    deactivated = 0
    stale_auto_rows = conn.execute(
        """
        SELECT id, vendor_name, frequency, account_id
          FROM subscriptions
         WHERE is_active = 1
           AND is_auto_detected = 1
        """
    ).fetchall()
    for row in stale_auto_rows:
        key = (str(row["vendor_name"]), str(row["frequency"]), row["account_id"])
        if key in detected_keys:
            continue
        conn.execute("UPDATE subscriptions SET is_active = 0 WHERE id = ?", (row["id"],))
        deactivated += 1

    deactivated += _deactivate_stale_subscriptions(conn)

    conn.commit()
    recurring_txn_count = len({txn_id for pattern in recurring_patterns for txn_id in pattern.transaction_ids})
    return {
        "inserted": inserted,
        "updated": updated,
        "detected": inserted + updated,
        "metered_inserted": metered_inserted,
        "metered_updated": metered_updated,
        "metered_detected": metered_inserted + metered_updated,
        "deactivated": deactivated,
        "recurring_patterns": len(recurring_patterns),
        "recurring_txns": recurring_txn_count,
    }


def subscription_burn(conn: sqlite3.Connection, view: str = "all") -> dict[str, int]:
    where = ["is_active = 1"]
    if view == "business":
        where.append("use_type = 'Business'")
    elif view == "personal":
        where.append("(use_type = 'Personal' OR use_type IS NULL)")

    rows = conn.execute(
        f"""
        SELECT amount_cents, frequency
          FROM subscriptions
         WHERE {' AND '.join(where)}
        """
    ).fetchall()

    monthly = sum(_monthly_equivalent(int(row["amount_cents"]), row["frequency"]) for row in rows)
    yearly = monthly * 12
    return {
        "active_subscriptions": len(rows),
        "monthly_burn_cents": monthly,
        "yearly_burn_cents": yearly,
    }
