"""Cross-format transaction deduplication helpers."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date as _date

from .categorizer import normalize_description
from .models import normalize_date
from .transaction_heuristics import (
    POSSIBLE_MULTI_PASSENGER_TRAVEL_FLAG,
    duplicate_charge_review_flags,
)

logger = logging.getLogger(__name__)

SOURCE_PRIORITY: dict[str, int] = {
    "manual": 0,
    "csv_import": 1,
    "schwab": 2,
    "stripe": 3,
    "plaid": 4,
    "pdf_import": 5,
}
@dataclass(frozen=True)
class DedupMatch:
    keep_id: str
    remove_id: str
    sources: tuple[str, str]
    date: str
    amount_cents: int
    match_type: str

    def as_dict(self) -> dict[str, object]:
        return {
            "keep_id": self.keep_id,
            "remove_id": self.remove_id,
            "keep_source": self.sources[0],
            "remove_source": self.sources[1],
            "date": self.date,
            "amount_cents": self.amount_cents,
            "match_type": self.match_type,
        }


@dataclass
class DedupReport:
    matches: list[DedupMatch]
    scan_count: int
    elapsed_ms: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "scan_count": self.scan_count,
            "match_count": len(self.matches),
            "elapsed_ms": self.elapsed_ms,
            "matches": [match.as_dict() for match in self.matches],
        }


@dataclass
class SameSourceGroupRow:
    """Per-row detail within a same-source duplicate candidate group."""

    transaction_id: str
    date: str
    description: str
    amount_cents: int
    source: str
    created_at: str
    keep: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "transaction_id": self.transaction_id,
            "date": self.date,
            "description": self.description,
            "amount_cents": self.amount_cents,
            "source": self.source,
            "created_at": self.created_at,
            "keep": self.keep,
        }


@dataclass
class SameSourceGroup:
    account_id: str
    institution_name: str
    card_ending: str
    date: str
    amount_cents: int
    normalized_desc: str
    count: int
    excess: int
    rows: list[SameSourceGroupRow]
    suspicion: str
    review_flags: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "account_id": self.account_id,
            "institution_name": self.institution_name,
            "card_ending": self.card_ending,
            "date": self.date,
            "amount_cents": self.amount_cents,
            "normalized_desc": self.normalized_desc,
            "count": self.count,
            "excess": self.excess,
            "rows": [row.as_dict() for row in self.rows],
            "suspicion": self.suspicion,
            "review_flags": list(self.review_flags),
        }


def _source_rank(source: object) -> int:
    return SOURCE_PRIORITY.get(str(source or ""), 99)


def _descriptions_match(desc_a: str, desc_b: str) -> str | None:
    norm_a = normalize_description(desc_a or "")
    norm_b = normalize_description(desc_b or "")
    if len(norm_a) < 3 or len(norm_b) < 3:
        return None
    if norm_a == norm_b:
        return "exact"
    if norm_a in norm_b or norm_b in norm_a:
        return "substring"
    return None


def _has_meaningful_description(desc: str) -> bool:
    return len(normalize_description(desc or "")) >= 3


def _date_offset_days(date_a: str, date_b: str) -> int:
    """Absolute day difference between two ISO date strings.
    Returns 999 on parse failure to safely skip non-ISO dates."""
    try:
        return abs((_date.fromisoformat(date_a) - _date.fromisoformat(date_b)).days)
    except (ValueError, TypeError):
        return 999


def _pick_keeper(txn_a: sqlite3.Row, txn_b: sqlite3.Row) -> tuple[sqlite3.Row, sqlite3.Row]:
    a_key = (_source_rank(txn_a["source"]), str(txn_a["id"]))
    b_key = (_source_rank(txn_b["source"]), str(txn_b["id"]))
    if a_key <= b_key:
        return txn_a, txn_b
    return txn_b, txn_a


def _same_source_review_flags(count: int, normalized_desc: str) -> tuple[str, ...]:
    return duplicate_charge_review_flags(count, normalized_desc)


def _same_source_suspicion(
    count: int,
    amount_cents: int,
    review_flags: tuple[str, ...] = (),
) -> str:
    abs_amount = abs(int(amount_cents))
    if POSSIBLE_MULTI_PASSENGER_TRAVEL_FLAG in review_flags:
        return "medium"
    if count == 2 and abs_amount > 1000:
        return "high"
    if count == 2 or abs_amount > 1000:
        return "medium"
    return "low"


def find_same_source_duplicates(
    conn: sqlite3.Connection,
    account_id: str | None = None,
    min_amount_cents: int = 0,
) -> list[SameSourceGroup]:
    alias_rows = conn.execute(
        "SELECT hash_account_id, canonical_id FROM account_aliases"
    ).fetchall()
    alias_map: dict[str, str] = {
        str(row["hash_account_id"]): str(row["canonical_id"])
        for row in alias_rows
    }
    reverse_aliases: dict[str, set[str]] = defaultdict(set)
    for hash_account_id, canonical_id in alias_map.items():
        reverse_aliases[canonical_id].add(hash_account_id)

    where = [
        "is_active = 1",
        "account_id IS NOT NULL",
        "source = 'csv_import'",
        "parent_transaction_id IS NULL",
    ]
    params: list[object] = []
    if account_id:
        canonical = alias_map.get(account_id, account_id)
        effective_ids = {account_id, canonical}
        effective_ids.update(reverse_aliases.get(canonical, set()))
        placeholders = ", ".join("?" for _ in sorted(effective_ids))
        where.append(f"account_id IN ({placeholders})")
        params.extend(sorted(effective_ids))

    rows = conn.execute(
        f"""
        SELECT id, account_id, date, amount_cents, description, source, created_at
          FROM transactions
         WHERE {' AND '.join(where)}
         ORDER BY account_id, date, amount_cents, description, created_at, id
        """,
        tuple(params),
    ).fetchall()

    account_ids = {str(row["account_id"]) for row in rows}
    resolved_account_ids = {alias_map.get(account_id_value, account_id_value) for account_id_value in account_ids}
    account_ids.update(resolved_account_ids)
    account_lookup: dict[str, sqlite3.Row] = {}
    if account_ids:
        placeholders = ", ".join("?" for _ in sorted(account_ids))
        account_rows = conn.execute(
            f"""
            SELECT id, institution_name, card_ending
              FROM accounts
             WHERE id IN ({placeholders})
            """,
            tuple(sorted(account_ids)),
        ).fetchall()
        account_lookup = {str(row["id"]): row for row in account_rows}

    grouped: dict[tuple[str, str, int], list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        resolved_account_id = alias_map.get(str(row["account_id"]), str(row["account_id"]))
        norm_date = normalize_date(str(row["date"]))
        key = (resolved_account_id, norm_date, int(row["amount_cents"]))
        grouped[key].append(row)

    threshold = max(0, int(min_amount_cents or 0))
    groups: list[SameSourceGroup] = []
    for (resolved_account_id, norm_date, amount_cents), bucket in grouped.items():
        if abs(amount_cents) < threshold:
            continue

        by_description: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in bucket:
            by_description[normalize_description(str(row["description"] or ""))].append(row)

        for norm_desc, desc_rows in by_description.items():
            if len(desc_rows) <= 1:
                continue

            ordered_rows = sorted(
                desc_rows,
                key=lambda row: (str(row["created_at"] or ""), str(row["id"])),
            )
            group_rows = [
                SameSourceGroupRow(
                    transaction_id=str(row["id"]),
                    date=str(row["date"]),
                    description=str(row["description"] or ""),
                    amount_cents=int(row["amount_cents"]),
                    source=str(row["source"] or ""),
                    created_at=str(row["created_at"] or ""),
                    keep=idx == 0,
                )
                for idx, row in enumerate(ordered_rows)
            ]

            account_row = account_lookup.get(resolved_account_id)
            if account_row is None and ordered_rows:
                account_row = account_lookup.get(str(ordered_rows[0]["account_id"]))

            review_flags = _same_source_review_flags(len(group_rows), norm_desc)
            groups.append(
                SameSourceGroup(
                    account_id=resolved_account_id,
                    institution_name=str(account_row["institution_name"] or "") if account_row else "",
                    card_ending=str(account_row["card_ending"] or "") if account_row else "",
                    date=norm_date,
                    amount_cents=amount_cents,
                    normalized_desc=norm_desc,
                    count=len(group_rows),
                    excess=len(group_rows) - 1,
                    rows=group_rows,
                    suspicion=_same_source_suspicion(len(group_rows), amount_cents, review_flags),
                    review_flags=review_flags,
                )
            )

    suspicion_rank = {"high": 0, "medium": 1, "low": 2}
    groups.sort(
        key=lambda group: (
            suspicion_rank.get(group.suspicion, 99),
            -abs(group.amount_cents),
            group.account_id,
            group.date,
            group.normalized_desc,
        )
    )
    return groups


def find_cross_format_duplicates(
    conn: sqlite3.Connection,
    account_id: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> DedupReport:
    started_at = time.perf_counter()
    alias_rows = conn.execute(
        "SELECT hash_account_id, canonical_id FROM account_aliases"
    ).fetchall()
    alias_map: dict[str, str] = {
        str(row["hash_account_id"]): str(row["canonical_id"])
        for row in alias_rows
    }
    reverse_aliases: dict[str, set[str]] = defaultdict(set)
    for hash_account_id, canonical_id in alias_map.items():
        reverse_aliases[canonical_id].add(hash_account_id)
    logger.debug("Loaded account alias map aliases=%s canonical_accounts=%s", len(alias_map), len(reverse_aliases))

    source_placeholders = ", ".join("?" for _ in SOURCE_PRIORITY)
    where = [
        "is_active = 1",
        "account_id IS NOT NULL",
        f"source IN ({source_placeholders})",
    ]
    params: list[object] = list(SOURCE_PRIORITY.keys())
    if account_id:
        canonical = alias_map.get(account_id, account_id)
        effective_ids = {account_id, canonical}
        effective_ids.update(reverse_aliases.get(canonical, set()))
        placeholders = ", ".join("?" for _ in sorted(effective_ids))
        where.append(f"account_id IN ({placeholders})")
        params.extend(sorted(effective_ids))

    # Fetch all active transactions from tracked sources, then group in Python
    # with date normalization (CSV uses MM/DD/YYYY, PDF uses YYYY-MM-DD).
    row_query = f"""
        SELECT id, account_id, date, amount_cents, description, source
          FROM transactions
         WHERE {' AND '.join(where)}
         ORDER BY account_id, date, amount_cents, id
    """
    rows = conn.execute(row_query, tuple(params)).fetchall()
    norm_date_from = normalize_date(date_from) if date_from else None
    norm_date_to = normalize_date(date_to) if date_to else None

    # Group by (account_id, normalized_date, amount_cents)
    grouped: dict[tuple[str, str, int], list[sqlite3.Row]] = {}
    for row in rows:
        norm_date = normalize_date(str(row["date"]))
        if norm_date_from and norm_date < norm_date_from:
            continue
        if norm_date_to and norm_date > norm_date_to:
            continue
        resolved_account_id = alias_map.get(str(row["account_id"]), str(row["account_id"]))
        key = (resolved_account_id, norm_date, int(row["amount_cents"]))
        grouped.setdefault(key, []).append(row)

    all_grouped = dict(grouped)  # snapshot before multi-source filter

    # Filter to groups with multiple distinct sources
    grouped = {
        k: v for k, v in grouped.items()
        if len(set(str(r["source"]) for r in v)) > 1
    }
    fuzzy_merged_keys: set[tuple[str, str, int]] = set()

    # --- Neighbor-merge pass for +/-2 day date tolerance ---
    # Only merge leftover single-source groups with each other.
    # No interaction with exact-match groups - keeps logic simple and consistent.
    single_source = {k: v for k, v in all_grouped.items() if k not in grouped}

    by_acct_amt: dict[tuple[str, int], list[tuple[str, set[str], list[sqlite3.Row]]]] = defaultdict(list)
    for (acct, dt, amt), txns in single_source.items():
        sources = {str(r["source"]) for r in txns}
        by_acct_amt[(acct, amt)].append((dt, sources, txns))

    claimed_neighbor_keys: set[tuple[str, str, int]] = set()
    for (acct, amt), date_groups in by_acct_amt.items():
        for i, (dt_a, src_a, txns_a) in enumerate(date_groups):
            if (acct, dt_a, amt) in claimed_neighbor_keys:
                continue
            for j, (dt_b, src_b, txns_b) in enumerate(date_groups):
                if j == i:
                    continue
                if (acct, dt_b, amt) in claimed_neighbor_keys:
                    continue
                if _date_offset_days(dt_a, dt_b) > 2:
                    continue
                if src_a & src_b:
                    continue
                # Description gate: require cross-source match with >=8 char descriptions.
                has_desc_match = False
                for ta in txns_a:
                    norm_a = normalize_description(str(ta["description"]))
                    if len(norm_a) < 8:
                        continue
                    for tb in txns_b:
                        norm_b = normalize_description(str(tb["description"]))
                        if len(norm_b) < 8:
                            continue
                        if str(ta["source"]) != str(tb["source"]):
                            if _descriptions_match(str(ta["description"]), str(tb["description"])):
                                has_desc_match = True
                                break
                    if has_desc_match:
                        break
                if not has_desc_match:
                    continue
                # Merge into grouped for processing.
                merged_key = (acct, min(dt_a, dt_b), amt)
                grouped[merged_key] = txns_a + txns_b
                fuzzy_merged_keys.add(merged_key)
                claimed_neighbor_keys.add((acct, dt_a, amt))
                claimed_neighbor_keys.add((acct, dt_b, amt))
                break  # dt_a claimed, move to the next i

    matches: list[DedupMatch] = []
    scan_count = sum(len(v) for v in grouped.values())
    logger.info(
        "Cross-format dedup scan prepared scan_count=%s candidate_groups=%s account_filter=%s",
        scan_count,
        len(grouped),
        account_id or "",
    )
    for key in sorted(grouped.keys()):
        norm_date = key[1]
        group = sorted(grouped[key], key=lambda row: (_source_rank(row["source"]), str(row["id"])))
        source_counts = Counter(str(row["source"]) for row in group)
        unambiguous_group = all(count == 1 for count in source_counts.values())
        claimed: set[str] = set()
        for row in group:
            row_id = str(row["id"])
            if row_id in claimed:
                continue

            row_rank = _source_rank(row["source"])
            candidates = [
                candidate
                for candidate in group
                if str(candidate["id"]) not in claimed
                and str(candidate["id"]) != row_id
                and _source_rank(candidate["source"]) > row_rank
            ]
            if not candidates:
                continue

            if unambiguous_group:
                if key in fuzzy_merged_keys:
                    # For fuzzy-date merged groups, enforce >=8 char descriptions per match.
                    if len(normalize_description(str(row["description"]))) < 8:
                        continue
                    candidates = [
                        c
                        for c in candidates
                        if len(normalize_description(str(c["description"]))) >= 8
                    ]
                    if not candidates:
                        continue
                for candidate in sorted(candidates, key=lambda c: (_source_rank(c["source"]), str(c["id"]))):
                    keeper, duplicate = _pick_keeper(row, candidate)
                    match_type = _descriptions_match(str(row["description"]), str(candidate["description"]))
                    if match_type is None:
                        if not (
                            _has_meaningful_description(str(row["description"]))
                            and _has_meaningful_description(str(candidate["description"]))
                        ):
                            continue
                        match_type = "key_only"
                    claimed.add(str(duplicate["id"]))
                    matches.append(
                        DedupMatch(
                            keep_id=str(keeper["id"]),
                            remove_id=str(duplicate["id"]),
                            sources=(str(keeper["source"]), str(duplicate["source"])),
                            date=norm_date,
                            amount_cents=int(keeper["amount_cents"]),
                            match_type=match_type,
                        )
                    )
                    logger.debug(
                        "Cross-format match keep_id=%s remove_id=%s keep_source=%s remove_source=%s match_type=%s",
                        str(keeper["id"]),
                        str(duplicate["id"]),
                        str(keeper["source"]),
                        str(duplicate["source"]),
                        match_type,
                    )
                continue

            best_match: sqlite3.Row | None = None
            best_type: str | None = None
            best_key: tuple[int, str] | None = None
            if key in fuzzy_merged_keys:
                if len(normalize_description(str(row["description"]))) < 8:
                    continue
            for candidate in candidates:
                candidate_id = str(candidate["id"])
                if key in fuzzy_merged_keys and len(normalize_description(str(candidate["description"]))) < 8:
                    continue
                match_type = _descriptions_match(str(row["description"]), str(candidate["description"]))
                if not match_type:
                    continue
                score = 0 if match_type == "exact" else 1
                tie_key = (score, candidate_id)
                if best_key is None or tie_key < best_key:
                    best_key = tie_key
                    best_match = candidate
                    best_type = match_type

            if best_match is None or best_type is None:
                continue

            keeper, duplicate = _pick_keeper(row, best_match)
            claimed.add(str(keeper["id"]))
            claimed.add(str(duplicate["id"]))
            matches.append(
                DedupMatch(
                    keep_id=str(keeper["id"]),
                    remove_id=str(duplicate["id"]),
                    sources=(str(keeper["source"]), str(duplicate["source"])),
                    date=norm_date,
                    amount_cents=int(keeper["amount_cents"]),
                    match_type=best_type,
                )
            )
            logger.debug(
                "Cross-format match keep_id=%s remove_id=%s keep_source=%s remove_source=%s match_type=%s",
                str(keeper["id"]),
                str(duplicate["id"]),
                str(keeper["source"]),
                str(duplicate["source"]),
                best_type,
            )

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "Cross-format dedup scan complete matches=%s elapsed_ms=%s",
        len(matches),
        elapsed_ms,
    )
    return DedupReport(matches=matches, scan_count=scan_count, elapsed_ms=elapsed_ms)


def apply_dedup(
    conn: sqlite3.Connection,
    report: DedupReport,
    exclude_match_types: set[str] | None = None,
) -> int:
    removed = 0
    skipped = 0
    excluded = exclude_match_types or set()
    for match in report.matches:
        if match.match_type in excluded:
            skipped += 1
            logger.debug(
                "Skipped dedup removal remove_id=%s keep_id=%s match_type=%s",
                match.remove_id,
                match.keep_id,
                match.match_type,
            )
            continue

        reason = f"cross-format-dedup: kept {match.keep_id} ({match.sources[0]})"
        cursor = conn.execute(
            """
            UPDATE transactions
               SET is_active = 0,
                   removed_at = datetime('now'),
                   notes = COALESCE(notes || ' | ', '') || ?,
                   updated_at = datetime('now')
             WHERE id = ? AND is_active = 1
            """,
            (reason, match.remove_id),
        )
        removed_count = int(cursor.rowcount or 0)
        removed += removed_count
        logger.debug(
            "Applied dedup removal remove_id=%s keep_id=%s rowcount=%s",
            match.remove_id,
            match.keep_id,
            removed_count,
        )

    if removed:
        conn.commit()
    logger.info(
        "Cross-format dedup apply complete removed=%s skipped=%s matches=%s",
        removed,
        skipped,
        len(report.matches),
    )
    return removed


def apply_same_source_dedup(
    conn: sqlite3.Connection,
    ids: list[str],
    candidate_ids: set[str],
    groups: list[SameSourceGroup],
) -> tuple[int, list[tuple[str, str]]]:
    rejected: list[tuple[str, str]] = []
    valid_ids: list[str] = []

    for txn_id in ids:
        if txn_id not in candidate_ids:
            rejected.append((txn_id, "not in candidate set"))
            continue
        row = conn.execute(
            """
            SELECT 1
              FROM transactions
             WHERE id = ?
               AND is_active = 1
               AND source = 'csv_import'
            """,
            (txn_id,),
        ).fetchone()
        if row is None:
            rejected.append((txn_id, "not active csv_import"))
            continue
        valid_ids.append(txn_id)

    if not valid_ids:
        return 0, rejected

    group_by_id: dict[str, tuple[int, SameSourceGroup, SameSourceGroupRow]] = {}
    for group_index, group in enumerate(groups):
        for row in group.rows:
            group_by_id[row.transaction_id] = (group_index, group, row)

    valid_by_group: dict[int, list[str]] = defaultdict(list)
    applicable_ids: list[str] = []
    for txn_id in valid_ids:
        mapping = group_by_id.get(txn_id)
        if mapping is None:
            rejected.append((txn_id, "not in candidate set"))
            continue
        valid_by_group[mapping[0]].append(txn_id)
        applicable_ids.append(txn_id)

    group_rejections: set[str] = set()
    for group_index, group_ids in valid_by_group.items():
        group = groups[group_index]
        if not group_ids:
            continue
        member_ids = [row.transaction_id for row in group.rows]
        placeholders = ", ".join("?" for _ in member_ids)
        current_active_row = conn.execute(
            f"""
            SELECT COUNT(*) AS n
              FROM transactions
             WHERE id IN ({placeholders})
               AND is_active = 1
            """,
            tuple(member_ids),
        ).fetchone()
        current_active = int(current_active_row["n"] if current_active_row else 0)
        deactivation_count = len(group_ids)
        if current_active - deactivation_count > 0:
            continue

        needed_rejections = deactivation_count - (current_active - 1)
        row_lookup = {row.transaction_id: row for row in group.rows}
        row_position = {
            row.transaction_id: idx
            for idx, row in enumerate(group.rows)
        }
        ordered_ids = sorted(
            group_ids,
            key=lambda txn_id: (
                0 if row_lookup[txn_id].keep else 1,
                row_position[txn_id],
            ),
        )
        for txn_id in ordered_ids[:needed_rejections]:
            group_rejections.add(txn_id)
            rejected.append((txn_id, "would remove last surviving row from group"))

    deactivated = 0
    for txn_id in applicable_ids:
        if txn_id in group_rejections:
            continue
        cursor = conn.execute(
            """
            UPDATE transactions
               SET is_active = 0,
                   notes = COALESCE(notes || ' | ', '') || 'same-source-dedup',
                   removed_at = datetime('now'),
                   updated_at = datetime('now')
             WHERE id = ?
               AND is_active = 1
               AND source = 'csv_import'
            """,
            (txn_id,),
        )
        deactivated += int(cursor.rowcount or 0)

    if deactivated:
        conn.commit()
    return deactivated, rejected


__all__ = [
    "DedupMatch",
    "DedupReport",
    "SameSourceGroup",
    "SameSourceGroupRow",
    "SOURCE_PRIORITY",
    "_descriptions_match",
    "_pick_keeper",
    "apply_same_source_dedup",
    "find_cross_format_duplicates",
    "find_same_source_duplicates",
    "apply_dedup",
]
