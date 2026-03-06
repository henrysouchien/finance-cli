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

    # --- Neighbor-merge pass for +/-1 day date tolerance ---
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
                if _date_offset_days(dt_a, dt_b) > 1:
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


__all__ = [
    "DedupMatch",
    "DedupReport",
    "SOURCE_PRIORITY",
    "_descriptions_match",
    "_pick_keeper",
    "find_cross_format_duplicates",
    "apply_dedup",
]
