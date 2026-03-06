"""Deduplication commands."""

from __future__ import annotations

import logging
import re
from collections import Counter
from itertools import combinations
from typing import Any

from ..db import backup_database
from ..dedup import DedupMatch, apply_dedup, find_cross_format_duplicates
from ..importers import _INSTITUTION_EQUIVALENTS, backfill_account_aliases, upsert_account_alias
from ..institution_names import canonicalize, is_known, similar_names

logger = logging.getLogger(__name__)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("dedup", parents=[format_parent], help="Deduplication commands")
    dedup_sub = parser.add_subparsers(dest="dedup_command", required=True)

    p_cross = dedup_sub.add_parser(
        "cross-format",
        parents=[format_parent],
        help="Find and optionally remove cross-format duplicate transactions",
    )
    p_cross.add_argument("--account-id")
    p_cross.add_argument("--from", dest="date_from")
    p_cross.add_argument("--to", dest="date_to")
    p_cross.add_argument("--commit", action="store_true", help="Apply dedup by deactivating duplicates")
    p_cross.add_argument(
        "--include-key-only",
        action="store_true",
        help="Include risky key_only matches when committing dedup changes",
    )
    p_cross.set_defaults(func=handle_cross_format, command_name="dedup.cross-format")

    p_backfill = dedup_sub.add_parser(
        "backfill-aliases",
        parents=[format_parent],
        help="Link hash-based accounts to canonical Plaid accounts",
    )
    p_backfill.add_argument("--commit", action="store_true", help="Persist alias changes")
    p_backfill.set_defaults(func=handle_backfill_aliases, command_name="dedup.backfill-aliases")

    p_audit_names = dedup_sub.add_parser(
        "audit-names",
        parents=[format_parent],
        help="Audit institution naming and account-linking gaps",
    )
    p_audit_names.set_defaults(func=handle_audit_names, command_name="dedup.audit-names")

    p_create_alias = dedup_sub.add_parser(
        "create-alias",
        parents=[format_parent],
        help="Create or update a manual account alias from hash account to Plaid account",
    )
    p_create_alias.add_argument("--from", dest="from_id", required=True, help="Hash account ID to alias from")
    p_create_alias.add_argument("--to", dest="to_id", required=True, help="Canonical Plaid account ID to alias to")
    p_create_alias.add_argument("--commit", action="store_true", help="Persist alias changes")
    p_create_alias.set_defaults(func=handle_create_alias, command_name="dedup.create-alias")

    p_suggest_aliases = dedup_sub.add_parser(
        "suggest-aliases",
        parents=[format_parent],
        help="Suggest manual alias candidates for unlinked hash accounts",
    )
    p_suggest_aliases.set_defaults(func=handle_suggest_aliases, command_name="dedup.suggest-aliases")

    p_detect_equiv = dedup_sub.add_parser(
        "detect-equivalences",
        parents=[format_parent],
        help="Detect potential institution equivalence pairs from overlapping transactions",
    )
    p_detect_equiv.add_argument(
        "--min-overlap",
        type=int,
        default=3,
        help="Minimum overlapping (date, amount) matches required to emit a suggestion",
    )
    p_detect_equiv.set_defaults(func=handle_detect_equivalences, command_name="dedup.detect-equivalences")

    p_review_key_only = dedup_sub.add_parser(
        "review-key-only",
        parents=[format_parent],
        help="Review key_only cross-format matches with enriched transaction details",
    )
    p_review_key_only.add_argument("--account-id")
    p_review_key_only.add_argument("--from", dest="date_from")
    p_review_key_only.add_argument("--to", dest="date_to")
    p_review_key_only.set_defaults(func=handle_review_key_only, command_name="dedup.review-key-only")


def _build_cli_report(
    matches: list[dict[str, object]],
    *,
    commit: bool,
    removed: int,
    include_key_only: bool,
    elapsed_ms: int,
) -> str:
    mode = "commit" if commit else "dry-run"
    if not matches:
        return f"No cross-format duplicates found ({mode}) elapsed={elapsed_ms}ms"

    key_only_count = sum(1 for item in matches if str(item.get("match_type")) == "key_only")
    lines = [f"Cross-format duplicates ({mode}): {len(matches)} match(es), removed={removed} elapsed={elapsed_ms}ms"]
    if key_only_count and not commit:
        lines.append(
            f"\u26a0 {key_only_count} key_only match(es) - will be skipped unless --include-key-only is used with --commit"
        )
    elif key_only_count and commit and not include_key_only:
        lines.append(f"\u26a0 {key_only_count} key_only match(es) skipped (use --include-key-only to apply)")
    for item in matches[:50]:
        amount = int(item["amount_cents"]) / 100
        lines.append(
            (
                f"{item['date']} | {amount:.2f} | keep={item['keep_source']}:{item['keep_id']} "
                f"remove={item['remove_source']}:{item['remove_id']} | {item['match_type']}"
            )
        )
    if len(matches) > 50:
        lines.append(f"... {len(matches) - 50} more")
    return "\n".join(lines)


def _build_backfill_cli_report(report: dict[str, int], *, commit: bool) -> str:
    mode = "commit" if commit else "dry-run"
    return (
        f"Alias backfill ({mode}): scanned={report['scanned']} "
        f"aliased={report['aliased']} removed={report['removed']} unchanged={report['unchanged']}"
    )


def _scrub_card_ending(card_ending: str | None) -> str | None:
    value = str(card_ending or "").strip()
    if value and re.fullmatch(r"\d{4}", value):
        return value
    return None


def _build_audit_cli_report(issues: list[dict[str, object]]) -> str:
    if not issues:
        return "Institution audit: no issues found"
    counts = Counter(str(item["type"]) for item in issues)
    pieces = [f"{key}={counts[key]}" for key in sorted(counts)]
    return f"Institution audit: {len(issues)} issue(s) ({', '.join(pieces)})"


def _account_description(row) -> dict[str, object]:
    return {
        "id": str(row["id"]),
        "institution_name": str(row["institution_name"] or ""),
        "account_name": str(row["account_name"] or ""),
        "account_type": str(row["account_type"] or ""),
        "card_ending": _scrub_card_ending(str(row["card_ending"] or "")),
        "plaid_account_id": str(row["plaid_account_id"] or "") or None,
        "is_active": int(row["is_active"] or 0),
    }


def _build_create_alias_cli_report(
    *,
    from_id: str,
    to_id: str,
    commit: bool,
    no_op: bool,
    replaced_canonical_id: str | None,
) -> str:
    mode = "commit" if commit else "dry-run"
    action = "noop"
    if not no_op and replaced_canonical_id:
        action = "replace"
    elif not no_op:
        action = "create"
    line = f"Create alias ({mode}): from={from_id} to={to_id} action={action}"
    if replaced_canonical_id:
        line += f" replaced={replaced_canonical_id}"
    return line


_REASON_PRIORITY: dict[str, int] = {
    "equivalent_institution_matching_card_ending": 0,
    "equivalent_institution_same_type": 0,
    "same_institution_and_type_missing_card_ending": 1,
    "similar_name_same_type": 2,
}


def _reason_sort_key(reason: str) -> tuple[int, str]:
    return (_REASON_PRIORITY.get(reason, 99), reason)


def _candidate_sort_key(candidate: dict[str, object]) -> tuple[int, str]:
    reasons = [str(item) for item in candidate.get("reasons", [])]
    best = min((_REASON_PRIORITY.get(reason, 99) for reason in reasons), default=99)
    return (best, str(candidate["plaid_account_id"]))


def _build_suggest_aliases_cli_report(suggestions: list[dict[str, object]]) -> str:
    if not suggestions:
        return "Alias suggestions: 0 unlinked hash account(s) with candidates"

    lines = [f"Alias suggestions: {len(suggestions)} unlinked hash account(s) with candidates", ""]
    for suggestion in suggestions:
        lines.append(
            "  "
            f"{suggestion['hash_account_id']} "
            f"({suggestion['institution_name']} / {suggestion['account_type']}) "
            f"[{suggestion['txn_count']} txns]"
        )
        for candidate in suggestion["candidates"]:
            card = candidate["card_ending"] or "none"
            reasons = ", ".join(candidate["reasons"])
            lines.append(
                "    "
                f"-> {candidate['plaid_account_id']} "
                f"({candidate['institution_name']} / {candidate['account_type']} / card={card}) "
                f"[{candidate['txn_count']} txns] reasons=[{reasons}]"
            )
    return "\n".join(lines)


def _build_detect_equivalences_cli_report(candidates: list[dict[str, object]]) -> str:
    if not candidates:
        return "Detected 0 candidate equivalence(s)"

    lines = [f"Detected {len(candidates)} candidate equivalence(s):"]
    for item in candidates:
        lines.append(
            "  "
            f"{item['institution_a']} <-> {item['institution_b']} "
            f"(card_ending={item['card_ending']}, overlap={item['overlap_count']} txns)"
        )
    lines.append("")
    lines.append("To apply, add entries to _INSTITUTION_EQUIVALENTS in finance_cli/importers/__init__.py")
    return "\n".join(lines)


def _is_existing_equivalence_pair(institution_a: str, institution_b: str) -> bool:
    a = str(institution_a or "").strip()
    b = str(institution_b or "").strip()
    if not a or not b:
        return False

    equivalents_a = {str(value) for value in _INSTITUTION_EQUIVALENTS.get(a, [])}
    equivalents_b = {str(value) for value in _INSTITUTION_EQUIVALENTS.get(b, [])}
    return (b in equivalents_a) or (a in equivalents_b)


def _count_transaction_overlap(
    conn,
    account_ids_a: list[str],
    account_ids_b: list[str],
) -> int:
    if not account_ids_a or not account_ids_b:
        return 0

    placeholders_a = ", ".join("?" for _ in account_ids_a)
    placeholders_b = ", ".join("?" for _ in account_ids_b)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS overlap_count
          FROM (
                SELECT t1.date, t1.amount_cents
                  FROM transactions t1
                 WHERE t1.is_active = 1
                   AND t1.account_id IN ({placeholders_a})
                   AND EXISTS (
                       SELECT 1
                         FROM transactions t2
                        WHERE t2.is_active = 1
                          AND t2.account_id IN ({placeholders_b})
                          AND t2.date = t1.date
                          AND t2.amount_cents = t1.amount_cents
                   )
                 GROUP BY t1.date, t1.amount_cents
               )
        """,
        (*account_ids_a, *account_ids_b),
    ).fetchone()
    return int(row["overlap_count"] if row else 0)


def _short_id(value: str) -> str:
    return value[:8]


def _enrich_key_only_matches(
    conn,
    key_only_matches: list[DedupMatch],
    *,
    chunk_size: int = 500,
) -> list[dict[str, object]]:
    if not key_only_matches:
        return []

    txn_ids = sorted({match.keep_id for match in key_only_matches} | {match.remove_id for match in key_only_matches})
    lookup: dict[str, dict[str, str]] = {}

    for start in range(0, len(txn_ids), chunk_size):
        chunk = txn_ids[start:start + chunk_size]
        placeholders = ", ".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT
                t.id,
                t.description,
                t.source,
                a.institution_name,
                a.account_name,
                a.account_type,
                a.card_ending
              FROM transactions t
              JOIN accounts a ON t.account_id = a.id
             WHERE t.id IN ({placeholders})
            """,
            tuple(chunk),
        ).fetchall()
        for row in rows:
            lookup[str(row["id"])] = {
                "id": str(row["id"]),
                "source": str(row["source"] or ""),
                "description": str(row["description"] or ""),
                "institution": str(row["institution_name"] or ""),
                "account": str(row["account_name"] or ""),
                "account_type": str(row["account_type"] or ""),
            }

    enriched: list[dict[str, object]] = []
    for match in key_only_matches:
        keep = lookup.get(match.keep_id)
        remove = lookup.get(match.remove_id)
        if not keep or not remove:
            logger.warning(
                "Skipping key-only review match due to missing transaction enrichment keep_id=%s remove_id=%s",
                match.keep_id,
                match.remove_id,
            )
            continue
        enriched.append(
            {
                "date": match.date,
                "amount_cents": int(match.amount_cents),
                "amount": f"{int(match.amount_cents) / 100:.2f}",
                "keep": keep,
                "remove": remove,
            }
        )

    return enriched


def _build_review_key_only_cli_report(
    matches: list[dict[str, object]],
    *,
    total_key_only: int,
    skipped: int,
    account_id: str | None,
    date_from: str | None,
    date_to: str | None,
) -> str:
    if total_key_only == 0:
        return "No key-only matches found"

    lines = [f"Key-only matches: {total_key_only} pending review"]
    if skipped:
        lines.append(f"Skipped {skipped} match(es) due to missing enrichment data")

    for idx, item in enumerate(matches[:50], start=1):
        keep = item["keep"]
        remove = item["remove"]
        lines.append("")
        lines.append(f"  #{idx}  {item['date']} | ${item['amount']}")
        lines.append(
            "      "
            f"KEEP   [{keep['source']}] "
            f"{keep['institution']} / {keep['account']}  ({_short_id(str(keep['id']))})"
        )
        lines.append(f"             {keep['description']}")
        lines.append(
            "      "
            f"REMOVE [{remove['source']}] "
            f"{remove['institution']} / {remove['account']}  ({_short_id(str(remove['id']))})"
        )
        lines.append(f"             {remove['description']}")

    if len(matches) > 50:
        lines.append("")
        lines.append(f"... {len(matches) - 50} more (use --format json for full output)")

    apply_cmd = ["dedup cross-format"]
    if account_id:
        apply_cmd.append(f"--account-id {account_id}")
    if date_from:
        apply_cmd.append(f"--from {date_from}")
    if date_to:
        apply_cmd.append(f"--to {date_to}")
    apply_cmd.extend(["--include-key-only", "--commit"])
    lines.append("")
    lines.append(f"To apply: {' '.join(apply_cmd)}")
    return "\n".join(lines)


def handle_cross_format(args, conn) -> dict[str, Any]:
    report = find_cross_format_duplicates(
        conn,
        account_id=args.account_id,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    removed = 0
    backup_path: str | None = None
    if args.commit:
        if report.matches:
            backup_path = str(backup_database(conn=conn))
        exclude_match_types = None if args.include_key_only else {"key_only"}
        removed = apply_dedup(conn, report, exclude_match_types=exclude_match_types)
    key_only_count = sum(1 for match in report.matches if match.match_type == "key_only")

    data = report.as_dict()
    data["dry_run"] = not args.commit
    data["removed"] = removed
    data["account_id"] = args.account_id
    data["date_from"] = args.date_from
    data["date_to"] = args.date_to
    if backup_path:
        data["backup_path"] = backup_path

    return {
        "data": data,
        "summary": {
            "total_matches": len(report.matches),
            "total_removed": removed,
            "key_only_count": key_only_count,
        },
        "cli_report": _build_cli_report(
            data["matches"],
            commit=args.commit,
            removed=removed,
            include_key_only=args.include_key_only,
            elapsed_ms=int(report.elapsed_ms),
        ),
    }


def handle_review_key_only(args, conn) -> dict[str, Any]:
    report = find_cross_format_duplicates(
        conn,
        account_id=args.account_id,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    key_only_matches = [match for match in report.matches if match.match_type == "key_only"]
    total_key_only = len(key_only_matches)
    matches = _enrich_key_only_matches(conn, key_only_matches)
    skipped = total_key_only - len(matches)

    data = {
        "total_key_only": total_key_only,
        "matches": matches,
        "filters": {
            "account_id": args.account_id,
            "date_from": args.date_from,
            "date_to": args.date_to,
        },
        "skipped": skipped,
    }
    return {
        "data": data,
        "summary": {
            "total_key_only": total_key_only,
            "skipped": skipped,
        },
        "cli_report": _build_review_key_only_cli_report(
            matches,
            total_key_only=total_key_only,
            skipped=skipped,
            account_id=args.account_id,
            date_from=args.date_from,
            date_to=args.date_to,
        ),
    }


def handle_backfill_aliases(args, conn) -> dict[str, Any]:
    report = backfill_account_aliases(conn, dry_run=not args.commit)
    if args.commit:
        conn.commit()

    data = {
        **report,
        "dry_run": not args.commit,
    }
    return {
        "data": data,
        "summary": {
            "total_scanned": report["scanned"],
            "total_aliased": report["aliased"],
            "total_removed": report["removed"],
        },
        "cli_report": _build_backfill_cli_report(report, commit=args.commit),
    }


def handle_create_alias(args, conn) -> dict[str, Any]:
    from_id = str(args.from_id or "").strip()
    to_id = str(args.to_id or "").strip()
    if from_id == to_id:
        raise ValueError("--from and --to must be different account IDs")

    from_row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, card_ending, plaid_account_id, is_active
          FROM accounts
         WHERE id = ?
        """,
        (from_id,),
    ).fetchone()
    if not from_row:
        raise ValueError(f"Source account '{from_id}' not found")

    to_row = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, card_ending, plaid_account_id, is_active
          FROM accounts
         WHERE id = ?
        """,
        (to_id,),
    ).fetchone()
    if not to_row:
        raise ValueError(f"Target account '{to_id}' not found")

    if from_row["plaid_account_id"]:
        raise ValueError(f"Source account '{from_id}' must be a hash account (plaid_account_id must be NULL)")
    if not to_row["plaid_account_id"]:
        raise ValueError(f"Target account '{to_id}' must be a Plaid account (plaid_account_id is required)")
    if int(to_row["is_active"] or 0) != 1:
        raise ValueError(f"Target account '{to_id}' is inactive")

    existing_alias = conn.execute(
        """
        SELECT canonical_id
          FROM account_aliases
         WHERE hash_account_id = ?
        """,
        (from_id,),
    ).fetchone()
    existing_canonical_id = str(existing_alias["canonical_id"]) if existing_alias else None
    no_op = existing_canonical_id == to_id

    replaced_alias: dict[str, object] | None = None
    if existing_canonical_id and existing_canonical_id != to_id:
        replaced_row = conn.execute(
            """
            SELECT id, institution_name, account_name, account_type, card_ending, plaid_account_id, is_active
              FROM accounts
             WHERE id = ?
            """,
            (existing_canonical_id,),
        ).fetchone()
        replaced_alias = {
            "canonical_id": existing_canonical_id,
            "canonical_account": _account_description(replaced_row) if replaced_row else None,
        }

    if args.commit and not no_op:
        upsert_account_alias(conn, hash_account_id=from_id, canonical_id=to_id)
        conn.commit()

    data = {
        "from_account": _account_description(from_row),
        "to_account": _account_description(to_row),
        "existing_alias_canonical_id": existing_canonical_id,
        "replaced_alias": replaced_alias,
        "no_op": no_op,
        "dry_run": not args.commit,
    }
    return {
        "data": data,
        "summary": {
            "alias_changed": int(bool(args.commit and not no_op)),
            "noop": int(no_op),
        },
        "cli_report": _build_create_alias_cli_report(
            from_id=from_id,
            to_id=to_id,
            commit=args.commit,
            no_op=no_op,
            replaced_canonical_id=existing_canonical_id if existing_canonical_id != to_id else None,
        ),
    }


def handle_suggest_aliases(args, conn) -> dict[str, Any]:
    _ = args
    account_rows = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, card_ending, plaid_account_id, is_active
          FROM accounts
         WHERE is_active = 1
        """
    ).fetchall()
    alias_rows = conn.execute("SELECT hash_account_id, canonical_id FROM account_aliases").fetchall()
    alias_by_hash = {str(row["hash_account_id"]): str(row["canonical_id"]) for row in alias_rows}
    txn_count_rows = conn.execute(
        """
        SELECT account_id, COUNT(*) AS txn_count
          FROM transactions
         WHERE is_active = 1
         GROUP BY account_id
        """
    ).fetchall()
    txn_count_by_account = {str(row["account_id"]): int(row["txn_count"]) for row in txn_count_rows}

    plaid_accounts = [row for row in account_rows if row["plaid_account_id"]]
    hash_accounts = [
        row
        for row in account_rows
        if not row["plaid_account_id"]
        and str(row["id"]) not in alias_by_hash
        and txn_count_by_account.get(str(row["id"]), 0) > 0
    ]

    suggestions: list[dict[str, object]] = []
    for hash_row in hash_accounts:
        hash_id = str(hash_row["id"])
        hash_type = str(hash_row["account_type"] or "")
        hash_card_ending = _scrub_card_ending(str(hash_row["card_ending"] or ""))
        hash_canonical = canonicalize(str(hash_row["institution_name"] or ""))
        equivalent_names = set(_INSTITUTION_EQUIVALENTS.get(hash_canonical, []))
        hash_missing_card_ending = hash_card_ending is None

        candidates: list[dict[str, object]] = []
        for plaid_row in plaid_accounts:
            if hash_type != str(plaid_row["account_type"] or ""):
                continue

            plaid_id = str(plaid_row["id"])
            plaid_card_ending = _scrub_card_ending(str(plaid_row["card_ending"] or ""))
            plaid_canonical = canonicalize(str(plaid_row["institution_name"] or ""))

            reasons: set[str] = set()
            if plaid_canonical in equivalent_names:
                if hash_card_ending and plaid_card_ending and hash_card_ending == plaid_card_ending:
                    reasons.add("equivalent_institution_matching_card_ending")
                elif hash_missing_card_ending:
                    reasons.add("equivalent_institution_same_type")

            if hash_missing_card_ending and hash_canonical == plaid_canonical:
                reasons.add("same_institution_and_type_missing_card_ending")

            if hash_missing_card_ending and hash_canonical != plaid_canonical and similar_names(hash_canonical, plaid_canonical):
                reasons.add("similar_name_same_type")

            if not reasons:
                continue

            candidates.append(
                {
                    "plaid_account_id": plaid_id,
                    "institution_name": plaid_canonical,
                    "account_type": str(plaid_row["account_type"] or ""),
                    "card_ending": plaid_card_ending,
                    "txn_count": txn_count_by_account.get(plaid_id, 0),
                    "reasons": sorted(reasons, key=_reason_sort_key),
                }
            )

        if not candidates:
            continue

        candidates.sort(key=_candidate_sort_key)
        suggestions.append(
            {
                "hash_account_id": hash_id,
                "institution_name": hash_canonical,
                "account_type": hash_type,
                "card_ending": hash_card_ending,
                "txn_count": txn_count_by_account.get(hash_id, 0),
                "candidates": candidates,
            }
        )

    suggestions.sort(
        key=lambda item: (
            str(item["institution_name"]),
            str(item["account_type"]),
            str(item["hash_account_id"]),
        )
    )

    data = {
        "scanned_hash_accounts": len(hash_accounts),
        "suggestions": suggestions,
    }
    return {
        "data": data,
        "summary": {
            "scanned_hash_accounts": len(hash_accounts),
            "suggested_hash_accounts": len(suggestions),
        },
        "cli_report": _build_suggest_aliases_cli_report(suggestions),
    }


def handle_detect_equivalences(args, conn) -> dict[str, Any]:
    min_overlap = int(args.min_overlap)
    if min_overlap < 1:
        raise ValueError("--min-overlap must be >= 1")

    rows = conn.execute(
        """
        SELECT id, institution_name, card_ending
          FROM accounts
         WHERE is_active = 1
           AND card_ending IS NOT NULL
        """
    ).fetchall()

    institutions_by_card_ending: dict[str, dict[str, set[str]]] = {}
    for row in rows:
        card_ending = _scrub_card_ending(str(row["card_ending"] or ""))
        if not card_ending:
            continue

        institution_name = canonicalize(str(row["institution_name"] or ""))
        account_id = str(row["id"])
        per_card = institutions_by_card_ending.setdefault(card_ending, {})
        per_card.setdefault(institution_name, set()).add(account_id)

    candidates: list[dict[str, object]] = []
    for card_ending in sorted(institutions_by_card_ending):
        institutions = institutions_by_card_ending[card_ending]
        if len(institutions) < 2:
            continue

        for institution_a, institution_b in combinations(sorted(institutions), 2):
            if _is_existing_equivalence_pair(institution_a, institution_b):
                continue

            overlap_count = _count_transaction_overlap(
                conn,
                sorted(institutions[institution_a]),
                sorted(institutions[institution_b]),
            )
            if overlap_count < min_overlap:
                continue

            candidates.append(
                {
                    "institution_a": institution_a,
                    "institution_b": institution_b,
                    "card_ending": card_ending,
                    "overlap_count": overlap_count,
                    "account_ids_a": sorted(institutions[institution_a]),
                    "account_ids_b": sorted(institutions[institution_b]),
                }
            )

    candidates.sort(
        key=lambda item: (
            -int(item["overlap_count"]),
            str(item["institution_a"]),
            str(item["institution_b"]),
            str(item["card_ending"]),
        )
    )
    data = {
        "min_overlap": min_overlap,
        "scanned_card_endings": len(institutions_by_card_ending),
        "candidates": candidates,
    }
    return {
        "data": data,
        "summary": {
            "candidate_count": len(candidates),
            "scanned_card_endings": len(institutions_by_card_ending),
        },
        "cli_report": _build_detect_equivalences_cli_report(candidates),
    }


def handle_audit_names(args, conn) -> dict[str, Any]:
    _ = args
    issues: list[dict[str, object]] = []

    account_rows = conn.execute(
        """
        SELECT id, institution_name, account_name, account_type, card_ending, plaid_account_id, is_active
          FROM accounts
         WHERE is_active = 1
        """
    ).fetchall()
    alias_rows = conn.execute("SELECT hash_account_id, canonical_id FROM account_aliases").fetchall()
    alias_by_hash = {str(row["hash_account_id"]): str(row["canonical_id"]) for row in alias_rows}

    for row in account_rows:
        institution_name = str(row["institution_name"] or "").strip()
        if is_known(institution_name):
            continue
        issues.append(
            {
                "type": "unmapped_name",
                "account_id": str(row["id"]),
                "institution_name": institution_name,
                "plaid_linked": bool(row["plaid_account_id"]),
            }
        )

    plaid_item_rows = conn.execute(
        """
        SELECT plaid_item_id, institution_name, status
          FROM plaid_items
        """
    ).fetchall()
    for row in plaid_item_rows:
        institution_name = str(row["institution_name"] or "").strip()
        if is_known(institution_name):
            continue
        issues.append(
            {
                "type": "unmapped_plaid_item",
                "plaid_item_id": str(row["plaid_item_id"]),
                "institution_name": institution_name,
                "status": str(row["status"] or ""),
            }
        )

    hash_accounts = [row for row in account_rows if not row["plaid_account_id"]]
    plaid_accounts = [row for row in account_rows if row["plaid_account_id"]]
    for hash_row in hash_accounts:
        hash_id = str(hash_row["id"])
        hash_canonical = canonicalize(str(hash_row["institution_name"] or ""))
        for plaid_row in plaid_accounts:
            plaid_id = str(plaid_row["id"])
            if alias_by_hash.get(hash_id) == plaid_id:
                continue
            plaid_canonical = canonicalize(str(plaid_row["institution_name"] or ""))
            if hash_canonical == plaid_canonical:
                continue
            if not similar_names(hash_canonical, plaid_canonical):
                continue
            issues.append(
                {
                    "type": "similar_unaliased",
                    "hash_account_id": hash_id,
                    "plaid_account_id": plaid_id,
                    "hash_institution_name": str(hash_row["institution_name"] or ""),
                    "plaid_institution_name": str(plaid_row["institution_name"] or ""),
                    "hash_canonical_name": hash_canonical,
                    "plaid_canonical_name": plaid_canonical,
                }
            )

    for hash_row in hash_accounts:
        hash_id = str(hash_row["id"])
        canonical_name = canonicalize(str(hash_row["institution_name"] or ""))
        equivalent_names = _INSTITUTION_EQUIVALENTS.get(canonical_name, [])
        card_ending = _scrub_card_ending(str(hash_row["card_ending"] or ""))
        if not equivalent_names or not card_ending:
            continue
        for plaid_row in plaid_accounts:
            plaid_id = str(plaid_row["id"])
            if alias_by_hash.get(hash_id) == plaid_id:
                continue
            plaid_canonical = canonicalize(str(plaid_row["institution_name"] or ""))
            plaid_card = _scrub_card_ending(str(plaid_row["card_ending"] or ""))
            if plaid_canonical not in equivalent_names or plaid_card != card_ending:
                continue
            issues.append(
                {
                    "type": "equivalence_gap",
                    "hash_account_id": hash_id,
                    "plaid_account_id": plaid_id,
                    "institution_name": canonical_name,
                    "equivalent_institution_name": plaid_canonical,
                    "card_ending": card_ending,
                }
            )

    orphan_rows = conn.execute(
        """
        SELECT a.id, a.institution_name, a.account_name, a.plaid_account_id
          FROM accounts a
         WHERE a.is_active = 1
           AND NOT EXISTS (
               SELECT 1
                 FROM transactions t
                WHERE t.account_id = a.id
                  AND t.is_active = 1
           )
        """
    ).fetchall()
    for row in orphan_rows:
        issues.append(
            {
                "type": "orphaned_account",
                "account_id": str(row["id"]),
                "institution_name": str(row["institution_name"] or ""),
                "account_name": str(row["account_name"] or ""),
                "plaid_linked": bool(row["plaid_account_id"]),
            }
        )

    by_type = Counter(str(item["type"]) for item in issues)
    summary = {
        "total_issues": len(issues),
        "by_type": {key: by_type[key] for key in sorted(by_type)},
    }
    return {
        "data": {
            "issues": issues,
        },
        "summary": summary,
        "cli_report": _build_audit_cli_report(issues),
    }
