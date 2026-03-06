"""Transaction commands."""

from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from ..categorizer import normalize_description
from ..importers import import_csv, import_income_csv
from ..models import dollars_to_cents
from ..user_rules import load_rules, match_keyword_rule
from .common import fmt_dollars, get_category_id_by_name, today_iso, txn_row_to_dict

_DEFAULT_FIELDS: tuple[str, ...] = (
    "id",
    "date",
    "description",
    "amount_cents",
    "category_id",
    "category_source",
    "use_type",
    "is_payment",
    "is_reviewed",
    "is_recurring",
    "source",
    "account_id",
    "project_id",
    "notes",
    "source_category",
)

_VERBOSE_FIELDS: tuple[str, ...] = (
    *_DEFAULT_FIELDS,
    "plaid_txn_id",
    "dedupe_key",
    "category_confidence",
    "category_rule_id",
    "raw_plaid_json",
    "split_group_id",
    "parent_transaction_id",
    "split_pct",
    "split_note",
    "is_active",
    "removed_at",
    "created_at",
    "updated_at",
)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("txn", parents=[format_parent], help="Transaction commands")
    txn_sub = parser.add_subparsers(dest="txn_command", required=True)

    p_list = txn_sub.add_parser("list", parents=[format_parent], help="List transactions")
    p_list.add_argument("--from", dest="date_from")
    p_list.add_argument("--to", dest="date_to")
    p_list.add_argument("--category")
    p_list.add_argument("--uncategorized", action="store_true")
    p_list.add_argument("--unreviewed", action="store_true")
    p_list.add_argument("--project")
    p_list.add_argument("--verbose", action="store_true")
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--offset", type=int, default=0)
    p_list.set_defaults(func=handle_list, command_name="txn.list")

    p_show = txn_sub.add_parser("show", parents=[format_parent], help="Show transaction")
    p_show.add_argument("id")
    p_show.set_defaults(func=handle_show, command_name="txn.show")

    p_explain = txn_sub.add_parser("explain", parents=[format_parent], help="Explain transaction categorization")
    p_explain.add_argument("id")
    p_explain.set_defaults(func=handle_explain, command_name="txn.explain")

    p_search = txn_sub.add_parser("search", parents=[format_parent], help="FTS transaction search")
    p_search.add_argument("--query", required=True)
    p_search.set_defaults(func=handle_search, command_name="txn.search")

    p_cat = txn_sub.add_parser("categorize", parents=[format_parent], help="Categorize transaction(s)")
    p_cat.add_argument("txn_id", nargs="?")
    p_cat.add_argument("--category", required=True)
    p_cat.add_argument("--remember", action="store_true")
    p_cat.add_argument("--bulk", action="store_true")
    p_cat.add_argument("--ids")
    p_cat.add_argument("--from", dest="date_from")
    p_cat.add_argument("--to", dest="date_to")
    p_cat.add_argument("--query")
    p_cat.set_defaults(func=handle_categorize, command_name="txn.categorize")

    p_edit = txn_sub.add_parser("edit", parents=[format_parent], help="Edit a transaction")
    p_edit.add_argument("id")
    p_edit.add_argument("--amount")
    p_edit.add_argument("--date")
    p_edit.add_argument("--description")
    p_edit.add_argument("--notes")
    p_edit.set_defaults(func=handle_edit, command_name="txn.edit")

    p_tag = txn_sub.add_parser("tag", parents=[format_parent], help="Tag transaction with a project")
    p_tag.add_argument("id")
    p_tag.add_argument("--project", required=True)
    p_tag.set_defaults(func=handle_tag, command_name="txn.tag")

    p_review = txn_sub.add_parser("review", parents=[format_parent], help="Mark reviewed")
    p_review.add_argument("txn_id", nargs="?")
    p_review.add_argument("--all-today", action="store_true")
    p_review.add_argument("--before", help="Review all transactions before this date (YYYY-MM-DD)")
    p_review.set_defaults(func=handle_review, command_name="txn.review")

    p_add = txn_sub.add_parser("add", parents=[format_parent], help="Add manual transaction")
    p_add.add_argument("--date", required=True)
    p_add.add_argument("--description", required=True)
    p_add.add_argument("--amount", required=True)
    p_add.add_argument("--category")
    p_add.set_defaults(func=handle_add, command_name="txn.add")

    p_coverage = txn_sub.add_parser("coverage", parents=[format_parent], help="Show data coverage by account")
    p_coverage.add_argument("--from", dest="date_from", help="Start of analysis window")
    p_coverage.add_argument("--to", dest="date_to", help="End of analysis window")
    p_coverage.set_defaults(func=handle_coverage, command_name="txn.coverage")

    p_import = txn_sub.add_parser("import", parents=[format_parent], help="Import CSV/PDF transactions")
    p_import.add_argument("--file")
    p_import.add_argument("--source")
    p_import.add_argument("--income-source")
    p_import.add_argument("--dry-run", action="store_true")
    p_import.set_defaults(func=handle_import, command_name="txn.import")


def _project_id_by_name(conn: sqlite3.Connection, project_name: str) -> str:
    row = conn.execute("SELECT id FROM projects WHERE name = ?", (project_name,)).fetchone()
    if row:
        return row["id"]

    project_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO projects (id, name, is_active) VALUES (?, ?, 1)",
        (project_id, project_name),
    )
    return project_id


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if args.limit < 1:
        raise ValueError("--limit must be >= 1")
    if args.offset < 0:
        raise ValueError("--offset must be >= 0")

    where = ["t.is_active = 1"]
    params: list[Any] = []

    if args.date_from:
        where.append("t.date >= ?")
        params.append(args.date_from)
    if args.date_to:
        where.append("t.date <= ?")
        params.append(args.date_to)
    if args.uncategorized:
        where.append("t.category_id IS NULL")
    if args.unreviewed:
        where.append("t.is_reviewed = 0")
    if args.category:
        where.append("c.name = ?")
        params.append(args.category)
    if hasattr(args, "account_id") and args.account_id:
        where.append("t.account_id = ?")
        params.append(args.account_id)
    if hasattr(args, "use_type") and args.use_type:
        where.append("t.use_type = ?")
        params.append(args.use_type)
    if args.project:
        where.append("p.name = ?")
        params.append(args.project)

    selected_fields = _VERBOSE_FIELDS if args.verbose else _DEFAULT_FIELDS
    select_clause = ", ".join(f"t.{field}" for field in selected_fields)
    base_query = f"""
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        LEFT JOIN projects p ON p.id = t.project_id
        WHERE {' AND '.join(where)}
    """

    total_count = int(
        conn.execute(
            f"SELECT COUNT(*) AS total_count {base_query}",
            tuple(params),
        ).fetchone()["total_count"]
        or 0
    )

    query = f"""
        SELECT {select_clause}, c.name AS category_name, p.name AS project_name
        {base_query}
        ORDER BY t.date DESC, t.created_at DESC
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, tuple([*params, args.limit, args.offset])).fetchall()
    txns = [txn_row_to_dict(row) for row in rows]
    total_cents = sum(int(row["amount_cents"]) for row in rows)
    returned_count = len(txns)
    has_more = args.offset + returned_count < total_count

    cli_lines = []
    for row in txns[:50]:
        desc = (row.get("description") or "")[:22].ljust(22)
        cat = (row.get("category_name") or "\u2014")[:16].ljust(16)
        amt = fmt_dollars(abs(row["amount"]))
        sign = "-" if row["amount"] < 0 else ""
        cli_lines.append(f"{row['date']}  {desc}  {cat}  {sign}{amt:>10s}")
    cli_report = "\n".join(cli_lines) if cli_lines else "No transactions"

    if txns:
        cli_report = f"{cli_report}\nreturned={returned_count} total={total_count}"

    return {
        "data": {
            "transactions": txns,
            "pagination": {
                "total_count": total_count,
                "limit": int(args.limit),
                "offset": int(args.offset),
                "has_more": has_more,
            },
        },
        "summary": {
            "total_transactions": returned_count,
            "total_amount": total_cents / 100,
            "total_count": total_count,
            "returned": returned_count,
        },
        "cli_report": cli_report,
    }


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT t.*, c.name AS category_name, p.name AS project_name
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        LEFT JOIN projects p ON p.id = t.project_id
        WHERE t.id = ?
        """,
        (args.id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transaction {args.id} not found")

    txn = txn_row_to_dict(row)
    return {
        "data": {"transaction": txn},
        "summary": {"total_transactions": 1, "total_amount": txn["amount"]},
        "cli_report": f"{txn['id']} | {txn['date']} | {txn['description']} | {txn['amount']:.2f}",
    }


def handle_explain(args, conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT t.id,
               t.date,
               t.description,
               t.amount_cents,
               t.category_source,
               t.category_confidence,
               t.category_rule_id,
               t.source_category,
               c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.id = ?
        """,
        (args.id,),
    ).fetchone()
    if not row:
        raise ValueError(f"Transaction {args.id} not found")

    txn = txn_row_to_dict(row)
    category_source = row["category_source"]
    category_rule_id = row["category_rule_id"]

    vendor_memory_rule = None
    if category_rule_id:
        rule_row = conn.execute(
            """
            SELECT vm.id,
                   vm.description_pattern,
                   vm.use_type,
                   vm.is_confirmed,
                   vm.match_count,
                   vm.priority,
                   c.name AS category_name
              FROM vendor_memory vm
              LEFT JOIN categories c ON c.id = vm.category_id
             WHERE vm.id = ?
            """,
            (category_rule_id,),
        ).fetchone()
        if rule_row:
            vendor_memory_rule = {
                "id": rule_row["id"],
                "description_pattern": rule_row["description_pattern"],
                "use_type": rule_row["use_type"],
                "is_confirmed": bool(int(rule_row["is_confirmed"] or 0)),
                "match_count": int(rule_row["match_count"] or 0),
                "priority": int(rule_row["priority"] or 0),
                "category_name": rule_row["category_name"],
            }

    keyword_rule_match = None
    if category_source == "keyword_rule":
        keyword_match = match_keyword_rule(txn["description"], load_rules())
        if keyword_match:
            keyword_rule_match = {
                "matched_keyword": keyword_match.matched_keyword,
                "category": keyword_match.category,
                "use_type": keyword_match.use_type,
                "rule_index": keyword_match.rule_index,
            }

    ai_reasoning = None
    if category_source == "ai":
        ai_row = conn.execute(
            """
            SELECT provider, model, category_name, use_type, confidence, reasoning, created_at
              FROM ai_categorization_log
             WHERE transaction_id = ?
             ORDER BY created_at DESC
             LIMIT 1
            """,
            (args.id,),
        ).fetchone()
        if ai_row:
            ai_reasoning = {
                "provider": ai_row["provider"],
                "model": ai_row["model"],
                "category_name": ai_row["category_name"],
                "use_type": ai_row["use_type"],
                "confidence": ai_row["confidence"],
                "reasoning": ai_row["reasoning"],
                "created_at": ai_row["created_at"],
            }

    data = {
        "transaction": txn,
        "final_category": row["category_name"],
        "category_source": category_source,
        "category_confidence": row["category_confidence"],
        "category_rule_id": category_rule_id,
        "source_category": row["source_category"],
        "vendor_memory_rule": vendor_memory_rule,
        "keyword_rule_match": keyword_rule_match,
        "ai_reasoning": ai_reasoning,
    }

    lines = [
        f"Transaction: {txn['id']}",
        f"Final category: {row['category_name'] or 'Uncategorized'}",
        f"Source: {category_source or 'unknown'}",
        f"Confidence: {row['category_confidence'] if row['category_confidence'] is not None else 'n/a'}",
    ]
    if row["source_category"]:
        lines.append(f"Provider category: {row['source_category']}")
    if keyword_rule_match:
        lines.append(
            f"Keyword rule matched '{keyword_rule_match['matched_keyword']}' "
            f"-> {keyword_rule_match['category']}"
        )
    if vendor_memory_rule:
        lines.append(
            f"Vendor memory: pattern='{vendor_memory_rule['description_pattern']}' "
            f"confirmed={vendor_memory_rule['is_confirmed']} matches={vendor_memory_rule['match_count']}"
        )
    if ai_reasoning:
        lines.append(f"AI model: {ai_reasoning['provider']}/{ai_reasoning['model']}")
        if ai_reasoning["reasoning"]:
            lines.append(f"AI reasoning: {ai_reasoning['reasoning']}")

    return {
        "data": data,
        "summary": {"total_transactions": 1, "total_amount": txn["amount"]},
        "cli_report": "\n".join(lines),
    }


def handle_search(args, conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = conn.execute(
            """
            SELECT t.*, c.name AS category_name
              FROM txn_fts f
              JOIN transactions t ON t.rowid = f.rowid
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE txn_fts MATCH ?
               AND t.is_active = 1
             ORDER BY t.date DESC
            """,
            (args.query,),
        ).fetchall()
    except sqlite3.Error as exc:
        # Fallback keeps search usable when user query contains FTS syntax
        # characters that would otherwise raise parser errors.
        fallback_term = args.query.replace("*", " ").replace('"', " ").strip()
        if not fallback_term:
            raise ValueError(f"Invalid search query: {exc}") from exc
        rows = conn.execute(
            """
            SELECT t.*, c.name AS category_name
              FROM transactions t
              LEFT JOIN categories c ON c.id = t.category_id
             WHERE t.is_active = 1
               AND lower(t.description) LIKE lower(?)
             ORDER BY t.date DESC
            """,
            (f"%{fallback_term}%",),
        ).fetchall()

    txns = [txn_row_to_dict(row) for row in rows]
    total_cents = sum(int(row["amount_cents"]) for row in rows)
    cli_report = "\n".join(f"{row['date']} | {row['description']} | {row['amount']:.2f}" for row in txns) if txns else "No matches"
    return {
        "data": {"transactions": txns, "query": args.query},
        "summary": {"total_transactions": len(txns), "total_amount": total_cents / 100},
        "cli_report": cli_report,
    }


def _upsert_vendor_memory_rule(
    conn: sqlite3.Connection,
    *,
    pattern: str,
    use_type: str,
    category_id: str,
) -> str:
    existing = conn.execute(
        "SELECT id FROM vendor_memory WHERE description_pattern = ? AND use_type = ?",
        (pattern, use_type),
    ).fetchone()
    if existing:
        rule_id = existing["id"]
        conn.execute(
            """
            UPDATE vendor_memory
               SET category_id = ?,
                   confidence = 1.0,
                   is_enabled = 1,
                   is_confirmed = 1
             WHERE id = ?
            """,
            (category_id, rule_id),
        )
        return rule_id

    rule_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id,
            description_pattern,
            category_id,
            use_type,
            confidence,
            priority,
            is_enabled,
            is_confirmed,
            match_count
        ) VALUES (?, ?, ?, ?, 1.0, 0, 1, 1, 0)
        """,
        (rule_id, pattern, category_id, use_type),
    )
    return rule_id


def handle_categorize(args, conn: sqlite3.Connection) -> dict[str, Any]:
    category_id = get_category_id_by_name(conn, args.category)
    if not category_id:
        raise ValueError(f"Category '{args.category}' not found")
    is_payment_val = 1 if args.category.lower() == "payments & transfers" else 0

    ids_csv = str(getattr(args, "ids", "") or "").strip()
    bulk_mode = bool(args.bulk or ids_csv)
    if bulk_mode:
        if ids_csv:
            requested_ids = [value.strip() for value in ids_csv.split(",") if value.strip()]
            if not requested_ids:
                raise ValueError("--ids must include at least one transaction id")
            placeholders = ",".join("?" for _ in requested_ids)
            rows = conn.execute(
                f"SELECT id FROM transactions WHERE is_active = 1 AND id IN ({placeholders})",
                tuple(requested_ids),
            ).fetchall()
        else:
            if not any([args.query, args.date_from, args.date_to]):
                raise ValueError("Bulk categorize requires at least one filter: --ids, --query, --from, or --to")
            where = ["is_active = 1"]
            params: list[Any] = []
            if args.date_from:
                where.append("date >= ?")
                params.append(args.date_from)
            if args.date_to:
                where.append("date <= ?")
                params.append(args.date_to)
            if args.query:
                like = args.query.replace("*", "%")
                where.append("description LIKE ?")
                params.append(like)
            rows = conn.execute(
                f"SELECT id FROM transactions WHERE {' AND '.join(where)}",
                tuple(params),
            ).fetchall()

        txn_ids = [row["id"] for row in rows]
        if not txn_ids:
            return {
                "data": {"updated": 0},
                "summary": {"total_transactions": 0, "total_amount": 0},
                "cli_report": "No matching transactions",
            }

        remembered_count = 0
        if args.remember:
            placeholders = ",".join("?" for _ in txn_ids)
            remember_rows = conn.execute(
                f"SELECT description, use_type FROM transactions WHERE id IN ({placeholders})",
                tuple(txn_ids),
            ).fetchall()
            patterns: set[tuple[str, str]] = set()
            for row in remember_rows:
                pattern = normalize_description(str(row["description"] or ""))
                use_type = row["use_type"] if row["use_type"] in {"Business", "Personal"} else "Any"
                patterns.add((pattern, use_type))
            for pattern, use_type in sorted(patterns):
                _upsert_vendor_memory_rule(conn, pattern=pattern, use_type=use_type, category_id=category_id)
            remembered_count = len(patterns)

        conn.executemany(
            """
            UPDATE transactions
               SET category_id = ?,
                   category_source = 'user',
                   category_confidence = 1.0,
                   is_payment = ?,
                   updated_at = datetime('now')
             WHERE id = ?
            """,
            [(category_id, is_payment_val, txn_id) for txn_id in txn_ids],
        )
        conn.commit()

        data: dict[str, Any] = {"updated": len(txn_ids)}
        if args.remember:
            data["remembered_count"] = remembered_count
        cli_report = f"Updated {len(txn_ids)} transactions"
        if args.remember:
            cli_report += f" and remembered {remembered_count} vendor patterns"
        return {
            "data": data,
            "summary": {"total_transactions": len(txn_ids), "total_amount": 0},
            "cli_report": cli_report,
        }

    if not args.txn_id:
        raise ValueError("txn_id is required unless --bulk or --ids is used")

    txn = conn.execute(
        """
        SELECT id, description, use_type, category_id, category_source, category_confidence, category_rule_id
          FROM transactions
         WHERE id = ?
        """,
        (args.txn_id,),
    ).fetchone()
    if not txn:
        raise ValueError(f"Transaction {args.txn_id} not found")

    rule_id = None
    if args.remember:
        pattern = normalize_description(str(txn["description"] or ""))
        use_type = txn["use_type"] if txn["use_type"] in {"Business", "Personal"} else "Any"
        rule_id = _upsert_vendor_memory_rule(conn, pattern=pattern, use_type=use_type, category_id=category_id)

    conn.execute(
        """
        UPDATE transactions
           SET category_id = ?,
               category_source = 'user',
               category_confidence = 1.0,
               category_rule_id = ?,
               is_payment = ?,
               updated_at = datetime('now')
         WHERE id = ?
        """,
        (category_id, rule_id, is_payment_val, args.txn_id),
    )
    conn.commit()

    return {
        "data": {
            "transaction_id": args.txn_id,
            "category": args.category,
            "remembered": bool(args.remember),
            "remembered_rule_id": rule_id,
            "previous": {
                "category_id": txn["category_id"],
                "category_source": txn["category_source"],
                "category_confidence": txn["category_confidence"],
                "category_rule_id": txn["category_rule_id"],
            },
            "updated": {
                "category_id": category_id,
                "category_source": "user",
                "category_confidence": 1.0,
                "category_rule_id": rule_id,
            },
        },
        "summary": {"total_transactions": 1, "total_amount": 0},
        "cli_report": f"Categorized {args.txn_id} as {args.category}",
    }


def handle_edit(args, conn: sqlite3.Connection) -> dict[str, Any]:
    sets = []
    params: list[Any] = []

    if args.amount is not None:
        sets.append("amount_cents = ?")
        params.append(dollars_to_cents(args.amount))
    if args.date is not None:
        sets.append("date = ?")
        params.append(args.date)
    if args.description is not None:
        sets.append("description = ?")
        params.append(args.description)
    if args.notes is not None:
        sets.append("notes = ?")
        params.append(args.notes)

    if not sets:
        raise ValueError("No fields provided for edit")

    sets.append("updated_at = datetime('now')")
    params.append(args.id)

    cursor = conn.execute(
        f"UPDATE transactions SET {', '.join(sets)} WHERE id = ?",
        tuple(params),
    )
    conn.commit()

    if cursor.rowcount == 0:
        raise ValueError(f"Transaction {args.id} not found")

    return {
        "data": {"transaction_id": args.id, "updated_fields": len(sets) - 1},
        "summary": {"total_transactions": 1, "total_amount": 0},
        "cli_report": f"Updated transaction {args.id}",
    }


def handle_tag(args, conn: sqlite3.Connection) -> dict[str, Any]:
    project_id = _project_id_by_name(conn, args.project)
    cursor = conn.execute(
        "UPDATE transactions SET project_id = ?, updated_at = datetime('now') WHERE id = ?",
        (project_id, args.id),
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Transaction {args.id} not found")

    return {
        "data": {"transaction_id": args.id, "project": args.project},
        "summary": {"total_transactions": 1, "total_amount": 0},
        "cli_report": f"Tagged {args.id} with project '{args.project}'",
    }


def handle_review(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if args.before:
        cursor = conn.execute(
            "UPDATE transactions SET is_reviewed = 1, updated_at = datetime('now') "
            "WHERE date < ? AND is_active = 1 AND is_reviewed = 0",
            (args.before,),
        )
        conn.commit()
        return {
            "data": {"updated": cursor.rowcount, "before": args.before},
            "summary": {"total_transactions": cursor.rowcount, "total_amount": 0},
            "cli_report": f"Marked {cursor.rowcount} transactions before {args.before} as reviewed",
        }

    if args.all_today:
        today = today_iso()
        cursor = conn.execute(
            "UPDATE transactions SET is_reviewed = 1, updated_at = datetime('now') WHERE date = ? AND is_active = 1",
            (today,),
        )
        conn.commit()
        return {
            "data": {"updated": cursor.rowcount, "date": today},
            "summary": {"total_transactions": cursor.rowcount, "total_amount": 0},
            "cli_report": f"Marked {cursor.rowcount} transactions reviewed for {today}",
        }

    if not args.txn_id:
        raise ValueError("txn_id is required unless --all-today or --before is used")

    cursor = conn.execute(
        "UPDATE transactions SET is_reviewed = 1, updated_at = datetime('now') WHERE id = ?",
        (args.txn_id,),
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Transaction {args.txn_id} not found")

    return {
        "data": {"transaction_id": args.txn_id, "is_reviewed": True},
        "summary": {"total_transactions": 1, "total_amount": 0},
        "cli_report": f"Marked {args.txn_id} reviewed",
    }


def handle_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    category_id = None
    if args.category:
        category_id = get_category_id_by_name(conn, args.category)
        if not category_id:
            raise ValueError(f"Category '{args.category}' not found")

    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            date,
            description,
            amount_cents,
            category_id,
            category_source,
            category_confidence,
            source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'manual')
        """,
        (
            txn_id,
            args.date,
            args.description,
            dollars_to_cents(args.amount),
            category_id,
            "user" if category_id else None,
            1.0 if category_id else None,
        ),
    )
    conn.commit()

    return {
        "data": {"transaction_id": txn_id},
        "summary": {"total_transactions": 1, "total_amount": float(args.amount)},
        "cli_report": f"Added transaction {txn_id}",
    }


def handle_import(args, conn: sqlite3.Connection) -> dict[str, Any]:
    if args.income_source:
        if not args.file:
            raise ValueError("--file is required when using --income-source")
        rules = load_rules()
        report = import_income_csv(
            conn,
            file_path=args.file,
            source_name=args.income_source,
            rules=rules,
            dry_run=args.dry_run,
        )
        return {
            "data": report.as_dict(),
            "summary": {
                "total_transactions": report.inserted,
                "total_amount": 0,
            },
            "cli_report": (
                f"inserted={report.inserted} skipped_duplicates={report.skipped_duplicates} errors={report.errors}"
            ),
        }

    if not args.file or not args.source:
        raise ValueError("--file and --source are required for CSV import")

    report = import_csv(conn, file_path=args.file, source_name=args.source, dry_run=args.dry_run)
    return {
        "data": report.as_dict(),
        "summary": {
            "total_transactions": report.inserted,
            "total_amount": 0,
        },
        "cli_report": (
            f"inserted={report.inserted} skipped_duplicates={report.skipped_duplicates} errors={report.errors}"
        ),
    }


def handle_coverage(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Show date range coverage per institution/account with gap detection."""
    rows = conn.execute(
        """
        SELECT a.institution_name,
               a.account_name,
               a.account_type,
               t.source,
               MIN(t.date) AS earliest,
               MAX(t.date) AS latest,
               COUNT(*)    AS txn_count
          FROM transactions t
          JOIN accounts a ON t.account_id = a.id
         WHERE t.is_active = 1
         GROUP BY a.institution_name, a.account_name, t.source
         ORDER BY earliest ASC, a.institution_name
        """
    ).fetchall()

    overall = conn.execute(
        "SELECT MIN(date) AS earliest, MAX(date) AS latest, COUNT(*) AS total FROM transactions WHERE is_active = 1"
    ).fetchone()

    accounts = []
    for r in rows:
        accounts.append({
            "institution": r["institution_name"],
            "account": r["account_name"],
            "account_type": r["account_type"],
            "source": r["source"],
            "earliest": r["earliest"],
            "latest": r["latest"],
            "txn_count": r["txn_count"],
        })

    # Detect gaps: accounts missing data before the reference date.
    # Use --from if given, otherwise the global earliest transaction date.
    window_from = args.date_from
    window_to = args.date_to
    reference_start = window_from or (overall["earliest"] if overall else None)

    gaps = []
    if reference_start:
        # Collapse to one gap per institution (use earliest account start)
        inst_earliest: dict[str, str] = {}
        for acct in accounts:
            key = f"{acct['institution']} ({acct['source']})"
            if key not in inst_earliest or acct["earliest"] < inst_earliest[key]:
                inst_earliest[key] = acct["earliest"]
        for key, earliest in sorted(inst_earliest.items(), key=lambda x: x[1]):
            if earliest > reference_start:
                gaps.append({"label": key, "missing_before": earliest})

    # Mark incomplete accounts when a window is specified
    if window_from or window_to:
        for acct in accounts:
            has_gap = False
            if window_from and acct["earliest"] > window_from:
                has_gap = True
            if window_to and acct["latest"] < window_to:
                has_gap = True
            acct["incomplete"] = has_gap

    # CLI report
    lines = []
    lines.append(f"{'Institution':<22} {'Account':<28} {'Source':<12} {'Earliest':<12} {'Latest':<12} {'Count':>6}")
    lines.append("-" * 95)
    for acct in accounts:
        marker = " *" if acct.get("incomplete") else ""
        lines.append(
            f"{acct['institution']:<22} "
            f"{(acct['account'] or '')[:27]:<28} "
            f"{acct['source']:<12} "
            f"{acct['earliest']:<12} "
            f"{acct['latest']:<12} "
            f"{acct['txn_count']:>6}{marker}"
        )

    if gaps and args.date_from:
        lines.append("")
        lines.append(f"Gaps (no data before {reference_start}):")
        for g in gaps:
            lines.append(f"  {g['label']}: starts {g['missing_before']}")

    return {
        "data": {
            "accounts": accounts,
            "gaps": gaps,
            "overall_earliest": overall["earliest"] if overall else None,
            "overall_latest": overall["latest"] if overall else None,
            "total_transactions": overall["total"] if overall else 0,
        },
        "summary": {
            "total_accounts": len(accounts),
            "total_transactions": overall["total"] if overall else 0,
            "accounts_with_gaps": len(gaps),
        },
        "cli_report": "\n".join(lines),
    }
