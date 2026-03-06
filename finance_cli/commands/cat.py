"""Category and vendor-memory commands."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from ..ai_categorizer import categorize_uncategorized
from ..categorizer import MatchResult, _apply_split_rule, apply_match, match_transaction, normalize_description
from ..user_rules import (
    CANONICAL_CATEGORIES,
    get_category_override,
    get_split_rule,
    load_rules,
    match_keyword_rule,
    resolve_category_alias,
)
from .common import get_category_id_by_name


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("cat", parents=[format_parent], help="Category commands")
    cat_sub = parser.add_subparsers(dest="cat_command", required=True)

    p_list = cat_sub.add_parser("list", parents=[format_parent], help="List categories")
    p_list.set_defaults(func=handle_list, command_name="cat.list")

    p_add = cat_sub.add_parser("add", parents=[format_parent], help="Add category")
    p_add.add_argument("name")
    p_add.add_argument("--parent")
    p_add.set_defaults(func=handle_add, command_name="cat.add")

    p_memory = cat_sub.add_parser("memory", parents=[format_parent], help="Vendor memory")
    memory_sub = p_memory.add_subparsers(dest="memory_command", required=True)

    p_m_list = memory_sub.add_parser("list", parents=[format_parent], help="List memory")
    p_m_list.add_argument("--unconfirmed", action="store_true")
    p_m_list.add_argument("--limit", type=int, default=50)
    p_m_list.add_argument("--search", help="Filter by pattern substring")
    p_m_list.set_defaults(func=handle_memory_list, command_name="cat.memory.list")

    p_m_add = memory_sub.add_parser("add", parents=[format_parent], help="Add memory rule")
    p_m_add.add_argument("--pattern", required=True)
    p_m_add.add_argument("--category", required=True)
    p_m_add.add_argument("--use-type", default="Any", choices=["Business", "Personal", "Any"])
    p_m_add.set_defaults(func=handle_memory_add, command_name="cat.memory.add")

    p_m_disable = memory_sub.add_parser("disable", parents=[format_parent], help="Disable memory rule")
    p_m_disable.add_argument("id")
    p_m_disable.set_defaults(func=handle_memory_disable, command_name="cat.memory.disable")

    p_m_confirm = memory_sub.add_parser("confirm", parents=[format_parent], help="Confirm memory rule")
    p_m_confirm.add_argument("id")
    p_m_confirm.set_defaults(func=handle_memory_confirm, command_name="cat.memory.confirm")

    p_m_delete = memory_sub.add_parser("delete", parents=[format_parent], help="Delete memory rule")
    p_m_delete.add_argument("id")
    p_m_delete.set_defaults(func=handle_memory_delete, command_name="cat.memory.delete")

    p_m_undo = memory_sub.add_parser("undo", parents=[format_parent], help="Undo memory from transaction")
    p_m_undo.add_argument("txn_id")
    p_m_undo.set_defaults(func=handle_memory_undo, command_name="cat.memory.undo")

    p_auto = cat_sub.add_parser("auto-categorize", parents=[format_parent], help="Auto-categorize transactions")
    p_auto.add_argument("--dry-run", action="store_true")
    p_auto.add_argument("--ai", action="store_true")
    p_auto.add_argument("--provider", choices=["claude", "openai"])
    p_auto.add_argument("--batch-size", type=int)
    p_auto.set_defaults(func=handle_auto_categorize, command_name="cat.auto-categorize")

    p_apply_splits = cat_sub.add_parser("apply-splits", parents=[format_parent], help="Apply split rules to matched transactions")
    p_apply_splits.add_argument("--commit", action="store_true")
    p_apply_splits.add_argument("--backfill", action="store_true")
    p_apply_splits.set_defaults(func=handle_apply_splits, command_name="cat.apply-splits")

    p_classify_use_type = cat_sub.add_parser("classify-use-type", parents=[format_parent], help="Classify missing use_type from rules")
    p_classify_use_type.add_argument("--commit", action="store_true")
    p_classify_use_type.set_defaults(func=handle_classify_use_type, command_name="cat.classify-use-type")

    p_tree = cat_sub.add_parser("tree", parents=[format_parent], help="Show category hierarchy tree")
    p_tree.set_defaults(func=handle_tree, command_name="cat.tree")

    p_normalize = cat_sub.add_parser("normalize", parents=[format_parent], help="Normalize categories")
    p_normalize.add_argument("--dry-run", action="store_true")
    p_normalize.set_defaults(func=handle_normalize, command_name="cat.normalize")


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT c.id, c.name, p.name AS parent_name, c.is_income, c.is_system, c.sort_order
          FROM categories c
          LEFT JOIN categories p ON p.id = c.parent_id
         ORDER BY c.sort_order ASC, c.name ASC
        """
    ).fetchall()

    categories = [dict(row) for row in rows]
    cli_report = "\n".join(row["name"] for row in categories) if categories else "No categories"
    return {
        "data": {"categories": categories},
        "summary": {"total_categories": len(categories)},
        "cli_report": cli_report,
    }


def handle_tree(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT c.id, c.name, c.parent_id, c.is_income,
               COUNT(t.id) AS txn_count,
               COALESCE(SUM(CASE WHEN t.amount_cents < 0 THEN t.amount_cents ELSE 0 END), 0) AS spend_cents
          FROM categories c
          LEFT JOIN transactions t ON t.category_id = c.id AND t.is_active = 1 AND t.is_payment = 0
         GROUP BY c.id
         ORDER BY c.name ASC
        """
    ).fetchall()

    categories = {row["id"]: dict(row) for row in rows}

    # Build parent → children mapping
    parents: list[dict] = []
    children_map: dict[str, list[dict]] = {}
    standalone: list[dict] = []
    for cat in categories.values():
        if cat["parent_id"] is None:
            # Check if this category has children
            has_children = any(c["parent_id"] == cat["id"] for c in categories.values())
            if has_children:
                parents.append(cat)
            else:
                standalone.append(cat)
        else:
            children_map.setdefault(cat["parent_id"], []).append(cat)

    # Sort parents and standalone by name
    parents.sort(key=lambda c: c["name"])
    standalone.sort(key=lambda c: c["name"])

    tree_data: list[dict] = []
    cli_lines: list[str] = []

    for parent in parents:
        children = sorted(children_map.get(parent["id"], []), key=lambda c: c["name"])
        child_txn_total = sum(c["txn_count"] for c in children)
        parent_total = parent["txn_count"] + child_txn_total

        cli_lines.append(f"{parent['name']} ({parent_total})")
        node = {"name": parent["name"], "txn_count": parent_total, "children": []}
        for i, child in enumerate(children):
            connector = "└── " if i == len(children) - 1 else "├── "
            cli_lines.append(f"{connector}{child['name']} ({child['txn_count']})")
            node["children"].append({"name": child["name"], "txn_count": child["txn_count"]})
        tree_data.append(node)

    for cat in standalone:
        cli_lines.append(f"{cat['name']} ({cat['txn_count']})")
        tree_data.append({"name": cat["name"], "txn_count": cat["txn_count"], "children": []})

    cli_report = "\n".join(cli_lines) if cli_lines else "No categories"
    return {
        "data": {"tree": tree_data},
        "summary": {"total_categories": len(categories)},
        "cli_report": cli_report,
    }


def handle_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    existing = conn.execute("SELECT id FROM categories WHERE name = ?", (args.name,)).fetchone()
    if existing:
        return {
            "data": {"category_id": existing["id"], "created": False},
            "summary": {"total_categories": 1},
            "cli_report": f"Category '{args.name}' already exists",
        }

    parent_id = None
    if args.parent:
        parent = conn.execute("SELECT id FROM categories WHERE name = ?", (args.parent,)).fetchone()
        if not parent:
            raise ValueError(f"Parent category '{args.parent}' not found")
        parent_id = parent["id"]

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, parent_id, is_system) VALUES (?, ?, ?, 0)",
        (category_id, args.name, parent_id),
    )
    conn.commit()

    return {
        "data": {"category_id": category_id, "created": True},
        "summary": {"total_categories": 1},
        "cli_report": f"Added category '{args.name}'",
    }


def handle_memory_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    where = []
    params: list[Any] = []
    if args.unconfirmed:
        where.append("m.is_confirmed = 0")
    if args.search:
        where.append("m.description_pattern LIKE ?")
        params.append(f"%{args.search}%")

    query = """
        SELECT m.id, m.description_pattern, m.use_type, m.confidence, m.priority,
               m.is_enabled, m.is_confirmed, m.match_count, c.name AS category_name
          FROM vendor_memory m
          LEFT JOIN categories c ON c.id = m.category_id
    """
    if where:
        query += f" WHERE {' AND '.join(where)}"
    query += " ORDER BY m.priority DESC, m.match_count DESC, m.description_pattern ASC LIMIT ?"
    params.append(int(args.limit))

    rows = conn.execute(query, tuple(params)).fetchall()
    rules = [dict(row) for row in rows]
    cli_report = (
        "\n".join(f"{r['description_pattern']} -> {r['category_name']}" for r in rules[: args.limit])
        if rules
        else "No memory rules"
    )

    return {
        "data": {"rules": rules},
        "summary": {"total_rules": len(rules)},
        "cli_report": cli_report,
    }


def handle_memory_add(args, conn: sqlite3.Connection) -> dict[str, Any]:
    category_id = get_category_id_by_name(conn, args.category)
    if not category_id:
        raise ValueError(f"Category '{args.category}' not found")

    pattern = normalize_description(args.pattern)
    if not pattern:
        raise ValueError("Pattern cannot be empty after normalization")

    existing = conn.execute(
        "SELECT id FROM vendor_memory WHERE description_pattern = ? AND use_type = ?",
        (pattern, args.use_type),
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
        created = False
    else:
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
            (rule_id, pattern, category_id, args.use_type),
        )
        created = True

    conn.commit()
    return {
        "data": {"rule_id": rule_id, "created": created},
        "summary": {"total_rules": 1},
        "cli_report": f"Saved vendor memory rule {rule_id}",
    }


def handle_memory_disable(args, conn: sqlite3.Connection) -> dict[str, Any]:
    cursor = conn.execute(
        "UPDATE vendor_memory SET is_enabled = 0 WHERE id = ?",
        (args.id,),
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Rule {args.id} not found")

    return {
        "data": {"rule_id": args.id, "is_enabled": False},
        "summary": {"total_rules": 1},
        "cli_report": f"Disabled rule {args.id}",
    }


def handle_memory_confirm(args, conn: sqlite3.Connection) -> dict[str, Any]:
    cursor = conn.execute(
        """
        UPDATE vendor_memory
           SET is_confirmed = 1,
               is_enabled = 1
         WHERE id = ?
        """,
        (args.id,),
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Rule {args.id} not found")

    return {
        "data": {"rule_id": args.id, "is_confirmed": True},
        "summary": {"total_rules": 1},
        "cli_report": f"Confirmed rule {args.id}",
    }


def handle_memory_delete(args, conn: sqlite3.Connection) -> dict[str, Any]:
    conn.execute(
        "UPDATE transactions SET category_rule_id = NULL WHERE category_rule_id = ?",
        (args.id,),
    )
    cursor = conn.execute("DELETE FROM vendor_memory WHERE id = ?", (args.id,))
    conn.commit()
    if cursor.rowcount == 0:
        raise ValueError(f"Rule {args.id} not found")

    return {
        "data": {"rule_id": args.id, "deleted": True},
        "summary": {"total_rules": 1},
        "cli_report": f"Deleted rule {args.id}",
    }


def handle_memory_undo(args, conn: sqlite3.Connection) -> dict[str, Any]:
    txn = conn.execute(
        "SELECT category_rule_id FROM transactions WHERE id = ?",
        (args.txn_id,),
    ).fetchone()
    if not txn:
        raise ValueError(f"Transaction {args.txn_id} not found")
    rule_id = txn["category_rule_id"]
    if not rule_id:
        raise ValueError(f"Transaction {args.txn_id} has no category rule")

    conn.execute(
        "UPDATE transactions SET category_rule_id = NULL, category_id = NULL, category_source = NULL, category_confidence = NULL WHERE category_rule_id = ?",
        (rule_id,),
    )
    conn.execute("DELETE FROM vendor_memory WHERE id = ?", (rule_id,))
    conn.commit()

    return {
        "data": {"transaction_id": args.txn_id, "removed_rule_id": rule_id},
        "summary": {"total_rules": 1},
        "cli_report": f"Removed rule {rule_id}",
    }


def handle_auto_categorize(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT id, description, use_type, source_category, is_payment
          FROM transactions
         WHERE is_active = 1
           AND is_reviewed = 0
           AND (
               category_id IS NULL
               OR category_source IN ('ambiguous', 'institution', 'plaid', 'category_mapping')
           )
        """
    ).fetchall()

    updated = 0
    ambiguous = 0
    by_source: dict[str, int] = {}

    for row in rows:
        result: MatchResult | None = match_transaction(
            conn,
            row["description"],
            row["use_type"],
            source_category=row["source_category"],
            is_payment=bool(row["is_payment"]),
        )
        if not result:
            continue
        # Skip payment_exclusion sentinel (category_source=None, category_id=None).
        # Do NOT skip ambiguous — apply_match persists category_source='ambiguous'.
        if result.category_source is None and result.category_id is None:
            continue
        if result.category_source == "ambiguous":
            ambiguous += 1
        if apply_match(conn, row["id"], result, dry_run=args.dry_run):
            updated += 1
            source = result.category_source or "none"
            by_source[source] = by_source.get(source, 0) + 1

    ai_report: dict[str, Any] | None = None
    if args.ai:
        ai_report = categorize_uncategorized(
            conn,
            limit=max(len(rows), 1),
            dry_run=args.dry_run,
            provider=args.provider,
            batch_size=args.batch_size,
        )
        if ai_report.get("categorized"):
            updated += int(ai_report["categorized"])
            by_source["ai"] = by_source.get("ai", 0) + int(ai_report["categorized"])
        ambiguous += int(ai_report.get("failed", 0))

    if args.dry_run:
        conn.rollback()

    cli_lines = [f"Auto-categorized {updated} transactions"]
    if args.ai and ai_report is not None:
        cli_lines.append(
            "ai: categorized={categorized} failed={failed} batches={batches} "
            "tokens=in:{input_tokens}/out:{output_tokens} elapsed={elapsed_ms}ms".format(
                categorized=int(ai_report.get("categorized", 0)),
                failed=int(ai_report.get("failed", 0)),
                batches=int(ai_report.get("batches", 0)),
                input_tokens=int(ai_report.get("input_tokens", 0)),
                output_tokens=int(ai_report.get("output_tokens", 0)),
                elapsed_ms=int(ai_report.get("elapsed_ms", 0)),
            )
        )

    return {
        "data": {"updated": updated, "ambiguous": ambiguous, "by_source": by_source, "ai": ai_report},
        "summary": {"total_transactions": updated, "total_amount": 0},
        "cli_report": "\n".join(cli_lines),
    }


def _category_name_exists(conn: sqlite3.Connection, category_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM categories WHERE lower(name) = lower(?) LIMIT 1",
        (category_name,),
    ).fetchone()
    return bool(row)


def _find_split_candidates(conn: sqlite3.Connection, *, backfill: bool) -> list[dict[str, Any]]:
    rules = load_rules()
    where = [
        "t.is_active = 1",
        "t.parent_transaction_id IS NULL",
        "t.split_group_id IS NULL",
        "NOT EXISTS (SELECT 1 FROM transactions child WHERE child.parent_transaction_id = t.id)",
    ]
    if not backfill:
        where.append("t.is_reviewed = 0")

    rows = conn.execute(
        f"""
        SELECT t.id, t.description, c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE {' AND '.join(where)}
         ORDER BY t.date DESC, t.created_at DESC
        """
    ).fetchall()

    category_exists_cache: dict[str, bool] = {}
    matches: list[dict[str, Any]] = []
    for row in rows:
        description = str(row["description"] or "")
        category_name = str(row["category_name"] or "")
        split_rule = get_split_rule(description, category_name, rules)
        if not split_rule:
            continue

        for category_name_probe in (split_rule.business_category, split_rule.personal_category):
            if category_name_probe not in category_exists_cache:
                category_exists_cache[category_name_probe] = _category_name_exists(conn, category_name_probe)
        if not category_exists_cache[split_rule.business_category] or not category_exists_cache[split_rule.personal_category]:
            continue

        matches.append(
            {
                "transaction_id": str(row["id"]),
                "description": description,
                "category_name": category_name or None,
                "business_pct": float(split_rule.business_pct),
                "business_category": split_rule.business_category,
                "personal_category": split_rule.personal_category,
                "note": split_rule.note,
                "rule_index": int(split_rule.rule_index),
            }
        )
    return matches


def handle_apply_splits(args, conn: sqlite3.Connection) -> dict[str, Any]:
    matches = _find_split_candidates(conn, backfill=bool(args.backfill))
    split_count = 0
    if args.commit:
        rules = load_rules()
        for match in matches:
            txn_id = str(match["transaction_id"])
            _apply_split_rule(conn, txn_id, rules)
            parent = conn.execute(
                "SELECT is_active, split_group_id FROM transactions WHERE id = ?",
                (txn_id,),
            ).fetchone()
            child_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM transactions WHERE parent_transaction_id = ?",
                (txn_id,),
            ).fetchone()["cnt"]
            if parent and int(parent["is_active"] or 0) == 0 and parent["split_group_id"] and int(child_count or 0) >= 2:
                split_count += 1
        conn.commit()

    candidate_count = len(matches)
    if args.commit:
        cli_report = f"{split_count} transactions split into {split_count * 2} children"
    else:
        cli_report = f"{candidate_count} transactions would be split"

    return {
        "data": {
            "commit": bool(args.commit),
            "backfill": bool(args.backfill),
            "candidate_transactions": candidate_count,
            "split_transactions": split_count,
            "created_children": split_count * 2,
            "matches": matches,
        },
        "summary": {
            "total_transactions": split_count if args.commit else candidate_count,
            "total_amount": 0,
        },
        "cli_report": cli_report,
    }


def handle_classify_use_type(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rules = load_rules()
    rows = conn.execute(
        """
        SELECT t.id, t.description, t.category_source, c.name AS category_name
          FROM transactions t
          LEFT JOIN categories c ON c.id = t.category_id
         WHERE t.is_active = 1
           AND t.use_type IS NULL
        """
    ).fetchall()

    updates: list[tuple[str, str, str]] = []
    by_reason = {"keyword_rule": 0, "category_override": 0}
    for row in rows:
        txn_id = str(row["id"])
        description = str(row["description"] or "")
        category_name = str(row["category_name"] or "").strip()
        category_source = str(row["category_source"] or "")

        resolved_use_type: str | None = None
        reason: str | None = None

        if category_source == "keyword_rule":
            keyword_match = match_keyword_rule(description, rules)
            if keyword_match and keyword_match.use_type in {"Business", "Personal"}:
                resolved_keyword_category = resolve_category_alias(keyword_match.category, rules)
                if category_name and resolved_keyword_category and resolved_keyword_category.lower() == category_name.lower():
                    resolved_use_type = keyword_match.use_type
                    reason = "keyword_rule"

        if resolved_use_type is None and category_name:
            override_use_type = get_category_override(category_name, category_source, rules)
            if override_use_type in {"Business", "Personal"}:
                resolved_use_type = override_use_type
                reason = "category_override"

        if resolved_use_type and reason:
            updates.append((txn_id, resolved_use_type, reason))
            by_reason[reason] += 1

    updated = 0
    if args.commit and updates:
        for txn_id, use_type, _reason in updates:
            conn.execute(
                """
                UPDATE transactions
                   SET use_type = ?,
                       updated_at = datetime('now')
                 WHERE id = ?
                   AND is_active = 1
                   AND use_type IS NULL
                """,
                (use_type, txn_id),
            )
            updated += 1
        conn.commit()

    candidate_updates = len(updates)
    cli_report = (
        f"{updated} transactions updated"
        if args.commit
        else f"{candidate_updates} transactions would be updated"
    )
    return {
        "data": {
            "commit": bool(args.commit),
            "scanned_null_use_type": len(rows),
            "candidate_updates": candidate_updates,
            "updated": updated,
            "by_reason": by_reason,
        },
        "summary": {
            "total_transactions": updated if args.commit else candidate_updates,
            "total_amount": 0,
        },
        "cli_report": cli_report,
    }


def _get_or_create_canonical_category(conn: sqlite3.Connection, name: str) -> str:
    row = conn.execute(
        "SELECT id FROM categories WHERE lower(name) = lower(?)",
        (name,),
    ).fetchone()
    if row:
        category_id = str(row["id"])
        conn.execute("UPDATE categories SET is_system = 1 WHERE id = ?", (category_id,))
        return category_id

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 1)",
        (category_id, name),
    )
    return category_id


def _backfill_source_category(conn: sqlite3.Connection) -> tuple[int, int]:
    plaid_backfilled = 0
    rows = conn.execute(
        """
        SELECT id, raw_plaid_json
          FROM transactions
         WHERE source = 'plaid'
           AND source_category IS NULL
           AND raw_plaid_json IS NOT NULL
        """
    ).fetchall()
    for row in rows:
        raw_payload = str(row["raw_plaid_json"] or "").strip()
        if not raw_payload:
            continue
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            continue
        pfc = payload.get("personal_finance_category")
        if not isinstance(pfc, dict):
            continue
        detailed = str(pfc.get("detailed") or "").strip()
        if not detailed:
            continue
        cursor = conn.execute(
            "UPDATE transactions SET source_category = ? WHERE id = ? AND source_category IS NULL",
            (detailed, row["id"]),
        )
        plaid_backfilled += int(cursor.rowcount or 0)

    canonical_lc = {name.lower() for name in CANONICAL_CATEGORIES}
    csv_pdf_backfilled = 0
    rows = conn.execute(
        """
        SELECT t.id, c.name AS category_name
          FROM transactions t
          JOIN categories c ON c.id = t.category_id
         WHERE t.source_category IS NULL
           AND t.source IN ('csv_import', 'pdf_import')
        """
    ).fetchall()
    for row in rows:
        category_name = str(row["category_name"] or "").strip()
        if not category_name or category_name.lower() in canonical_lc:
            continue
        cursor = conn.execute(
            "UPDATE transactions SET source_category = ? WHERE id = ? AND source_category IS NULL",
            (category_name, row["id"]),
        )
        csv_pdf_backfilled += int(cursor.rowcount or 0)
    return plaid_backfilled, csv_pdf_backfilled


def _seed_category_mappings(conn: sqlite3.Connection) -> int:
    rules = load_rules()
    seeded = 0
    for source_category, target_category in rules.category_aliases.items():
        category_id = None
        if target_category:
            category_id = _get_or_create_canonical_category(conn, target_category)
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO category_mappings (
                id,
                source_category,
                source,
                category_id,
                created_by,
                confidence,
                is_enabled
            ) VALUES (?, ?, NULL, ?, 'system', 1.0, 1)
            """,
            (uuid.uuid4().hex, source_category, category_id),
        )
        if int(cursor.rowcount or 0) > 0:
            seeded += 1
            continue

        update_cursor = conn.execute(
            """
            UPDATE category_mappings
               SET category_id = ?,
                   created_by = 'system',
                   confidence = 1.0,
                   is_enabled = 1,
                   updated_at = datetime('now')
             WHERE lower(source_category) = lower(?)
               AND COALESCE(source, '') = ''
            """,
            (category_id, source_category),
        )
        seeded += int(update_cursor.rowcount or 0)
    return seeded


def _remap_non_canonical_categories(conn: sqlite3.Connection) -> dict[str, Any]:
    rules = load_rules()
    categories = conn.execute("SELECT id, name FROM categories ORDER BY name ASC").fetchall()

    categories_remapped = 0
    transactions_moved = 0
    transactions_nulled = 0
    unmapped: list[str] = []

    for row in categories:
        category_id = str(row["id"])
        category_name = str(row["name"] or "").strip()
        if not category_name:
            continue
        if category_name in CANONICAL_CATEGORIES:
            continue

        resolved = resolve_category_alias(category_name, rules)
        if resolved is None:
            conn.execute("UPDATE category_mappings SET category_id = NULL WHERE category_id = ?", (category_id,))
            txn_cursor = conn.execute(
                """
                UPDATE transactions
                   SET category_id = NULL,
                       category_source = NULL,
                       category_confidence = NULL,
                       updated_at = datetime('now')
                 WHERE category_id = ?
                """,
                (category_id,),
            )
            transactions_nulled += int(txn_cursor.rowcount or 0)
            conn.execute("DELETE FROM categories WHERE id = ?", (category_id,))
            categories_remapped += 1
            continue

        if resolved not in CANONICAL_CATEGORIES:
            unmapped.append(category_name)
            continue

        canonical_id = _get_or_create_canonical_category(conn, resolved)
        if canonical_id == category_id:
            continue

        conn.execute("UPDATE category_mappings SET category_id = ? WHERE category_id = ?", (canonical_id, category_id))
        txn_cursor = conn.execute("UPDATE transactions SET category_id = ?, updated_at = datetime('now') WHERE category_id = ?", (canonical_id, category_id))
        conn.execute("UPDATE vendor_memory SET category_id = ? WHERE category_id = ?", (canonical_id, category_id))
        conn.execute("UPDATE budgets SET category_id = ? WHERE category_id = ?", (canonical_id, category_id))
        conn.execute("UPDATE subscriptions SET category_id = ? WHERE category_id = ?", (canonical_id, category_id))
        conn.execute("UPDATE recurring_flows SET category_id = ? WHERE category_id = ?", (canonical_id, category_id))
        conn.execute("DELETE FROM categories WHERE id = ?", (category_id,))

        transactions_moved += int(txn_cursor.rowcount or 0)
        categories_remapped += 1

    return {
        "categories_remapped": categories_remapped,
        "transactions_moved": transactions_moved,
        "transactions_nulled": transactions_nulled,
        "unmapped_categories": sorted(set(unmapped)),
    }


def _mark_canonical_categories_as_system(conn: sqlite3.Connection) -> int:
    marked = 0
    for name in sorted(CANONICAL_CATEGORIES):
        category_id = _get_or_create_canonical_category(conn, name)
        cursor = conn.execute("UPDATE categories SET is_system = 1 WHERE id = ?", (category_id,))
        marked += int(cursor.rowcount or 0)
    return marked


def handle_normalize(args, conn: sqlite3.Connection) -> dict[str, Any]:
    plaid_backfilled, csv_pdf_backfilled = _backfill_source_category(conn)
    seeded_mappings = _seed_category_mappings(conn)
    remap_report = _remap_non_canonical_categories(conn)
    canonical_marked = _mark_canonical_categories_as_system(conn)

    if args.dry_run:
        conn.rollback()
    else:
        conn.commit()

    data = {
        "dry_run": bool(args.dry_run),
        "source_category_backfilled": {
            "plaid": plaid_backfilled,
            "csv_pdf": csv_pdf_backfilled,
            "total": plaid_backfilled + csv_pdf_backfilled,
        },
        "mappings_seeded": seeded_mappings,
        "categories_remapped": remap_report["categories_remapped"],
        "transactions_moved": remap_report["transactions_moved"],
        "transactions_nulled": remap_report["transactions_nulled"],
        "unmapped_categories": remap_report["unmapped_categories"],
        "canonical_marked": canonical_marked,
    }
    cli_lines = [
        f"Normalized categories dry_run={bool(args.dry_run)}",
        f"backfilled source_category={data['source_category_backfilled']['total']}",
        f"seeded mappings={seeded_mappings}",
        f"categories remapped={remap_report['categories_remapped']}",
        f"transactions moved={remap_report['transactions_moved']}",
        f"transactions nulled={remap_report['transactions_nulled']}",
    ]
    if remap_report["unmapped_categories"]:
        cli_lines.append("unmapped: " + ", ".join(remap_report["unmapped_categories"]))
    return {
        "data": data,
        "summary": {"total_categories": remap_report["categories_remapped"]},
        "cli_report": "\n".join(cli_lines),
    }
