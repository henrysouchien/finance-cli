"""User rules commands."""

from __future__ import annotations

import os
import shlex
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

import yaml

from .. import storage_files
from ..storage_client import _dispatch as storage_dispatch
from ..storage_lease import optional_lease_scope
from ..user_rules import (
    CANONICAL_CATEGORIES,
    get_category_override,
    get_split_rule,
    load_rules,
    match_keyword_rule,
    match_payment_keyword,
    resolve_category_alias,
    resolve_rules_path,
)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("rules", parents=[format_parent], help="User rules config")
    rules_sub = parser.add_subparsers(dest="rules_command", required=True)

    p_add_keyword = rules_sub.add_parser(
        "add-keyword",
        parents=[format_parent],
        help="Add a keyword categorization rule",
    )
    p_add_keyword.add_argument("--keyword", required=True)
    p_add_keyword.add_argument("--category", required=True)
    p_add_keyword.add_argument("--use-type", dest="use_type", choices=["Business", "Personal"])
    p_add_keyword.add_argument("--priority", type=int, default=0)
    p_add_keyword.set_defaults(func=handle_add_keyword, command_name="rules.add-keyword")

    p_add_split = rules_sub.add_parser(
        "add-split",
        parents=[format_parent],
        help="Add a business/personal split rule",
    )
    p_add_split.add_argument("--business-pct", type=float, required=True)
    p_add_split.add_argument("--business-category", required=True)
    p_add_split.add_argument("--personal-category", required=True)
    p_add_split.add_argument("--match-category")
    p_add_split.add_argument("--match-keywords", nargs="+")
    p_add_split.add_argument("--note")
    p_add_split.set_defaults(func=handle_add_split, command_name="rules.add-split")

    p_remove_keyword = rules_sub.add_parser(
        "remove-keyword",
        parents=[format_parent],
        help="Remove a keyword categorization rule",
    )
    p_remove_keyword.add_argument("--keyword", required=True)
    p_remove_keyword.set_defaults(func=handle_remove_keyword, command_name="rules.remove-keyword")

    p_show = rules_sub.add_parser("show", parents=[format_parent], help="Show rules.yaml")
    p_show.set_defaults(func=handle_show, command_name="rules.show")

    p_edit = rules_sub.add_parser("edit", parents=[format_parent], help="Edit rules.yaml")
    p_edit.set_defaults(func=handle_edit, command_name="rules.edit")

    p_validate = rules_sub.add_parser("validate", parents=[format_parent], help="Validate rules.yaml")
    p_validate.set_defaults(func=handle_validate, command_name="rules.validate")

    p_test = rules_sub.add_parser("test", parents=[format_parent], help="Test rules against a description")
    p_test.add_argument("--description", required=True)
    p_test.add_argument("--category")
    p_test.add_argument("--source", default="plaid")
    p_test.set_defaults(func=handle_test, command_name="rules.test")


def _ensure_rules_file(rules_path: Path | None = None) -> Path:
    target = resolve_rules_path(rules_path)
    if target.exists():
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    from ..config import PACKAGE_TEMPLATE_DIR
    package_default = PACKAGE_TEMPLATE_DIR / "rules_template.yaml"
    if package_default.exists():
        shutil.copyfile(package_default, target)
    else:
        target.write_text("{}\n", encoding="utf-8")
    return target


def _known_categories(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM categories").fetchall()
    return {str(row["name"]).strip().lower() for row in rows if str(row["name"]).strip()}


def _load_raw_rules_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("rules.yaml root must be a mapping")
    keyword_rules = payload.get("keyword_rules")
    if keyword_rules is None:
        payload["keyword_rules"] = []
    elif not isinstance(keyword_rules, list):
        raise ValueError("keyword_rules must be a list")
    split_rules = payload.get("split_rules")
    if split_rules is None:
        payload["split_rules"] = []
    elif not isinstance(split_rules, list):
        raise ValueError("split_rules must be a list")
    return payload


def _write_raw_rules_yaml(path: Path, payload: dict[str, Any]) -> None:
    rendered = yaml.safe_dump(payload, sort_keys=False)
    user_id = storage_dispatch.user_id_from_user_file_path(path)
    with optional_lease_scope(
        user_id,
        operation="request",
        metadata={"source": "rules._write_raw_rules_yaml"},
    ):
        remote_target = (
            storage_dispatch.remote_file_target_for_user(user_id)
            if user_id is not None
            else None
        )
        if remote_target and user_id is not None:
            storage_files.write_file(
                remote_target,
                user_id=user_id,
                product="finance_cli",
                relative_path="rules.yaml",
                content=rendered.encode("utf-8"),
            )
        else:
            path.write_text(rendered, encoding="utf-8")
    # Invalidate mtime cache so load_rules() re-reads immediately
    from ..user_rules import invalidate_rules_cache

    invalidate_rules_cache()


def _normalize_rule_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _normalize_rule_keywords(values: list[str]) -> tuple[str, ...]:
    normalized = {
        keyword
        for keyword in (_normalize_rule_text(value) for value in values)
        if keyword is not None
    }
    return tuple(sorted(normalized))


def _split_rule_signature(
    *,
    match_category: str | None,
    match_keywords: list[str],
    business_pct: float,
    business_category: str,
    personal_category: str,
    note: str | None,
) -> tuple[Any, ...]:
    return (
        _normalize_rule_text(match_category),
        _normalize_rule_keywords(match_keywords),
        float(business_pct),
        _normalize_rule_text(business_category),
        _normalize_rule_text(personal_category),
        _normalize_rule_text(note),
    )


def _validate_rules_against_categories(
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> list[str]:
    rules = load_rules(path=rules_path)
    known = _known_categories(conn)

    unknown: set[str] = set()

    for rule in rules.keyword_rules:
        if rule.category.strip().lower() not in known:
            unknown.add(rule.category)

    for split_rule in rules.split_rules:
        if split_rule.match_category and split_rule.match_category.strip().lower() not in known:
            unknown.add(split_rule.match_category)
        if split_rule.business_category.strip().lower() not in known:
            unknown.add(split_rule.business_category)
        if split_rule.personal_category.strip().lower() not in known:
            unknown.add(split_rule.personal_category)

    for override in rules.category_overrides:
        for category in override.categories:
            if category.strip().lower() not in known:
                unknown.add(category)

    for canonical in rules.category_aliases.values():
        if canonical is None:
            continue
        if canonical.strip().lower() not in known:
            unknown.add(canonical)

    essential_categories_raw = rules.raw.get("essential_categories") if isinstance(rules.raw, dict) else None
    if isinstance(essential_categories_raw, list):
        for category in essential_categories_raw:
            normalized = str(category).strip()
            if not normalized:
                continue
            if normalized.lower() not in known:
                unknown.add(normalized)

    for source_name, source_cfg in rules.income_sources.items():
        category = str(source_cfg.get("category") or "").strip()
        if category and category.lower() not in known:
            unknown.add(f"{category} (income_sources.{source_name})")

    return sorted(unknown)


def _validate_category_name(
    conn: sqlite3.Connection,
    category: Any,
    *,
    field_name: str,
) -> str:
    normalized = str(category or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if normalized not in CANONICAL_CATEGORIES:
        raise ValueError(f"{field_name} must be a canonical category")
    if normalized.lower() not in _known_categories(conn):
        raise ValueError(f"Category '{normalized}' not found")
    return normalized


def _normalize_business_pct(value: Any) -> int | float:
    pct = float(value)
    if pct <= 0 or pct >= 100:
        raise ValueError("business_pct must be > 0 and < 100")
    if pct.is_integer():
        return int(pct)
    return pct


def handle_add_keyword(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    keyword = str(args.keyword or "").strip()
    if not keyword:
        raise ValueError("--keyword is required")

    category = str(args.category or "").strip()
    if not category:
        raise ValueError("--category is required")

    use_type = args.use_type
    if use_type is not None:
        use_type = str(use_type).strip()
        if use_type not in {"Business", "Personal"}:
            raise ValueError("--use-type must be Business or Personal")

    if category.lower() not in _known_categories(conn):
        raise ValueError(f"Category '{category}' not found")

    path = _ensure_rules_file(rules_path=rules_path)
    payload = _load_raw_rules_yaml(path)
    keyword_rules = payload["keyword_rules"]
    if not isinstance(keyword_rules, list):  # pragma: no cover (guarded by loader)
        raise ValueError("keyword_rules must be a list")

    needle = keyword.lower()
    for index, rule in enumerate(keyword_rules):
        if not isinstance(rule, dict):
            raise ValueError(f"keyword_rules[{index}] must be a mapping")
        keywords = rule.get("keywords") or []
        if not isinstance(keywords, list):
            raise ValueError(f"keyword_rules[{index}].keywords must be a list")
        for existing in keywords:
            if str(existing).strip().lower() == needle:
                raise ValueError(f"Keyword '{keyword}' already exists")

    action = "added"
    matched_rule: dict[str, Any] | None = None
    for index, rule in enumerate(keyword_rules):
        if not isinstance(rule, dict):
            raise ValueError(f"keyword_rules[{index}] must be a mapping")
        rule_category = str(rule.get("category") or "").strip()
        rule_use_type_raw = rule.get("use_type")
        rule_use_type = str(rule_use_type_raw).strip() if rule_use_type_raw is not None else None
        if rule_category.lower() == category.lower() and rule_use_type == use_type:
            matched_rule = rule
            break

    if matched_rule is not None:
        keywords = matched_rule.get("keywords") or []
        if not isinstance(keywords, list):
            raise ValueError("Matched keyword rule has invalid keywords list")
        keywords.append(keyword)
        matched_rule["keywords"] = keywords
        action = "appended"
    else:
        rule_entry: dict[str, Any] = {
            "keywords": [keyword],
            "category": category,
            "priority": int(args.priority),
        }
        if use_type is not None:
            rule_entry["use_type"] = use_type
        keyword_rules.append(rule_entry)

    _write_raw_rules_yaml(path, payload)

    use_type_report = use_type or "Any"
    return {
        "data": {
            "keyword": keyword,
            "category": category,
            "use_type": use_type,
            "action": action,
        },
        "summary": {"updated": 1},
        "cli_report": f"{action.capitalize()} keyword '{keyword}' for {category} ({use_type_report})",
    }


def handle_add_split(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    match_category_raw = getattr(args, "match_category", None)
    match_category = str(match_category_raw).strip() if match_category_raw is not None else ""
    match_category = match_category or None

    raw_keywords = getattr(args, "match_keywords", None) or []
    if isinstance(raw_keywords, str):
        raw_keywords = [raw_keywords]
    match_keywords = [str(value).strip() for value in raw_keywords if str(value).strip()]
    if not match_category and not match_keywords:
        raise ValueError("At least one of match_category or match_keywords is required")

    business_category = _validate_category_name(
        conn,
        getattr(args, "business_category", None),
        field_name="business_category",
    )
    personal_category = _validate_category_name(
        conn,
        getattr(args, "personal_category", None),
        field_name="personal_category",
    )
    if match_category is not None:
        match_category = _validate_category_name(
            conn,
            match_category,
            field_name="match_category",
        )

    business_pct = _normalize_business_pct(getattr(args, "business_pct", 0))
    note_raw = getattr(args, "note", None)
    note = str(note_raw).strip() if note_raw is not None else ""
    note = note or None

    path = _ensure_rules_file(rules_path=rules_path)
    payload = _load_raw_rules_yaml(path)
    split_rules = payload["split_rules"]
    if not isinstance(split_rules, list):  # pragma: no cover (guarded by loader)
        raise ValueError("split_rules must be a list")
    existing_split_rules = load_rules(path=path).split_rules

    new_signature = _split_rule_signature(
        match_category=match_category,
        match_keywords=match_keywords,
        business_pct=business_pct,
        business_category=business_category,
        personal_category=personal_category,
        note=note,
    )
    for split_rule in existing_split_rules:
        existing_signature = _split_rule_signature(
            match_category=split_rule.match_category,
            match_keywords=split_rule.match_keywords,
            business_pct=split_rule.business_pct,
            business_category=split_rule.business_category,
            personal_category=split_rule.personal_category,
            note=split_rule.note,
        )
        if existing_signature == new_signature:
            raise ValueError("Split rule already exists")

    normalized_match_category = _normalize_rule_text(match_category)
    normalized_match_keywords = _normalize_rule_keywords(match_keywords)
    for split_rule in existing_split_rules:
        existing_match_category = _normalize_rule_text(split_rule.match_category)
        if normalized_match_category and existing_match_category == normalized_match_category:
            raise ValueError(f"Split rule for category '{match_category}' already exists")

        existing_keywords = _normalize_rule_keywords(split_rule.match_keywords)
        for new_keyword in normalized_match_keywords:
            for existing_keyword in existing_keywords:
                if new_keyword in existing_keyword or existing_keyword in new_keyword:
                    raise ValueError(
                        "Split rule keyword overlap: "
                        f"'{new_keyword}' overlaps existing keyword '{existing_keyword}'"
                    )

    match_payload: dict[str, Any] = {}
    if match_category is not None:
        match_payload["category"] = match_category
    if match_keywords:
        match_payload["keywords"] = match_keywords

    rule_entry: dict[str, Any] = {
        "match": match_payload,
        "business_pct": business_pct,
        "business_category": business_category,
        "personal_category": personal_category,
    }
    if note is not None:
        rule_entry["note"] = note

    split_rules.append(rule_entry)
    _write_raw_rules_yaml(path, payload)

    return {
        "data": {
            "rule": rule_entry,
            "split_rule_count": len(split_rules),
        },
        "summary": {"updated": 1, "split_rule_count": len(split_rules)},
        "cli_report": (
            f"Added split rule ({business_pct}% business) for "
            f"{match_category or ', '.join(match_keywords)}"
        ),
    }


def handle_remove_keyword(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    keyword = str(args.keyword or "").strip()
    dry_run = bool(getattr(args, "dry_run", False))
    if not keyword:
        raise ValueError("--keyword is required")

    path = _ensure_rules_file(rules_path=rules_path)
    payload = _load_raw_rules_yaml(path)
    keyword_rules = payload["keyword_rules"]
    if not isinstance(keyword_rules, list):  # pragma: no cover (guarded by loader)
        raise ValueError("keyword_rules must be a list")

    probe = keyword.lower()
    for rule_index, rule in enumerate(keyword_rules):
        if not isinstance(rule, dict):
            raise ValueError(f"keyword_rules[{rule_index}] must be a mapping")
        keywords = rule.get("keywords") or []
        if not isinstance(keywords, list):
            raise ValueError(f"keyword_rules[{rule_index}].keywords must be a list")
        for keyword_index, existing in enumerate(keywords):
            if str(existing).strip().lower() != probe:
                continue

            removed_keyword = str(existing).strip() or keyword
            del keywords[keyword_index]
            cleaned_keywords = [str(value).strip() for value in keywords if str(value).strip()]
            removed_rule = False
            category = str(rule.get("category") or "").strip() or None
            if cleaned_keywords:
                rule["keywords"] = cleaned_keywords
            else:
                del keyword_rules[rule_index]
                removed_rule = True

            if dry_run:
                return {
                    "data": {
                        "dry_run": True,
                        "keyword": removed_keyword,
                        "category": category,
                        "removed_rule": removed_rule,
                        "remaining_keywords": cleaned_keywords,
                    },
                    "summary": {"dry_run": True, "would_update": 1},
                    "cli_report": f"[DRY RUN] Would remove keyword '{removed_keyword}'",
                }

            _write_raw_rules_yaml(path, payload)
            return {
                "data": {
                    "keyword": removed_keyword,
                    "category": category,
                    "removed_rule": removed_rule,
                },
                "summary": {"updated": 1},
                "cli_report": f"Removed keyword '{removed_keyword}'",
            }

    raise ValueError(f"Keyword '{keyword}' not found")


def handle_list(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    """Return structured keyword_rules list."""
    limit = int(getattr(args, "limit", 200))
    offset = int(getattr(args, "offset", 0))
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    rules = load_rules(path=rules_path)
    items = []
    for rule in rules.keyword_rules:
        items.append(
            {
                "rule_index": rule.rule_index,
                "category": rule.category,
                "keywords": rule.keywords,
                "use_type": rule.use_type,
                "priority": rule.priority,
            }
        )
    total_count = len(items)
    items = items[offset:offset + limit]
    cli_lines = []
    for item in items:
        ut = f" [{item['use_type']}]" if item["use_type"] else ""
        pri = f" (priority={item['priority']})" if item["priority"] else ""
        kws = ", ".join(item["keywords"][:5])
        if len(item["keywords"]) > 5:
            kws += f" (+{len(item['keywords']) - 5} more)"
        cli_lines.append(f"  [{item['rule_index']}] {item['category']}{ut}{pri}: {kws}")
    return {
        "data": {
            "rules": items,
            "count": len(items),
            "total_count": total_count,
            "limit": limit,
            "offset": offset,
        },
        "summary": {"count": total_count, "total_rules": total_count},
        "cli_report": f"Keyword rules ({len(items)}):\n" + "\n".join(cli_lines),
    }


def handle_update_priority(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    rule_index = int(args.rule_index)
    priority = int(args.priority)

    path = _ensure_rules_file(rules_path=rules_path)
    payload = _load_raw_rules_yaml(path)
    keyword_rules = payload["keyword_rules"]

    if rule_index < 0 or rule_index >= len(keyword_rules):
        raise ValueError(
            f"rule_index {rule_index} out of range (0..{len(keyword_rules) - 1})"
        )

    rule = keyword_rules[rule_index]
    if not isinstance(rule, dict):
        raise ValueError(f"keyword_rules[{rule_index}] is not a mapping")

    old_priority = int(rule.get("priority", 0))
    rule["priority"] = priority
    _write_raw_rules_yaml(path, payload)

    rule_category = str(rule.get("category") or "").strip()
    rule_use_type_raw = rule.get("use_type")
    rule_use_type = str(rule_use_type_raw).strip() if rule_use_type_raw is not None else None
    return {
        "data": {
            "category": rule_category,
            "use_type": rule_use_type,
            "old_priority": old_priority,
            "new_priority": priority,
            "rule_index": rule_index,
        },
        "summary": {"updated": 1},
        "cli_report": f"Updated priority for {rule_category}: {old_priority} -> {priority}",
    }


def handle_show(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    path = _ensure_rules_file(rules_path=rules_path)
    rules = load_rules(path=path)
    rendered = yaml.safe_dump(rules.raw or {}, sort_keys=False, allow_unicode=False)
    keyword_rules = [
        {
            "keywords": rule.keywords,
            "category": rule.category,
            "use_type": rule.use_type,
        }
        for rule in rules.keyword_rules
    ]
    split_rules = [
        {
            "match": {
                "category": rule.match_category,
                "keywords": rule.match_keywords,
            },
            "business_pct": rule.business_pct,
            "business_category": rule.business_category,
            "personal_category": rule.personal_category,
            "note": rule.note,
        }
        for rule in rules.split_rules
    ]
    counts = {
        "keyword_rules": len(keyword_rules),
        "payment_keywords": len(rules.payment_keywords),
        "category_aliases": len(rules.category_aliases),
        "split_rules": len(split_rules),
    }

    return {
        "data": {
            "path": str(path),
            "raw": rules.raw,
            "rules": rules.raw,
            "keyword_rules": keyword_rules,
            "payment_keywords": list(rules.payment_keywords),
            "category_aliases": dict(rules.category_aliases),
            "split_rules": split_rules,
            "counts": counts,
        },
        "summary": {"total_rules": len(rules.keyword_rules)},
        "cli_report": rendered.strip() or "{}",
    }


def handle_edit(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    path = _ensure_rules_file(rules_path=rules_path)
    editor = os.getenv("EDITOR") or "vi"
    editor_cmd = shlex.split(editor)
    if not editor_cmd:
        raise ValueError("EDITOR is empty")

    subprocess.run([*editor_cmd, str(path)], check=True)

    return {
        "data": {"path": str(path), "editor": editor},
        "summary": {"edited": 1},
        "cli_report": f"Opened {path} in {editor}",
    }


def handle_validate(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    rules = load_rules(path=rules_path)
    unknown_categories = _validate_rules_against_categories(conn, rules_path=rules_path)
    valid = len(unknown_categories) == 0

    if valid:
        cli_report = "rules.yaml is valid"
    else:
        cli_report = "Unknown categories:\n" + "\n".join(f"- {name}" for name in unknown_categories)

    return {
        "data": {
            "valid": valid,
            "errors": unknown_categories,
            "keyword_rule_count": len(rules.keyword_rules),
            "split_rule_count": len(rules.split_rules),
            "override_count": len(rules.category_overrides),
        },
        "summary": {"error_count": len(unknown_categories)},
        "cli_report": cli_report,
    }


def handle_test(
    args,
    conn: sqlite3.Connection,
    rules_path: Path | None = None,
) -> dict[str, Any]:
    rules = load_rules(path=rules_path)

    payment_match = match_payment_keyword(args.description, rules)
    keyword = match_keyword_rule(args.description, rules)
    split = get_split_rule(args.description, args.category or "", rules)

    resolved_category = None
    override = None
    if keyword:
        resolved_category = resolve_category_alias(keyword.category, rules)
        if resolved_category is not None:
            override = get_category_override(resolved_category, "keyword_rule", rules)
    elif args.category:
        resolved_category = resolve_category_alias(args.category, rules)
        if resolved_category is not None:
            override = get_category_override(resolved_category, args.source, rules)

    keyword_data = None
    if keyword:
        keyword_data = {
            "category": keyword.category,
            "resolved_category": resolved_category,
            "use_type": keyword.use_type,
            "matched_keyword": keyword.matched_keyword,
            "rule_index": keyword.rule_index,
        }

    split_data = None
    if split:
        split_data = {
            "business_pct": split.business_pct,
            "business_category": split.business_category,
            "personal_category": split.personal_category,
            "note": split.note,
            "rule_index": split.rule_index,
        }

    cli_parts = []
    if payment_match:
        cli_parts.append("payment keyword -> Payments & Transfers (takes priority)")
    if keyword_data:
        if payment_match:
            cli_parts.append(
                f"keyword (informational only) -> {keyword_data['resolved_category'] or keyword_data['category']} "
                f"({keyword_data['matched_keyword']})"
            )
        else:
            cli_parts.append(
                f"keyword -> {keyword_data['resolved_category'] or keyword_data['category']} "
                f"({keyword_data['matched_keyword']})"
            )
    if split_data:
        cli_parts.append(
            f"split -> {split_data['business_pct']}% business "
            f"({split_data['business_category']} / {split_data['personal_category']})"
        )
    if override:
        cli_parts.append(f"override -> {override}")
    if not cli_parts:
        cli_parts.append("No rules matched")

    return {
        "data": {
            "description": args.description,
            "category": args.category,
            "source": args.source,
            "payment_match": payment_match,
            "keyword_match": keyword_data,
            "split_rule": split_data,
            "category_override": override,
        },
        "summary": {
            "matches": int(payment_match) + int(bool(keyword_data)) + int(bool(split_data)) + int(bool(override))
        },
        "cli_report": "\n".join(cli_parts),
    }
