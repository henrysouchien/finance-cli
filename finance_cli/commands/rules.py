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

from ..user_rules import (
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


def _ensure_rules_file() -> Path:
    target = resolve_rules_path()
    if target.exists():
        return target

    target.parent.mkdir(parents=True, exist_ok=True)
    package_default = Path(__file__).resolve().parents[1] / "data" / "rules.yaml"
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
    return payload


def _write_raw_rules_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    # Invalidate mtime cache so load_rules() re-reads immediately
    from ..user_rules import invalidate_rules_cache

    invalidate_rules_cache()


def _validate_rules_against_categories(conn: sqlite3.Connection) -> list[str]:
    rules = load_rules()
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


def handle_add_keyword(args, conn: sqlite3.Connection) -> dict[str, Any]:
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

    path = _ensure_rules_file()
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


def handle_remove_keyword(args, conn: sqlite3.Connection) -> dict[str, Any]:
    keyword = str(args.keyword or "").strip()
    if not keyword:
        raise ValueError("--keyword is required")

    path = _ensure_rules_file()
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


def handle_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    """Return structured keyword_rules list."""
    rules = load_rules()
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
    cli_lines = []
    for item in items:
        ut = f" [{item['use_type']}]" if item["use_type"] else ""
        pri = f" (priority={item['priority']})" if item["priority"] else ""
        kws = ", ".join(item["keywords"][:5])
        if len(item["keywords"]) > 5:
            kws += f" (+{len(item['keywords']) - 5} more)"
        cli_lines.append(f"  [{item['rule_index']}] {item['category']}{ut}{pri}: {kws}")
    return {
        "data": {"rules": items, "count": len(items)},
        "summary": {"count": len(items)},
        "cli_report": f"Keyword rules ({len(items)}):\n" + "\n".join(cli_lines),
    }


def handle_update_priority(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rule_index = int(args.rule_index)
    priority = int(args.priority)

    path = _ensure_rules_file()
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


def handle_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    path = _ensure_rules_file()
    rules = load_rules(path)
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


def handle_edit(args, conn: sqlite3.Connection) -> dict[str, Any]:
    path = _ensure_rules_file()
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


def handle_validate(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rules = load_rules()
    unknown_categories = _validate_rules_against_categories(conn)
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


def handle_test(args, conn: sqlite3.Connection) -> dict[str, Any]:
    rules = load_rules()

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
