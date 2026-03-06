"""Guided setup and onboarding commands."""

from __future__ import annotations

import os
import shutil
import uuid
import webbrowser
from pathlib import Path
from typing import Any

from ..config import DEFAULT_DATA_DIR, DEFAULT_ENV_PATH, get_db_path
from ..plaid_client import (
    PlaidConfigStatus,
    PlaidUnavailableError,
    complete_link_session,
    config_status,
    create_hosted_link_session,
    refresh_balances,
    run_sync,
    sanitize_client_user_id,
)
from ..user_rules import CANONICAL_CATEGORIES, _CATEGORY_HIERARCHY as USER_RULES_CATEGORY_HIERARCHY, resolve_rules_path
from . import db_cmd, plaid_cmd

_ENV_DISABLE_VALUES = {"1", "true", "yes"}

_CATEGORY_HIERARCHY: dict[str, list[str]] = USER_RULES_CATEGORY_HIERARCHY

_INCOME_NAMES: frozenset[str] = frozenset(
    {
        "Income",
        "Income: Salary",
        "Income: Business",
        "Income: Other",
    }
)

_ENV_TEMPLATE = """# finance_cli environment configuration
# Fill in your values below.

# --- Plaid (required for bank sync) ---
PLAID_CLIENT_ID=
PLAID_SECRET=
PLAID_ENV=sandbox
# Replace example.com with your production callback domain.
PLAID_COMPLETION_REDIRECT_URI=https://example.com/plaid/complete

# --- AWS (for Plaid token storage) ---
# boto3 credential chain is used (env vars, profile, or IAM role).
# Set region; access keys only needed if not using profile/role.
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1

# --- AI categorization (optional) ---
# OPENAI_API_KEY=
# ANTHROPIC_API_KEY=

# --- Optional integrations ---
# BSC_API_KEY=
# AZURE_DI_ENDPOINT=
# AZURE_DI_KEY=
# SCHWAB_APP_KEY=
# SCHWAB_APP_SECRET=
"""


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("setup", parents=[format_parent], help="Environment setup and onboarding")
    setup_sub = parser.add_subparsers(dest="setup_command", required=True)

    p_check = setup_sub.add_parser("check", parents=[format_parent], help="Validate setup readiness")
    p_check.set_defaults(func=handle_check, command_name="setup.check")

    p_init = setup_sub.add_parser("init", parents=[format_parent], help="Initialize categories and config templates")
    p_init.add_argument("--dry-run", action="store_true")
    p_init.set_defaults(func=handle_init, command_name="setup.init")

    p_connect = setup_sub.add_parser("connect", parents=[format_parent], help="Link a Plaid institution and sync")
    p_connect.add_argument("--user-id", default="default")
    p_connect.add_argument("--include-liabilities", action="store_true")
    p_connect.add_argument("--timeout", type=int, default=300)
    p_connect.add_argument("--skip-sync", action="store_true")
    p_connect.add_argument("--open-browser", action="store_true")
    p_connect.set_defaults(func=handle_connect, command_name="setup.connect")

    p_status = setup_sub.add_parser("status", parents=[format_parent], help="Show overall setup dashboard")
    p_status.set_defaults(func=handle_status, command_name="setup.status")


def _resolve_env_path() -> Path:
    env_override = os.getenv("FINANCE_CLI_ENV_FILE")
    env_path = Path(env_override).expanduser() if env_override else DEFAULT_ENV_PATH
    return env_path.resolve()


def _build_check(check_id: str, label: str, status: str, detail: str, next_step: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": check_id,
        "label": label,
        "status": status,
        "detail": detail,
    }
    if next_step:
        payload["next_step"] = next_step
    return payload


def _aws_readiness() -> dict[str, Any]:
    try:
        import boto3
    except Exception as exc:  # pragma: no cover - import path tested via monkeypatch
        return {
            "ok": False,
            "has_boto3": False,
            "region": None,
            "region_source": None,
            "error": f"boto3 not importable: {exc}",
        }

    env_default = str(os.getenv("AWS_DEFAULT_REGION") or "").strip()
    if env_default:
        return {
            "ok": True,
            "has_boto3": True,
            "region": env_default,
            "region_source": "AWS_DEFAULT_REGION",
            "error": None,
        }

    env_region = str(os.getenv("AWS_REGION") or "").strip()
    if env_region:
        return {
            "ok": True,
            "has_boto3": True,
            "region": env_region,
            "region_source": "AWS_REGION",
            "error": None,
        }

    try:
        region_from_profile = str(boto3.session.Session().region_name or "").strip()
    except Exception as exc:  # pragma: no cover - defensive
        return {
            "ok": False,
            "has_boto3": True,
            "region": None,
            "region_source": None,
            "error": f"Unable to resolve AWS profile region: {exc}",
        }

    if region_from_profile:
        return {
            "ok": True,
            "has_boto3": True,
            "region": region_from_profile,
            "region_source": "aws_profile",
            "error": None,
        }

    return {
        "ok": False,
        "has_boto3": True,
        "region": None,
        "region_source": None,
        "error": "AWS region missing (set AWS_DEFAULT_REGION/AWS_REGION or configure profile region)",
    }


def _category_coverage(conn) -> dict[str, Any]:
    rows = conn.execute("SELECT name FROM categories").fetchall()
    existing = {str(row["name"]).strip().lower() for row in rows if str(row["name"]).strip()}
    expected = sorted(CANONICAL_CATEGORIES)
    missing = [name for name in expected if name.lower() not in existing]
    total_expected = len(CANONICAL_CATEGORIES)
    return {
        "expected_total": total_expected,
        "present_count": total_expected - len(missing),
        "missing_count": len(missing),
        "missing": missing,
    }


def _run_env_checks(conn) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    db_path = get_db_path().expanduser().resolve()
    try:
        conn.execute("SELECT 1").fetchone()
        checks.append(
            _build_check(
                "database",
                "Database",
                "OK",
                f"Database accessible at {db_path}",
            )
        )
    except Exception as exc:
        checks.append(
            _build_check(
                "database",
                "Database",
                "FAIL",
                f"Database not accessible at {db_path}: {exc}",
                "Set FINANCE_CLI_DB to a writable location and rerun setup check.",
            )
        )

    env_path = _resolve_env_path()
    dotenv_disabled = str(os.getenv("FINANCE_CLI_DISABLE_DOTENV") or "").strip().lower() in _ENV_DISABLE_VALUES
    if env_path.exists() and env_path.is_file():
        if dotenv_disabled:
            checks.append(
                _build_check(
                    "dotenv",
                    ".env",
                    "WARN",
                    f".env found at {env_path}, but loading is disabled by FINANCE_CLI_DISABLE_DOTENV",
                    "Unset FINANCE_CLI_DISABLE_DOTENV to load credentials from .env automatically.",
                )
            )
        else:
            checks.append(
                _build_check(
                    "dotenv",
                    ".env",
                    "OK",
                    f".env loaded from {env_path}",
                )
            )
    else:
        checks.append(
            _build_check(
                "dotenv",
                ".env",
                "FAIL",
                f".env file not found at {env_path}",
                "Run `finance_cli setup init` to create an .env template, then fill in credentials.",
            )
        )

    plaid = config_status()
    if plaid.configured and plaid.has_sdk:
        checks.append(
            _build_check(
                "plaid",
                "Plaid",
                "OK",
                f"Configured for env={plaid.env or 'unset'} with plaid-python installed",
            )
        )
    else:
        missing_parts: list[str] = []
        if not plaid.has_sdk:
            missing_parts.append("plaid-python missing")
        if plaid.missing_env:
            missing_parts.append("missing " + ", ".join(plaid.missing_env))
        checks.append(
            _build_check(
                "plaid",
                "Plaid",
                "FAIL",
                "; ".join(missing_parts) or "Plaid not configured",
                "Install plaid-python and set PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV.",
            )
        )

    aws = _aws_readiness()
    if aws["ok"]:
        checks.append(
            _build_check(
                "aws",
                "AWS",
                "OK",
                f"boto3 available, region={aws['region']} ({aws['region_source']})",
            )
        )
    else:
        checks.append(
            _build_check(
                "aws",
                "AWS",
                "WARN",
                str(aws["error"] or "AWS not ready"),
                "Set AWS_DEFAULT_REGION or configure your ~/.aws/config profile before `setup connect`.",
            )
        )

    has_openai = bool(str(os.getenv("OPENAI_API_KEY") or "").strip())
    has_anthropic = bool(str(os.getenv("ANTHROPIC_API_KEY") or "").strip())
    if has_openai or has_anthropic:
        checks.append(
            _build_check(
                "ai_keys",
                "AI Keys",
                "OK",
                f"OPENAI_API_KEY={has_openai} ANTHROPIC_API_KEY={has_anthropic}",
            )
        )
    else:
        checks.append(
            _build_check(
                "ai_keys",
                "AI Keys",
                "WARN",
                "No AI keys configured (optional)",
            )
        )

    coverage = _category_coverage(conn)
    if coverage["missing_count"] == 0:
        checks.append(
            _build_check(
                "categories",
                "Categories",
                "OK",
                f"Canonical categories present: {coverage['present_count']}/{coverage['expected_total']}",
            )
        )
    else:
        checks.append(
            _build_check(
                "categories",
                "Categories",
                "FAIL",
                f"Canonical categories present: {coverage['present_count']}/{coverage['expected_total']}",
                "Run `finance_cli setup init` to seed canonical categories.",
            )
        )

    rules_path = resolve_rules_path()
    if rules_path.exists():
        checks.append(
            _build_check(
                "rules",
                "Rules File",
                "OK",
                f"rules.yaml found at {rules_path}",
            )
        )
    else:
        checks.append(
            _build_check(
                "rules",
                "Rules File",
                "FAIL",
                f"rules.yaml missing at {rules_path}",
                "Run `finance_cli setup init` to bootstrap rules.yaml.",
            )
        )

    counts = {
        "ok": sum(1 for check in checks if check["status"] == "OK"),
        "warn": sum(1 for check in checks if check["status"] == "WARN"),
        "fail": sum(1 for check in checks if check["status"] == "FAIL"),
    }
    next_steps: list[str] = []
    for check in checks:
        next_step = str(check.get("next_step") or "").strip()
        if next_step and next_step not in next_steps:
            next_steps.append(next_step)

    return {
        "ready": counts["fail"] == 0,
        "checks": checks,
        "counts": counts,
        "next_steps": next_steps,
    }


def _format_checks_cli(payload: dict[str, Any]) -> str:
    lines = [f"[{check['status']}] {check['label']}: {check['detail']}" for check in payload["checks"]]
    lines.append(
        f"Summary: ok={payload['counts']['ok']} warn={payload['counts']['warn']} fail={payload['counts']['fail']}"
    )
    if payload["next_steps"]:
        lines.append("Next steps:")
        lines.extend(f"- {step}" for step in payload["next_steps"])
    return "\n".join(lines)


def handle_check(args, conn) -> dict[str, Any]:
    payload = _run_env_checks(conn)
    return {
        "data": payload,
        "summary": {
            "ready": payload["ready"],
            "fail_count": payload["counts"]["fail"],
            "warn_count": payload["counts"]["warn"],
        },
        "cli_report": _format_checks_cli(payload),
    }


def _fetch_category_row(conn, name: str):
    return conn.execute(
        """
        SELECT id, name, parent_id, level, is_income, is_system
          FROM categories
         WHERE lower(trim(name)) = lower(trim(?))
         ORDER BY rowid ASC
         LIMIT 1
        """,
        (name,),
    ).fetchone()


def _normalize_parent_id(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _category_updates(row, *, expected_parent_id: str | None, expected_level: int, expected_is_income: int) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    if _normalize_parent_id(row["parent_id"]) != _normalize_parent_id(expected_parent_id):
        updates["parent_id"] = expected_parent_id
    if int(row["level"] or 0) != int(expected_level):
        updates["level"] = int(expected_level)
    if int(row["is_income"] or 0) != int(expected_is_income):
        updates["is_income"] = int(expected_is_income)
    if int(row["is_system"] or 0) != 1:
        updates["is_system"] = 1
    return updates


def _reconcile_single_category(
    conn,
    *,
    name: str,
    expected_parent_id: str | None,
    expected_level: int,
    expected_is_income: int,
    dry_run: bool,
) -> tuple[str, str]:
    row = _fetch_category_row(conn, name)
    if row is None:
        new_id = uuid.uuid4().hex
        if not dry_run:
            conn.execute(
                """
                INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
                VALUES (?, ?, ?, ?, ?, 1, 0)
                """,
                (new_id, name, expected_parent_id, expected_level, expected_is_income),
            )
        return "created", new_id

    updates = _category_updates(
        row,
        expected_parent_id=expected_parent_id,
        expected_level=expected_level,
        expected_is_income=expected_is_income,
    )
    category_id = str(row["id"])
    if updates:
        if not dry_run:
            assignments = ", ".join(f"{column} = ?" for column in updates)
            conn.execute(
                f"UPDATE categories SET {assignments} WHERE id = ?",
                (*updates.values(), category_id),
            )
        return "updated", category_id
    return "already_correct", category_id


def _seed_canonical_categories(conn, *, dry_run: bool) -> dict[str, Any]:
    expected_names = set(_CATEGORY_HIERARCHY.keys())
    for children in _CATEGORY_HIERARCHY.values():
        expected_names.update(children)
    if expected_names != set(CANONICAL_CATEGORIES):
        raise ValueError("Canonical category hierarchy is out of sync with CANONICAL_CATEGORIES")

    created = 0
    updated = 0
    already_correct = 0
    parent_ids: dict[str, str] = {}

    for parent_name in _CATEGORY_HIERARCHY:
        status, category_id = _reconcile_single_category(
            conn,
            name=parent_name,
            expected_parent_id=None,
            expected_level=0,
            expected_is_income=int(parent_name in _INCOME_NAMES),
            dry_run=dry_run,
        )
        parent_ids[parent_name] = category_id
        if status == "created":
            created += 1
        elif status == "updated":
            updated += 1
        else:
            already_correct += 1

    for parent_name, children in _CATEGORY_HIERARCHY.items():
        for child_name in children:
            status, _ = _reconcile_single_category(
                conn,
                name=child_name,
                expected_parent_id=parent_ids[parent_name],
                expected_level=1,
                expected_is_income=int(child_name in _INCOME_NAMES),
                dry_run=dry_run,
            )
            if status == "created":
                created += 1
            elif status == "updated":
                updated += 1
            else:
                already_correct += 1

    if not dry_run:
        conn.commit()

    return {
        "dry_run": dry_run,
        "created": 0 if dry_run else created,
        "updated": 0 if dry_run else updated,
        "already_correct": already_correct,
        "would_create": created if dry_run else 0,
        "would_update": updated if dry_run else 0,
        "expected_total": len(CANONICAL_CATEGORIES),
    }


def _ensure_env_template(*, dry_run: bool) -> dict[str, Any]:
    env_path = _resolve_env_path()
    exists = env_path.exists()

    if exists:
        return {
            "path": str(env_path),
            "created": False,
            "would_create": False,
            "dry_run": dry_run,
        }

    if dry_run:
        return {
            "path": str(env_path),
            "created": False,
            "would_create": True,
            "dry_run": True,
        }

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text(_ENV_TEMPLATE.strip() + "\n", encoding="utf-8")
    return {
        "path": str(env_path),
        "created": True,
        "would_create": False,
        "dry_run": False,
    }


def _bootstrap_rules_file(*, dry_run: bool) -> dict[str, Any]:
    target = resolve_rules_path()
    if target.exists():
        return {
            "path": str(target),
            "created": False,
            "source_path": None,
            "would_create": False,
            "dry_run": dry_run,
        }

    source = DEFAULT_DATA_DIR / "rules.yaml"
    if not source.exists():
        raise ValueError(f"Packaged rules template missing at {source}")

    if dry_run:
        return {
            "path": str(target),
            "created": False,
            "source_path": str(source),
            "would_create": True,
            "dry_run": True,
        }

    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return {
        "path": str(target),
        "created": True,
        "source_path": str(source),
        "would_create": False,
        "dry_run": False,
    }


def handle_init(args, conn) -> dict[str, Any]:
    env_template = _ensure_env_template(dry_run=bool(args.dry_run))
    categories = _seed_canonical_categories(conn, dry_run=bool(args.dry_run))
    rules_file = _bootstrap_rules_file(dry_run=bool(args.dry_run))

    cli_lines = [f"setup init ({'dry-run' if args.dry_run else 'apply'})"]
    if args.dry_run:
        cli_lines.append(
            f"categories: would_create={categories['would_create']} would_update={categories['would_update']}"
        )
    else:
        cli_lines.append(f"categories: created={categories['created']} updated={categories['updated']}")
    cli_lines.append(
        f".env template: {'created' if env_template['created'] else 'unchanged'} ({env_template['path']})"
    )
    if rules_file["created"]:
        cli_lines.append(f"rules.yaml: created ({rules_file['path']})")
    elif rules_file["would_create"]:
        cli_lines.append(f"rules.yaml: would_create ({rules_file['path']})")
    else:
        cli_lines.append(f"rules.yaml: unchanged ({rules_file['path']})")

    return {
        "data": {
            "dry_run": bool(args.dry_run),
            "env_template": env_template,
            "categories": categories,
            "rules_file": rules_file,
            "expected_categories": len(CANONICAL_CATEGORIES),
        },
        "summary": {
            "dry_run": bool(args.dry_run),
            "categories_created": categories["created"],
            "categories_updated": categories["updated"],
        },
        "cli_report": "\n".join(cli_lines),
    }


def _connect_preflight() -> tuple[PlaidConfigStatus, dict[str, Any]]:
    plaid = config_status()
    aws = _aws_readiness()
    return plaid, aws


def _connect_preflight_error(plaid: PlaidConfigStatus, aws: dict[str, Any]) -> str | None:
    errors: list[str] = []
    if not plaid.has_sdk:
        errors.append("Plaid SDK missing: install plaid-python")
    if plaid.missing_env:
        errors.append("Plaid env missing: " + ", ".join(plaid.missing_env))
    if not aws["ok"]:
        errors.append(str(aws.get("error") or "AWS not configured"))
    if not errors:
        return None
    return "; ".join(errors)


def handle_connect(args, conn) -> dict[str, Any]:
    plaid, aws = _connect_preflight()
    preflight_error = _connect_preflight_error(plaid, aws)
    if preflight_error:
        raise ValueError(f"setup connect preflight failed: {preflight_error}")

    client_user_id = sanitize_client_user_id(str(args.user_id or "default"))
    try:
        session = create_hosted_link_session(
            conn,
            user_id=client_user_id,
            include_balance=True,
            include_liabilities=bool(args.include_liabilities),
        )
    except PlaidUnavailableError as exc:
        raise ValueError(str(exc)) from exc

    hosted_link_url = str(session.get("hosted_link_url") or "").strip()
    if args.open_browser and hosted_link_url:
        webbrowser.open(hosted_link_url)

    try:
        linked_item = complete_link_session(
            conn,
            user_id=client_user_id,
            link_token=str(session["link_token"]),
            timeout_seconds=int(args.timeout),
            requested_products=session.get("requested_products"),
        )
    except Exception as exc:
        # Return partial result with session URL so agent/user can retry
        return {
            "data": {
                "session": session,
                "hosted_link_url": hosted_link_url,
                "error": str(exc),
            },
            "summary": {"linked": False, "error": str(exc)},
            "cli_report": f"Link session created but not completed: {exc}\nURL: {hosted_link_url}",
        }

    linked_item_id = str(linked_item.get("plaid_item_id") or "").strip()
    if not linked_item_id:
        raise ValueError("Link completed but no plaid_item_id returned")

    sync_result: dict[str, Any] | None = None
    balance_result: dict[str, Any] | None = None
    partial_errors: list[str] = []

    if not args.skip_sync:
        try:
            sync_result = run_sync(conn, item_id=linked_item_id)
        except Exception as exc:  # pragma: no cover - exercised in tests with monkeypatch
            partial_errors.append(f"sync failed: {exc}")
        try:
            balance_result = refresh_balances(conn, item_id=linked_item_id)
        except Exception as exc:  # pragma: no cover - exercised in tests with monkeypatch
            partial_errors.append(f"balance refresh failed: {exc}")

    partial_success = len(partial_errors) > 0
    institution = str(linked_item.get("institution_name") or "Unknown Institution")
    if partial_success:
        cli_report = f"Linked {institution} ({linked_item_id}) with partial success"
    elif args.skip_sync:
        cli_report = f"Linked {institution} ({linked_item_id}); post-link sync skipped"
    else:
        added = int((sync_result or {}).get("added", 0))
        accounts_updated = int((balance_result or {}).get("accounts_updated", 0))
        cli_report = (
            f"Linked {institution} ({linked_item_id}); transactions_added={added} accounts_updated={accounts_updated}"
        )

    return {
        "data": {
            "preflight": {
                "plaid": {
                    "configured": plaid.configured,
                    "has_sdk": plaid.has_sdk,
                    "missing_env": plaid.missing_env,
                    "env": plaid.env,
                },
                "aws": aws,
            },
            "session": session,
            "linked_item": linked_item,
            "post_link": {
                "skipped": bool(args.skip_sync),
                "sync": sync_result,
                "balance_refresh": balance_result,
            },
            "partial_success": partial_success,
            "partial_errors": partial_errors,
        },
        "summary": {
            "linked": True,
            "partial_success": partial_success,
            "post_link_skipped": bool(args.skip_sync),
        },
        "cli_report": cli_report,
    }


def handle_status(args, conn) -> dict[str, Any]:
    env = _run_env_checks(conn)
    db_status = db_cmd.handle_status(args, conn).get("data", {})
    plaid_status = plaid_cmd.handle_status(args, conn).get("data", {})
    coverage = _category_coverage(conn)

    rules_path = resolve_rules_path()
    rules_exists = rules_path.exists()

    vendor_memory_row = conn.execute(
        "SELECT COUNT(*) AS enabled_count FROM vendor_memory WHERE is_enabled = 1"
    ).fetchone()
    vendor_memory_enabled = int(vendor_memory_row["enabled_count"] or 0)

    next_steps = list(env["next_steps"])
    if int(plaid_status.get("active_count") or 0) == 0:
        suggestion = "Run `finance_cli setup connect --open-browser` to link your first institution."
        if suggestion not in next_steps:
            next_steps.append(suggestion)
    if int(db_status.get("transaction_counts", {}).get("active") or 0) == 0:
        suggestion = "No active transactions yet; run `setup connect` or import statements."
        if suggestion not in next_steps:
            next_steps.append(suggestion)
    if coverage["missing_count"] > 0:
        suggestion = "Canonical categories incomplete; run `finance_cli setup init`."
        if suggestion not in next_steps:
            next_steps.append(suggestion)
    if not rules_exists:
        suggestion = "rules.yaml missing; run `finance_cli setup init`."
        if suggestion not in next_steps:
            next_steps.append(suggestion)

    data = {
        "ready": env["ready"],
        "env": env,
        "db": db_status,
        "plaid": plaid_status,
        "categories": coverage,
        "vendor_memory": {"enabled_count": vendor_memory_enabled},
        "rules": {"path": str(rules_path), "exists": rules_exists},
        "next_steps": next_steps,
    }

    active_txns = int(db_status.get("transaction_counts", {}).get("active") or 0)
    plaid_items = plaid_status.get("items") or []
    plaid_total = len(plaid_items)
    plaid_active = int(plaid_status.get("active_count") or 0)
    env_total = env["counts"]["ok"] + env["counts"]["warn"] + env["counts"]["fail"]

    ready_label = "Ready" if env["ready"] else "Not Ready"
    cli_lines = [
        f"System Status: {ready_label}",
        "",
        f"  Environment:   {env['counts']['ok']}/{env_total} checks passed",
        f"  Transactions:  {active_txns:,} active",
        f"  Plaid Items:   {plaid_total} total, {plaid_active} active",
        f"  Categories:    {coverage['present_count']}/{coverage['expected_total']} canonical",
        f"  Vendor Memory: {vendor_memory_enabled:,} enabled rules",
        f"  Rules File:    {'rules.yaml found' if rules_exists else 'rules.yaml missing'}",
    ]
    if next_steps:
        cli_lines.append("")
        cli_lines.append("Next Steps:")
        cli_lines.extend(f"  - {step}" for step in next_steps)
    else:
        cli_lines.append("")
        cli_lines.append("Next Steps:")
        cli_lines.append("  (none — all systems operational)")

    return {
        "data": data,
        "summary": {
            "ready": env["ready"],
            "warn_count": env["counts"]["warn"],
            "fail_count": env["counts"]["fail"],
        },
        "cli_report": "\n".join(cli_lines),
    }
