"""Direct CLI bridge for commands that are server-proxied in local sync mode."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from finance_cli.config import get_db_path
from finance_cli.db import set_db_encryption_mode_override
from finance_cli.models import cents_to_dollars

from .auth import LocalAuth
from .config import CASHNERD_DB_PATH, load_config
from .engine import SyncEngine


@dataclass(frozen=True)
class LocalSyncProxySpec:
    tool_name: str
    build_arguments: Callable[[Any], dict[str, Any]]
    build_cli_report: Callable[[dict[str, Any]], str | None] | None = None
    pull_after: bool = True


def _plaid_sync_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "items_synced" not in data:
        return None
    from finance_cli.commands import plaid_cmd

    return plaid_cmd._sync_cli_report(data)


def _plaid_balance_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "items_refreshed" not in data:
        return None
    from finance_cli.commands import plaid_cmd

    return plaid_cmd._balance_cli_report(data)


def _plaid_link_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    session = data.get("session")
    if isinstance(session, dict) and session.get("hosted_link_url"):
        return f"Plaid hosted link session created\nURL: {session['hosted_link_url']}"
    linked = data.get("linked_item")
    if isinstance(linked, dict) and linked.get("plaid_item_id"):
        return f"Plaid link completed and item stored: {linked['plaid_item_id']}"
    return None


def _plaid_unlink_report(envelope: dict[str, Any]) -> str | None:
    summary = envelope.get("summary")
    if isinstance(summary, dict) and summary.get("item"):
        return f"Unlinked item {summary['item']}"
    return None


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _plaid_status_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "items" not in data:
        return None
    from finance_cli.commands import plaid_cmd

    items = data.get("items")
    if not isinstance(items, list):
        items = []
    return plaid_cmd._status_cli_report(
        items,
        configured=bool(data.get("configured")),
        has_sdk=bool(data.get("has_sdk")),
        webhook_url_configured=bool(data.get("webhook_url_configured")),
    )


def _setup_status_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None

    env = data.get("env") if isinstance(data.get("env"), dict) else {}
    env_counts = env.get("counts") if isinstance(env.get("counts"), dict) else {}
    db_status = data.get("db") if isinstance(data.get("db"), dict) else {}
    txn_counts = (
        db_status.get("transaction_counts")
        if isinstance(db_status.get("transaction_counts"), dict)
        else {}
    )
    plaid_status = data.get("plaid") if isinstance(data.get("plaid"), dict) else {}
    categories = data.get("categories") if isinstance(data.get("categories"), dict) else {}
    vendor_memory = (
        data.get("vendor_memory")
        if isinstance(data.get("vendor_memory"), dict)
        else {}
    )
    rules = data.get("rules") if isinstance(data.get("rules"), dict) else {}

    env_total = _int_value(env_counts.get("ok")) + _int_value(env_counts.get("warn")) + _int_value(
        env_counts.get("fail")
    )
    plaid_items = plaid_status.get("items") if isinstance(plaid_status.get("items"), list) else []
    plaid_line = (
        f"  Plaid Items:   {len(plaid_items)} total, "
        f"{_int_value(plaid_status.get('active_count'))} active"
    )
    plaid_token_missing = _int_value(plaid_status.get("token_missing_count"))
    if plaid_token_missing:
        plaid_line += f", {plaid_token_missing} token missing"

    ready_label = "Ready" if bool(data.get("ready")) else "Not Ready"
    lines = [
        f"System Status: {ready_label}",
        "",
        f"  Environment:   {_int_value(env_counts.get('ok'))}/{env_total} checks passed",
        f"  Transactions:  {_int_value(txn_counts.get('active')):,} active",
        plaid_line,
        (
            "  Categories:    "
            f"{_int_value(categories.get('present_count'))}/{_int_value(categories.get('expected_total'))} canonical"
        ),
        f"  Vendor Memory: {_int_value(vendor_memory.get('enabled_count')):,} enabled rules",
        f"  Rules File:    {'rules.yaml found' if bool(rules.get('exists')) else 'rules.yaml missing'}",
    ]
    env_checks = env.get("checks") if isinstance(env.get("checks"), list) else []
    failed_checks = [
        check
        for check in env_checks
        if isinstance(check, dict) and str(check.get("status") or "").lower() == "fail"
    ]
    if failed_checks:
        lines.extend(["", "Failed Checks:"])
        for check in failed_checks:
            name = str(check.get("label") or check.get("name") or check.get("id") or "unknown").strip()
            message = str(check.get("detail") or check.get("message") or "").strip()
            lines.append(f"  - {name}: {message}" if message else f"  - {name}")
    lines.extend(["", "Next Steps:"])
    next_steps = [str(step) for step in data.get("next_steps") or [] if str(step).strip()]
    if next_steps:
        lines.extend(f"  - {step}" for step in next_steps)
    else:
        lines.append("  (none - all systems operational)")
    return "\n".join(lines)


def _stripe_status_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "configured" not in data:
        return None
    from finance_cli.commands import stripe_cmd

    return stripe_cmd._status_cli_report(data)


def _stripe_revenue_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    period_label = str(data.get("period") or "all")
    rows = data.get("rows") if isinstance(data.get("rows"), list) else []
    totals = data.get("totals") if isinstance(data.get("totals"), dict) else {}
    if not rows:
        return f"No Stripe revenue data for period={period_label}"

    lines = [
        (
            f"period={period_label} months={len(rows)} "
            f"gross=${cents_to_dollars(_int_value(totals.get('gross_cents'))):,.2f} "
            f"fees=${cents_to_dollars(_int_value(totals.get('fees_cents'))):,.2f} "
            f"refunds=${cents_to_dollars(_int_value(totals.get('refunds_cents'))):,.2f} "
            f"net=${cents_to_dollars(_int_value(totals.get('net_cents'))):,.2f}"
        )
    ]
    for row in rows:
        if not isinstance(row, dict):
            continue
        lines.append(
            (
                f"  {row.get('month')}: "
                f"gross=${cents_to_dollars(_int_value(row.get('gross_cents'))):,.2f} "
                f"fees=${cents_to_dollars(_int_value(row.get('fees_cents'))):,.2f} "
                f"refunds=${cents_to_dollars(_int_value(row.get('refunds_cents'))):,.2f} "
                f"net=${cents_to_dollars(_int_value(row.get('net_cents'))):,.2f}"
            )
        )
    return "\n".join(lines)


def _stripe_link_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    if data.get("ready") is False:
        if data.get("has_sdk") is False:
            return "stripe package not installed. Run: pip install stripe"
        if data.get("configured") is False:
            return "Set STRIPE_API_KEY environment variable"
    account_name = str(data.get("account_name") or "").strip()
    if account_name:
        return f"Connected to {account_name}"
    stripe_account_id = str(data.get("stripe_account_id") or "").strip()
    if stripe_account_id:
        return f"Connected to {stripe_account_id}"
    return None


def _stripe_sync_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "charges_added" not in data:
        return None
    from finance_cli.commands import stripe_cmd

    return stripe_cmd._sync_cli_report(data)


def _stripe_unlink_report(envelope: dict[str, Any]) -> str | None:
    summary = envelope.get("summary")
    if not isinstance(summary, dict):
        summary = {}
    data = envelope.get("data")
    if not isinstance(data, dict):
        data = {}
    if "updated" not in summary and "updated" not in data:
        return None
    updated = _int_value(summary.get("updated") if "updated" in summary else data.get("updated"))
    return "Stripe disconnected" if updated else "No Stripe connection found"


def _schwab_sync_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "accounts_synced" not in data:
        return None
    from finance_cli.commands import schwab_cmd

    return schwab_cmd._sync_cli_report(data)


def _schwab_status_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or "configured" not in data:
        return None
    from finance_cli.commands import schwab_cmd

    return schwab_cmd._status_cli_report(data)


def _setup_connect_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    summary = envelope.get("summary")
    if not isinstance(summary, dict):
        summary = {}

    session = data.get("session") if isinstance(data.get("session"), dict) else {}
    hosted_link_url = str(data.get("hosted_link_url") or session.get("hosted_link_url") or "").strip()
    linked = data.get("linked_item") if isinstance(data.get("linked_item"), dict) else {}
    linked_item_id = str(linked.get("plaid_item_id") or "").strip()
    if not linked_item_id:
        error = str(data.get("error") or summary.get("error") or "").strip()
        if hosted_link_url:
            if error:
                return f"Link session created but not completed: {error}\nURL: {hosted_link_url}"
            return f"Plaid hosted link session created\nURL: {hosted_link_url}"
        return None

    institution = str(linked.get("institution_name") or "Unknown Institution")
    partial_errors = data.get("partial_errors") if isinstance(data.get("partial_errors"), list) else []
    if bool(summary.get("partial_success")) or partial_errors:
        return f"Linked {institution} ({linked_item_id}) with partial success"
    if bool(summary.get("post_link_skipped")):
        return f"Linked {institution} ({linked_item_id}); post-link sync skipped"

    post_link = data.get("post_link") if isinstance(data.get("post_link"), dict) else {}
    sync_result = post_link.get("sync") if isinstance(post_link.get("sync"), dict) else {}
    balance_result = (
        post_link.get("balance_refresh")
        if isinstance(post_link.get("balance_refresh"), dict)
        else {}
    )
    return (
        f"Linked {institution} ({linked_item_id}); "
        f"transactions_added={_int_value(sync_result.get('added'))} "
        f"accounts_updated={_int_value(balance_result.get('accounts_updated'))}"
    )


def _monthly_run_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    summary = envelope.get("summary")
    if not isinstance(data, dict) or not isinstance(summary, dict):
        return None
    if "steps_run" not in summary:
        return None

    month = str(data.get("month") or "unknown")
    prefix = "[DRY RUN] " if bool(data.get("dry_run")) else ""
    lines = [
        (
            f"{prefix}month={month} "
            f"steps_run={_int_value(summary.get('steps_run'))} "
            f"steps_succeeded={_int_value(summary.get('steps_succeeded'))} "
            f"steps_failed={_int_value(summary.get('steps_failed'))} "
            f"steps_skipped={_int_value(summary.get('steps_skipped'))}"
        )
    ]
    steps = data.get("steps") if isinstance(data.get("steps"), dict) else {}
    for name, step in steps.items():
        if not isinstance(step, dict):
            lines.append(f"  {name}: {step}")
            continue
        line = f"  {name}: {step.get('status') or 'unknown'}"
        error = str(step.get("error") or "").strip()
        if error:
            line += f" - {error}"
        lines.append(line)

    health = data.get("health") if isinstance(data.get("health"), dict) else {}
    if health:
        lines.append(
            (
                "health "
                f"unreviewed={_int_value(health.get('unreviewed_count'))} "
                f"uncategorized={_int_value(health.get('uncategorized_count'))} "
                f"budget_over={_int_value(health.get('budget_over_count'))}"
            )
        )
    return "\n".join(lines)


def _db_import_preferences_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    summary = envelope.get("summary")
    if not isinstance(data, dict) or not isinstance(summary, dict):
        return None
    if "total_imported" not in summary or "total_skipped" not in summary:
        return None

    categories_missing = data.get("categories_missing")
    missing_count = len(categories_missing) if isinstance(categories_missing, list) else 0
    return (
        f"{'DRY RUN: ' if bool(data.get('dry_run')) else ''}"
        f"Imported {_int_value(summary.get('total_imported'))} rows, "
        f"skipped {_int_value(summary.get('total_skipped'))} conflicts "
        f"({missing_count} missing categories, "
        f"{_int_value(data.get('accounts_unresolved'))} unresolved accounts)"
    )


def _db_restore_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    summary = envelope.get("summary")
    if not isinstance(data, dict):
        return None
    if not isinstance(summary, dict):
        summary = {}
    if "dry_run" not in data or "restored" not in data:
        return None

    dry_run = bool(data.get("dry_run"))
    restored = bool(data.get("restored"))
    status = "DRY RUN: would restore" if dry_run else ("Restored" if restored else "Restore skipped")
    bundle_path = str(data.get("bundle_path") or "").strip()
    report = f"{status} from {bundle_path}" if bundle_path else status
    warnings = data.get("warnings")
    warning_count = len(warnings) if isinstance(warnings, list) else _int_value(summary.get("warning_count"))
    if warning_count:
        report += f" ({warning_count} warnings)"
    return report


def _intervention_action_report(action: str) -> Callable[[dict[str, Any]], str | None]:
    def _report(envelope: dict[str, Any]) -> str | None:
        data = envelope.get("data")
        summary = envelope.get("summary")
        if not isinstance(data, dict):
            data = {}
        if not isinstance(summary, dict):
            summary = {}
        log_id = summary.get("log_id", data.get("id"))
        if log_id is None:
            return None
        return f"log_id={log_id}\naction={action}"

    return _report


def _intervention_mute_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    summary = envelope.get("summary")
    if not isinstance(data, dict):
        data = {}
    if not isinstance(summary, dict):
        summary = {}
    pattern_id = str(summary.get("pattern_id") or data.get("pattern_id") or "").strip()
    if not pattern_id:
        return None
    return f"pattern_id={pattern_id}\ncreated={bool(data.get('created'))}"


def _intervention_unmute_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    summary = envelope.get("summary")
    if not isinstance(data, dict):
        data = {}
    if not isinstance(summary, dict):
        summary = {}
    pattern_id = str(data.get("pattern_id") or "").strip()
    if not pattern_id:
        return None
    deleted = bool(summary.get("deleted", data.get("deleted")))
    return f"pattern_id={pattern_id}\ndeleted={deleted}"


def _rules_add_keyword_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    keyword = str(data.get("keyword") or "").strip()
    category = str(data.get("category") or "").strip()
    if not keyword or not category:
        return None
    action = str(data.get("action") or "added").strip() or "added"
    use_type_report = str(data.get("use_type") or "Any")
    return f"{action.capitalize()} keyword '{keyword}' for {category} ({use_type_report})"


def _rules_add_split_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    rule = data.get("rule") if isinstance(data.get("rule"), dict) else {}
    business_pct = rule.get("business_pct")
    business_category = str(rule.get("business_category") or "").strip()
    personal_category = str(rule.get("personal_category") or "").strip()
    if business_pct is None or not business_category or not personal_category:
        return None
    match_category = str(rule.get("match_category") or "").strip()
    keywords = rule.get("match_keywords") if isinstance(rule.get("match_keywords"), list) else []
    match_label = match_category or ", ".join(str(keyword) for keyword in keywords)
    return f"Added split rule ({business_pct}% business) for {match_label}"


def _rules_remove_keyword_report(envelope: dict[str, Any]) -> str | None:
    data = envelope.get("data")
    if not isinstance(data, dict):
        return None
    keyword = str(data.get("keyword") or "").strip()
    if not keyword:
        return None
    if bool(data.get("dry_run")):
        return f"[DRY RUN] Would remove keyword '{keyword}'"
    return f"Removed keyword '{keyword}'"


_LOCAL_SYNC_PROXY_COMMANDS: dict[str, LocalSyncProxySpec] = {
    "interventions.act": LocalSyncProxySpec(
        tool_name="interventions_act",
        build_arguments=lambda args: {"log_id": getattr(args, "log_id", None)},
        build_cli_report=_intervention_action_report("acted"),
    ),
    "interventions.dismiss": LocalSyncProxySpec(
        tool_name="interventions_dismiss",
        build_arguments=lambda args: {"log_id": getattr(args, "log_id", None)},
        build_cli_report=_intervention_action_report("dismissed"),
    ),
    "interventions.mute": LocalSyncProxySpec(
        tool_name="interventions_mute",
        build_arguments=lambda args: {
            "pattern_id": getattr(args, "pattern_id", None),
            "reason": str(getattr(args, "reason", "") or ""),
        },
        build_cli_report=_intervention_mute_report,
    ),
    "interventions.unmute": LocalSyncProxySpec(
        tool_name="interventions_unmute",
        build_arguments=lambda args: {"pattern_id": getattr(args, "pattern_id", None)},
        build_cli_report=_intervention_unmute_report,
    ),
    "plaid.link": LocalSyncProxySpec(
        tool_name="plaid_link",
        build_arguments=lambda args: {
            "user_id": getattr(args, "user_id", None),
            "wait": bool(getattr(args, "wait", False)),
            "timeout": int(getattr(args, "timeout", 300) or 300),
            "open_browser": bool(getattr(args, "open_browser", False)),
            "update": bool(getattr(args, "update", False)),
            "item": getattr(args, "item", None),
            "product": getattr(args, "product", None),
            "include_balance": bool(getattr(args, "include_balance", False)),
            "include_liabilities": bool(getattr(args, "include_liabilities", False)),
            "allow_duplicate": bool(getattr(args, "allow_duplicate", False)),
        },
        build_cli_report=_plaid_link_report,
    ),
    "plaid.sync": LocalSyncProxySpec(
        tool_name="plaid_sync",
        build_arguments=lambda args: {
            "days": getattr(args, "days", None),
            "item": getattr(args, "item", None),
            "force": bool(getattr(args, "force", False)),
            "backfill": bool(getattr(args, "backfill", False)),
        },
        build_cli_report=_plaid_sync_report,
    ),
    "plaid.balance_refresh": LocalSyncProxySpec(
        tool_name="plaid_balance_refresh",
        build_arguments=lambda args: {
            "item": getattr(args, "item", None),
            "force": bool(getattr(args, "force", False)),
        },
        build_cli_report=_plaid_balance_report,
    ),
    "plaid.unlink": LocalSyncProxySpec(
        tool_name="plaid_unlink",
        build_arguments=lambda args: {"item": getattr(args, "item", None)},
        build_cli_report=_plaid_unlink_report,
    ),
    "plaid.status": LocalSyncProxySpec(
        tool_name="plaid_status",
        build_arguments=lambda _args: {},
        build_cli_report=_plaid_status_report,
        pull_after=False,
    ),
    "setup.status": LocalSyncProxySpec(
        tool_name="setup_status",
        build_arguments=lambda _args: {},
        build_cli_report=_setup_status_report,
        pull_after=False,
    ),
    "setup.connect": LocalSyncProxySpec(
        tool_name="setup_connect",
        build_arguments=lambda args: {
            "user_id": getattr(args, "user_id", None) or "default",
            "include_liabilities": bool(getattr(args, "include_liabilities", False)),
            "timeout": int(getattr(args, "timeout", 300) or 300),
            "skip_sync": bool(getattr(args, "skip_sync", False)),
            "open_browser": bool(getattr(args, "open_browser", False)),
        },
        build_cli_report=_setup_connect_report,
    ),
    "stripe.link": LocalSyncProxySpec(
        tool_name="stripe_link",
        build_arguments=lambda _args: {},
        build_cli_report=_stripe_link_report,
    ),
    "stripe.sync": LocalSyncProxySpec(
        tool_name="stripe_sync",
        build_arguments=lambda args: {
            "days": getattr(args, "days", None),
            "force": bool(getattr(args, "force", False)),
            "backfill": bool(getattr(args, "backfill", False)),
        },
        build_cli_report=_stripe_sync_report,
    ),
    "stripe.status": LocalSyncProxySpec(
        tool_name="stripe_status",
        build_arguments=lambda _args: {},
        build_cli_report=_stripe_status_report,
        pull_after=False,
    ),
    "stripe.revenue": LocalSyncProxySpec(
        tool_name="stripe_revenue",
        build_arguments=lambda args: {
            "month": getattr(args, "month", None),
            "quarter": getattr(args, "quarter", None),
            "year": getattr(args, "year", None),
        },
        build_cli_report=_stripe_revenue_report,
        pull_after=False,
    ),
    "stripe.unlink": LocalSyncProxySpec(
        tool_name="stripe_unlink",
        build_arguments=lambda _args: {},
        build_cli_report=_stripe_unlink_report,
    ),
    "schwab.sync": LocalSyncProxySpec(
        tool_name="schwab_sync",
        build_arguments=lambda _args: {},
        build_cli_report=_schwab_sync_report,
    ),
    "schwab.status": LocalSyncProxySpec(
        tool_name="schwab_status",
        build_arguments=lambda _args: {},
        build_cli_report=_schwab_status_report,
        pull_after=False,
    ),
    "rules.add-keyword": LocalSyncProxySpec(
        tool_name="rules_add_keyword",
        build_arguments=lambda args: {
            "keyword": getattr(args, "keyword", None),
            "category": getattr(args, "category", None),
            "use_type": getattr(args, "use_type", None),
            "priority": int(getattr(args, "priority", 0) or 0),
        },
        build_cli_report=_rules_add_keyword_report,
    ),
    "rules.add-split": LocalSyncProxySpec(
        tool_name="rules_add_split",
        build_arguments=lambda args: {
            "business_pct": float(getattr(args, "business_pct", 0) or 0),
            "business_category": getattr(args, "business_category", None),
            "personal_category": getattr(args, "personal_category", None),
            "match_category": getattr(args, "match_category", None),
            "match_keywords": getattr(args, "match_keywords", None),
            "note": getattr(args, "note", None),
        },
        build_cli_report=_rules_add_split_report,
    ),
    "rules.remove-keyword": LocalSyncProxySpec(
        tool_name="rules_remove_keyword",
        build_arguments=lambda args: {
            "keyword": getattr(args, "keyword", None),
            "dry_run": False,
        },
        build_cli_report=_rules_remove_keyword_report,
    ),
    "monthly run": LocalSyncProxySpec(
        tool_name="monthly_run",
        build_arguments=lambda args: {
            "month": getattr(args, "month", None),
            "sync": bool(getattr(args, "sync", False)),
            "ai": bool(getattr(args, "ai", False)),
            "dry_run": bool(getattr(args, "dry_run", False)),
            "skip": list(getattr(args, "skip", []) or []),
            "summary_only": True,
        },
        build_cli_report=_monthly_run_report,
    ),
    "db.restore": LocalSyncProxySpec(
        tool_name="db_restore",
        build_arguments=lambda args: {
            "bundle_path": getattr(args, "file", None),
            "dry_run": not bool(getattr(args, "yes", False)),
        },
        build_cli_report=_db_restore_report,
        pull_after=False,
    ),
    "db.import-preferences": LocalSyncProxySpec(
        tool_name="db_import_preferences",
        build_arguments=lambda args: {
            "bundle_path": getattr(args, "file", None),
            "mode": getattr(args, "mode", "merge"),
            "create_missing_categories": bool(getattr(args, "create_missing_categories", False)),
            "dry_run": not bool(getattr(args, "yes", False)),
        },
        build_cli_report=_db_import_preferences_report,
    ),
}


def _truthy_env(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _using_local_sync_db() -> bool:
    try:
        configured_db = get_db_path().expanduser().resolve()
        local_sync_db = CASHNERD_DB_PATH.expanduser().resolve()
    except OSError:
        return False
    return configured_db == local_sync_db


def local_sync_proxy_spec(args: Any) -> LocalSyncProxySpec | None:
    """Return proxy metadata when a direct CLI command should use local sync."""
    if _truthy_env("CASHNERD_DISABLE_CLI_SYNC_PROXY"):
        return None
    spec = _LOCAL_SYNC_PROXY_COMMANDS.get(str(getattr(args, "command_name", "") or ""))
    if spec is None or not _using_local_sync_db():
        return None
    return spec


async def _run_local_sync_proxy_async(spec: LocalSyncProxySpec, args: Any) -> dict[str, Any]:
    set_db_encryption_mode_override("off")
    config = load_config()
    auth = LocalAuth(config.server_url)
    engine = SyncEngine(config, auth)
    envelope = await engine.proxy_tool(
        spec.tool_name,
        spec.build_arguments(args),
        wait_for_subscriber=False,
    )
    if spec.pull_after:
        await engine.pull()
    return envelope


def run_local_sync_proxy(spec: LocalSyncProxySpec, args: Any) -> dict[str, Any]:
    envelope = asyncio.run(_run_local_sync_proxy_async(spec, args))
    result = {
        "data": envelope.get("data", {}) if isinstance(envelope.get("data"), dict) else {},
        "summary": envelope.get("summary", {}) if isinstance(envelope.get("summary"), dict) else {},
    }
    if spec.build_cli_report is not None:
        cli_report = spec.build_cli_report(envelope)
        if cli_report:
            result["cli_report"] = cli_report
    return result
