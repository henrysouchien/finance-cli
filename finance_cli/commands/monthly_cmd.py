"""Monthly pipeline runner command."""

from __future__ import annotations

import logging
import time
from argparse import Namespace
from datetime import datetime
from typing import Any

from ..db import backup_database
from ..models import cents_to_dollars
from ..subscriptions import subscription_burn

logger = logging.getLogger(__name__)


def register(subparsers, format_parent) -> None:
    parser = subparsers.add_parser("monthly", parents=[format_parent], help="Monthly pipeline runner")
    monthly_sub = parser.add_subparsers(dest="monthly_action")
    p_run = monthly_sub.add_parser("run", parents=[format_parent], help="Run monthly pipeline")
    p_run.add_argument("--month", default=datetime.now().strftime("%Y-%m"))
    p_run.add_argument("--sync", action="store_true")
    p_run.add_argument("--ai", action="store_true")
    p_run.add_argument("--export-dir")
    p_run.add_argument("--dry-run", action="store_true")
    p_run.add_argument("--skip", action="append", default=[], choices=["dedup", "categorize", "detect"])
    p_run.set_defaults(func=handle_run, command_name="monthly run")


def _step_entry(status: str = "skipped", result: Any = None, error: str | None = None) -> dict:
    return {"status": status, "result": result, "error": error}


def _run_step(name: str, steps: dict, fn, *args, **kwargs) -> Any:
    """Execute a pipeline step, recording success or error."""
    try:
        result = fn(*args, **kwargs)
        steps[name] = _step_entry("success", result)
        return result
    except Exception as exc:
        logger.warning("monthly run: step '%s' failed: %s", name, exc)
        steps[name] = _step_entry("error", error=str(exc))
        return None


def handle_run(args, conn) -> dict[str, Any]:
    t0 = time.monotonic()
    steps: dict[str, dict] = {}
    skips = set(args.skip)

    # --- DB backup before first mutating step (not dry-run) ---
    if not args.dry_run:
        try:
            backup_database(conn=conn)
        except Exception as exc:
            logger.warning("monthly run: backup failed: %s", exc)

    # --- Step 1: Plaid sync + balance refresh ---
    if args.sync:
        try:
            from ..plaid_client import config_status

            status = config_status()
            if not status.configured:
                steps["sync"] = _step_entry("skipped", error="Plaid not configured — run setup check")
                steps["balance"] = _step_entry("skipped", error="Plaid not configured")
            else:
                from .plaid_cmd import handle_balance_refresh, handle_sync

                _run_step(
                    "sync",
                    steps,
                    handle_sync,
                    Namespace(days=None, item=None, force=False),
                    conn,
                )
                _run_step(
                    "balance",
                    steps,
                    handle_balance_refresh,
                    Namespace(item=None, force=False),
                    conn,
                )
        except Exception as exc:
            logger.warning("monthly run: Plaid import failed: %s", exc)
            steps.setdefault("sync", _step_entry("skipped", error=str(exc)))
            steps.setdefault("balance", _step_entry("skipped", error=str(exc)))
    else:
        steps["sync"] = _step_entry("skipped")
        steps["balance"] = _step_entry("skipped")

    # --- Step 2: Cross-format dedup ---
    if "dedup" not in skips:
        from .dedup_cmd import handle_cross_format

        _run_step(
            "dedup",
            steps,
            handle_cross_format,
            Namespace(
                account_id=None,
                date_from=None,
                date_to=None,
                commit=not args.dry_run,
                include_key_only=False,
            ),
            conn,
        )
    else:
        steps["dedup"] = _step_entry("skipped")

    # --- Step 3: Auto-categorize ---
    if "categorize" not in skips:
        from .cat import handle_auto_categorize

        _run_step(
            "categorize",
            steps,
            handle_auto_categorize,
            Namespace(dry_run=args.dry_run, ai=args.ai, provider=None, batch_size=None),
            conn,
        )
        # handle_auto_categorize rolls back internally when dry_run=True,
        # but does NOT commit when dry_run=False — we must commit:
        if not args.dry_run and steps.get("categorize", {}).get("status") == "success":
            conn.commit()
    else:
        steps["categorize"] = _step_entry("skipped")

    # --- Step 4: Subscription detection ---
    if "detect" not in skips:
        from .subs import handle_detect

        _run_step("detect", steps, handle_detect, Namespace(), conn)
        # detect_subscriptions writes but does not commit:
        if steps.get("detect", {}).get("status") == "success":
            if not args.dry_run:
                conn.commit()
            else:
                conn.rollback()
    else:
        steps["detect"] = _step_entry("skipped")

    # --- Subscription burn summary (after detect) ---
    active_count = 0
    burn_cents = 0
    try:
        burn_result = subscription_burn(conn)
        active_count = burn_result["active_subscriptions"]
        burn_cents = burn_result["monthly_burn_cents"]
    except Exception:
        pass

    # --- Step 5: Wave export ---
    if args.export_dir:
        from .export import handle_wave

        _run_step(
            "export",
            steps,
            handle_wave,
            Namespace(month=args.month, output=args.export_dir),
            conn,
        )
    else:
        steps["export"] = _step_entry("skipped")

    # --- Step 6: Health checks ---
    health = {
        "unreviewed_count": 0,
        "uncategorized_count": 0,
        "null_use_type_count": 0,
        "budget_over_count": 0,
        "budget_alert_count": 0,
        "budget_warn_count": 0,
    }
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE is_active = 1 AND is_reviewed = 0"
        ).fetchone()
        health["unreviewed_count"] = int(row["cnt"]) if row else 0
    except Exception:
        pass
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE is_active = 1 AND category_id IS NULL"
        ).fetchone()
        health["uncategorized_count"] = int(row["cnt"]) if row else 0
    except Exception:
        pass
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE is_active = 1 AND use_type IS NULL"
        ).fetchone()
        health["null_use_type_count"] = int(row["cnt"]) if row else 0
    except Exception:
        pass
    try:
        from ..budget_engine import budget_alerts

        alert_result = budget_alerts(conn, month=args.month)
        health["budget_over_count"] = int(alert_result.get("over_count", 0))
        health["budget_alert_count"] = int(alert_result.get("alert_count", 0))
        health["budget_warn_count"] = int(alert_result.get("warn_count", 0))
    except Exception:
        pass

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    # --- Build summary counts ---
    steps_run = 0
    steps_succeeded = 0
    steps_failed = 0
    steps_skipped = 0
    for entry in steps.values():
        st = entry["status"]
        if st == "success":
            steps_run += 1
            steps_succeeded += 1
        elif st == "error":
            steps_run += 1
            steps_failed += 1
        else:
            steps_skipped += 1

    # --- Build CLI report ---
    cli_report = _build_cli_report(
        month=args.month,
        elapsed_ms=elapsed_ms,
        steps=steps,
        health=health,
        active_count=active_count,
        burn_cents=burn_cents,
        export_dir=args.export_dir,
    )

    return {
        "data": {
            "month": args.month,
            "elapsed_ms": elapsed_ms,
            "steps": steps,
            "health": health,
        },
        "summary": {
            "steps_run": steps_run,
            "steps_succeeded": steps_succeeded,
            "steps_failed": steps_failed,
            "steps_skipped": steps_skipped,
        },
        "cli_report": cli_report,
    }


_STATUS_ICON = {"success": "\u2713", "error": "\u2717", "skipped": "\u2014"}


def _step_line(label: str, detail: str, status: str) -> str:
    icon = _STATUS_ICON.get(status, "?")
    return f"  {label:<14s} {detail:<50s} {icon}"


def _build_cli_report(
    *,
    month: str,
    elapsed_ms: int,
    steps: dict,
    health: dict,
    active_count: int,
    burn_cents: int,
    export_dir: str | None,
) -> str:
    elapsed_s = elapsed_ms / 1000
    lines = [f"Monthly Run: {month} \u2014 completed in {elapsed_s:.1f}s", ""]

    # Sync
    sync_entry = steps.get("sync", {})
    if sync_entry.get("status") == "success":
        r = sync_entry.get("result", {}) or {}
        d = r.get("data", {}) if isinstance(r, dict) else {}
        detail = (
            f"{d.get('items_synced', 0)} items synced, "
            f"{d.get('added', 0)} added, "
            f"{d.get('modified', 0)} modified"
        )
    elif sync_entry.get("status") == "error":
        detail = sync_entry.get("error", "unknown error")
    else:
        detail = "skipped"
    lines.append(_step_line("Sync:", detail, sync_entry.get("status", "skipped")))

    # Dedup
    dedup_entry = steps.get("dedup", {})
    if dedup_entry.get("status") == "success":
        r = dedup_entry.get("result", {}) or {}
        d = r.get("data", {}) if isinstance(r, dict) else {}
        removed = d.get("removed", 0)
        detail = f"{removed} duplicates removed"
    elif dedup_entry.get("status") == "error":
        detail = dedup_entry.get("error", "unknown error")
    else:
        detail = "skipped"
    lines.append(_step_line("Dedup:", detail, dedup_entry.get("status", "skipped")))

    # Categorize
    cat_entry = steps.get("categorize", {})
    if cat_entry.get("status") == "success":
        r = cat_entry.get("result", {}) or {}
        d = r.get("data", {}) if isinstance(r, dict) else {}
        updated = d.get("updated", 0)
        by_source = d.get("by_source", {}) if isinstance(d, dict) else {}
        parts = []
        for src, cnt in by_source.items():
            parts.append(f"{cnt} {src}")
        source_detail = f" ({', '.join(parts)})" if parts else ""
        detail = f"{updated} categorized{source_detail}"
    elif cat_entry.get("status") == "error":
        detail = cat_entry.get("error", "unknown error")
    else:
        detail = "skipped"
    lines.append(_step_line("Categorize:", detail, cat_entry.get("status", "skipped")))

    # Detect
    detect_entry = steps.get("detect", {})
    if detect_entry.get("status") == "success":
        burn_dollars = cents_to_dollars(burn_cents)
        detail = f"{active_count} active subscriptions at ${burn_dollars:,.0f}/mo"
    elif detect_entry.get("status") == "error":
        detail = detect_entry.get("error", "unknown error")
    else:
        detail = "skipped"
    lines.append(_step_line("Detect:", detail, detect_entry.get("status", "skipped")))

    # Export
    export_entry = steps.get("export", {})
    if export_entry.get("status") == "success":
        r = export_entry.get("result", {}) or {}
        d = r.get("data", {}) if isinstance(r, dict) else {}
        files = d.get("files", [])
        detail = f"{len(files)} Wave CSVs written to {export_dir}"
    elif export_entry.get("status") == "error":
        detail = export_entry.get("error", "unknown error")
    else:
        detail = "skipped"
    lines.append(_step_line("Export:", detail, export_entry.get("status", "skipped")))

    # Health checks
    lines.append("")
    lines.append(f"  Unreviewed:    {health.get('unreviewed_count', 0)} transactions need review")
    lines.append(f"  Uncategorized: {health.get('uncategorized_count', 0)} transactions need categorization")
    lines.append(f"  Unclassified:  {health.get('null_use_type_count', 0)} transactions have NULL use_type")
    over_count = int(health.get("budget_over_count", 0))
    alert_count = int(health.get("budget_alert_count", 0))
    warn_count = int(health.get("budget_warn_count", 0))
    if over_count == 0 and alert_count == 0 and warn_count == 0:
        lines.append("  \u2713 Budget check: all on track")
    else:
        lines.append(
            f"  \u2713 Budget check: {over_count} over budget, {alert_count} at risk, {warn_count} warnings"
        )

    return "\n".join(lines)
