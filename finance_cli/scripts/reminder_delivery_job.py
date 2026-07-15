"""Deliver due per-user reminders."""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finance_cli import account_alerts
from finance_cli.commands import reminder_cmd
from finance_cli.db import connect
from finance_cli.storage_lease import LeaseQueuedError, optional_lease_scope
from finance_cli.user_provisioning import user_db_path


@dataclass(frozen=True)
class ReminderDeliverySettings:
    data_root: Path
    database_url: str


@dataclass
class ReminderDeliverySummary:
    user_count: int = 0
    processed_users: int = 0
    skipped_users: int = 0
    error_users: int = 0
    due_reminders: int = 0
    sent_reminders: int = 0
    failed_reminders: int = 0
    preview_reminders: int = 0
    checked_account_alerts: int = 0
    sent_account_alerts: int = 0
    failed_account_alerts: int = 0
    preview_account_alerts: int = 0


def _json_log(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))


def default_data_root() -> Path:
    raw_root = os.getenv("FINANCE_WEB_DATA_ROOT") or os.getenv("FINANCE_GATEWAY_DATA_ROOT")
    if raw_root:
        return Path(raw_root).expanduser().resolve()

    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "finance-web" / "server" / "config.py"
    spec = importlib.util.spec_from_file_location("finance_web_server_config", config_path)
    if spec is not None and spec.loader is not None:
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            default_factory = getattr(module, "_default_data_root", None)
            if callable(default_factory):
                return Path(default_factory()).expanduser().resolve()
        except Exception:
            pass
    return (repo_root / "finance-web" / "data" / "users").resolve()


def load_settings(
    *,
    data_root: Path | None = None,
    database_url: str | None = None,
) -> ReminderDeliverySettings:
    return ReminderDeliverySettings(
        data_root=(data_root or default_data_root()).expanduser().resolve(),
        database_url=str(database_url if database_url is not None else os.getenv("DATABASE_URL") or "").strip(),
    )


def _local_user_ids(data_root: Path, only_user_id: str | None = None) -> Iterable[str]:
    if only_user_id is not None:
        yield str(only_user_id)
        return
    if not data_root.exists():
        return
    for child in sorted(data_root.iterdir()):
        if child.name.startswith(".") or not child.is_dir():
            continue
        if not (child / "finance.db").exists():
            continue
        yield child.name


def _postgres_connect(database_url: str):
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ValueError("psycopg2 is required when DATABASE_URL is set") from exc
    return psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)


def iter_user_ids(settings: ReminderDeliverySettings, only_user_id: str | None = None) -> Iterable[str]:
    if only_user_id is not None:
        yield str(only_user_id)
        return
    if not settings.database_url:
        yield from _local_user_ids(settings.data_root)
        return

    with _postgres_connect(settings.database_url) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT id
              FROM users
             WHERE deleted_at IS NULL
             ORDER BY id
            """
        )
        for row in cursor.fetchall():
            yield str(row["id"])


def deliver_user_reminders(
    *,
    data_root: Path,
    user_id: str,
    now: datetime | None = None,
    limit: int = 50,
    dry_run: bool = False,
    use_lease: bool = True,
) -> dict[str, Any]:
    lease_context = (
        optional_lease_scope(
            user_id,
            operation="reminder_delivery",
            metadata={"source": "reminder_delivery_job"},
            heartbeat=True,
        )
        if use_lease
        else contextlib.nullcontext()
    )
    with lease_context:
        with connect(
            db_path=user_db_path(data_root, user_id),
            expected_user_id=user_id,
            busy_timeout=5000,
        ) as conn:
            reminders = reminder_cmd.send_due_reminders(
                conn,
                now=now,
                limit=limit,
                dry_run=dry_run,
            )
            reminders["account_alerts"] = account_alerts.evaluate_account_alert_rules(
                conn,
                now=now,
                limit=limit,
                dry_run=dry_run,
            )
            return reminders


def _parse_now(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    raw = str(raw_value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("--now must be an ISO datetime") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def run_delivery(
    *,
    settings: ReminderDeliverySettings,
    user_id: str | None = None,
    now: datetime | None = None,
    limit: int = 50,
    dry_run: bool = False,
) -> ReminderDeliverySummary:
    user_ids = list(iter_user_ids(settings, user_id))
    summary = ReminderDeliverySummary(user_count=len(user_ids))
    started = datetime.now(timezone.utc)
    _json_log(
        "reminder_delivery_start",
        data_root=str(settings.data_root),
        user=user_id,
        user_count=summary.user_count,
        dry_run=dry_run,
        started_at=started.isoformat(),
    )

    for current_user_id in user_ids:
        try:
            result = deliver_user_reminders(
                data_root=settings.data_root,
                user_id=current_user_id,
                now=now,
                limit=limit,
                dry_run=dry_run,
                use_lease=bool(settings.database_url),
            )
        except LeaseQueuedError as exc:
            summary.skipped_users += 1
            _json_log(
                "reminder_delivery_user_skipped",
                user_id=current_user_id,
                reason=str(exc),
            )
            continue
        except Exception as exc:
            summary.error_users += 1
            _json_log(
                "reminder_delivery_user_error",
                user_id=current_user_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        sent_count = len(result["sent"])
        failed_count = len(result["failed"])
        preview_count = len(result["previews"])
        account_alert_result = result.get("account_alerts") or {}
        sent_alert_count = len(account_alert_result.get("sent", []))
        failed_alert_count = len(account_alert_result.get("failed", []))
        preview_alert_count = len(account_alert_result.get("previews", []))
        summary.processed_users += 1
        summary.due_reminders += int(result["due_count"])
        summary.sent_reminders += sent_count
        summary.failed_reminders += failed_count
        summary.preview_reminders += preview_count
        summary.checked_account_alerts += int(account_alert_result.get("checked_count", 0) or 0)
        summary.sent_account_alerts += sent_alert_count
        summary.failed_account_alerts += failed_alert_count
        summary.preview_account_alerts += preview_alert_count
        _json_log(
            "reminder_delivery_user_complete",
            user_id=current_user_id,
            due=result["due_count"],
            sent=sent_count,
            failed=failed_count,
            previews=preview_count,
            account_alerts_checked=account_alert_result.get("checked_count", 0),
            account_alerts_sent=sent_alert_count,
            account_alerts_failed=failed_alert_count,
            account_alerts_previews=preview_alert_count,
        )

    finished = datetime.now(timezone.utc)
    _json_log(
        "reminder_delivery_finish",
        user_count=summary.user_count,
        processed_users=summary.processed_users,
        skipped_users=summary.skipped_users,
        error_users=summary.error_users,
        due_reminders=summary.due_reminders,
        sent_reminders=summary.sent_reminders,
        failed_reminders=summary.failed_reminders,
        preview_reminders=summary.preview_reminders,
        checked_account_alerts=summary.checked_account_alerts,
        sent_account_alerts=summary.sent_account_alerts,
        failed_account_alerts=summary.failed_account_alerts,
        preview_account_alerts=summary.preview_account_alerts,
        finished_at=finished.isoformat(),
        duration_ms=int((finished - started).total_seconds() * 1000),
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", help="deliver reminders for a single finance-web user id")
    parser.add_argument("--data-root", type=Path, default=None, help="per-user SQLite data root")
    parser.add_argument("--database-url", default=None, help="PostgreSQL DATABASE_URL override")
    parser.add_argument("--now", help="Override current time in ISO format")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_delivery(
        settings=load_settings(data_root=args.data_root, database_url=args.database_url),
        user_id=args.user,
        now=_parse_now(args.now),
        limit=args.limit,
        dry_run=bool(args.dry_run),
    )
    return 1 if summary.error_users else 0


if __name__ == "__main__":
    raise SystemExit(main())
