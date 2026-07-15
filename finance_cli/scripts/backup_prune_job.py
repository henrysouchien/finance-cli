"""Daily multi-user backup retention job for finance-web deployments."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finance_cli.backup import prune_backups
from finance_cli.db import connect
from finance_cli.user_provisioning import user_db_path


@dataclass(frozen=True)
class BackupPruneSettings:
    data_root: Path


@dataclass
class BackupPruneSummary:
    user_count: int = 0
    processed_users: int = 0
    skipped_users: int = 0
    error_users: int = 0
    kept: int = 0
    deleted: int = 0
    freed_bytes: int = 0
    scheduled_key_deletions: int = 0


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
    return (Path(__file__).resolve().parents[2] / "finance-web" / "data" / "users").resolve()


def load_settings(*, data_root: Path | None = None) -> BackupPruneSettings:
    return BackupPruneSettings(data_root=(data_root or default_data_root()).expanduser().resolve())


def iter_user_ids(data_root: Path, only_user_id: str | None = None) -> Iterable[str]:
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


def prune_user_backups(*, data_root: Path, user_id: str, dry_run: bool):
    db_path = user_db_path(data_root, user_id)
    with connect(db_path=db_path, expected_user_id=user_id, busy_timeout=5000) as conn:
        return prune_backups(conn, dry_run=dry_run, data_dir=db_path.parent, user_id=user_id)


def run_backup_prune(
    *,
    settings: BackupPruneSettings,
    dry_run: bool = True,
    user_id: str | None = None,
) -> BackupPruneSummary:
    user_ids = list(iter_user_ids(settings.data_root, user_id))
    summary = BackupPruneSummary(user_count=len(user_ids))
    started = datetime.now(timezone.utc)
    _json_log(
        "backup_prune_start",
        data_root=str(settings.data_root),
        dry_run=dry_run,
        user=user_id,
        user_count=summary.user_count,
        started_at=started.isoformat(),
    )

    for current_user_id in user_ids:
        db_path = user_db_path(settings.data_root, current_user_id)
        if not db_path.exists():
            summary.skipped_users += 1
            _json_log(
                "backup_prune_user_skipped",
                user_id=current_user_id,
                reason="missing_finance_db",
                db_path=str(db_path),
            )
            continue

        try:
            result = prune_user_backups(
                data_root=settings.data_root,
                user_id=current_user_id,
                dry_run=dry_run,
            )
        except Exception as exc:
            summary.error_users += 1
            _json_log(
                "backup_prune_user_error",
                user_id=current_user_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        summary.processed_users += 1
        summary.kept += result.kept
        summary.deleted += result.deleted
        summary.freed_bytes += result.freed_bytes
        summary.scheduled_key_deletions += result.scheduled_key_deletions
        _json_log(
            "backup_prune_user_complete",
            user_id=current_user_id,
            dry_run=result.dry_run,
            kept=result.kept,
            deleted=result.deleted,
            freed_bytes=result.freed_bytes,
            scheduled_key_deletions=result.scheduled_key_deletions,
        )

    finished = datetime.now(timezone.utc)
    _json_log(
        "backup_prune_finish",
        user_count=summary.user_count,
        processed_users=summary.processed_users,
        skipped_users=summary.skipped_users,
        error_users=summary.error_users,
        kept=summary.kept,
        deleted=summary.deleted,
        freed_bytes=summary.freed_bytes,
        scheduled_key_deletions=summary.scheduled_key_deletions,
        finished_at=finished.isoformat(),
        duration_ms=int((finished - started).total_seconds() * 1000),
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true", help="actually delete pruned backups; default is dry-run")
    parser.add_argument("--user", help="prune a single finance-web user id")
    parser.add_argument("--data-root", type=Path, default=None, help="per-user SQLite data root")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run_backup_prune(
        settings=load_settings(data_root=args.data_root),
        dry_run=not args.yes,
        user_id=args.user,
    )
    return 1 if summary.error_users else 0


if __name__ == "__main__":
    raise SystemExit(main())
