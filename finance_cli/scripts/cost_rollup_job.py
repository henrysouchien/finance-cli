"""Nightly AI cost rollup from per-user SQLite into shared PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from finance_cli.billing import effective_plan
from finance_cli.db import connect
from finance_cli.storage_lease import (
    LeaseScope,
    LeaseUnavailableError,
    Queued,
    lease_enforcement_enabled,
)
from finance_cli.user_provisioning import user_db_path


@dataclass(frozen=True)
class RollupSettings:
    data_root: Path
    database_url: str
    stripe_price_lite: str = ""


@dataclass(frozen=True)
class CostRollupRow:
    date: str
    user_hash: str
    provider: str
    operation: str
    total_usd6: int
    request_count: int
    tier: str | None
    plan_code: str | None


@dataclass
class RollupSummary:
    date: str
    user_count: int = 0
    processed_users: int = 0
    skipped_users: int = 0
    error_users: int = 0
    row_count: int = 0


@dataclass(frozen=True)
class UserRollupResult:
    user_id: str
    db_path: Path
    rows: list[CostRollupRow]
    skipped: bool = False
    skip_reason: str | None = None


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


def load_settings(*, data_root: Path | None = None, database_url: str | None = None) -> RollupSettings:
    return RollupSettings(
        data_root=(data_root or default_data_root()).expanduser().resolve(),
        database_url=str(database_url if database_url is not None else os.getenv("DATABASE_URL") or "").strip(),
        stripe_price_lite=str(os.getenv("STRIPE_PRICE_LITE") or "").strip(),
    )


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


def user_hash(user_id: str | int) -> str:
    return hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()


def _postgres_connect(database_url: str):
    if not database_url:
        raise ValueError("DATABASE_URL is required for cost rollup job")
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise ValueError("psycopg2 is required for cost rollup job") from exc
    return psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)


def _fetch_user_snapshots(pg_conn: Any, settings: RollupSettings, only_user_id: str | None) -> dict[str, dict[str, Any]]:
    cursor = pg_conn.cursor()
    if only_user_id is None:
        cursor.execute(
            """
            SELECT id, tier, lifetime_deal, stripe_price_id, storage_mode
              FROM users
             WHERE deleted_at IS NULL
             ORDER BY id
            """
        )
    else:
        cursor.execute(
            """
            SELECT id, tier, lifetime_deal, stripe_price_id, storage_mode
              FROM users
             WHERE id = %s
            """,
            (only_user_id,),
        )
    snapshots: dict[str, dict[str, Any]] = {}
    for row in cursor.fetchall():
        user = dict(row)
        plan = effective_plan(user, settings)
        user["plan_code"] = plan.code
        user["storage_mode"] = _normalize_storage_mode(user.get("storage_mode"))
        snapshots[str(user["id"])] = user
    return snapshots


def _normalize_storage_mode(value: Any) -> str:
    return str(value or "local").strip().lower() or "local"


def _default_session_manager():
    if not str(os.getenv("DATABASE_URL") or "").strip():
        return None
    try:
        from app_platform.db.session import SessionManager
    except Exception:
        return None
    try:
        return SessionManager._get_default_manager()
    except Exception:
        return None


def _rollup_user_ids(
    *,
    settings: RollupSettings,
    snapshots: dict[str, dict[str, Any]],
    only_user_id: str | None,
) -> list[str]:
    if only_user_id is not None:
        return [str(only_user_id)]
    if settings.database_url:
        return list(snapshots.keys())
    return list(iter_user_ids(settings.data_root))


def _rollup_window(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _sqlite_ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def aggregate_user_costs(
    *,
    data_root: Path,
    user_id: str,
    target_date: date,
    tier: str | None,
    plan_code: str | None,
    expected_user_id: str | None = None,
    storage_session_manager=None,
) -> list[CostRollupRow]:
    db_path = user_db_path(data_root, user_id)
    start, end = _rollup_window(target_date)
    tenant_guard_user_id = str(expected_user_id) if expected_user_id is not None else str(user_id)
    with connect(
        db_path=db_path,
        expected_user_id=tenant_guard_user_id,
        storage_session_manager=storage_session_manager,
        busy_timeout=5000,
    ) as conn:
        rows = conn.execute(
            """
            SELECT
                date(created_at) AS day,
                provider,
                operation,
                COALESCE(SUM(cost_usd6), 0) AS total_usd6,
                COUNT(*) AS request_count
            FROM cost_ledger
            WHERE datetime(created_at) >= datetime(?)
              AND datetime(created_at) < datetime(?)
              AND provider IN ('claude', 'openai')
              AND is_byok = 0
            GROUP BY day, provider, operation
            ORDER BY day, provider, operation
            """,
            (_sqlite_ts(start), _sqlite_ts(end)),
        ).fetchall()

    hashed = user_hash(user_id)
    return [
        CostRollupRow(
            date=str(row["day"]),
            user_hash=hashed,
            provider=str(row["provider"]),
            operation=str(row["operation"]),
            total_usd6=int(row["total_usd6"] or 0),
            request_count=int(row["request_count"] or 0),
            tier=tier,
            plan_code=plan_code,
        )
        for row in rows
        if row["day"] is not None
    ]


def _aggregate_user_with_optional_lease(
    *,
    data_root: Path,
    user_id: str,
    target_date: date,
    tier: str | None,
    plan_code: str | None,
    storage_mode: str | None,
    storage_session_manager,
) -> UserRollupResult:
    db_path = user_db_path(data_root, user_id)
    normalized_storage_mode = _normalize_storage_mode(storage_mode)
    if normalized_storage_mode == "local" and not db_path.exists():
        return UserRollupResult(
            user_id=user_id,
            db_path=db_path,
            rows=[],
            skipped=True,
            skip_reason="missing_finance_db",
        )

    manager = storage_session_manager if storage_session_manager is not None else _default_session_manager()
    if normalized_storage_mode in {"remote", "migrating", "replaying"} and manager is None:
        raise LeaseUnavailableError("session_manager_unavailable")
    if manager is None:
        if lease_enforcement_enabled():
            raise LeaseUnavailableError("session_manager_unavailable")
        rows = aggregate_user_costs(
            data_root=data_root,
            user_id=user_id,
            target_date=target_date,
            tier=tier,
            plan_code=plan_code,
        )
        return UserRollupResult(user_id=user_id, db_path=db_path, rows=rows)

    try:
        with LeaseScope.acquire(
            user_id,
            session_manager=manager,
            operation="cost_rollup",
            metadata={"source": "cost_rollup_job"},
        ) as scope:
            if isinstance(scope, Queued):
                return UserRollupResult(
                    user_id=user_id,
                    db_path=db_path,
                    rows=[],
                    skipped=True,
                    skip_reason=f"storage_mode_{scope.storage_mode}",
                )
            rows = aggregate_user_costs(
                data_root=data_root,
                user_id=user_id,
                target_date=target_date,
                tier=tier,
                plan_code=plan_code,
                expected_user_id=user_id,
                storage_session_manager=manager,
            )
            return UserRollupResult(user_id=user_id, db_path=db_path, rows=rows)
    except LeaseUnavailableError:
        if lease_enforcement_enabled() or normalized_storage_mode != "local":
            raise
        rows = aggregate_user_costs(
            data_root=data_root,
            user_id=user_id,
            target_date=target_date,
            tier=tier,
            plan_code=plan_code,
        )
        return UserRollupResult(user_id=user_id, db_path=db_path, rows=rows)


def upsert_rollups(pg_conn: Any, rows: list[CostRollupRow]) -> int:
    if not rows:
        return 0
    cursor = pg_conn.cursor()
    for row in rows:
        cursor.execute(
            """
            INSERT INTO ops_cost_rollups (
                date, user_hash, provider, operation,
                total_usd6, request_count, tier, plan_code
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, user_hash, provider, operation) DO UPDATE SET
                total_usd6 = EXCLUDED.total_usd6,
                request_count = EXCLUDED.request_count,
                tier = EXCLUDED.tier,
                plan_code = EXCLUDED.plan_code
            """,
            (
                row.date,
                row.user_hash,
                row.provider,
                row.operation,
                row.total_usd6,
                row.request_count,
                row.tier,
                row.plan_code,
            ),
        )
    return len(rows)


def run_rollup(
    *,
    settings: RollupSettings,
    target_date: date,
    user_id: str | None = None,
    storage_session_manager=None,
) -> RollupSummary:
    started = datetime.now(timezone.utc)
    with _postgres_connect(settings.database_url) as pg_conn:
        snapshots = _fetch_user_snapshots(pg_conn, settings, user_id)
        user_ids = _rollup_user_ids(settings=settings, snapshots=snapshots, only_user_id=user_id)
        summary = RollupSummary(date=target_date.isoformat(), user_count=len(user_ids))
        _json_log(
            "cost_rollup_start",
            date=summary.date,
            data_root=str(settings.data_root),
            user_count=summary.user_count,
            user=user_id,
            started_at=started.isoformat(),
        )
        for current_user_id in user_ids:
            user = snapshots.get(str(current_user_id))
            if user is None:
                summary.skipped_users += 1
                _json_log(
                    "cost_rollup_user_skipped",
                    date=summary.date,
                    user_id=current_user_id,
                    reason="missing_postgres_user",
                )
                continue

            try:
                result = _aggregate_user_with_optional_lease(
                    data_root=settings.data_root,
                    user_id=current_user_id,
                    target_date=target_date,
                    tier=str(user.get("tier") or "") or None,
                    plan_code=str(user.get("plan_code") or "") or None,
                    storage_mode=str(user.get("storage_mode") or "local"),
                    storage_session_manager=storage_session_manager,
                )
                if result.skipped:
                    summary.skipped_users += 1
                    _json_log(
                        "cost_rollup_user_skipped",
                        date=summary.date,
                        user_id=result.user_id,
                        db_path=str(result.db_path),
                        reason=result.skip_reason,
                    )
                    continue

                row_count = upsert_rollups(pg_conn, result.rows)
                pg_conn.commit()
            except Exception as exc:
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
                summary.error_users += 1
                _json_log(
                    "cost_rollup_user_error",
                    date=summary.date,
                    user_id=current_user_id,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                continue

            summary.processed_users += 1
            summary.row_count += row_count
            _json_log(
                "cost_rollup_user_complete",
                date=summary.date,
                user_id=current_user_id,
                row_count=row_count,
                tier=user.get("tier"),
                plan_code=user.get("plan_code"),
            )

    finished = datetime.now(timezone.utc)
    _json_log(
        "cost_rollup_finish",
        date=summary.date,
        user_count=summary.user_count,
        processed_users=summary.processed_users,
        skipped_users=summary.skipped_users,
        error_users=summary.error_users,
        row_count=summary.row_count,
        finished_at=finished.isoformat(),
        duration_ms=int((finished - started).total_seconds() * 1000),
    )
    return summary


def _parse_date(raw_value: str | None) -> date:
    if not raw_value:
        return datetime.now(timezone.utc).date() - timedelta(days=1)
    try:
        return date.fromisoformat(raw_value)
    except ValueError as exc:
        raise ValueError("--date must be in YYYY-MM-DD format") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="UTC rollup date in YYYY-MM-DD form; default is yesterday")
    parser.add_argument("--user", help="roll up a single PostgreSQL users.id")
    parser.add_argument("--data-root", type=Path, default=None, help="per-user SQLite data root")
    parser.add_argument("--database-url", default=None, help="PostgreSQL DATABASE_URL override")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_rollup(
        settings=load_settings(data_root=args.data_root, database_url=args.database_url),
        target_date=_parse_date(args.date),
        user_id=args.user,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
