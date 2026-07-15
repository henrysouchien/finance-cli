from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
from typing import Any

from finance_cli.db import connect, initialize_database
from finance_cli.scripts import cost_rollup_job
from finance_cli.user_provisioning import user_db_path


class FakePgCursor:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.cursor: sqlite3.Cursor | None = None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        self.cursor = self.conn.execute(sql.replace("%s", "?"), params or ())

    def fetchall(self) -> list[sqlite3.Row]:
        assert self.cursor is not None
        return self.cursor.fetchall()

    def fetchone(self) -> sqlite3.Row | None:
        assert self.cursor is not None
        return self.cursor.fetchone()


class FakePg:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                tier TEXT,
                lifetime_deal INTEGER DEFAULT 0,
                stripe_price_id TEXT,
                storage_mode TEXT DEFAULT 'local',
                deleted_at TEXT
            );
            CREATE TABLE ops_cost_rollups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                user_hash TEXT NOT NULL,
                provider TEXT NOT NULL,
                operation TEXT NOT NULL,
                total_usd6 INTEGER NOT NULL DEFAULT 0,
                request_count INTEGER NOT NULL DEFAULT 0,
                tier TEXT,
                plan_code TEXT,
                UNIQUE (date, user_hash, provider, operation)
            );
            """
        )

    def cursor(self) -> FakePgCursor:
        return FakePgCursor(self.conn)

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def __enter__(self) -> "FakePg":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def insert_user(
        self,
        user_id: str,
        *,
        tier: str = "paid",
        lifetime_deal: bool = False,
        stripe_price_id: str | None = None,
        storage_mode: str = "local",
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO users (id, tier, lifetime_deal, stripe_price_id, storage_mode, deleted_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            ON CONFLICT(id) DO UPDATE SET
                tier = excluded.tier,
                lifetime_deal = excluded.lifetime_deal,
                stripe_price_id = excluded.stripe_price_id,
                storage_mode = excluded.storage_mode
            """,
            (user_id, tier, int(lifetime_deal), stripe_price_id, storage_mode),
        )
        self.conn.commit()

    def rollup_rows(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT date, user_hash, provider, operation, total_usd6,
                   request_count, tier, plan_code
              FROM ops_cost_rollups
             ORDER BY date, user_hash, provider, operation
            """
        ).fetchall()
        return [dict(row) for row in rows]


def _settings(data_root: Path) -> cost_rollup_job.RollupSettings:
    return cost_rollup_job.RollupSettings(
        data_root=data_root,
        database_url="postgresql://fake/finance",
        stripe_price_lite="price_lite",
    )


def _install_fake_pg(monkeypatch, pg: FakePg) -> None:
    monkeypatch.setattr(cost_rollup_job, "_postgres_connect", lambda _database_url: pg)


def _init_user_db(data_root: Path, user_id: str) -> Path:
    db_path = user_db_path(data_root, user_id)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tenant_marker (singleton, user_id) VALUES (1, ?)",
            (user_id,),
        )
        conn.commit()
    return db_path


def _insert_cost(
    data_root: Path,
    user_id: str,
    *,
    provider: str = "claude",
    operation: str = "chat",
    cost_usd6: int = 100_000,
    is_byok: int = 0,
    created_at: str = "2026-04-28 12:00:00",
) -> None:
    with connect(user_db_path(data_root, user_id), expected_user_id=user_id) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, is_byok, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (provider, operation, cost_usd6, is_byok, created_at),
        )
        conn.commit()


def _run(
    pg: FakePg,
    monkeypatch,
    data_root: Path,
    target_date: date,
    user_id: str | None = None,
    storage_session_manager=None,
):
    _install_fake_pg(monkeypatch, pg)
    return cost_rollup_job.run_rollup(
        settings=_settings(data_root),
        target_date=target_date,
        user_id=user_id,
        storage_session_manager=storage_session_manager,
    )


def test_cost_rollup_idempotency(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("u1", tier="paid")
    _init_user_db(data_root, "u1")
    _insert_cost(data_root, "u1", cost_usd6=250_000)

    first = _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    first_rows = pg.rollup_rows()
    second = _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    second_rows = pg.rollup_rows()

    assert first.row_count == 1
    assert second.row_count == 1
    assert len(second_rows) == 1
    assert second_rows == first_rows
    assert second_rows[0]["total_usd6"] == 250_000
    assert second_rows[0]["request_count"] == 1


def test_cost_rollup_multi_user_distinct_hashes(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    for index, user_id in enumerate(("u1", "u2", "u3"), start=1):
        pg.insert_user(user_id, tier="paid")
        _init_user_db(data_root, user_id)
        _insert_cost(data_root, user_id, cost_usd6=index * 100_000)

    summary = _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    rows = pg.rollup_rows()

    assert summary.processed_users == 3
    assert len(rows) == 3
    assert len({row["user_hash"] for row in rows}) == 3
    assert sorted(row["total_usd6"] for row in rows) == [100_000, 200_000, 300_000]


def test_cost_rollup_user_hash_privacy(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("raw-user-id", tier="paid")
    _init_user_db(data_root, "raw-user-id")
    _insert_cost(data_root, "raw-user-id")

    _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    row = pg.rollup_rows()[0]

    assert row["user_hash"] != "raw-user-id"
    assert re.fullmatch(r"[0-9a-f]{64}", row["user_hash"])


def test_cost_rollup_excludes_byok_and_plaid_rows(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("u1", tier="paid")
    _init_user_db(data_root, "u1")
    _insert_cost(data_root, "u1", provider="claude", cost_usd6=100_000, is_byok=0)
    _insert_cost(data_root, "u1", provider="claude", cost_usd6=900_000, is_byok=1)
    _insert_cost(data_root, "u1", provider="plaid", operation="transactions_sync", cost_usd6=500_000)

    _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    rows = pg.rollup_rows()

    assert len(rows) == 1
    assert rows[0]["provider"] == "claude"
    assert rows[0]["total_usd6"] == 100_000
    assert rows[0]["request_count"] == 1


def test_cost_rollup_tier_and_plan_code_snapshotted_at_rollup_time(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("u1", tier="paid", stripe_price_id="price_lite")
    _init_user_db(data_root, "u1")
    _insert_cost(data_root, "u1", cost_usd6=100_000, created_at="2026-04-27 08:00:00")
    _insert_cost(data_root, "u1", cost_usd6=200_000, created_at="2026-04-28 08:00:00")

    _run(pg, monkeypatch, data_root, date(2026, 4, 27))
    pg.insert_user("u1", tier="lifetime", lifetime_deal=True)
    _run(pg, monkeypatch, data_root, date(2026, 4, 28))

    rows = pg.rollup_rows()
    assert [(row["date"], row["tier"], row["plan_code"]) for row in rows] == [
        ("2026-04-27", "paid", "lite"),
        ("2026-04-28", "lifetime", "lifetime"),
    ]


def test_cost_rollup_empty_cost_ledger_writes_no_rows(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("u1", tier="paid")
    _init_user_db(data_root, "u1")

    summary = _run(pg, monkeypatch, data_root, date(2026, 4, 28))

    assert summary.processed_users == 1
    assert summary.row_count == 0
    assert pg.rollup_rows() == []


def test_cost_rollup_per_user_error_isolation(tmp_path: Path, monkeypatch, capsys) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    for user_id in ("good1", "bad", "good2"):
        pg.insert_user(user_id, tier="paid")
    for user_id, cost in (("good1", 100_000), ("good2", 200_000)):
        _init_user_db(data_root, user_id)
        _insert_cost(data_root, user_id, cost_usd6=cost)
    bad_path = user_db_path(data_root, "bad")
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("not sqlite", encoding="utf-8")

    summary = _run(pg, monkeypatch, data_root, date(2026, 4, 28))
    captured = capsys.readouterr().out

    assert summary.processed_users == 2
    assert summary.error_users == 1
    assert len(pg.rollup_rows()) == 2
    assert "cost_rollup_user_error" in captured
    assert "bad" in captured


def test_cost_rollup_routes_remote_users_without_local_file(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("42", tier="paid", storage_mode="remote")
    routed: list[tuple[Path, str, str | None, object | None]] = []

    @contextmanager
    def remote_acquire(*_args, **_kwargs):
        yield SimpleNamespace(storage_mode="remote")

    def fake_aggregate_user_costs(
        *,
        data_root,
        user_id,
        target_date,
        tier,
        plan_code,
        expected_user_id=None,
        storage_session_manager=None,
    ):
        routed.append((data_root, user_id, expected_user_id, storage_session_manager))
        return [
            cost_rollup_job.CostRollupRow(
                date=target_date.isoformat(),
                user_hash=cost_rollup_job.user_hash(user_id),
                provider="claude",
                operation="chat",
                total_usd6=123_456,
                request_count=2,
                tier=tier,
                plan_code=plan_code,
            )
        ]

    manager = object()
    _install_fake_pg(monkeypatch, pg)
    monkeypatch.setattr(cost_rollup_job.LeaseScope, "acquire", remote_acquire)
    monkeypatch.setattr(cost_rollup_job, "aggregate_user_costs", fake_aggregate_user_costs)

    summary = cost_rollup_job.run_rollup(
        settings=_settings(data_root),
        target_date=date(2026, 4, 28),
        storage_session_manager=manager,
    )

    assert routed == [(data_root, "42", "42", manager)]
    assert not (data_root / "42" / "finance.db").exists()
    assert summary.user_count == 1
    assert summary.processed_users == 1
    assert summary.skipped_users == 0
    assert summary.error_users == 0
    assert summary.row_count == 1
    assert pg.rollup_rows()[0]["total_usd6"] == 123_456


def test_cost_rollup_remote_user_without_session_manager_is_error(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("42", tier="paid", storage_mode="remote")
    monkeypatch.setattr(cost_rollup_job, "_default_session_manager", lambda: None)

    summary = _run(pg, monkeypatch, data_root, date(2026, 4, 28))

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 0
    assert summary.error_users == 1
    assert pg.rollup_rows() == []


def test_cost_rollup_reports_missing_pg_local_db(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("42", tier="paid", storage_mode="local")

    summary = _run(pg, monkeypatch, data_root, date(2026, 4, 28))

    assert not (data_root / "42" / "finance.db").exists()
    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0
    assert pg.rollup_rows() == []


def test_cost_rollup_skips_queued_storage_mode(tmp_path: Path, monkeypatch) -> None:
    data_root = tmp_path / "users"
    pg = FakePg()
    pg.insert_user("42", tier="paid", storage_mode="migrating")

    @contextmanager
    def queued_acquire(*_args, **_kwargs):
        yield cost_rollup_job.Queued(storage_mode="migrating")

    monkeypatch.setattr(cost_rollup_job.LeaseScope, "acquire", queued_acquire)

    summary = _run(
        pg,
        monkeypatch,
        data_root,
        date(2026, 4, 28),
        storage_session_manager=object(),
    )

    assert summary.user_count == 1
    assert summary.processed_users == 0
    assert summary.skipped_users == 1
    assert summary.error_users == 0
    assert pg.rollup_rows() == []
