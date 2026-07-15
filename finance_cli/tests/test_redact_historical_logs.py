from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.frontend_logs import record_frontend_log
from finance_cli.scripts.redact_historical_logs import redact_database, run_audit
from finance_cli.user_provisioning import user_db_path


def _init_user_db(data_root: Path, user_id: str = "123") -> Path:
    db_path = user_db_path(data_root, user_id)
    db_path.parent.mkdir(parents=True)
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, ?)",
            (user_id,),
        )
    return db_path


def _insert_log(
    db_path: Path,
    message: str,
    *,
    namespace: str | None = "ChatPanel",
    page: str | None = "/dashboard",
    metadata: str | None = None,
) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO frontend_logs (level, namespace, message, page, metadata)
            VALUES ('warn', ?, ?, ?, ?)
            """,
            (namespace, message, page, metadata),
        )


def _seed_six_rows(data_root: Path) -> Path:
    db_path = _init_user_db(data_root)
    _insert_log(db_path, "card 4111-1111-1111-1111")
    _insert_log(
        db_path,
        "jwt eyJhbGciOiJIUzI1NiIs.eyJzdWIiOiIxMjM.signature_here_xxxxx",
    )
    _insert_log(db_path, "clean row", metadata=json.dumps({"description": "coffee"}))
    with connect(db_path) as conn:
        conn.execute("PRAGMA ignore_check_constraints = 1")
        conn.execute(
            """
            INSERT INTO frontend_logs (level, namespace, message, page, metadata)
            VALUES ('warn', 'ChatPanel', 'bad metadata', '/dashboard', '{not json')
            """
        )
    _insert_log(db_path, "json", metadata='{"token":"abc","other":"ok"}')
    _insert_log(db_path, "url", page="/callback?code=abc")
    return db_path


def test_redact_historical_logs_dry_run_apply_and_idempotency(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _seed_six_rows(data_root)

    dry = run_audit(data_root=data_root, user_id="123", sample=2)

    assert dry.scanned == 6
    assert dry.changed == 4
    assert dry.updated == 0
    assert dry.malformed_skipped == 1
    assert len(dry.samples) == 2

    with connect(db_path) as conn:
        raw_card = conn.execute(
            "SELECT message FROM frontend_logs WHERE message LIKE 'card%'"
        ).fetchone()["message"]
    assert raw_card == "card 4111-1111-1111-1111"

    applied = run_audit(data_root=data_root, apply=True, user_id="123")

    assert applied.changed == 4
    assert applied.updated == 4
    assert applied.malformed_skipped == 1
    assert applied.marker_counts["[CARD]"] == 1
    assert applied.marker_counts["[JWT]"] == 1
    assert applied.marker_counts["[REDACTED]"] == 2

    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT message, page, metadata FROM frontend_logs ORDER BY id"
        ).fetchall()

    assert rows[0]["message"] == "card [CARD]"
    assert rows[1]["message"] == "jwt [JWT]"
    assert json.loads(rows[4]["metadata"]) == {"other": "ok", "token": "[REDACTED]"}
    assert rows[5]["page"] == "/callback?code=[REDACTED]"

    rerun = run_audit(data_root=data_root, apply=True, user_id="123")

    assert rerun.scanned == 6
    assert rerun.changed == 0
    assert rerun.updated == 0
    assert rerun.malformed_skipped == 1


def test_redact_historical_logs_uses_user_db_path(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _init_user_db(data_root, "123")
    _insert_log(db_path, "email user@example.com")

    summary = run_audit(data_root=data_root, apply=True, user_id="123")

    assert summary.updated == 1
    with connect(user_db_path(data_root, "123")) as conn:
        row = conn.execute("SELECT message FROM frontend_logs").fetchone()
    assert row["message"] == "email [EMAIL]"


def test_redact_historical_logs_stale_write_guard_skips_clobber(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _init_user_db(data_root, "123")
    _insert_log(db_path, "card 4111-1111-1111-1111")

    def mutate(conn, row) -> None:
        conn.execute(
            "UPDATE frontend_logs SET message = 'changed elsewhere' WHERE id = ?",
            (row["id"],),
        )

    summary = redact_database(
        data_root=data_root,
        user_id="123",
        apply=True,
        before_update=mutate,
    )

    assert summary.changed == 1
    assert summary.updated == 0
    assert summary.stale_skipped == 1
    with connect(db_path) as conn:
        row = conn.execute("SELECT message FROM frontend_logs").fetchone()
    assert row["message"] == "changed elsewhere"


def test_redact_historical_logs_concurrent_runners_complete(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _init_user_db(data_root, "123")
    for _index in range(10):
        _insert_log(db_path, "card 4111-1111-1111-1111")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: run_audit(data_root=data_root, apply=True, user_id="123"),
                range(2),
            )
        )

    assert sum(result.updated for result in results) >= 10
    with connect(db_path) as conn:
        raw_count = conn.execute(
            "SELECT COUNT(*) FROM frontend_logs WHERE message LIKE '%4111%'"
        ).fetchone()[0]
    assert raw_count == 0


def test_redact_historical_logs_live_writer_succeeds_during_apply(tmp_path: Path) -> None:
    data_root = tmp_path / "users"
    db_path = _init_user_db(data_root, "123")
    _insert_log(db_path, "card 4111-1111-1111-1111")
    wrote = False

    def write_live(_conn, _row) -> None:
        nonlocal wrote
        if wrote:
            return
        record_frontend_log(
            db_path,
            "warn",
            "LiveWriter",
            "live sk-live-secret",
            metadata={"url": "/callback?code=abc"},
        )
        wrote = True

    summary = redact_database(
        data_root=data_root,
        user_id="123",
        apply=True,
        before_update=write_live,
    )

    assert summary.updated == 1
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT namespace, message, metadata FROM frontend_logs ORDER BY id"
        ).fetchall()

    assert rows[0]["message"] == "card [CARD]"
    assert rows[1]["namespace"] == "LiveWriter"
    assert rows[1]["message"] == "live [KEY]"
    assert json.loads(rows[1]["metadata"]) == {"url": "/callback?code=[REDACTED]"}
