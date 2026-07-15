from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

import pytest

import finance_cli.cost_tracking as cost_tracking
from finance_cli.cost_tracking import (
    _record_cost_strict,
    _period_bucket,
    check_cost_limit,
    estimate_ai_cost_usd6,
    prune_cost_ledger,
    record_and_settle_cost,
    record_cost,
)
from finance_cli.db import connect, initialize_database


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def test_record_cost_writes_to_cost_ledger(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_cost(
        db_path,
        "claude",
        "chat",
        123_456,
        input_tokens=11,
        output_tokens=7,
        cache_creation_tokens=3,
        cache_read_tokens=2,
        model="claude-sonnet-4-5-20250929",
        request_id="req_1",
        idempotency_key="cost_1",
    )

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT provider, operation, cost_usd6, input_tokens, output_tokens,
                   cache_creation_tokens, cache_read_tokens, model, request_id,
                   is_estimated, idempotency_key
            FROM cost_ledger
            """
        ).fetchone()

    assert row is not None
    assert row["provider"] == "claude"
    assert row["operation"] == "chat"
    assert row["cost_usd6"] == 123_456
    assert row["input_tokens"] == 11
    assert row["output_tokens"] == 7
    assert row["cache_creation_tokens"] == 3
    assert row["cache_read_tokens"] == 2
    assert row["model"] == "claude-sonnet-4-5-20250929"
    assert row["request_id"] == "req_1"
    assert row["is_estimated"] == 0
    assert row["idempotency_key"] == "cost_1"


def test_record_cost_idempotency_key_prevents_duplicates(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    record_cost(db_path, "openai", "categorize", 500, idempotency_key="dup_key")
    record_cost(db_path, "openai", "categorize", 900, idempotency_key="dup_key")

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS count, MIN(cost_usd6) AS cost_usd6 FROM cost_ledger"
        ).fetchone()

    assert row is not None
    assert row["count"] == 1
    assert row["cost_usd6"] == 500


def test_record_cost_never_raises_on_db_errors(tmp_path: Path, monkeypatch) -> None:
    broken_db_path = tmp_path / "missing" / "finance.db"
    warnings: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        cost_tracking.log,
        "warning",
        lambda message, *args: warnings.append((str(message), args)),
    )

    assert record_cost(broken_db_path, "claude", "chat", 100) is None
    assert warnings
    assert warnings[0][0] == "Failed to record cost: %s"


def test_record_cost_strict_raises_on_db_errors() -> None:
    with sqlite3.connect(":memory:") as conn:
        with pytest.raises(sqlite3.OperationalError):
            _record_cost_strict(
                conn,
                "claude",
                "chat",
                100,
                idempotency_key="strict_missing_table",
            )


def test_haiku_45_pricing_matches_anthropic_table() -> None:
    assert estimate_ai_cost_usd6(
        "claude",
        model="claude-haiku-4-5-20251001",
        input_tokens=1_000_000,
        output_tokens=1_000_000,
        cache_creation_tokens=1_000_000,
        cache_read_tokens=1_000_000,
    ) == 7_350_000


def test_check_cost_limit_returns_true_when_under_limit(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cost_tracking, "_fire_cost_alert", lambda *_args, **_kwargs: None)

    allowed, reason = check_cost_limit(db_path, "claude")

    assert allowed is True
    assert reason is None


def test_check_cost_limit_returns_false_when_block_limit_exceeded(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cost_tracking, "_fire_cost_alert", lambda *_args, **_kwargs: None)

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'daily'
            """
        )
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('claude', 'chat', 1500000, datetime('now'))
            """
        )
        conn.commit()

    allowed, reason = check_cost_limit(db_path, "claude")

    assert allowed is False
    assert reason is not None
    assert "claude daily limit reached" in reason


def test_check_cost_limit_evaluates_all_limits(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    fired: list[tuple[str, str, int, int, str]] = []

    monkeypatch.setattr(
        cost_tracking,
        "_fire_cost_alert",
        lambda db_path, provider, period, spent_usd6, limit_usd6, source="api": fired.append(
            (provider, period, spent_usd6, limit_usd6, source)
        ),
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'daily'
            """
        )
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'block'
             WHERE provider = 'all'
               AND period = 'monthly'
            """
        )
        conn.execute(
            """
            INSERT INTO cost_limits (provider, period, limit_usd6, action)
            VALUES ('all', 'daily', 1000000, 'warn')
            """
        )
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('claude', 'chat', 1500000, datetime('now'))
            """
        )
        conn.commit()

    allowed, reason = check_cost_limit(db_path, "claude", source="telegram")

    assert allowed is False
    assert reason == "claude daily limit reached ($1.50 / $1.00)"
    assert fired == [
        ("claude", "daily", 1500000, 1000000, "telegram"),
        ("claude", "monthly", 1500000, 1000000, "telegram"),
        ("all", "daily", 1500000, 1000000, "telegram"),
        ("all", "monthly", 1500000, 1000000, "telegram"),
    ]


def test_check_cost_limit_uses_nullable_user_and_system_limits(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cost_tracking, "_fire_cost_alert", lambda *_args, **_kwargs: None)

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = NULL,
                   system_limit_usd6 = 5000000,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()
    allowed, reason = check_cost_limit(db_path, "claude", projected_cost_usd6=5_000_000)
    assert allowed is False
    assert reason == "claude monthly limit reached ($5.00 / $5.00)"

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 3000000,
                   system_limit_usd6 = NULL,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()
    allowed, reason = check_cost_limit(db_path, "claude", projected_cost_usd6=3_000_000)
    assert allowed is False
    assert reason == "claude monthly limit reached ($3.00 / $3.00)"

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = NULL,
                   system_limit_usd6 = NULL,
                   action = 'block'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()
    allowed, reason = check_cost_limit(db_path, "claude", projected_cost_usd6=100_000_000)
    assert allowed is True
    assert reason is None


def test_check_cost_limit_fires_alert_on_warn_limit(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    sent: list[tuple[str, str]] = []

    monkeypatch.setattr(cost_tracking, "get_db_path", lambda: db_path)
    monkeypatch.setattr(cost_tracking, "_HAS_ALERTS", True)
    monkeypatch.setattr(
        cost_tracking,
        "alerts",
        SimpleNamespace(send=lambda body, channel="telegram", **kw: sent.append((body, channel))),
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'daily'
            """
        )
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('claude', 'chat', 1000000, datetime('now'))
            """
        )
        conn.commit()

    allowed, reason = check_cost_limit(db_path, "claude", source="telegram")

    assert allowed is True
    assert reason is None
    assert len(sent) == 1
    assert sent[0][1] == "telegram"

    with connect(db_path) as conn:
        alert_row = conn.execute(
            "SELECT provider, period, threshold FROM cost_alert_log"
        ).fetchone()
        analytics_row = conn.execute(
            "SELECT event, source, properties FROM analytics_events WHERE event = 'cost.limit_warning'"
        ).fetchone()

    assert alert_row is not None
    assert alert_row["provider"] == "claude"
    assert alert_row["period"] == "daily"
    assert alert_row["threshold"] == "100pct"
    assert analytics_row is not None
    assert analytics_row["event"] == "cost.limit_warning"
    assert analytics_row["source"] == "telegram"


def test_period_bucket_returns_correct_format(monkeypatch) -> None:
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 13, 14, 30, 0, tzinfo=tz or timezone.utc)

    monkeypatch.setattr(cost_tracking, "datetime", _FixedDateTime)

    assert _period_bucket("daily") == "2026-03-13"
    assert _period_bucket("monthly") == "2026-03"


def test_cost_alert_idempotency_same_period_bucket_only_sends_once(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    sent: list[str] = []

    monkeypatch.setattr(cost_tracking, "get_db_path", lambda: db_path)
    monkeypatch.setattr(cost_tracking, "_HAS_ALERTS", True)
    monkeypatch.setattr(
        cost_tracking,
        "alerts",
        SimpleNamespace(send=lambda body, channel="telegram", **kw: sent.append(body)),
    )

    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 1000000,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'daily'
            """
        )
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('claude', 'chat', 1000000, datetime('now'))
            """
        )
        conn.commit()

    assert check_cost_limit(db_path, "claude")[0] is True
    assert check_cost_limit(db_path, "claude")[0] is True

    with connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM cost_alert_log").fetchone()

    assert sent and len(sent) == 1
    assert row is not None
    assert row["count"] == 1


def test_prune_cost_ledger_deletes_old_records(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('claude', 'chat', 100, datetime('now', '-400 days'))
            """
        )
        conn.execute(
            """
            INSERT INTO cost_ledger (provider, operation, cost_usd6, created_at)
            VALUES ('openai', 'categorize', 200, datetime('now'))
            """
        )
        prune_cost_ledger(conn, retention_days=365)
        rows = conn.execute(
            "SELECT provider FROM cost_ledger ORDER BY provider"
        ).fetchall()

    assert [row["provider"] for row in rows] == ["openai"]


def _set_monthly_cap_and_balance(
    db_path: Path,
    *,
    system_limit_usd6: int | None,
    credit_balance_usd6: int = 0,
) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = NULL,
                   system_limit_usd6 = ?,
                   action = 'warn'
             WHERE provider = 'claude'
               AND period = 'monthly'
            """,
            (system_limit_usd6,),
        )
        conn.execute(
            """
            UPDATE credit_balance
               SET balance_usd6 = ?,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (credit_balance_usd6,),
        )
        conn.commit()


def test_record_and_settle_cost_attribution_invariant(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    _set_monthly_cap_and_balance(db_path, system_limit_usd6=1_000, credit_balance_usd6=500)

    result = record_and_settle_cost(db_path, "claude", "chat", 2_000, "settle_1")

    assert result.status == "settled"
    assert result.allowance_debited + result.credits_debited + result.overflow_unattributed == 2_000
    assert result.allowance_debited == 1_000
    assert result.credits_debited == 500
    assert result.overflow_unattributed == 500

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT allowance_debit_usd6, credits_debit_usd6, overflow_unattributed_usd6
            FROM cost_ledger
            WHERE id = ?
            """,
            (result.ledger_id,),
        ).fetchone()
    assert row["allowance_debit_usd6"] + row["credits_debit_usd6"] + row["overflow_unattributed_usd6"] == 2_000


def test_record_and_settle_cost_replay_no_op(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    first = record_and_settle_cost(db_path, "claude", "chat", 123, "settle_replay")
    second = record_and_settle_cost(db_path, "claude", "chat", 456, "settle_replay")

    assert first.status == "settled"
    assert second.status == "replay_no_op"
    assert second.ledger_id is None
    with connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) AS count, SUM(cost_usd6) AS cost FROM cost_ledger").fetchone()
    assert row["count"] == 1
    assert row["cost"] == 123


def test_record_and_settle_cost_byok_short_circuit(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    _set_monthly_cap_and_balance(db_path, system_limit_usd6=0, credit_balance_usd6=1_000)

    result = record_and_settle_cost(
        db_path,
        "claude",
        "chat",
        800,
        "settle_byok",
        is_byok=True,
    )

    assert result == cost_tracking.SettlementResult("settled", result.ledger_id, 0, 0, 0)
    with connect(db_path) as conn:
        ledger = conn.execute(
            """
            SELECT is_byok, allowance_debit_usd6, credits_debit_usd6, overflow_unattributed_usd6
            FROM cost_ledger
            WHERE id = ?
            """,
            (result.ledger_id,),
        ).fetchone()
        credits = conn.execute("SELECT COUNT(*) AS count FROM credit_ledger").fetchone()
        balance = conn.execute("SELECT balance_usd6 FROM credit_balance WHERE id = 1").fetchone()

    assert ledger["is_byok"] == 1
    assert ledger["allowance_debit_usd6"] == 0
    assert ledger["credits_debit_usd6"] == 0
    assert ledger["overflow_unattributed_usd6"] == 0
    assert credits["count"] == 0
    assert balance["balance_usd6"] == 1_000


def test_record_and_settle_cost_concurrent_writers_do_not_overdraw_credits(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    _set_monthly_cap_and_balance(db_path, system_limit_usd6=0, credit_balance_usd6=1_000)
    results: list[cost_tracking.SettlementResult] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def _worker(index: int) -> None:
        try:
            result = record_and_settle_cost(
                db_path,
                "claude",
                "chat",
                800,
                f"settle_thread_{index}",
            )
            with lock:
                results.append(result)
        except Exception as exc:  # pragma: no cover - failure path reported by assertion
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(results) == 2
    with connect(db_path) as conn:
        balance = conn.execute("SELECT balance_usd6 FROM credit_balance WHERE id = 1").fetchone()
        credits = conn.execute(
            "SELECT COALESCE(SUM(credits_debit_usd6), 0) AS total FROM cost_ledger"
        ).fetchone()

    assert balance["balance_usd6"] == 0
    assert credits["total"] == 1_000
    assert all(result.credits_debited >= 0 for result in results)


def test_default_limits_are_seeded_by_migration(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT provider, period, limit_usd6, action
            FROM cost_limits
            ORDER BY provider, period
            """
        ).fetchall()

    assert [(row["provider"], row["period"], row["limit_usd6"], row["action"]) for row in rows] == [
        ("all", "monthly", 100_000_000, "warn"),
        ("claude", "daily", 5_000_000, "warn"),
        ("claude", "monthly", 50_000_000, "warn"),
        ("openai", "monthly", 20_000_000, "warn"),
        ("plaid", "daily", 1_000_000, "warn"),
        ("plaid", "monthly", 10_000_000, "warn"),
    ]


def test_cost_management_phase2_trigger_recreation_matches_change_feed_shape(tmp_path: Path) -> None:
    db_path = _init_db(tmp_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name LIKE '_sync_log_cost_limits_%'
            ORDER BY name
            """
        ).fetchall()

    trigger_sql = {row["name"]: row["sql"] for row in rows}
    assert set(trigger_sql) == {
        "_sync_log_cost_limits_delete",
        "_sync_log_cost_limits_insert",
        "_sync_log_cost_limits_update",
    }
    for sql in trigger_sql.values():
        assert "WHEN current_session_id() != '__STREAM__'" in sql
        assert "INSERT INTO _sync_changelog (table_name, op, pk_json, old_json, new_json, origin_session_id)" in sql
        assert "json(json_object" in sql
        assert "system_limit_usd6" in sql
    assert "'INSERT'" in trigger_sql["_sync_log_cost_limits_insert"]
    assert "NULL" in trigger_sql["_sync_log_cost_limits_insert"]
    assert "'DELETE'" in trigger_sql["_sync_log_cost_limits_delete"]
    assert "NULL" in trigger_sql["_sync_log_cost_limits_delete"]
