from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import textwrap
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.sync import config as sync_config
from finance_cli.sync.exceptions import SyncDegradedError
from finance_cli.sync.subscriber import ChangeFeedSubscriber
from finance_cli.sync.subscriber_lock import InstallSubscriberLock

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _patch_subscriber_paths(monkeypatch, base_dir: Path) -> None:
    import finance_cli.sync.subscriber as subscriber_module

    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(sync_config, "CASHNERD_DB_PATH", base_dir / "data" / "finance.db")
    monkeypatch.setattr(subscriber_module, "CASHNERD_DATA_DIR", sync_config.CASHNERD_DATA_DIR)
    monkeypatch.setattr(subscriber_module, "CASHNERD_DB_PATH", sync_config.CASHNERD_DB_PATH)


def _payload(*, op_id: int, op: str, origin: str = "", **kwargs: Any) -> dict[str, Any]:
    payload = {
        "id": op_id,
        "table": "transactions",
        "op": op,
        "pk_json": json.dumps({"id": "txn-1"}),
        "old_json": None,
        "new_json": None,
        "origin_session_id": origin,
    }
    payload.update(kwargs)
    return payload


class FakeEngine:
    def __init__(self, db_path: Path, *, install_id: str = "install-123") -> None:
        self._db_path = db_path
        self.install_id = install_id
        self.server_url = "https://cashnerd.example"
        self.schema_version = 59
        self.refresh_calls = 0
        self.degraded_marks = 0
        self.release_calls = 0
        self.sidecar_content: dict[tuple[str, str], bytes | None] = {}
        self.stream_attempts = 0
        self.stream_behaviors: list[Any] = []
        self.stream_requests: list[dict[str, Any]] = []

    @property
    def last_applied_op_id(self) -> int:
        with sqlite3.connect(str(self._db_path)) as conn:
            row = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
        return int(row[0] or 0)

    @property
    def reset_epoch(self) -> str:
        with sqlite3.connect(str(self._db_path)) as conn:
            row = conn.execute("SELECT reset_epoch FROM sync_reset_state WHERE id = 0").fetchone()
        return str((row[0] if row else "") or "")

    async def get_sync_token(self) -> str:
        return "sync-token"

    async def refresh_credentials(self) -> None:
        self.refresh_calls += 1

    def bump_last_applied(self, op_id: int) -> None:
        with connect(self._db_path) as conn:
            conn.execute("UPDATE sync_state SET last_applied_op_id = ? WHERE id = 0", (op_id,))
            conn.commit()

    def mark_subscriber_degraded(self) -> None:
        self.degraded_marks += 1
        with connect(self._db_path) as conn:
            conn.execute("UPDATE sync_state SET subscriber_status = 'degraded' WHERE id = 0")
            conn.commit()

    def release_install_subscriber_lock(self) -> None:
        self.release_calls += 1

    async def fetch_sidecar_content(self, key: str, sha256: str) -> bytes | None:
        return self.sidecar_content[(key, sha256)]

    @asynccontextmanager
    async def stream_request(self, _method: str, _url: str, **_kwargs: Any):
        self.stream_attempts += 1
        self.stream_requests.append(dict(_kwargs))
        behavior = self.stream_behaviors.pop(0)
        if isinstance(behavior, Exception):
            raise behavior
        yield behavior


def _init_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(path)


def _wait_for_path(path: Path, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    pytest.fail(f"Timed out waiting for {path}")


def _wait_for_path_or_process_exit(
    path: Path,
    process: subprocess.Popen[str],
    *,
    timeout: float = 20.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            detail = stderr or stdout or f"process exited with code {process.returncode}"
            pytest.fail(f"Process exited before writing {path}: {detail}")
        time.sleep(0.05)
    pytest.fail(f"Timed out waiting for {path} while process was still running")


def test_subscriber_applies_insert_update_delete_and_updates_cursor(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(engine)

    asyncio.run(
        subscriber._apply_op(
            _payload(
                op_id=1,
                op="INSERT",
                new_json=json.dumps(
                    {
                        "id": "txn-1",
                        "date": "2026-04-16",
                        "description": "Inserted",
                        "amount_cents": -100,
                        "source": "manual",
                        "is_active": 1,
                    }
                ),
            )
        )
    )
    asyncio.run(
        subscriber._apply_op(
            _payload(
                op_id=2,
                op="UPDATE",
                new_json=json.dumps(
                    {
                        "id": "txn-1",
                        "date": "2026-04-16",
                        "description": "Updated",
                        "amount_cents": -125,
                        "source": "manual",
                        "is_active": 1,
                    }
                ),
            )
        )
    )
    asyncio.run(subscriber._apply_op(_payload(op_id=3, op="DELETE")))

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute("SELECT id FROM transactions WHERE id = 'txn-1'").fetchone()
        cursor = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
    assert row is None
    assert cursor[0] == 3


def test_subscriber_applies_downstream_only_cost_ledger_without_relogging(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(engine)

    asyncio.run(
        subscriber._apply_op(
            _payload(
                op_id=10,
                op="INSERT",
                table="cost_ledger",
                pk_json=json.dumps({"id": 42}),
                new_json=json.dumps(
                    {
                        "id": 42,
                        "provider": "plaid",
                        "operation": "transactions_sync",
                        "cost_usd6": 123,
                        "request_id": "req-cost-42",
                        "is_estimated": 0,
                        "is_byok": 0,
                        "allowance_debit_usd6": 0,
                        "credits_debit_usd6": 0,
                        "overflow_unattributed_usd6": 123,
                    }
                ),
            )
        )
    )

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT provider, operation, cost_usd6, overflow_unattributed_usd6
              FROM cost_ledger
             WHERE id = 42
            """
        ).fetchone()
        cursor = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
        relogged = conn.execute(
            "SELECT COUNT(*) FROM _sync_changelog WHERE table_name = 'cost_ledger'"
        ).fetchone()

    assert dict(row) == {
        "provider": "plaid",
        "operation": "transactions_sync",
        "cost_usd6": 123,
        "overflow_unattributed_usd6": 123,
    }
    assert cursor[0] == 10
    assert relogged[0] == 0


def test_subscriber_echo_filter_skips_apply_but_bumps_cursor(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH, install_id="install-echo")
    subscriber = ChangeFeedSubscriber(engine)

    asyncio.run(
        subscriber._dispatch_event(
            "op",
            [
                json.dumps(
                    _payload(
                        op_id=9,
                        op="INSERT",
                        origin="install-echo",
                        new_json=json.dumps(
                            {
                                "id": "txn-echo",
                                "date": "2026-04-16",
                                "description": "Echo",
                                "amount_cents": -10,
                                "source": "manual",
                                "is_active": 1,
                            }
                        ),
                    )
                )
            ],
        )
    )

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute("SELECT id FROM transactions WHERE id = 'txn-echo'").fetchone()
        cursor = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
    assert row is None
    assert cursor[0] == 9


def test_dispatch_event_rejects_empty_install_id(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH, install_id="")
    subscriber = ChangeFeedSubscriber(engine)

    with pytest.raises(
        SyncDegradedError,
        match="install_id is empty during op dispatch; echo filter is unsafe",
    ):
        asyncio.run(
            subscriber._dispatch_event(
                "op",
                [
                    json.dumps(
                        _payload(
                            op_id=1,
                            op="INSERT",
                            new_json=json.dumps(
                                {
                                    "id": "txn-empty-install",
                                    "date": "2026-04-16",
                                    "description": "Should not dispatch",
                                    "amount_cents": -10,
                                    "source": "manual",
                                    "is_active": 1,
                                }
                            ),
                        )
                    )
                ],
            )
        )


def test_stream_apply_uses_stream_session_and_does_not_relog(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(engine)

    asyncio.run(
        subscriber._apply_op(
            _payload(
                op_id=1,
                op="INSERT",
                new_json=json.dumps(
                    {
                        "id": "txn-stream",
                        "date": "2026-04-16",
                        "description": "Stream Apply",
                        "amount_cents": -100,
                        "source": "manual",
                        "is_active": 1,
                    }
                ),
            )
        )
    )

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) FROM _sync_changelog").fetchone()
    assert row[0] == 0


def test_sync_reset_op_no_longer_special_cased(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(engine)

    with pytest.raises(SyncDegradedError, match=r"unknown op type 'SYNC_RESET'"):
        asyncio.run(
            subscriber._dispatch_event(
                "op",
                [json.dumps(_payload(op_id=1, op="sync_reset"))],
            )
        )


def test_subscriber_reconnect_backoff_on_http_error(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    response = httpx.Response(200, content=b"")
    engine.stream_behaviors = [httpx.ConnectError("boom"), response]
    subscriber = ChangeFeedSubscriber(engine)
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        subscriber._stop.set()

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    asyncio.run(subscriber._run())

    assert sleeps == [1.0]
    assert engine.release_calls == 0


def test_subscriber_sends_reset_epoch_header(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    engine.stream_behaviors = [
        httpx.Response(
            200,
            content=b"",
            request=httpx.Request("GET", "https://cashnerd.example/api/sync/subscribe"),
        )
    ]
    subscriber = ChangeFeedSubscriber(engine)

    asyncio.run(subscriber._subscribe_once())

    assert engine.stream_requests[0]["headers"]["X-CashNerd-Reset-Epoch"] == engine.reset_epoch


def test_subscriber_reset_required_error_degrades(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(FakeEngine(sync_config.CASHNERD_DB_PATH))

    with pytest.raises(SyncDegradedError, match="subscriber error: reset_required"):
        asyncio.run(
            subscriber._dispatch_event(
                "error",
                [json.dumps({"code": "reset_required", "server_reset_epoch": "server-epoch"})],
            )
        )


def test_subscriber_keeps_lock_on_degraded_exit(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _init_db(db_path)
    engine = FakeEngine(db_path)
    subscriber = ChangeFeedSubscriber(engine)

    async def fake_subscribe_once() -> None:
        raise SyncDegradedError("degraded")

    setattr(subscriber, "_subscribe_once", fake_subscribe_once)
    asyncio.run(subscriber._run())

    assert engine.degraded_marks == 1
    assert engine.release_calls == 0


def test_subscriber_keeps_lock_on_stop_event(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _init_db(db_path)
    engine = FakeEngine(db_path)
    subscriber = ChangeFeedSubscriber(engine)
    started = asyncio.Event()
    allow_exit = asyncio.Event()

    async def fake_subscribe_once() -> None:
        started.set()
        await allow_exit.wait()

    setattr(subscriber, "_subscribe_once", fake_subscribe_once)

    async def run_test() -> None:
        task = asyncio.create_task(subscriber._run())
        await started.wait()
        subscriber._stop.set()
        allow_exit.set()
        await task

    asyncio.run(run_test())

    assert engine.release_calls == 0


def test_subscriber_keeps_lock_on_cancellation(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _init_db(db_path)
    engine = FakeEngine(db_path)
    subscriber = ChangeFeedSubscriber(engine)
    started = asyncio.Event()
    blocker = asyncio.Event()

    async def fake_subscribe_once() -> None:
        started.set()
        await blocker.wait()

    setattr(subscriber, "_subscribe_once", fake_subscribe_once)

    async def run_test() -> None:
        task = asyncio.create_task(subscriber._run())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run_test())

    assert engine.release_calls == 0


def test_subscriber_keeps_lock_on_unhandled_exception(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    _init_db(db_path)
    engine = FakeEngine(db_path)
    subscriber = ChangeFeedSubscriber(engine)

    async def fake_subscribe_once() -> None:
        raise RuntimeError("boom")

    setattr(subscriber, "_subscribe_once", fake_subscribe_once)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(subscriber._run())

    assert engine.release_calls == 0


def test_install_lock_survives_subscriber_task_exit_real_fcntl(tmp_path: Path) -> None:
    lock_path = tmp_path / "subscriber.lock"
    ready_path = tmp_path / "ready.txt"
    stop_path = tmp_path / "stop.txt"
    child_script = textwrap.dedent(
        """\
        import asyncio
        import sys
        from pathlib import Path

        from finance_cli.sync.subscriber import ChangeFeedSubscriber
        from finance_cli.sync.subscriber_lock import InstallSubscriberLock

        lock_path = Path(sys.argv[1])
        ready_path = Path(sys.argv[2])
        stop_path = Path(sys.argv[3])

        class Engine:
            def __init__(self, lock):
                self._lock = lock
                self.release_calls = 0

            async def refresh_credentials(self):
                return None

            def mark_subscriber_degraded(self):
                return None

            def release_install_subscriber_lock(self):
                self.release_calls += 1
                self._lock.release()

        async def main():
            lock = InstallSubscriberLock(lock_path)
            assert lock.try_acquire() is True

            engine = Engine(lock)
            subscriber = ChangeFeedSubscriber(engine)
            started = asyncio.Event()
            allow_exit = asyncio.Event()

            async def fake_subscribe_once():
                started.set()
                await allow_exit.wait()
                subscriber._stop.set()

            subscriber._subscribe_once = fake_subscribe_once
            task = asyncio.create_task(subscriber._run())
            await started.wait()
            allow_exit.set()
            await task
            ready_path.write_text(str(engine.release_calls), encoding="utf-8")

            while not stop_path.exists():
                await asyncio.sleep(0.05)

        asyncio.run(main())
        """
    )
    process = subprocess.Popen(
        [sys.executable, "-c", child_script, str(lock_path), str(ready_path), str(stop_path)],
        cwd=_PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        _wait_for_path_or_process_exit(ready_path, process)
        assert process.poll() is None
        assert ready_path.read_text(encoding="utf-8") == "0"

        competing_lock = InstallSubscriberLock(lock_path)
        try:
            assert competing_lock.try_acquire() is False
        finally:
            if competing_lock.is_held:
                competing_lock.release()

        stop_path.write_text("stop", encoding="utf-8")
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr or stdout

        post_exit_lock = InstallSubscriberLock(lock_path)
        assert post_exit_lock.try_acquire() is True
        post_exit_lock.release()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)


def test_subscriber_keeps_cursor_atomic_on_apply_failure(monkeypatch, tmp_path: Path) -> None:
    _patch_subscriber_paths(monkeypatch, tmp_path / ".cashnerd")
    _init_db(sync_config.CASHNERD_DB_PATH)
    engine = FakeEngine(sync_config.CASHNERD_DB_PATH)
    subscriber = ChangeFeedSubscriber(engine)

    with pytest.raises(json.JSONDecodeError):
        asyncio.run(
            subscriber._apply_op(
                _payload(
                    op_id=5,
                    op="INSERT",
                    new_json="{not-json",
                )
            )
        )

    with connect(sync_config.CASHNERD_DB_PATH) as conn:
        cursor = conn.execute("SELECT last_applied_op_id FROM sync_state WHERE id = 0").fetchone()
        row = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
    assert cursor[0] == 0
    assert row[0] == 0
