from __future__ import annotations

import json
import os
import site
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _subprocess_env(tmpdir: str) -> dict[str, str]:
    env = os.environ.copy()
    env["HOME"] = tmpdir
    env["FINANCE_CLI_DISABLE_DOTENV"] = "1"
    pythonpath_parts = [site.getusersitepackages()]
    if env.get("PYTHONPATH"):
        pythonpath_parts.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    return env


def test_configure_local_sets_runtime_contextvars_and_middleware() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _subprocess_env(tmpdir)
        env.pop("FINANCE_CLI_DATA_DIR", None)
        env.pop("FINANCE_CLI_DB", None)
        env.pop("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", None)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    import json
                    import finance_cli.mcp_local as mcp_local
                    from finance_cli.config import get_data_dir, get_db_path
                    from finance_cli.db import db_encryption_mode
                    from finance_cli.user_context import get_user_context

                    middleware = mcp_local.configure_local_mcp()
                    ctx = get_user_context()
                    payload = {
                        "env_data_dir_present": "FINANCE_CLI_DATA_DIR" in mcp_local.os.environ,
                        "env_encryption_present": "FINANCE_CLI_REQUIRE_DB_ENCRYPTION" in mcp_local.os.environ,
                        "runtime_data_dir": str(get_data_dir()),
                        "runtime_db_path": str(get_db_path()),
                        "runtime_encryption": db_encryption_mode(),
                        "db_path": ctx.db_path if ctx else None,
                        "rules_path": ctx.rules_path if ctx else None,
                        "uploads_dir": ctx.uploads_dir if ctx else None,
                        "local_mode": ctx.local_mode if ctx else None,
                        "middleware_type": type(middleware).__name__,
                        "middleware_names": [type(item).__name__ for item in mcp_local.mcp.middleware[:5]],
                    }
                    print(json.dumps(payload))
                    """
                ),
            ],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    expected_root = str(Path(tmpdir) / ".cashnerd" / "data")
    resolved_root = Path(expected_root).resolve()

    assert payload["env_data_dir_present"] is False
    assert payload["env_encryption_present"] is False
    assert payload["runtime_data_dir"] == str(resolved_root)
    assert payload["runtime_db_path"] == str(resolved_root / "finance.db")
    assert payload["runtime_encryption"] == "off"
    assert payload["db_path"] == str(resolved_root / "finance.db")
    assert payload["rules_path"] == str(resolved_root / "rules.yaml")
    assert payload["uploads_dir"] == str(resolved_root / "uploads")
    assert payload["local_mode"] is True
    assert payload["middleware_type"] == "SyncMiddleware"
    assert payload["middleware_names"] == [
        "SyncMiddleware",
        "DereferenceRefsMiddleware",
        "UserContextMiddleware",
        "OperationLogMiddleware",
        "PathSanitizeMiddleware",
    ]


def test_configure_local_runtime_overrides_take_precedence_without_mutating_env() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _subprocess_env(tmpdir)
        env["FINANCE_CLI_DATA_DIR"] = str(Path(tmpdir) / "wrong")
        env["FINANCE_CLI_REQUIRE_DB_ENCRYPTION"] = "require"

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    import json

                    import finance_cli.mcp_local as mcp_local
                    from finance_cli.config import get_data_dir, get_db_path
                    from finance_cli.db import db_encryption_mode

                    mcp_local.configure_local_mcp()
                    payload = {
                        "env_data_dir": mcp_local.os.environ.get("FINANCE_CLI_DATA_DIR"),
                        "env_encryption": mcp_local.os.environ.get("FINANCE_CLI_REQUIRE_DB_ENCRYPTION"),
                        "runtime_data_dir": str(get_data_dir()),
                        "runtime_db_path": str(get_db_path()),
                        "runtime_encryption": db_encryption_mode(),
                    }
                    print(json.dumps(payload))
                    """
                ),
            ],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    expected_root = Path(tmpdir) / ".cashnerd" / "data"

    assert payload["env_data_dir"] == str(Path(tmpdir) / "wrong")
    assert payload["env_encryption"] == "require"
    assert payload["runtime_data_dir"] == str(expected_root.resolve())
    assert payload["runtime_db_path"] == str(expected_root.resolve() / "finance.db")
    assert payload["runtime_encryption"] == "off"


def test_configure_local_reconciles_sessions_backup_and_loads_install_id() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _subprocess_env(tmpdir)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    import json
                    from pathlib import Path

                    from finance_cli.db import _install_id_var, connect, initialize_database
                    from finance_cli.sync import config as sync_config

                    sync_config.ensure_dirs()
                    initialize_database(sync_config.CASHNERD_DB_PATH)
                    with connect(sync_config.CASHNERD_DB_PATH) as conn:
                        conn.execute(
                            "UPDATE sync_state SET install_id = 'install-xyz' WHERE id = 0"
                        )
                        conn.commit()

                    backup = sync_config.CASHNERD_DATA_DIR / "sessions.old"
                    backup.mkdir(parents=True, exist_ok=True)
                    (backup / "2026-04-16.md").write_text("restored\\n", encoding="utf-8")

                    import finance_cli.mcp_local as mcp_local

                    mcp_local.configure_local_mcp()
                    payload = {
                        "install_id": _install_id_var.get(),
                        "restored": (sync_config.CASHNERD_DATA_DIR / "sessions" / "2026-04-16.md").exists(),
                        "backup_exists": backup.exists(),
                    }
                    print(json.dumps(payload))
                    """
                ),
            ],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["install_id"] == "install-xyz"
    assert payload["restored"] is True
    assert payload["backup_exists"] is False


def test_subscriber_lock_poller_retries_until_lock_acquired() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _subprocess_env(tmpdir)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    import asyncio
                    import json
                    from contextlib import suppress

                    import finance_cli.mcp_local as mcp_local

                    class Engine:
                        def __init__(self):
                            self.try_calls = 0
                            self.start_calls = 0
                            self.acquired = asyncio.Event()

                        def try_acquire_install_subscriber_lock(self):
                            self.try_calls += 1
                            return self.try_calls >= 2

                        def start_subscriber(self):
                            self.start_calls += 1
                            self.acquired.set()

                    async def main():
                        engine = Engine()
                        task = asyncio.create_task(
                            mcp_local._poll_install_subscriber_lock(engine, interval=0.01)
                        )
                        await asyncio.wait_for(engine.acquired.wait(), timeout=1.0)
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task
                        print(json.dumps({
                            "try_calls": engine.try_calls,
                            "start_calls": engine.start_calls,
                            "first_try_failed": engine.try_calls >= 2,
                        }))

                    asyncio.run(main())
                    """
                ),
            ],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["first_try_failed"] is True
    assert payload["try_calls"] >= 2
    assert payload["start_calls"] >= 1


def test_sync_lifespan_stops_subscriber_lock_poller_on_exit() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _subprocess_env(tmpdir)

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    import asyncio
                    import json
                    from contextlib import asynccontextmanager

                    import finance_cli.mcp_local as mcp_local

                    @asynccontextmanager
                    async def base_lifespan(_server):
                        yield

                    class Engine:
                        def __init__(self):
                            self.try_calls = 0
                            self.start_calls = 0
                            self.stop_calls = 0

                        def try_acquire_install_subscriber_lock(self):
                            self.try_calls += 1
                            return True

                        def start_subscriber(self):
                            self.start_calls += 1

                        async def stop_subscriber(self):
                            self.stop_calls += 1

                    async def main():
                        engine = Engine()
                        mcp_local._SYNC_LIFESPAN_INSTALLED = False
                        mcp_local._SUBSCRIBER_LOCK_POLL_SECONDS = 0.01
                        mcp_local.mcp._lifespan = base_lifespan
                        mcp_local._install_sync_lifespan(engine)

                        async with mcp_local.mcp._lifespan(object()):
                            await asyncio.sleep(0.035)

                        try_calls_at_exit = engine.try_calls
                        await asyncio.sleep(0.035)
                        print(json.dumps({
                            "try_calls_at_exit": try_calls_at_exit,
                            "try_calls_after_wait": engine.try_calls,
                            "start_calls": engine.start_calls,
                            "stop_calls": engine.stop_calls,
                        }))

                    asyncio.run(main())
                    """
                ),
            ],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["try_calls_at_exit"] >= 1
    assert payload["try_calls_after_wait"] == payload["try_calls_at_exit"]
    assert payload["start_calls"] >= 1
    assert payload["stop_calls"] == 1
