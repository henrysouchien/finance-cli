from __future__ import annotations

import asyncio
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import mcp.types as mt
import pytest
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools.tool import ToolResult

import finance_cli.mcp_server as mcp_server
from agent_gateway.tool_dispatcher import ToolDispatcher
from finance_cli import db as db_module
from finance_cli.commands import memory_cmd
from finance_cli.db import _connected_main_db_path, connect, initialize_database
from finance_cli.gateway.server import UserScopedDispatcher
from finance_cli.storage_lease import LocalLease, LeaseScope, current_lease_scope
from finance_cli.user_context import (
    UserContext,
    get_user_context,
    reset_user_context,
    set_user_context,
)
from finance_cli.user_provisioning import provision_user, user_db_path, user_rules_path


@pytest.fixture()
def global_db(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "global" / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


@pytest.fixture()
def workspace_factory(tmp_path: Path) -> Iterator[callable]:
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")
    data_root = tmp_path / "users"

    def _create(user_id: str) -> dict[str, Path]:
        provision_user(
            data_root=data_root,
            user_id=user_id,
            template_rules_path=template_rules,
        )
        db_path = user_db_path(data_root, user_id)
        return {
            "data_dir": db_path.parent,
            "db_path": db_path,
            "rules_path": user_rules_path(data_root, user_id),
            "uploads_dir": db_path.parent / "uploads",
        }

    return _create


@contextmanager
def _user_context(
    *,
    db_path: Path | None = None,
    rules_path: Path | None = None,
    uploads_dir: Path | None = None,
    expected_user_id: str | None = None,
    local_mode: bool = False,
):
    token_user_context = (
        set_user_context(
            UserContext.from_paths(
                db_path=db_path,
                expected_user_id=expected_user_id,
                rules_path=rules_path,
                uploads_dir=uploads_dir,
                local_mode=local_mode,
            )
        )
        if db_path is not None
        else None
    )
    try:
        yield
    finally:
        if token_user_context is not None:
            reset_user_context(token_user_context)


def test_get_conn_returns_default_connection_without_user_context(
    global_db: Path,
) -> None:
    with mcp_server._get_conn() as conn:
        assert _connected_main_db_path(conn) == global_db.resolve()
        assert int(conn.execute("PRAGMA busy_timeout").fetchone()[0]) == 5000


def test_get_conn_returns_user_scoped_connection_with_context(
    global_db: Path,
    workspace_factory,
) -> None:
    user = workspace_factory("alice")

    with _user_context(db_path=user["db_path"]):
        with mcp_server._get_conn() as conn:
            assert _connected_main_db_path(conn) == user["db_path"].resolve()
            assert int(conn.execute("PRAGMA busy_timeout").fetchone()[0]) == 5000


def test_setup_init_gateway_no_path_leakage(global_db: Path, workspace_factory) -> None:
    del global_db
    user = workspace_factory("alice")

    with _user_context(db_path=user["db_path"]):
        result = mcp_server.setup_init(dry_run=False)

    assert result["data"]["env_template"]["path"] is None
    assert result["data"]["rules_file"]["path"] is None


def test_get_rules_path_returns_none_without_user_context(global_db: Path) -> None:
    del global_db

    assert mcp_server._get_rules_path() is None


def test_get_rules_path_returns_user_scoped_rules_path(
    global_db: Path, workspace_factory
) -> None:
    user = workspace_factory("alice")

    with _user_context(db_path=user["db_path"], rules_path=user["rules_path"]):
        assert mcp_server._get_rules_path() == user["rules_path"].resolve()


def test_db_status_reads_directly_set_user_context(
    global_db: Path, workspace_factory
) -> None:
    del global_db
    user = workspace_factory("alice")

    with connect(user["db_path"]) as conn:
        conn.execute(
            """
            INSERT INTO transactions
                (id, date, description, amount_cents, category_id, is_active,
                 is_reviewed, source, account_id, use_type)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                "txn-user-context-db-status",
                "2026-02-15",
                "Context-only txn",
                -1000,
                None,
                0,
                "manual",
                None,
                None,
            ),
        )
        conn.commit()

    ctx = UserContext.from_paths(
        db_path=user["db_path"],
        expected_user_id=None,
        rules_path=user["rules_path"],
        uploads_dir=user["uploads_dir"],
        local_mode=True,
    )
    token = set_user_context(ctx)
    try:
        assert get_user_context() == ctx
        result = mcp_server.db_status()
    finally:
        reset_user_context(token)

    assert result["data"]["transaction_counts"]["active"] == 1
    assert result["data"]["unreviewed_count"] == 1
    assert get_user_context() is None


def test_middleware_sets_context_and_strips_user_args(
    global_db: Path, workspace_factory
) -> None:
    user = workspace_factory("alice")
    seen: dict[str, object] = {}

    async def call_next(context):
        user_context = get_user_context()
        seen["arguments"] = context.message.arguments
        seen["db_path"] = user_context.db_path if user_context else None
        seen["rules_path"] = user_context.rules_path if user_context else None
        seen["uploads_dir"] = user_context.uploads_dir if user_context else None
        seen["request_id"] = mcp_server._request_id_var.get()
        seen["session_id"] = mcp_server._session_id_var.get()
        seen["user_context"] = user_context
        seen["lease_scope"] = current_lease_scope()
        return ToolResult(content=[])

    context = MiddlewareContext(
        message=mt.CallToolRequestParams(
            name="goal_list",
            arguments={
                "_user_db_path": str(user["db_path"]),
                "_user_id": "test-user-42",
                "_user_rules_path": str(user["rules_path"]),
                "_user_uploads_dir": str(user["uploads_dir"]),
                "_request_id": "req-123",
                "_session_id": "sess-123",
                "_storage_mode": "local",
                "_storage_lease_id": "lease-123",
                "limit": 10,
            },
        )
    )

    asyncio.run(mcp_server.UserContextMiddleware().on_call_tool(context, call_next))

    assert seen["arguments"] == {"limit": 10}
    assert seen["db_path"] == str(user["db_path"])
    assert seen["rules_path"] == str(user["rules_path"])
    assert seen["uploads_dir"] == str(user["uploads_dir"])
    assert seen["request_id"] == "req-123"
    assert seen["session_id"] == "sess-123"
    assert seen["lease_scope"] is not None
    assert seen["lease_scope"].storage_mode == "local"
    assert seen["lease_scope"].lease_id == "lease-123"
    assert seen["user_context"] == UserContext.from_paths(
        db_path=user["db_path"],
        expected_user_id="test-user-42",
        rules_path=user["rules_path"],
        uploads_dir=user["uploads_dir"],
        local_mode=False,
        storage_mode="local",
    )
    assert get_user_context() is None
    assert current_lease_scope() is None
    assert mcp_server._request_id_var.get() is None
    assert mcp_server._session_id_var.get() is None


def test_middleware_resets_context_after_exception(
    global_db: Path, workspace_factory
) -> None:
    user = workspace_factory("alice")

    async def call_next(_context):
        raise RuntimeError("boom")

    context = MiddlewareContext(
        message=mt.CallToolRequestParams(
            name="goal_list",
            arguments={
                "_user_db_path": str(user["db_path"]),
                "_user_rules_path": str(user["rules_path"]),
                "_user_uploads_dir": str(user["uploads_dir"]),
            },
        )
    )

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(mcp_server.UserContextMiddleware().on_call_tool(context, call_next))

    assert get_user_context() is None


def test_get_conn_reuses_gateway_storage_scope_without_reacquiring_lease(
    global_db: Path,
    workspace_factory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del global_db
    user = workspace_factory("alice")

    def fail_acquire(*args, **kwargs):
        raise AssertionError("MCP child should reuse gateway storage context")

    monkeypatch.setattr(db_module, "_acquire_connection_lease", fail_acquire)

    with LeaseScope(
        user_id="alice",
        lease=LocalLease("lease-local"),
        session_manager=object(),
        owns_lease=False,
    ):
        with _user_context(db_path=user["db_path"], expected_user_id="alice"):
            with mcp_server._get_conn() as conn:
                assert _connected_main_db_path(conn) == user["db_path"].resolve()


def test_middleware_without_user_args_preserves_default_paths(global_db: Path) -> None:
    seen: dict[str, object] = {}

    async def call_next(context):
        user_context = get_user_context()
        seen["arguments"] = context.message.arguments
        seen["db_path"] = user_context.db_path if user_context else None
        seen["rules_path"] = user_context.rules_path if user_context else None
        seen["uploads_dir"] = user_context.uploads_dir if user_context else None
        return ToolResult(content=[])

    context = MiddlewareContext(
        message=mt.CallToolRequestParams(name="goal_list", arguments={"limit": 5})
    )

    asyncio.run(mcp_server.UserContextMiddleware().on_call_tool(context, call_next))

    assert seen["arguments"] == {"limit": 5}
    assert seen["db_path"] is None
    assert seen["rules_path"] is None
    assert seen["uploads_dir"] is None


def test_get_conn_resolves_path_with_parent_traversal_safely(
    global_db: Path, workspace_factory
) -> None:
    user = workspace_factory("alice")
    raw_path = user["data_dir"] / "nested" / ".." / "finance.db"

    with _user_context(db_path=raw_path):
        with mcp_server._get_conn() as conn:
            assert _connected_main_db_path(conn) == user["db_path"].resolve()


def test_user_scoped_dispatcher_replaces_model_supplied_user_paths(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_dispatch(self, tool_call_id, tool_name, tool_input, *, call_index=0):
        captured["tool_call_id"] = tool_call_id
        captured["tool_name"] = tool_name
        captured["tool_input"] = tool_input
        captured["call_index"] = call_index
        return None

    monkeypatch.setattr(ToolDispatcher, "dispatch", fake_dispatch)

    dispatcher = UserScopedDispatcher(
        mcp_client=SimpleNamespace(),
        user_paths={
            "_user_id": "safe-user-42",
            "_user_db_path": "/safe/db.sqlite3",
            "_user_rules_path": "/safe/rules.yaml",
            "_user_uploads_dir": "/safe/uploads",
            "_request_id": "safe-request",
            "_session_id": "safe-session",
            "_storage_mode": "local",
            "_storage_lease_id": "safe-lease",
        },
    )

    asyncio.run(
        dispatcher.dispatch(
            "tool-1",
            "goal_list",
            {
                "_user_id": "evil-user-42",
                "_user_db_path": "/evil/db.sqlite3",
                "_user_rules_path": "/evil/rules.yaml",
                "_user_uploads_dir": "/evil/uploads",
                "_request_id": "evil-request",
                "_session_id": "evil-session",
                "_storage_mode": "remote",
                "_storage_lease_id": "evil-lease",
                "limit": 5,
            },
            call_index=2,
        )
    )

    assert captured["tool_call_id"] == "tool-1"
    assert captured["tool_name"] == "goal_list"
    assert captured["call_index"] == 2
    assert captured["tool_input"] == {
        "limit": 5,
        "_user_id": "safe-user-42",
        "_user_db_path": "/safe/db.sqlite3",
        "_user_rules_path": "/safe/rules.yaml",
        "_user_uploads_dir": "/safe/uploads",
        "_request_id": "safe-request",
        "_session_id": "safe-session",
        "_storage_mode": "local",
        "_storage_lease_id": "safe-lease",
    }


def test_user_scoped_tools_isolate_db_rules_and_memory(
    global_db: Path, workspace_factory
) -> None:
    user_a = workspace_factory("alice")
    user_b = workspace_factory("bob")

    (global_db.parent / memory_cmd.MEMORY_FILENAME).write_text(
        "global memory", encoding="utf-8"
    )
    (user_a["data_dir"] / memory_cmd.MEMORY_FILENAME).write_text(
        "alice memory", encoding="utf-8"
    )
    (user_b["data_dir"] / memory_cmd.MEMORY_FILENAME).write_text(
        "bob memory", encoding="utf-8"
    )
    user_a["rules_path"].write_text(
        "keyword_rules:\n  - keywords: [coffee]\n    category: Coffee\n",
        encoding="utf-8",
    )
    user_b["rules_path"].write_text(
        "keyword_rules:\n  - keywords: [coffee]\n    category: Dining\n",
        encoding="utf-8",
    )

    with _user_context(db_path=user_a["db_path"], rules_path=user_a["rules_path"]):
        mcp_server.goal_set(name="Alice Goal", target=1000)
        assert [goal["name"] for goal in mcp_server.goal_list()["data"]["goals"]] == [
            "Alice Goal"
        ]
        assert mcp_server.rules_list()["data"]["rules"][0]["category"] == "Coffee"
        assert mcp_server.agent_memory_read()["data"]["content"] == "alice memory"

    with _user_context(db_path=user_b["db_path"], rules_path=user_b["rules_path"]):
        mcp_server.goal_set(name="Bob Goal", target=2000)
        assert [goal["name"] for goal in mcp_server.goal_list()["data"]["goals"]] == [
            "Bob Goal"
        ]
        assert mcp_server.rules_list()["data"]["rules"][0]["category"] == "Dining"
        assert mcp_server.agent_memory_read()["data"]["content"] == "bob memory"

    with connect(db_path=user_a["db_path"]) as conn_a:
        goals_a = conn_a.execute(
            "SELECT name FROM goals ORDER BY created_at"
        ).fetchall()
    with connect(db_path=user_b["db_path"]) as conn_b:
        goals_b = conn_b.execute(
            "SELECT name FROM goals ORDER BY created_at"
        ).fetchall()

    assert [row["name"] for row in goals_a] == ["Alice Goal"]
    assert [row["name"] for row in goals_b] == ["Bob Goal"]


def test_skill_state_store_is_user_scoped(global_db: Path, workspace_factory) -> None:
    del global_db
    user_a = workspace_factory("alice")
    user_b = workspace_factory("bob")

    with _user_context(db_path=user_a["db_path"], rules_path=user_a["rules_path"]):
        mcp_server.skill_state_set(
            "onboarding", {"phase": "profile", "data_connected": True}
        )
        assert mcp_server.skill_state_get("onboarding")["data"]["state"] == {
            "phase": "profile",
            "data_connected": True,
        }

    with _user_context(db_path=user_b["db_path"], rules_path=user_b["rules_path"]):
        assert mcp_server.skill_state_get("onboarding")["data"]["state"] == {}
        mcp_server.skill_state_set("onboarding", {"phase": "assessment"})
        assert mcp_server.skill_state_get("onboarding")["data"]["state"] == {
            "phase": "assessment"
        }

    with _user_context(db_path=user_a["db_path"], rules_path=user_a["rules_path"]):
        mcp_server.skill_state_clear("onboarding")
        assert mcp_server.skill_state_get("onboarding")["data"]["state"] == {}

    with _user_context(db_path=user_b["db_path"], rules_path=user_b["rules_path"]):
        assert mcp_server.skill_state_get("onboarding")["data"]["state"] == {
            "phase": "assessment"
        }
