from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
from typing import Any

from agent_gateway.multi_user.billing import UsageEvent
from agent_gateway.tool_dispatcher import InterceptContext
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.gateway.config import GatewaySettings
from finance_cli.gateway import prompt as gateway_prompt
from finance_cli.gateway import server as gateway_server
from finance_cli.gateway.prompt import (
    _BASE_SYSTEM_PROMPT,
    _WEB_SYSTEM_PROMPT,
    build_code_execution_prompt,
)
from finance_cli.gateway.socket_bridge import build_client_module_source, build_tool_catalog
from finance_cli.gateway.tools import (
    ALL_NORMALIZER_TOOLS,
    BRIDGE_TOOLS,
    EXCLUDED_TOOLS,
    ONBOARDING_AUTO_APPROVED,
    REGULATED_SCOPE_EXCLUDED_TOOLS,
    WEB_IMPORT_TOOLS,
    _NON_ACTIVATABLE_SKILLS,
    needs_approval,
    web_excluded_tools,
)
from finance_cli.perf import _conversation_id_var, _request_id_var, _session_id_var, set_conversation_id
from finance_cli.skills import SkillProfile
from finance_cli.storage_lease import LocalLease
from finance_cli.user_rules import CANONICAL_CATEGORIES


class FakeMcpClientManager:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False

    async def startup(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.stopped = True

    def get_tool_definitions(self) -> list[dict[str, Any]]:
        return [{"name": "goal_list", "description": "List goals"}]


class FakeChatRuntime:
    def __init__(self, **kwargs) -> None:
        self.system_prompt = kwargs["system_prompt"]
        self.build_runner = kwargs.get("build_runner")
        self.get_tool_definitions = kwargs["get_tool_definitions"]
        self.build_dispatcher = kwargs["build_dispatcher"]
        self.model_override = kwargs["model_override"]
        self.excluded_tools = kwargs["excluded_tools"]
        self.on_usage = kwargs.get("on_usage")
        self.post_runner_init = kwargs.get("post_runner_init")
        self.max_turns = kwargs["max_turns"]


@pytest.fixture(autouse=True)
def stub_system_prompt(monkeypatch) -> None:
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, data_dir=None, skill=None, **kwargs: (
            _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT
        ),
    )


def _make_settings(
    tmp_path: Path,
    auth_token: str = "sk-ant-oat-shared-token",
    *,
    code_execution_enabled: bool = False,
    **overrides: Any,
) -> GatewaySettings:
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")
    kwargs: dict[str, Any] = dict(
        ANTHROPIC_AUTH_TOKEN=auth_token,
        GATEWAY_USER_KEYS=json.dumps(
            [
                {
                    "key": "gateway-key",
                    "channel": "web",
                    "user_id": 1,
                    "email": "user1@example.test",
                    "role": "owner",
                }
            ]
        ),
        FINANCE_GATEWAY_JWT_SECRET="jwt-secret-for-tests-at-least-32-bytes",
        FINANCE_GATEWAY_HOST="127.0.0.1",
        FINANCE_GATEWAY_PORT=8002,
        FINANCE_GATEWAY_DATA_ROOT=tmp_path / "users",
        FINANCE_GATEWAY_RULES_TEMPLATE=template_rules,
        FINANCE_GATEWAY_CODE_EXECUTION=code_execution_enabled,
    )
    kwargs.update(overrides)
    return GatewaySettings(**kwargs)


def _make_session(**overrides: Any) -> SimpleNamespace:
    kwargs: dict[str, Any] = {
        "approved_tool_types": set(),
        "session_id": "sess-test",
        "result_queue": asyncio.Queue(),
        "auth_config": None,
    }
    kwargs.update(overrides)
    return SimpleNamespace(**kwargs)


def _seed_onboarding_complete_data(user_dir: Path) -> None:
    db_path = user_dir / "finance.db"
    initialize_database(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO accounts (id, institution_name, account_name, account_type, is_active)
            VALUES ('acct_1', 'Test Bank', 'Checking', 'checking', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
            VALUES ('txn_1', 'acct_1', '2026-04-01', 'Coffee', -500, 'manual', 1)
            """
        )
        conn.execute(
            """
            INSERT INTO transactions (id, account_id, date, description, amount_cents, source, is_active)
            VALUES ('txn_2', 'acct_1', '2026-05-01', 'Coffee', -600, 'manual', 1)
            """
        )


def test_create_app_returns_fastapi_app(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)

    app = gateway_server.create_app(_make_settings(tmp_path))

    assert isinstance(app, FastAPI)


def test_create_app_registers_code_execution_cleanup_hook(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)

    app = gateway_server.create_app(_make_settings(tmp_path, code_execution_enabled=True))

    assert app.state.auth.session_store._on_expiry is gateway_server.cleanup_code_execution


def test_build_chat_runtime_non_web_uses_base_prompt_and_default_exclusions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types={"txn"}),
            SimpleNamespace(model="claude-opus-4-6", context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert runtime.system_prompt == _BASE_SYSTEM_PROMPT
    assert runtime.model_override == "claude-opus-4-6"
    assert runtime.excluded_tools == set(EXCLUDED_TOOLS) | set(REGULATED_SCOPE_EXCLUDED_TOOLS)


def test_build_chat_runtime_cli_seeds_canonical_categories(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(
        settings,
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "2"}),
            "cli",
            SimpleNamespace(),
        )
    )

    with connect(settings.data_root / "2" / "finance.db") as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM categories WHERE is_system = 1"
        ).fetchone()["n"]

    assert count == len(CANONICAL_CATEGORIES)


def test_build_chat_runtime_passes_storage_lease_context_to_dispatcher(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_dispatcher_init(self, **kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server.UserScopedDispatcher, "__init__", fake_dispatcher_init)
    monkeypatch.setattr(
        gateway_server,
        "_ensure_gateway_storage_lease",
        lambda *args, **kwargs: LocalLease("lease-local-123"),
    )
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(
        settings,
        FakeMcpClientManager(),
        lease_session_manager=object(),
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert captured["user_paths"]["_user_id"] == "alice"
    assert captured["user_paths"]["_storage_mode"] == "local"
    assert captured["user_paths"]["_storage_lease_id"] == "lease-local-123"


def test_renamed_tool_approval_classification() -> None:
    assert needs_approval("bank_account_activate") is True
    assert needs_approval("bank_account_deactivate") is True
    assert needs_approval("statement_normalizer_sample_csv") is False
    assert needs_approval("statement_normalizer_test") is False


def test_build_chat_runtime_wires_code_execution_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    handler_calls: list[dict[str, Any]] = []

    async def fake_code_execute_handler(tool_input: dict[str, Any], **kwargs):
        handler_calls.append({"tool_input": dict(tool_input), "kwargs": dict(kwargs)})
        return {"stdout": "ok"}, None

    bundle = SimpleNamespace(
        handlers={
            "code_execute": fake_code_execute_handler,
            "code_execute_status": fake_code_execute_handler,
        },
        tool_definitions=[
            {
                "name": "code_execute",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "background": {"type": "boolean"},
                        "host": {"type": "string"},
                    },
                    "required": ["code"],
                },
            },
            {"name": "code_execute_status", "input_schema": {"type": "object", "properties": {}}},
        ],
        approval_qualifier=lambda tool_name, tool_input: "docker",
        needs_approval=lambda tool_name, tool_input, qualifier: qualifier != "docker",
    )

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    monkeypatch.setattr(gateway_server, "build_code_execution", lambda session, config: bundle)

    mcp = FakeMcpClientManager()
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path, code_execution_enabled=True),
        mcp,
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(code_execution_work_dir=None),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    tool_defs = runtime.get_tool_definitions()
    expected_prompt = _BASE_SYSTEM_PROMPT + build_code_execution_prompt(
        build_tool_catalog(mcp, BRIDGE_TOOLS)
    )

    assert runtime.system_prompt == expected_prompt
    assert [tool_def["name"] for tool_def in tool_defs] == ["goal_list", "code_execute"]
    assert "background" not in tool_defs[1]["input_schema"]["properties"]
    assert "host" not in tool_defs[1]["input_schema"]["properties"]
    assert "code_execute" in captured["local_tool_handlers"]
    assert captured["approval_key_qualifier"] is bundle.approval_qualifier
    assert captured["needs_approval"]("code_execute", {"code": "print(1)"}, "docker") is False
    assert captured["needs_approval"]("goal_list", {}, "") is needs_approval("goal_list")

    result, error = asyncio.run(captured["local_tool_handlers"]["code_execute"]({"code": "print(1)"}))
    assert error is None
    assert result == {"stdout": "ok"}
    assert handler_calls[-1]["tool_input"] == {"code": "print(1)"}

    result, error = asyncio.run(
        captured["local_tool_handlers"]["code_execute"]({"code": "print(1)", "background": True})
    )
    assert result is None
    assert error == {
        "code": "invalid_input",
        "message": "Background execution is not supported",
    }


def test_build_chat_runtime_appends_code_execution_prompt_for_cacheable_prompt_lists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bundle = SimpleNamespace(
        handlers={
            "code_execute": lambda *args, **kwargs: None,
            "code_execute_status": lambda *args, **kwargs: None,
        },
        tool_definitions=[
            {"name": "code_execute", "input_schema": {"type": "object", "properties": {}}},
            {"name": "code_execute_status", "input_schema": {"type": "object", "properties": {}}},
        ],
        approval_qualifier=lambda tool_name, tool_input: "docker",
        needs_approval=lambda tool_name, tool_input, qualifier: False,
    )

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda **kwargs: [(_BASE_SYSTEM_PROMPT, True)],
    )
    monkeypatch.setattr(gateway_server, "build_code_execution", lambda session, config: bundle)
    mcp = FakeMcpClientManager()
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path, code_execution_enabled=True),
        mcp,
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(code_execution_work_dir=None),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert runtime.system_prompt == [
        (_BASE_SYSTEM_PROMPT, True),
        (build_code_execution_prompt(build_tool_catalog(mcp, BRIDGE_TOOLS)), False),
    ]


def test_guarded_code_execute_rewrites_finance_client_module_every_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    written_sources: list[str] = []
    short_tmp_dir = "/tmp" if Path("/tmp").is_dir() else None
    with tempfile.TemporaryDirectory(prefix="fcb_", dir=short_tmp_dir) as tmpdir:
        work_dir = Path(tmpdir)

        async def fake_code_execute_handler(tool_input: dict[str, Any], **kwargs):
            del tool_input, kwargs
            client_module_path = work_dir / "finance_client.py"
            written_sources.append(client_module_path.read_text(encoding="utf-8"))
            return {"stdout": "ok"}, None

        bundle = SimpleNamespace(
            handlers={
                "code_execute": fake_code_execute_handler,
                "code_execute_status": fake_code_execute_handler,
            },
            tool_definitions=[
                {"name": "code_execute", "input_schema": {"type": "object", "properties": {}}},
                {"name": "code_execute_status", "input_schema": {"type": "object", "properties": {}}},
            ],
            approval_qualifier=lambda tool_name, tool_input: "docker",
            needs_approval=lambda tool_name, tool_input, qualifier: False,
        )

        class FakeDispatcher:
            def __init__(self, **kwargs) -> None:
                captured.update(kwargs)

        monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
        monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
        monkeypatch.setattr(gateway_server, "build_code_execution", lambda session, config: bundle)

        runtime = asyncio.run(
            gateway_server._make_build_chat_runtime(
                _make_settings(tmp_path, code_execution_enabled=True),
                FakeMcpClientManager(),
            )(
                _make_session(code_execution_work_dir=str(work_dir)),
                SimpleNamespace(model=None, context={"user_id": "alice"}),
                "telegram",
                SimpleNamespace(),
            )
        )
        runtime.build_dispatcher(
            SimpleNamespace(
                mcp_client=None,
                request_approval="approval-callback",
                event_log=None,
            )
        )

        handler = captured["local_tool_handlers"]["code_execute"]
        expected_source = build_client_module_source()

        result, error = asyncio.run(handler({"code": "print(1)"}))
        assert error is None
        assert result == {"stdout": "ok"}
        assert written_sources == [expected_source]

        (work_dir / "finance_client.py").unlink()

        result, error = asyncio.run(handler({"code": "print(2)"}))
        assert error is None
        assert result == {"stdout": "ok"}
        assert written_sources == [expected_source, expected_source]


def test_build_chat_runtime_passes_skill_to_prompt_builder(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        captured["channel"] = channel
        captured["data_dir"] = data_dir
        captured["skill"] = skill
        captured["skill_context"] = kwargs.get("skill_context")
        captured["upload_context"] = kwargs.get("upload_context")
        return _BASE_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "skill": "normalizer_builder",
                    "upload_path": "/uploads/abc.csv",
                    "sample_rows": ["Header1", "Row1"],
                },
            ),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert runtime.system_prompt == _BASE_SYSTEM_PROMPT
    assert captured == {
        "channel": "telegram",
        "data_dir": (tmp_path / "users" / "alice").resolve(),
        "skill": "normalizer_builder",
        "skill_context": {
            "upload_path": "/uploads/abc.csv",
            "sample_rows": ["Header1", "Row1"],
        },
        "upload_context": {
            "upload_path": "/uploads/abc.csv",
        },
    }


def test_build_chat_runtime_passes_none_skill_when_missing(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir
        captured["skill"] = skill
        captured["upload_context"] = kwargs.get("upload_context")
        return _BASE_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] is None
    assert captured["upload_context"] is None


def test_web_channel_rejects_unknown_skill(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": "evil"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] is None


def test_web_onboarding_stripped_when_complete(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)
    user_dir = settings.data_root / "alice"
    user_dir.mkdir(parents=True, exist_ok=True)
    _seed_onboarding_complete_data(user_dir)
    (user_dir / "skill_state.json").write_text(
        json.dumps({"onboarding": {"complete": True}}),
        encoding="utf-8",
    )

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] is None


def test_telegram_onboarding_stripped_when_complete(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)
    user_dir = settings.data_root / "alice"
    user_dir.mkdir(parents=True, exist_ok=True)
    _seed_onboarding_complete_data(user_dir)
    (user_dir / "skill_state.json").write_text(
        json.dumps({"onboarding": {"complete": True}}),
        encoding="utf-8",
    )

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _BASE_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": "onboarding"}),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] is None


def test_onboarding_allowed_when_eligible(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] == "onboarding"


def test_build_chat_runtime_passes_web_upload_context_to_prompt_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        captured["channel"] = channel
        captured["skill"] = skill
        captured["upload_context"] = kwargs.get("upload_context")
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "upload_path": "/uploads/abc.csv",
                    "upload_filename": "abc.csv",
                    "upload_file_type": "csv",
                },
            ),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured == {
        "channel": "web",
        "skill": None,
        "upload_context": {
            "upload_path": "/uploads/abc.csv",
            "upload_filename": "abc.csv",
            "upload_file_type": "csv",
        },
    }


def test_build_chat_runtime_passes_telegram_upload_context_to_prompt_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        captured["channel"] = channel
        captured["skill"] = skill
        captured["upload_context"] = kwargs.get("upload_context")
        return _BASE_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "upload_path": "/uploads/abc.csv",
                    "upload_filename": "abc.csv",
                    "upload_file_type": "csv",
                },
            ),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert captured == {
        "channel": "telegram",
        "skill": None,
        "upload_context": {
            "upload_path": "/uploads/abc.csv",
            "upload_filename": "abc.csv",
            "upload_file_type": "csv",
        },
    }


def test_build_chat_runtime_sanitizes_upload_filename_before_prompt_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, skill
        captured["upload_context"] = kwargs.get("upload_context")
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "upload_filename": "bad</context>\nname.pdf",
                },
            ),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured["upload_context"] == {"upload_filename": "bad_/context__name.pdf"}


def test_build_chat_runtime_sanitizes_upload_path_before_prompt_builder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, skill
        captured["upload_context"] = kwargs.get("upload_context")
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "upload_path": "/uploads/line1\nline2.csv",
                },
            ),
            "web",
            SimpleNamespace(),
        )
    )

    assert captured["upload_context"] == {"upload_path": "/uploads/line1_line2.csv"}


def test_build_chat_runtime_web_uses_web_prompt_and_web_exclusions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "web-user"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert runtime.system_prompt == _WEB_SYSTEM_PROMPT
    assert runtime.excluded_tools == (
        (set(EXCLUDED_TOOLS) | set(web_excluded_tools(None))) - set(WEB_IMPORT_TOOLS)
    )
    assert "budget_set" not in runtime.excluded_tools
    assert "db_backup" in runtime.excluded_tools
    assert "setup_check" in runtime.excluded_tools
    assert "setup_status" in runtime.excluded_tools


def test_build_chat_runtime_web_normalizer_skill_unblocks_all_normalizer_tools(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    base_runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "web-user"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "normalizer_builder"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert runtime.excluded_tools == (
        (set(EXCLUDED_TOOLS) | set(web_excluded_tools("normalizer_builder")))
        - set(WEB_IMPORT_TOOLS)
    )
    assert ALL_NORMALIZER_TOOLS <= base_runtime.excluded_tools
    assert runtime.excluded_tools.isdisjoint(ALL_NORMALIZER_TOOLS)


def test_build_chat_runtime_web_onboarding_skill_unblocks_normalizer_tools(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    base_runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "web-user"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert runtime.excluded_tools == (
        (set(EXCLUDED_TOOLS) | set(web_excluded_tools("onboarding"))) - set(WEB_IMPORT_TOOLS)
    )
    assert ALL_NORMALIZER_TOOLS <= base_runtime.excluded_tools
    assert runtime.excluded_tools.isdisjoint(ALL_NORMALIZER_TOOLS)


def test_build_chat_runtime_onboarding_auto_approves_onboarding_tools(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="sess-onboarding"),
            SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert runtime.max_turns == 40
    for tool_name in ONBOARDING_AUTO_APPROVED:
        assert captured["needs_approval"](tool_name, {}, "") is False
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": False, "include_key_only": True},
        "",
    ) is True
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": "false", "include_key_only": "true"},
        "",
    ) is True
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": " true ", "include_key_only": "true"},
        "",
    ) is True
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": False, "include_key_only": False},
        "",
    ) is False
    assert captured["needs_approval"]("budget_set", {}, "") is True


def test_onboarding_key_only_dedup_still_requires_approval_with_code_execution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_code_execute_handler(tool_input: dict[str, Any], **kwargs):
        return {"stdout": "ok"}, None

    bundle = SimpleNamespace(
        handlers={
            "code_execute": fake_code_execute_handler,
            "code_execute_status": fake_code_execute_handler,
        },
        tool_definitions=[],
        approval_qualifier=lambda tool_name, tool_input: "",
        needs_approval=lambda tool_name, tool_input, qualifier: False,
    )

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    monkeypatch.setattr(gateway_server, "build_code_execution", lambda session, config: bundle)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path, code_execution_enabled=True),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="sess-onboarding"),
            SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": False, "include_key_only": True},
        "",
    ) is True
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": " true ", "include_key_only": "true"},
        "",
    ) is True
    assert captured["needs_approval"](
        "dedup_cross_format",
        {"dry_run": False, "include_key_only": False},
        "",
    ) is False


def test_onboarding_approval_gate_requires_approval_for_agent_memory_update(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="sess-onboarding"),
            SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )

    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert captured["needs_approval"]("agent_memory_update", {}, "") is True


def test_build_chat_runtime_compaction_ignores_skill(tmp_path: Path, monkeypatch) -> None:
    def fail_build_system_prompt(channel=None, data_dir=None, skill=None) -> str:
        del channel, data_dir, skill
        raise AssertionError("build_system_prompt should not be called for compaction")

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fail_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(
                model=None,
                context={"user_id": "alice", "compaction": True, "skill": "normalizer_builder"},
            ),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert runtime.system_prompt == gateway_server._COMPACTION_SYSTEM_PROMPT


@pytest.mark.parametrize("raw_skill", [123, ["array"]])
def test_build_chat_runtime_non_string_skill_normalizes_to_none(
    tmp_path: Path,
    monkeypatch,
    raw_skill: Any,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir
        captured["skill"] = skill
        return _BASE_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": raw_skill}),
            "telegram",
            SimpleNamespace(),
        )
    )

    assert captured["skill"] is None


def test_build_chat_runtime_compaction_restricts_tools_and_disables_approval(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class RichFakeMcp(FakeMcpClientManager):
        def get_tool_definitions(self) -> list[dict[str, Any]]:
            return [
                {"name": "agent_session_write"},
                {"name": "agent_session_search"},
                {"name": "agent_session_read"},
                {"name": "agent_memory_read"},
                {"name": "agent_memory_update"},
                {"name": "budget_status"},
            ]

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    build_runtime = gateway_server._make_build_chat_runtime(_make_settings(tmp_path), RichFakeMcp())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="test-sess"),
            SimpleNamespace(model=None, context={"user_id": "alice", "compaction": True}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert runtime.system_prompt == gateway_server._COMPACTION_SYSTEM_PROMPT
    assert runtime.max_turns == 3
    assert runtime.excluded_tools == {"agent_memory_update", "budget_status"}
    assert captured["request_approval"] is None
    assert captured["needs_approval"]("agent_session_write") is False
    assert captured["needs_approval"]("budget_status") is False


def test_build_chat_runtime_rejects_web_compaction(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    with pytest.raises(HTTPException, match="Compaction not supported on web channel") as exc_info:
        asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model=None, context={"user_id": "web-user", "compaction": True}),
                "web",
                SimpleNamespace(),
            )
        )

    assert exc_info.value.status_code == 400


def test_build_chat_runtime_wires_dispatcher_with_user_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types={"txn"}, session_id="test-sess"),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert captured["needs_approval"] is needs_approval
    assert captured["request_approval"] == "approval-callback"
    assert captured["approved_tool_types"] == {"txn"}
    assert captured["user_paths"] == {
        "_user_id": "alice",
        "_user_db_path": str((settings.data_root / "alice" / "finance.db").resolve()),
        "_user_rules_path": str((settings.data_root / "alice" / "rules.yaml").resolve()),
        "_user_uploads_dir": str((settings.data_root / "alice" / "uploads").resolve()),
    }


def test_build_chat_runtime_telegram_wires_dispatcher_with_user_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types={"txn"}, session_id="test-sess"),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    assert captured["needs_approval"] is needs_approval
    assert captured["request_approval"] == "approval-callback"
    assert captured["approved_tool_types"] == {"txn"}
    assert captured["user_paths"] == {
        "_user_id": "alice",
        "_user_db_path": str((settings.data_root / "alice" / "finance.db").resolve()),
        "_user_rules_path": str((settings.data_root / "alice" / "rules.yaml").resolve()),
        "_user_uploads_dir": str((settings.data_root / "alice" / "uploads").resolve()),
    }


def test_build_chat_runtime_passes_interceptors_to_dispatcher(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    settings = _make_settings(
        tmp_path,
        FINANCE_GATEWAY_RATE_LIMIT_RPM=2,
        FINANCE_GATEWAY_MAX_INPUT_BYTES=20,
    )
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="sess-normal"),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_dispatcher(
        SimpleNamespace(
            mcp_client=None,
            request_approval="approval-callback",
            event_log=None,
        )
    )

    interceptors = captured["interceptors"]
    assert len(interceptors) == 2

    rate_limit_ctx = InterceptContext(
        tool_call_id="tool-1",
        tool_name="goal_list",
        tool_input={},
        session_id="sess-normal",
    )
    assert asyncio.run(interceptors[0](rate_limit_ctx)).action == "allow"
    assert asyncio.run(interceptors[0](rate_limit_ctx)).action == "allow"
    assert asyncio.run(interceptors[0](rate_limit_ctx)).action == "deny"

    size_ctx = InterceptContext(
        tool_call_id="tool-2",
        tool_name="goal_list",
        tool_input={"value": "x" * 30},
        session_id="sess-normal",
    )
    assert asyncio.run(interceptors[1](size_ctx)).action == "deny"


def test_build_chat_runtime_injects_request_and_session_ids(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    request_token = _request_id_var.set("req-123")
    session_token = _session_id_var.set("sess-456")
    try:
        runtime = asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set(), session_id="test-sess"),
                SimpleNamespace(model=None, context={"user_id": "alice"}),
                "web",
                SimpleNamespace(),
            )
        )
        runtime.build_dispatcher(
            SimpleNamespace(
                mcp_client=None,
                request_approval="approval-callback",
                event_log=None,
            )
        )
    finally:
        _session_id_var.reset(session_token)
        _request_id_var.reset(request_token)

    assert captured["user_paths"]["_request_id"] == "req-123"
    assert captured["user_paths"]["_session_id"] == "sess-456"


def test_build_runner_wires_agent_runner_for_normal_request(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path, FINANCE_GATEWAY_CLIENT_TIMEOUT=123.5)
    mcp = FakeMcpClientManager()

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    build_runtime = gateway_server._make_build_chat_runtime(settings, mcp)

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    fake_event_log = gateway_server.EventLog()
    runner = runtime.build_runner(fake_event_log, "sess-1")

    assert runner is captured["runner"]
    assert captured["args"][0] is fake_event_log
    assert captured["args"][2] == "sess-1"
    assert "event_log" not in captured["kwargs"]
    assert "session_id" not in captured["kwargs"]
    assert isinstance(captured["kwargs"]["provider"], gateway_server.AnthropicProvider)
    assert captured["kwargs"]["auth_config"] == {
        "model": settings.model,
        "max_tokens": settings.max_tokens,
        "thinking": settings.thinking,
        "auth_mode": "oauth",
        "api_key": "",
        "auth_token": settings.anthropic_auth_token,
    }
    assert captured["kwargs"]["excluded_tools"] == set(EXCLUDED_TOOLS) | set(
        REGULATED_SCOPE_EXCLUDED_TOOLS
    )
    assert callable(captured["kwargs"]["get_tool_definitions"])
    assert captured["kwargs"]["on_usage"] is None
    assert captured["kwargs"]["per_turn_timeout"] == settings.telegram_per_turn_timeout
    assert captured["kwargs"]["client_timeout"] == 123.5
    assert captured["kwargs"]["mcp_client"] is mcp


def test_build_runner_keeps_default_timeout_for_non_telegram_request(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "cli",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-cli")

    assert captured["kwargs"]["per_turn_timeout"] == settings.per_turn_timeout


def test_build_runner_wires_onboarding_runtime_options(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    perf_calls: list[dict[str, Any]] = []
    settings = _make_settings(
        tmp_path,
        FINANCE_GATEWAY_WEB_MAX_BUDGET_USD=11.5,
        FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER=98765,
    )

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    def fake_record_perf_sample(
        db_path,
        source,
        metric,
        value_ms,
        tags=None,
        is_error=False,
    ) -> None:
        perf_calls.append(
            {
                "db_path": db_path,
                "source": source,
                "metric": metric,
                "value_ms": value_ms,
                "tags": tags,
                "is_error": is_error,
            }
        )

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(gateway_server, "_record_perf_sample", fake_record_perf_sample)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice", "skill": "onboarding"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-onboarding")

    assert runtime.max_turns == 40
    assert captured["kwargs"]["compaction_trigger"] == 150_000
    assert "Preserve all onboarding markers" in captured["kwargs"]["compaction_instructions"]
    assert captured["kwargs"]["max_budget_usd"] == 3.0
    assert callable(captured["kwargs"]["on_tool_timing"])

    captured["kwargs"]["on_tool_timing"](
        "sess-onboarding",
        "plaid_sync",
        "finance-cli",
        321,
        False,
        1024,
    )

    assert perf_calls == [
        {
            "db_path": str((settings.data_root / "alice" / "finance.db").resolve()),
            "source": "onboarding",
            "metric": "onboarding.tool.plaid_sync",
            "value_ms": 321,
            "tags": {
                "tool_name": "plaid_sync",
                "server": "finance-cli",
                "session_id": "sess-onboarding",
                "result_bytes": 1024,
            },
            "is_error": False,
        }
    ]


def test_build_runner_wires_code_execution_tool_defs_and_sanitization_hook(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path, code_execution_enabled=True)

    async def fake_code_execute_handler(tool_input: dict[str, Any], **kwargs):
        del tool_input, kwargs
        return {"stdout": "ok"}, None

    bundle = SimpleNamespace(
        handlers={
            "code_execute": fake_code_execute_handler,
            "code_execute_status": fake_code_execute_handler,
        },
        tool_definitions=[
            {
                "name": "code_execute",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "background": {"type": "boolean"},
                        "host": {"type": "string"},
                    },
                    "required": ["code"],
                },
            },
            {"name": "code_execute_status", "input_schema": {"type": "object", "properties": {}}},
        ],
        approval_qualifier=lambda tool_name, tool_input: "docker",
        needs_approval=lambda tool_name, tool_input, qualifier: False,
    )

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(gateway_server, "build_code_execution", lambda session, config: bundle)

    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert callable(captured["kwargs"]["get_tool_definitions"])
    assert captured["kwargs"]["on_tool_result"] is gateway_server._on_tool_result
    assert [tool_def["name"] for tool_def in captured["kwargs"]["get_tool_definitions"]()] == [
        "goal_list",
        "code_execute",
    ]


def test_build_runner_calls_post_runner_init_for_web(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    seen: dict[str, Any] = {}
    settings = _make_settings(tmp_path)

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    def fake_post_runner_init_factory(*, resolution, settings):
        seen["resolution"] = resolution
        seen["settings"] = settings

        def _post_runner_init(runner: Any) -> None:
            seen["runner"] = runner

        return _post_runner_init

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(
        gateway_server,
        "_make_web_guardrail_post_runner_init",
        fake_post_runner_init_factory,
    )
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runner = runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert callable(captured["kwargs"]["on_usage"])
    assert seen["runner"] is runner
    assert seen["resolution"].action == "allow"
    assert seen["settings"] is settings


def test_build_runner_wires_web_budget_and_compaction_settings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(
        tmp_path,
        FINANCE_GATEWAY_WEB_MAX_BUDGET_USD=12.75,
        FINANCE_GATEWAY_WEB_COMPACTION_TRIGGER=87654,
    )

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            _make_session(session_id="sess-web"),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-web")

    assert captured["kwargs"]["compaction_trigger"] == 87654
    assert captured["kwargs"]["compaction_instructions"] is None
    assert captured["kwargs"]["max_budget_usd"] == 12.75


def test_build_runner_compaction_disables_approval(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_dispatcher_init(self, **kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(gateway_server.UserScopedDispatcher, "__init__", fake_dispatcher_init)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice", "compaction": True}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert captured["request_approval"] is None
    assert captured["interceptors"] == []
    assert captured["needs_approval"]("agent_session_write") is False
    assert captured["needs_approval"]("budget_status") is False


def test_build_runner_passes_client_timeout_for_explicit_compaction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["runner"] = self
        captured["args"] = args
        captured["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path, FINANCE_GATEWAY_CLIENT_TIMEOUT=45.5),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(session_id="sess-compact"),
            SimpleNamespace(model=None, context={"user_id": "alice", "compaction": True}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-compact")

    assert captured["kwargs"]["client_timeout"] == 45.5


def test_build_runner_passes_event_log_and_session_id_to_dispatcher(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_dispatcher_init(self, **kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(gateway_server.UserScopedDispatcher, "__init__", fake_dispatcher_init)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(session_id="sess-42"),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    fake_event_log = gateway_server.EventLog()
    runtime.build_runner(fake_event_log, "sess-42")

    assert captured["event_log"] is fake_event_log
    assert captured["session_id"] == "sess-42"


def test_on_tool_result_strips_code_execute_base64() -> None:
    result_entry = {
        "content": json.dumps(
            {
                "stdout": "",
                "images": [
                    {
                        "filename": "chart.png",
                        "media_type": "image/png",
                        "data_base64": "ZmFrZQ==",
                    }
                ],
            }
        )
    }

    ctx = gateway_server.ToolResultContext(
        tool_name="code_execute",
        tool_input={"code": "print(1)"},
        result={"stdout": "", "images": [{"filename": "chart.png", "data_base64": "ZmFrZQ=="}]},
        error=None,
        duration_ms=5,
        tool_call_id="tool-1",
        session_id="sess-1",
        server=None,
        result_entry=result_entry,
    )

    asyncio.run(gateway_server._on_tool_result(ctx))

    assert json.loads(result_entry["content"])["images"][0]["data_base64"] == "[image: chart.png]"


def test_make_activate_skill_hook_stores_skill_by_conversation(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: (
            SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True)
            if name == "normalizer_builder"
            else None
        ),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {"activated": True}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert session._activated_skills == {"conv-1": "normalizer_builder"}


def test_make_activate_skill_hook_noops_on_tool_error(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result=None,
        error={"message": "failed"},
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_make_activate_skill_hook_noops_for_other_tool_names(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="get_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-get-skill",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_make_activate_skill_hook_noops_for_unknown_skill(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(gateway_server, "load_skill_profile", lambda name: None)
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "nonexistent"},
        result={"data": {"activated": False}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_make_activate_skill_hook_noops_for_skills_without_tool_packs(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=[]),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "read_only_skill"},
        result={"data": {"activated": False}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_activate_skill_hook_tool_packs_disabled(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SkillProfile(
            name=name,
            system_prompt="Prompt",
            tool_packs=["finance"],
            tool_packs_enabled=False,
        ),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {"activated": False}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_activate_skill_hook_tool_packs_empty_regression(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SkillProfile(
            name=name,
            system_prompt="Prompt",
            tool_packs=[],
            tool_packs_enabled=True,
        ),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {"activated": False}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert not hasattr(session, "_activated_skills")


def test_make_activate_skill_hook_rejects_non_activatable_skills(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "onboarding"},
        result={"data": {"activated": False}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert "onboarding" in _NON_ACTIVATABLE_SKILLS
    assert not hasattr(session, "_activated_skills")


def test_make_activate_skill_hook_chains_existing_hook(monkeypatch) -> None:
    session = SimpleNamespace()
    chain_calls: list[str] = []

    async def fake_chain(ctx: gateway_server.ToolResultContext) -> None:
        chain_calls.append(ctx.tool_name)

    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True),
    )
    hook = gateway_server._make_activate_skill_hook(session, chain=fake_chain)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {"activated": True}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert chain_calls == ["activate_skill"]
    assert session._activated_skills == {"conv-1": "normalizer_builder"}


def test_make_activate_skill_hook_is_idempotent(monkeypatch) -> None:
    session = SimpleNamespace()
    monkeypatch.setattr(
        gateway_server,
        "load_skill_profile",
        lambda name: SimpleNamespace(tool_packs=["normalizer"], tool_packs_enabled=True),
    )
    hook = gateway_server._make_activate_skill_hook(session)
    ctx = gateway_server.ToolResultContext(
        tool_name="activate_skill",
        tool_input={"name": "normalizer_builder"},
        result={"data": {"activated": True}},
        error=None,
        duration_ms=5,
        tool_call_id="tool-activate",
        session_id="sess-1",
        server="finance-cli",
        result_entry=None,
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        asyncio.run(hook(ctx))
        asyncio.run(hook(ctx))
    finally:
        _conversation_id_var.reset(conversation_token)

    assert session._activated_skills == {"conv-1": "normalizer_builder"}


def test_build_chat_runtime_replays_activated_skill_for_web_conversation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", gateway_prompt.build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )
    session = SimpleNamespace(
        approved_tool_types=set(),
        _activated_skills={"conv-1": "normalizer_builder"},
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        runtime = asyncio.run(
            build_runtime(
                session,
                SimpleNamespace(model=None, context={"user_id": "web-user"}),
                "web",
                SimpleNamespace(),
            )
        )
    finally:
        _conversation_id_var.reset(conversation_token)

    rendered = "".join(text for text, _ in runtime.system_prompt)
    assert runtime.excluded_tools.isdisjoint(ALL_NORMALIZER_TOOLS)
    assert '<skill name="normalizer_builder">' in rendered
    assert "# Normalizer Builder Skill" in rendered


def test_load_onboarding_phase_fragment_reads_prompt_file() -> None:
    fragment = gateway_server._load_onboarding_phase_fragment(
        "prompts/onboarding/phase_profile.md"
    )

    assert fragment is not None
    assert "prompt_chip_select" in fragment


def test_build_chat_runtime_request_context_skill_takes_precedence_over_replay(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )
    session = SimpleNamespace(
        approved_tool_types=set(),
        _activated_skills={"conv-1": "normalizer_builder"},
    )

    conversation_token = set_conversation_id("conv-1")
    try:
        runtime = asyncio.run(
            build_runtime(
                session,
                SimpleNamespace(model=None, context={"user_id": "web-user", "skill": "onboarding"}),
                "web",
                SimpleNamespace(),
            )
        )
    finally:
        _conversation_id_var.reset(conversation_token)

    assert captured["skill"] == "onboarding"
    assert runtime.excluded_tools.isdisjoint(ALL_NORMALIZER_TOOLS)


def test_build_chat_runtime_does_not_replay_skill_for_different_conversation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_build_system_prompt(channel=None, data_dir=None, skill=None, **kwargs) -> str:
        del channel, data_dir, kwargs
        captured["skill"] = skill
        return _WEB_SYSTEM_PROMPT

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "build_system_prompt", fake_build_system_prompt)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )
    session = SimpleNamespace(
        approved_tool_types=set(),
        _activated_skills={"conv-1": "normalizer_builder"},
    )

    conversation_token = set_conversation_id("conv-2")
    try:
        runtime = asyncio.run(
            build_runtime(
                session,
                SimpleNamespace(model=None, context={"user_id": "web-user"}),
                "web",
                SimpleNamespace(),
            )
        )
    finally:
        _conversation_id_var.reset(conversation_token)

    assert captured["skill"] is None
    assert ALL_NORMALIZER_TOOLS <= runtime.excluded_tools


def test_build_chat_runtime_web_usage_hook_records_cost(tmp_path: Path, monkeypatch) -> None:
    seen: dict[str, Any] = {}

    def fake_record_and_settle_cost(*args, **kwargs) -> None:
        seen["args"] = args
        seen["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "record_and_settle_cost", fake_record_and_settle_cost)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    request_token = _request_id_var.set("req-web-1")
    try:
        runtime = asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model="claude-sonnet-4-6", context={"user_id": "alice"}),
                "web",
                SimpleNamespace(),
            )
        )
        assert runtime.on_usage is not None
        runtime.on_usage(
            UsageEvent(
                user_id="alice",
                session_id="sess-web-1",
                request_id="req-web-1",
                parent_turn_id=None,
                timestamp=1.0,
                model="claude-sonnet-4-6",
                input_tokens=100,
                output_tokens=200,
                cache_read_tokens=40,
                cache_creation_tokens=30,
                cost_usd=0.1234,
                rate_table_version="v1",
                billing_mode="metered",
                channel="web",
            )
        )
    finally:
        _request_id_var.reset(request_token)

    assert seen["args"][:4] == (
        str((tmp_path / "users" / "alice" / "finance.db").resolve()),
        "claude",
        "web_chat",
        123400,
    )
    assert seen["kwargs"]["is_byok"] is False
    assert seen["kwargs"]["request_id"] == "req-web-1"
    assert seen["kwargs"]["input_tokens"] == 100
    assert seen["kwargs"]["output_tokens"] == 200
    assert seen["kwargs"]["cache_creation_tokens"] == 30
    assert seen["kwargs"]["cache_read_tokens"] == 40
    assert seen["kwargs"]["idempotency_key"] == "web_chat_req-web-1_t0"
    assert seen["kwargs"]["model"] == "claude-sonnet-4-6"


def test_build_chat_runtime_telegram_leaves_usage_to_telegram_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    request_token = _request_id_var.set("req-telegram-1")
    try:
        runtime = asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model="claude-sonnet-4-6", context={"user_id": "alice"}),
                "telegram",
                SimpleNamespace(),
            )
        )
        assert runtime.on_usage is None
    finally:
        _request_id_var.reset(request_token)


def test_usage_hook_multi_turn_records_each_turn(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    hook = gateway_server._make_web_usage_hook(
        str(db_path),
        request_id="req-multi",
        model="claude-sonnet-4-6",
        is_byok=False,
    )

    events = [
        UsageEvent(
            user_id="alice",
            session_id="sess-multi",
            request_id="req-multi",
            parent_turn_id=None,
            timestamp=1.0,
            model="claude-sonnet-4-6",
            input_tokens=100,
            output_tokens=200,
            cache_read_tokens=10,
            cache_creation_tokens=20,
            cost_usd=0.101,
            rate_table_version="v1",
            billing_mode="metered",
            channel="web",
        ),
        UsageEvent(
            user_id="alice",
            session_id="sess-multi",
            request_id="req-multi",
            parent_turn_id="turn-0",
            timestamp=2.0,
            model="claude-sonnet-4-6",
            input_tokens=101,
            output_tokens=201,
            cache_read_tokens=11,
            cache_creation_tokens=21,
            cost_usd=0.202,
            rate_table_version="v1",
            billing_mode="metered",
            channel="web",
        ),
        UsageEvent(
            user_id="alice",
            session_id="sess-multi",
            request_id="req-multi",
            parent_turn_id="turn-1",
            timestamp=3.0,
            model="claude-sonnet-4-6",
            input_tokens=102,
            output_tokens=202,
            cache_read_tokens=12,
            cache_creation_tokens=22,
            cost_usd=0.303,
            rate_table_version="v1",
            billing_mode="metered",
            channel="web",
        ),
    ]

    for event in events:
        hook(event)

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT idempotency_key, cost_usd6, allowance_debit_usd6, credits_debit_usd6, overflow_unattributed_usd6
            FROM cost_ledger
            WHERE operation = 'web_chat'
            ORDER BY idempotency_key
            """
        ).fetchall()

    assert [(row["idempotency_key"], row["cost_usd6"]) for row in rows] == [
        ("web_chat_req-multi_t0", gateway_server.dollars_to_usd6(0.101)),
        ("web_chat_req-multi_t1", gateway_server.dollars_to_usd6(0.202)),
        ("web_chat_req-multi_t2", gateway_server.dollars_to_usd6(0.303)),
    ]
    assert all(
        row["allowance_debit_usd6"] + row["credits_debit_usd6"] + row["overflow_unattributed_usd6"]
        == row["cost_usd6"]
        for row in rows
    )


def test_usage_hook_partial_event_degrades_gracefully(monkeypatch) -> None:
    seen: dict[str, Any] = {}

    def fake_record_and_settle_cost(*args, **kwargs) -> None:
        seen["args"] = args
        seen["kwargs"] = kwargs

    monkeypatch.setattr(gateway_server, "record_and_settle_cost", fake_record_and_settle_cost)
    hook = gateway_server._make_web_usage_hook(
        "ignored.db",
        request_id="req-partial",
        model="claude-sonnet-4-6",
        is_byok=True,
    )

    hook(SimpleNamespace(input_tokens=100, output_tokens=200))

    assert seen["args"][3] == 0
    assert seen["kwargs"]["is_byok"] is True
    assert seen["kwargs"]["model"] == "claude-sonnet-4-6"


def test_build_chat_runtime_web_guardrail_block_emits_error_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeRunner:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []
            self.inner_calls = 0

        def _append(self, event: dict[str, Any]) -> None:
            self.events.append(event)

        async def run(self, *args, **kwargs):
            del args, kwargs
            self.inner_calls += 1
            return "ok"

    settings = _make_settings(tmp_path)
    gateway_server.provision_user(
        data_root=settings.data_root,
        user_id="alice",
        template_rules_path=settings.template_rules_path,
    )
    db_path = settings.data_root / "alice" / "finance.db"
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 0,
                   system_limit_usd6 = 0
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "_load_user_billing_snapshot",
        lambda settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "stripe_price_id": "price_lite",
        },
    )
    monkeypatch.setenv("STRIPE_PRICE_LITE", "price_lite")
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runner = FakeRunner()
    assert runtime.post_runner_init is not None
    runtime.post_runner_init(runner)
    asyncio.run(runner.run())

    assert runner.inner_calls == 0
    assert runner.events == [
        {
            "type": "error",
            "error": "AI usage limit reached. Buy credits in Billing settings: /settings/billing",
            "status_code": 402,
            "code": "payment_required",
        },
        {"type": "stream_complete", "usage": {}},
    ]


def test_build_chat_runtime_web_standard_cap_downgrades_to_haiku(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _make_settings(tmp_path)
    gateway_server.provision_user(
        data_root=settings.data_root,
        user_id="standard-user",
        template_rules_path=settings.template_rules_path,
    )
    db_path = settings.data_root / "standard-user" / "finance.db"
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 0,
                   system_limit_usd6 = 0
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "standard-user"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert runtime.model_override == "claude-haiku-4-5"


def test_build_chat_runtime_web_byok_usage_records_byok_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _make_settings(tmp_path)
    gateway_server.provision_user(
        data_root=settings.data_root,
        user_id="byok-user",
        template_rules_path=settings.template_rules_path,
    )
    db_path = settings.data_root / "byok-user" / "finance.db"
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE cost_limits
               SET limit_usd6 = 0,
                   system_limit_usd6 = 0
             WHERE provider = 'claude'
               AND period = 'monthly'
            """
        )
        conn.commit()

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "_load_user_billing_snapshot",
        lambda settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "anthropic_api_key_secret_ref": "secret-ref",
        },
    )
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model="claude-sonnet-4-6", context={"user_id": "byok-user"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.on_usage(
        UsageEvent(
            user_id="byok-user",
            session_id="sess-byok",
            request_id="req-byok",
            parent_turn_id=None,
            timestamp=1.0,
            model="claude-sonnet-4-6",
            input_tokens=10,
            output_tokens=20,
            cache_read_tokens=0,
            cache_creation_tokens=0,
            cost_usd=0.01,
            rate_table_version="v1",
            billing_mode="byok",
            channel="web",
        )
    )

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT is_byok,
                   allowance_debit_usd6,
                   credits_debit_usd6,
                   overflow_unattributed_usd6
              FROM cost_ledger
             ORDER BY created_at DESC
             LIMIT 1
            """
        ).fetchone()

    assert row["is_byok"] == 1
    assert row["allowance_debit_usd6"] == 0
    assert row["credits_debit_usd6"] == 0
    assert row["overflow_unattributed_usd6"] == 0


def test_build_chat_runtime_web_cost_resolution_failure_captures_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake_capture_error(exc, **kwargs):
        seen["exc"] = exc
        seen["kwargs"] = kwargs
        return "err-1"

    def user_snapshot(settings, user_id):
        del settings
        return {"id": user_id, "tier": "paid", "ai_egress_mode": "full"}

    def fail_resolution(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "capture_error", fake_capture_error)
    monkeypatch.setattr(gateway_server, "_load_user_billing_snapshot", user_snapshot)
    monkeypatch.setattr(gateway_server, "resolve_request", fail_resolution)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    request_token = _request_id_var.set("req-cost-fail")
    try:
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                build_runtime(
                    SimpleNamespace(approved_tool_types=set()),
                    SimpleNamespace(model=None, context={"user_id": "alice"}),
                    "web",
                    SimpleNamespace(),
                )
            )
    finally:
        _request_id_var.reset(request_token)

    assert exc_info.value.status_code == 503
    assert isinstance(seen["exc"], RuntimeError)
    assert seen["kwargs"]["endpoint"] == "cost_resolve"
    assert seen["kwargs"]["source"] == "gateway"


def test_build_chat_runtime_web_redacted_excludes_finance_tools(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "_load_user_billing_snapshot",
        lambda settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "ai_egress_mode": "redacted",
        },
    )
    monkeypatch.setattr(
        gateway_server,
        "resolve_request",
        lambda *args, **kwargs: SimpleNamespace(
            mode="subscription",
            action="allow",
            effective_model="claude-sonnet-4-6",
            warn_threshold_hit=False,
            credits_available=0,
        ),
    )
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )

    assert "goal_list" in runtime.excluded_tools
    assert set(WEB_IMPORT_TOOLS).issubset(runtime.excluded_tools)
    assert "AI privacy mode is redacted" in runtime.system_prompt


def test_build_chat_runtime_web_off_blocks_before_cost_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "_load_user_billing_snapshot",
        lambda settings, user_id: {
            "id": user_id,
            "tier": "paid",
            "ai_egress_mode": "off",
        },
    )

    def unexpected_cost_resolution(*args, **kwargs):
        del args, kwargs
        raise AssertionError("cost resolution should not run when AI egress is off")

    monkeypatch.setattr(gateway_server, "resolve_request", unexpected_cost_resolution)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            build_runtime(
                _make_session(),
                SimpleNamespace(model=None, context={"user_id": "alice"}),
                "web",
                SimpleNamespace(),
            )
        )

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["error"] == "ai_egress_blocked"
    assert exc_info.value.detail["mode"] == "off"


def test_build_chat_runtime_web_requires_user_id(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    with pytest.raises(HTTPException, match="user_id is required for web chat") as exc_info:
        asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model=None, context={}),
                "web",
                SimpleNamespace(),
            )
        )

    assert exc_info.value.status_code == 400


def test_build_chat_runtime_telegram_requires_user_id(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    with pytest.raises(HTTPException, match="user_id is required for telegram chat") as exc_info:
        asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model=None, context={}),
                "telegram",
                SimpleNamespace(),
            )
        )

    assert exc_info.value.status_code == 400


def test_build_chat_runtime_rejects_user_id_path_traversal(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        FakeMcpClientManager(),
    )

    with pytest.raises(HTTPException, match="Invalid user_id") as exc_info:
        asyncio.run(
            build_runtime(
                SimpleNamespace(approved_tool_types=set()),
                SimpleNamespace(model=None, context={"user_id": "../../etc"}),
                "web",
                SimpleNamespace(),
            )
        )

    assert exc_info.value.status_code == 400


def test_build_chat_runtime_provisions_new_user_workspace(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    settings = _make_settings(tmp_path)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set()),
            SimpleNamespace(model=None, context={"user_id": "new-user"}),
            "web",
            SimpleNamespace(),
        )
    )

    db_path = settings.data_root / "new-user" / "finance.db"
    rules_path = settings.data_root / "new-user" / "rules.yaml"

    assert runtime.system_prompt == _WEB_SYSTEM_PROMPT
    assert db_path.exists()
    assert rules_path.exists()

    with connect(db_path=db_path) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'schema_version'"
        ).fetchone()

    assert row is not None


def test_create_app_sets_oauth_auth_config(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)

    app = gateway_server.create_app(_make_settings(tmp_path, auth_token="sk-ant-oat-token"))

    assert isinstance(app, FastAPI)
    assert captured["config"].auth_config["auth_mode"] == "oauth"
    assert captured["config"].auth_config["auth_token"] == "sk-ant-oat-token"
    assert captured["config"].auth_config["api_key"] == ""


def test_create_app_shared_auth_config_ignores_env_api_key(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "env-categorizer-key")

    app = gateway_server.create_app(_make_settings(tmp_path, auth_token="sk-ant-oat-token"))

    assert isinstance(app, FastAPI)
    assert captured["config"].auth_config["auth_mode"] == "oauth"
    assert captured["config"].auth_config["auth_token"] == "sk-ant-oat-token"
    assert captured["config"].auth_config["api_key"] == ""


def test_create_app_supports_resolver_without_global_auth_token(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakePool:
        def getconn(self):
            raise AssertionError("resolver pool should not be used in this test")

        def putconn(self, conn) -> None:
            del conn

        def closeall(self) -> None:
            captured["closed"] = True

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", lambda *args, **kwargs: FakePool())

    settings = _make_settings(
        tmp_path,
        auth_token="",
        DATABASE_URL="postgres://gateway:secret@localhost/finance",
        SESSION_SECRET="session-secret",
    )

    app = gateway_server.create_app(settings)

    assert isinstance(app, FastAPI)
    assert captured["config"].auth_config == {}
    assert callable(captured["config"].credentials_resolver)
    assert captured["config"].resolver_timeout_seconds == settings.resolver_timeout_seconds


def test_create_app_uses_lazy_gateway_postgres_pool(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakePool:
        def closeall(self) -> None:
            pass

    def fake_pool(*args, **kwargs):
        captured["pool_args"] = args
        captured["pool_kwargs"] = kwargs
        return FakePool()

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", fake_pool)

    settings = _make_settings(
        tmp_path,
        auth_token="",
        DATABASE_URL="postgres://gateway:secret@localhost/finance",
        SESSION_SECRET="session-secret",
    )

    gateway_server.create_app(settings)

    assert captured["pool_args"] == (0, 5)
    assert captured["pool_kwargs"] == {"dsn": settings.database_url}
    assert callable(captured["config"].credentials_resolver)


def test_create_app_chat_init_resolver_round_trip(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FakeCursor:
        def execute(self, query: str, params: tuple[object, ...]) -> None:
            assert "SELECT anthropic_api_key_secret_ref, anthropic_api_key_enc" in query
            assert params == ("2",)

        def fetchone(self) -> dict[str, object | None]:
            return {
                "anthropic_api_key_secret_ref": "vault://users/2/anthropic/api_key",
                "anthropic_api_key_enc": None,
            }

    class FakeConnection:
        def cursor(self, *, cursor_factory=None):
            assert cursor_factory is not None
            return FakeCursor()

    class FakePool:
        def __init__(self) -> None:
            self.connection = FakeConnection()

        def getconn(self):
            return self.connection

        def putconn(self, conn) -> None:
            assert conn is self.connection

        def closeall(self) -> None:
            pass

    def fake_get_user_api_key(
        user_id: str,
        provider: str,
        *,
        secret_ref: str | None = None,
    ) -> str:
        assert (user_id, provider, secret_ref) == (
            "2",
            "anthropic",
            "vault://users/2/anthropic/api_key",
        )
        return "sk-ant-api03-user-key"

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr("psycopg2.pool.ThreadedConnectionPool", lambda *args, **kwargs: FakePool())
    monkeypatch.setattr("finance_cli.secrets_store.get_user_api_key", fake_get_user_api_key)

    app = gateway_server.create_app(
        _make_settings(
            tmp_path,
            auth_token="",
            DATABASE_URL="postgres://gateway:secret@localhost/finance",
            SESSION_SECRET="session-secret",
            GATEWAY_USER_KEYS=json.dumps(
                [
                    {
                        "key": "gateway-key",
                        "channel": "cli",
                        "user_id": 2,
                        "email": "user2@example.test",
                        "role": "owner",
                    }
                ]
            ),
        )
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/chat/init",
            json={
                "api_key": "gateway-key",
                "user_id": "2",
                "context": {"channel": "cli"},
            },
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["user_id"] == "2"
    assert payload["session_token"]
    assert payload["session_id"]
    assert payload["expires_at"]


def test_create_app_rewrites_credentials_unavailable_init_401_to_402(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_create_gateway_app(config) -> FastAPI:
        del config
        app = FastAPI()

        @app.post("/api/chat/init")
        async def chat_init(request: Request):
            payload = await request.json()
            error = str(payload.get("error") or "credentials_unavailable")
            return JSONResponse(
                {"error": error, "message": "missing credential"},
                status_code=401,
            )

        return app

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)

    app = gateway_server.create_app(_make_settings(tmp_path))

    with TestClient(app) as client:
        response = client.post("/api/chat/init", json={})
        auth_response = client.post("/api/chat/init", json={"error": "cross_user_reuse"})

    assert response.status_code == 402
    assert response.json() == {
        "error": "credentials_unavailable",
        "message": "missing credential",
    }
    assert auth_response.status_code == 401
    assert auth_response.json()["error"] == "cross_user_reuse"


def test_build_chat_runtime_handles_empty_global_auth_config(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(
        tmp_path,
        auth_token="",
        DATABASE_URL="postgres://gateway:secret@localhost/finance",
        SESSION_SECRET="session-secret",
    )

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["auth_config"] = kwargs["auth_config"]

    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())

    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "telegram",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-no-global-auth")

    assert captured["auth_config"] == {}


def test_build_chat_runtime_reraises_lease_unavailable_when_enforced(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _make_settings(tmp_path)
    monkeypatch.setenv("STORAGE_LEASE_ENFORCE", "true")

    def fail_lease(*args, **kwargs):
        raise gateway_server.LeaseUnavailableError("lease infra unavailable")

    monkeypatch.setattr(gateway_server, "_ensure_gateway_storage_lease", fail_lease)
    build_runtime = gateway_server._make_build_chat_runtime(
        settings,
        FakeMcpClientManager(),
        lease_session_manager=object(),
    )

    with pytest.raises(gateway_server.LeaseUnavailableError):
        asyncio.run(
            build_runtime(
                _make_session(background_tasks={}),
                SimpleNamespace(model=None, context={"user_id": "alice"}),
                "web",
                SimpleNamespace(),
            )
        )
    assert not (settings.data_root / "alice").exists()


def test_create_app_sets_on_event_callback(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)

    gateway_server.create_app(_make_settings(tmp_path))

    assert callable(captured["config"].on_event)


def test_create_app_on_event_logs_interceptor_decision(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_create_gateway_app(config) -> FastAPI:
        captured["config"] = config
        return FastAPI()

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)

    gateway_server.create_app(_make_settings(tmp_path))

    target_logger = logging.getLogger("finance_cli.gateway.server")
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = lambda record: records.append(record)  # type: ignore[assignment]
    handler.setLevel(logging.WARNING)
    original_level = target_logger.level
    target_logger.addHandler(handler)
    target_logger.setLevel(logging.WARNING)
    try:
        captured["config"].on_event(
            {
                "type": "interceptor_decision",
                "action": "deny",
                "code": "rate_limit_exceeded",
                "message": "Too many tool calls",
            },
            "session-1234567890",
        )
    finally:
        target_logger.removeHandler(handler)
        target_logger.setLevel(original_level)

    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    assert "interceptor_decision" in records[0].getMessage()
    assert "rate_limit_exceeded" in records[0].getMessage()
    assert "Too many tool calls" in records[0].getMessage()


def test_make_startup_skips_validation_for_oauth_keys(tmp_path: Path) -> None:
    mcp = FakeMcpClientManager()
    settings = _make_settings(tmp_path, auth_token="sk-ant-oat-token")
    startup = gateway_server._make_startup(settings, mcp)

    asyncio.run(startup())

    assert mcp.started is True


def test_dispatcher_strips_approval_reason() -> None:
    """_approval_reason should be stripped before reaching the MCP tool."""
    captured: dict[str, Any] = {}

    class FakeMcp:
        def is_mcp_tool(self, name):
            return True

        def get_server_for_tool(self, name):
            return "fake-server"

        async def call_tool(self, tool_name, tool_input):
            captured["tool_input"] = dict(tool_input)
            return [SimpleNamespace(text="{}", type="text")], None

    dispatcher = gateway_server.UserScopedDispatcher(mcp_client=FakeMcp())
    asyncio.run(
        dispatcher.dispatch(
            "call-1",
            "txn_list",
            {
                "limit": 10,
                "_approval_reason": "Reducing budget as requested",
                "_request_id": "evil-request",
                "_session_id": "evil-session",
                "_storage_mode": "remote",
                "_storage_lease_id": "evil-lease",
                "_user_db_path": "/should/be/stripped",
            },
        )
    )
    assert "_approval_reason" not in captured["tool_input"]
    assert "_request_id" not in captured["tool_input"]
    assert "_session_id" not in captured["tool_input"]
    assert "_storage_mode" not in captured["tool_input"]
    assert "_storage_lease_id" not in captured["tool_input"]
    assert "_user_db_path" not in captured["tool_input"]
    assert captured["tool_input"]["limit"] == 10


def test_dispatcher_replaces_model_supplied_request_context(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    async def fake_dispatch(self, tool_call_id, tool_name, tool_input, *, call_index=0):
        del self, tool_call_id, tool_name, call_index
        captured["tool_input"] = dict(tool_input)
        return None

    monkeypatch.setattr(gateway_server.ToolDispatcher, "dispatch", fake_dispatch)

    dispatcher = gateway_server.UserScopedDispatcher(
        mcp_client=SimpleNamespace(),
        user_paths={
            "_user_id": "safe-user-42",
            "_user_db_path": "/safe/db.sqlite3",
            "_request_id": "req-safe",
            "_session_id": "sess-safe",
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
                "_request_id": "req-evil",
                "_session_id": "sess-evil",
                "_storage_mode": "remote",
                "_storage_lease_id": "evil-lease",
                "limit": 5,
            },
        )
    )

    assert captured["tool_input"] == {
        "limit": 5,
        "_user_id": "safe-user-42",
        "_user_db_path": "/safe/db.sqlite3",
        "_request_id": "req-safe",
        "_session_id": "sess-safe",
        "_storage_mode": "local",
        "_storage_lease_id": "safe-lease",
    }


def test_dispatcher_transport_failure_is_captured(monkeypatch) -> None:
    seen: dict[str, Any] = {}

    async def fake_dispatch(self, tool_call_id, tool_name, tool_input, *, call_index=0):
        del self, tool_call_id, tool_name, tool_input, call_index
        raise RuntimeError("transport down")

    def fake_capture_error(exc, **kwargs):
        seen["exc"] = exc
        seen["kwargs"] = kwargs
        return "err-1"

    monkeypatch.setattr(gateway_server.ToolDispatcher, "dispatch", fake_dispatch)
    monkeypatch.setattr(gateway_server, "capture_error", fake_capture_error)

    dispatcher = gateway_server.UserScopedDispatcher(
        mcp_client=SimpleNamespace(),
        user_paths={"_user_db_path": "/safe/db.sqlite3"},
    )

    with pytest.raises(RuntimeError, match="transport down"):
        asyncio.run(dispatcher.dispatch("tool-1", "goal_list", {"limit": 5}))

    assert seen["kwargs"]["source"] == "gateway"
    assert seen["kwargs"]["endpoint"] == "goal_list"
    assert seen["kwargs"]["db_path"] == "/safe/db.sqlite3"


def test_create_app_sets_request_context_from_forwarded_headers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_create_gateway_app(_config) -> FastAPI:
        app = FastAPI()

        @app.get("/probe")
        async def probe(request: Request) -> dict[str, str | None]:
            return {
                "request_id": gateway_server.get_request_id(),
                "session_id": gateway_server.get_session_id(),
                "conversation_id": gateway_server._conversation_id_var.get(None),
                "state_request_id": getattr(request.state, "request_id", None),
                "state_session_id": getattr(request.state, "session_id", None),
                "state_conversation_id": getattr(request.state, "conversation_id", None),
            }

        return app

    monkeypatch.setattr(gateway_server, "McpClientManager", FakeMcpClientManager)
    monkeypatch.setattr(gateway_server, "create_gateway_app", fake_create_gateway_app)

    app = gateway_server.create_app(_make_settings(tmp_path))
    with TestClient(app) as client:
        response = client.get(
            "/probe",
            headers={
                "X-Request-ID": "req-42",
                "X-Session-ID": "sess-42",
                "X-Conversation-ID": "conv-42",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "request_id": "req-42",
        "session_id": "sess-42",
        "conversation_id": "conv-42",
        "state_request_id": "req-42",
        "state_session_id": "sess-42",
        "state_conversation_id": "conv-42",
    }
