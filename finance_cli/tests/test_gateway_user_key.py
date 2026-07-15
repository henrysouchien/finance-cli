from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from agent_gateway import AuthConfig, MissingUserIdError

from finance_cli.gateway import server as gateway_server
from finance_cli.gateway.config import GatewaySettings
from finance_cli.gateway.prompt import _BASE_SYSTEM_PROMPT, _WEB_SYSTEM_PROMPT
from finance_cli.gateway.user_keys import load_gateway_user_key_set


class FakeMcpClientManager:
    def get_tool_definitions(self) -> list[dict[str, Any]]:
        return [{"name": "goal_list"}]


class FakeChatRuntime:
    def __init__(self, **kwargs) -> None:
        self.system_prompt = kwargs["system_prompt"]
        self.build_runner = kwargs["build_runner"]
        self.get_tool_definitions = kwargs["get_tool_definitions"]
        self.build_dispatcher = kwargs["build_dispatcher"]
        self.model_override = kwargs["model_override"]
        self.excluded_tools = kwargs["excluded_tools"]
        self.max_turns = kwargs["max_turns"]


def _make_settings(
    tmp_path: Path,
    auth_token: str = "sk-ant-oat-shared-token",
) -> GatewaySettings:
    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")
    return GatewaySettings(
        **{
            "ANTHROPIC_AUTH_TOKEN": auth_token,
            "GATEWAY_USER_KEYS": json.dumps(
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
            "FINANCE_GATEWAY_JWT_SECRET": "jwt-secret",
            "FINANCE_GATEWAY_HOST": "127.0.0.1",
            "FINANCE_GATEWAY_PORT": 8002,
            "FINANCE_GATEWAY_DATA_ROOT": tmp_path / "users",
            "FINANCE_GATEWAY_RULES_TEMPLATE": template_rules,
        }
    )


def _make_session(*, auth_config: AuthConfig | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        approved_tool_types=set(),
        session_id="sess-test",
        result_queue=asyncio.Queue(),
        auth_config=auth_config,
    )


def _make_user_key_set():
    return load_gateway_user_key_set(
        json.dumps(
            [
                {
                    "key": "web-user-key",
                    "channel": "web",
                    "user_id": 1,
                    "email": "user1@example.test",
                    "role": "owner",
                },
                {
                    "key": "cli-user-key",
                    "channel": "cli",
                    "user_id": 1,
                    "email": "user1@example.test",
                    "role": "invite",
                },
            ]
        )
    )


def test_credentials_resolver_binds_identity_to_gateway_user_key() -> None:
    resolver = gateway_server._make_credentials_resolver(
        get_session_fn=None,
        session_secret="",
        fallback_auth_token="sk-ant-oat-shared-token",
        model="claude-sonnet-4-6",
        max_tokens=16000,
        thinking=True,
        key_set=_make_user_key_set(),
    )

    result = asyncio.run(
        resolver(
            "cli-user-key",
            SimpleNamespace(user_id="1", context={"channel": "cli"}),
        )
    )

    assert result.user_id == "1"
    assert result.risk_user_id == 1
    assert result.channel == "cli"
    assert result.role == "invite"
    assert result.user_email == "user1@example.test"


def test_credentials_resolver_rejects_user_mismatch() -> None:
    resolver = gateway_server._make_credentials_resolver(
        get_session_fn=None,
        session_secret="",
        fallback_auth_token="sk-ant-oat-shared-token",
        model="claude-sonnet-4-6",
        max_tokens=16000,
        thinking=True,
        key_set=_make_user_key_set(),
    )

    with pytest.raises(MissingUserIdError, match="different user_id"):
        asyncio.run(
            resolver(
                "web-user-key",
                SimpleNamespace(user_id="2", context={"channel": "web"}),
            )
        )


def test_credentials_resolver_rejects_channel_mismatch() -> None:
    resolver = gateway_server._make_credentials_resolver(
        get_session_fn=None,
        session_secret="",
        fallback_auth_token="sk-ant-oat-shared-token",
        model="claude-sonnet-4-6",
        max_tokens=16000,
        thinking=True,
        key_set=_make_user_key_set(),
    )

    with pytest.raises(MissingUserIdError, match="different channel"):
        asyncio.run(
            resolver(
                "web-user-key",
                SimpleNamespace(user_id="1", context={"channel": "telegram"}),
            )
        )


def test_session_auth_config_is_used_by_runner(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)
    session_auth = AuthConfig.from_dict(
        {
            "provider": "anthropic",
            "billing_mode": "byok",
            "model": settings.model,
            "max_tokens": settings.max_tokens,
            "thinking": settings.thinking,
            "api_key": "sk-ant-user-1234",
        }
    )

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["auth_config"] = kwargs["auth_config"]

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, **kwargs: _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT,
    )

    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    runtime = asyncio.run(
        build_runtime(
            _make_session(auth_config=session_auth),
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert captured["auth_config"] is session_auth


def test_session_auth_config_to_dict_drives_usage_metadata(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)

    class ToDictAuth:
        def to_dict(self) -> dict[str, Any]:
            return {
                "billing_mode": "byok",
                "rate_table_version": "custom-v2",
            }

    session_auth = ToDictAuth()

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["auth_config"] = kwargs["auth_config"]
        captured["billing_mode"] = kwargs["billing_mode"]
        captured["rate_table_version"] = kwargs["rate_table_version"]

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, **kwargs: _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT,
    )

    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    runtime = asyncio.run(
        build_runtime(
            _make_session(auth_config=session_auth),  # type: ignore[arg-type]
            SimpleNamespace(model=None, context={"user_id": "alice"}),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert captured["auth_config"] is session_auth
    assert captured["billing_mode"] == "byok"
    assert captured["rate_table_version"] == "custom-v2"


def test_runner_falls_back_to_global_auth_config(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path, auth_token="sk-ant-oat-shared-token")

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["auth_config"] = kwargs["auth_config"]

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, **kwargs: _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT,
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
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert captured["auth_config"] == {
        "model": settings.model,
        "max_tokens": settings.max_tokens,
        "thinking": settings.thinking,
        "auth_mode": "oauth",
        "api_key": "",
        "auth_token": "sk-ant-oat-shared-token",
    }


def test_context_anthropic_key_is_ignored_by_runner(tmp_path: Path, monkeypatch) -> None:
    captured: dict[str, Any] = {}
    settings = _make_settings(tmp_path)

    def fake_agent_runner_init(self, *args, **kwargs) -> None:
        captured["auth_config"] = kwargs["auth_config"]

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(gateway_server.AgentRunner, "__init__", fake_agent_runner_init)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, **kwargs: _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT,
    )

    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    runtime = asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(
                model=None,
                context={"user_id": "alice", "anthropic_api_key": "sk-ant-user-secret"},
            ),
            "web",
            SimpleNamespace(),
        )
    )
    runtime.build_runner(gateway_server.EventLog(), "sess-1")

    assert captured["auth_config"]["auth_token"] == settings.anthropic_auth_token
    assert captured["auth_config"]["api_key"] == ""


def test_user_anthropic_key_is_not_forwarded_to_prompt_builder(tmp_path: Path, monkeypatch) -> None:
    seen_prompt_kwargs: dict[str, Any] = {}
    settings = _make_settings(tmp_path)

    monkeypatch.setattr(gateway_server, "ChatRuntime", FakeChatRuntime)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda channel=None, **kwargs: seen_prompt_kwargs.update(kwargs) or (
            _WEB_SYSTEM_PROMPT if channel == "web" else _BASE_SYSTEM_PROMPT
        ),
    )

    build_runtime = gateway_server._make_build_chat_runtime(settings, FakeMcpClientManager())
    asyncio.run(
        build_runtime(
            _make_session(),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "alice",
                    "anthropic_api_key": "sk-ant-user-secret",
                    "skill": "onboarding",
                    "sample_rows": ["row-1"],
                },
            ),
            "web",
            SimpleNamespace(),
        )
    )

    assert "sk-ant-user-secret" not in json.dumps(seen_prompt_kwargs, default=str)
