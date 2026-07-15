from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any


def test_coach_retirement_income_readiness_auto_approved_set_is_routine_only() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED == frozenset(
        {
            "agent_session_write",
            "coach_retirement_income_readiness_artifact_save",
            "skill_state_clear",
            "skill_state_set",
        }
    )
    assert (
        "coach_retirement_income_readiness_artifact_read"
        not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    )
    assert "goal_set" not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    assert "budget_set" not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    assert "notify_schedule" not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    assert (
        "setup_monthly_transfer_goal"
        not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    )
    assert (
        "set_monthly_retirement_target"
        not in tools.COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
    )


def test_other_skill_auto_approved_sets_do_not_include_retirement_income_save() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    artifact_save = "coach_retirement_income_readiness_artifact_save"

    assert artifact_save not in tools.ONBOARDING_AUTO_APPROVED
    assert artifact_save not in tools.COACH_DEBT_PAYOFF_AUTO_APPROVED
    assert artifact_save not in tools.COACH_EMERGENCY_FUND_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SAVINGS_GOAL_AUTO_APPROVED
    assert artifact_save not in tools.COACH_SPENDING_PLAN_AUTO_APPROVED
    assert artifact_save not in tools.COACH_TAX_READINESS_AUTO_APPROVED
    assert artifact_save not in tools.COACH_HOMEBUYING_READINESS_AUTO_APPROVED
    assert (
        artifact_save
        not in tools.COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
    )
    assert artifact_save not in tools.COACH_INVESTMENT_READINESS_AUTO_APPROVED
    assert artifact_save not in tools.COACH_ESTATE_DOCUMENT_READINESS_AUTO_APPROVED
    assert artifact_save not in tools.COACH_FINANCIAL_PLAN_INTAKE_AUTO_APPROVED
    assert artifact_save not in tools.COACH_RISK_INSURANCE_READINESS_AUTO_APPROVED
    assert artifact_save not in tools.COACH_ADVISOR_HANDOFF_READINESS_AUTO_APPROVED


def test_retirement_income_read_tool_is_read_only_and_save_requires_approval() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools

    assert "coach_retirement_income_readiness_artifact_read" in tools.READ_ONLY_TOOLS
    assert (
        "coach_retirement_income_readiness_artifact_save"
        in tools.APPROVAL_REQUIRED_TOOLS
    )


def test_retirement_income_readiness_is_valid_non_activatable_skill() -> None:
    from finance_cli.gateway import tools

    assert "coach_retirement_income_readiness" in tools.VALID_SKILLS
    assert "coach_retirement_income_readiness" in tools._NON_ACTIVATABLE_SKILLS


class _FakeMcpClientManager:
    def get_tool_definitions(self) -> list[dict[str, str]]:
        return [{"name": "goal_list", "description": "List goals"}]


class _FakeChatRuntime:
    def __init__(self, **kwargs: Any) -> None:
        self.build_dispatcher = kwargs["build_dispatcher"]


def _make_settings(tmp_path: Path):
    from finance_cli.gateway.config import GatewaySettings

    template_rules = tmp_path / "rules-template.yaml"
    template_rules.write_text("keyword_rules: []\n", encoding="utf-8")
    return GatewaySettings(
        ANTHROPIC_AUTH_TOKEN="sk-ant-oat-shared-token",
        GATEWAY_USER_KEYS=json.dumps(
            [
                {
                    "key": "gateway-key",
                    "channel": "cli",
                    "user_id": 1,
                    "email": "user@example.test",
                    "role": "owner",
                }
            ]
        ),
        FINANCE_GATEWAY_JWT_SECRET="jwt-secret-for-tests-at-least-32-bytes",
        FINANCE_GATEWAY_DATA_ROOT=tmp_path / "users",
        FINANCE_GATEWAY_RULES_TEMPLATE=template_rules,
    )


def test_retirement_income_readiness_runtime_auto_approves_only_own_routine_tools(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from finance_cli.gateway import server as gateway_server

    captured: dict[str, Any] = {}

    class FakeDispatcher:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(gateway_server, "ChatRuntime", _FakeChatRuntime)
    monkeypatch.setattr(gateway_server, "UserScopedDispatcher", FakeDispatcher)
    monkeypatch.setattr(
        gateway_server,
        "build_system_prompt",
        lambda **_kwargs: "test prompt",
    )
    build_runtime = gateway_server._make_build_chat_runtime(
        _make_settings(tmp_path),
        _FakeMcpClientManager(),
    )

    runtime = asyncio.run(
        build_runtime(
            SimpleNamespace(approved_tool_types=set(), session_id="sess-income"),
            SimpleNamespace(
                model=None,
                context={
                    "user_id": "1",
                    "skill": "coach_retirement_income_readiness",
                },
            ),
            "cli",
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
        "coach_retirement_income_readiness_artifact_save",
        {},
        "",
    ) is False
    assert captured["needs_approval"]("skill_state_set", {}, "") is False
    assert captured["needs_approval"](
        "coach_retirement_contribution_readiness_artifact_save",
        {},
        "",
    ) is True
    assert captured["needs_approval"](
        "coach_investment_readiness_artifact_save",
        {},
        "",
    ) is True
    assert captured["needs_approval"](
        "coach_financial_plan_intake_artifact_save",
        {},
        "",
    ) is True
    assert captured["needs_approval"]("goal_set", {}, "") is True
    assert captured["needs_approval"]("budget_set", {}, "") is True
    assert captured["needs_approval"]("setup_monthly_transfer_goal", {}, "") is True
