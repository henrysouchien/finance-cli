from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from finance_cli import tool_registry
from finance_cli.tool_registry import ToolMetadata, iter_registry, register, validate_registry

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _restore_registry_state():
    registered = set(tool_registry._REGISTERED_TOOL_NAMES)
    registry = dict(tool_registry._TOOL_REGISTRY)
    tool_registry._REGISTERED_TOOL_NAMES.clear()
    tool_registry._TOOL_REGISTRY.clear()
    try:
        yield
    finally:
        tool_registry._REGISTERED_TOOL_NAMES.clear()
        tool_registry._REGISTERED_TOOL_NAMES.update(registered)
        tool_registry._TOOL_REGISTRY.clear()
        tool_registry._TOOL_REGISTRY.update(registry)


def test_tool_metadata_rejects_onboarding_without_approval() -> None:
    with pytest.raises(ValueError, match="onboarding_auto_approved requires approval_required=True"):
        ToolMetadata(sync_behavior="no_sync", onboarding_auto_approved=True)


def test_tool_metadata_rejects_investment_readiness_auto_approval_without_approval() -> None:
    with pytest.raises(
        ValueError,
        match="coach_investment_readiness_auto_approved requires approval_required=True",
    ):
        ToolMetadata(
            sync_behavior="no_sync",
            coach_investment_readiness_auto_approved=True,
        )


def test_tool_metadata_rejects_retirement_income_auto_approval_without_approval() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "coach_retirement_income_readiness_auto_approved "
            "requires approval_required=True"
        ),
    ):
        ToolMetadata(
            sync_behavior="no_sync",
            coach_retirement_income_readiness_auto_approved=True,
        )


def test_tool_metadata_rejects_financial_plan_intake_auto_approval_without_approval() -> None:
    with pytest.raises(
        ValueError,
        match="coach_financial_plan_intake_auto_approved requires approval_required=True",
    ):
        ToolMetadata(
            sync_behavior="no_sync",
            coach_financial_plan_intake_auto_approved=True,
        )


def test_tool_metadata_rejects_risk_insurance_auto_approval_without_approval() -> None:
    with pytest.raises(
        ValueError,
        match="coach_risk_insurance_readiness_auto_approved requires approval_required=True",
    ):
        ToolMetadata(
            sync_behavior="no_sync",
            coach_risk_insurance_readiness_auto_approved=True,
        )


def test_tool_metadata_rejects_advisor_handoff_auto_approval_without_approval() -> None:
    with pytest.raises(
        ValueError,
        match="coach_advisor_handoff_readiness_auto_approved requires approval_required=True",
    ):
        ToolMetadata(
            sync_behavior="no_sync",
            coach_advisor_handoff_readiness_auto_approved=True,
        )


def test_tool_metadata_rejects_db_write_read_only() -> None:
    with pytest.raises(ValueError, match="db_write tools cannot be read_only"):
        ToolMetadata(sync_behavior="db_write", read_only=True)


def test_register_name_rejects_duplicates() -> None:
    tool_registry._register_name("dup_tool")

    with pytest.raises(RuntimeError, match="registered twice"):
        tool_registry._register_name("dup_tool")


def test_register_stores_metadata_and_rejects_duplicates() -> None:
    meta = ToolMetadata(sync_behavior="server_proxied", read_only=True)

    register("tool_a", meta)

    assert dict(iter_registry()) == {"tool_a": meta}

    with pytest.raises(RuntimeError, match="registered twice"):
        register("tool_a", meta)


def test_validate_registry_warns_for_unclassified_tools(monkeypatch: pytest.MonkeyPatch) -> None:
    tool_registry._register_name("missing_meta")
    messages: list[str] = []

    monkeypatch.setattr(
        tool_registry.logger,
        "warning",
        lambda msg, *args: messages.append(msg % args if args else msg),
    )
    validate_registry({"missing_meta"}, strict=False)

    assert messages == ["tool_registry warn: 1 unclassified tools"]


def test_validate_registry_strict_raises_for_unclassified_tools() -> None:
    tool_registry._register_name("missing_meta")

    with pytest.raises(RuntimeError, match="1 unclassified tools"):
        validate_registry({"missing_meta"}, strict=True)


def test_validate_registry_warns_for_classified_tool_not_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register("ghost_tool", ToolMetadata(sync_behavior="no_sync"))
    messages: list[str] = []

    monkeypatch.setattr(
        tool_registry.logger,
        "warning",
        lambda msg, *args: messages.append(msg % args if args else msg),
    )
    validate_registry(set(), strict=False)

    assert messages == ["tool_registry warn: 1 classified tools not registered: ['ghost_tool']"]


def test_clear_resets_registry_and_derived_caches() -> None:
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync import tool_classification

    gateway_tools._all.cache_clear()
    gateway_tools._derived.cache_clear()
    tool_classification._all.cache_clear()
    tool_classification._derived.cache_clear()
    gateway_tools._derived("READ_ONLY_TOOLS")
    tool_classification._derived("NO_SYNC_TOOLS")
    tool_registry._register_name("stale_tool")
    register("stale_tool", ToolMetadata(sync_behavior="no_sync"))

    assert gateway_tools._all.cache_info().currsize == 1
    assert gateway_tools._derived.cache_info().currsize == 1
    assert tool_classification._all.cache_info().currsize == 1
    assert tool_classification._derived.cache_info().currsize == 1

    tool_registry.clear()

    assert tool_registry._REGISTERED_TOOL_NAMES == set()
    assert tool_registry._TOOL_REGISTRY == {}
    assert gateway_tools._all.cache_info().currsize == 0
    assert gateway_tools._derived.cache_info().currsize == 0
    assert tool_classification._all.cache_info().currsize == 0
    assert tool_classification._derived.cache_info().currsize == 0


def test_clear_does_not_import_derived_cache_modules() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """\
                import sys
                from finance_cli import tool_registry

                sys.modules.pop("finance_cli.gateway.tools", None)
                sys.modules.pop("finance_cli.sync.tool_classification", None)

                tool_registry.clear()

                assert "finance_cli.gateway.tools" not in sys.modules
                assert "finance_cli.sync.tool_classification" not in sys.modules
                """
            ),
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_mcp_tool_registry_kwargs_smoke() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """\
                import asyncio

                from fastmcp import FastMCP

                from finance_cli import tool_registry
                import finance_cli.mcp_server as mcp_server

                tool_registry._REGISTERED_TOOL_NAMES.clear()
                tool_registry._TOOL_REGISTRY.clear()

                fresh_mcp = FastMCP("tool-registry-smoke")
                mcp_server._orig_mcp_tool = fresh_mcp.tool

                @mcp_server._tool_with_coercion(sync_behavior="db_write")
                def smoke_registry_tool() -> dict:
                    return {}

                assert "smoke_registry_tool" in tool_registry._REGISTERED_TOOL_NAMES
                assert tool_registry._TOOL_REGISTRY["smoke_registry_tool"].sync_behavior == "db_write"
                assert {
                    tool.name
                    for tool in asyncio.run(fresh_mcp.list_tools(run_middleware=False))
                } == {"smoke_registry_tool"}
                """
            ),
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr
