from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import mcp.types as mt
import pytest
from fastmcp.server.auth import AccessToken
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools.tool import ToolResult

import finance_cli.mcp_remote as mcp_remote


def _cursor_context(cursor: mock.MagicMock) -> mock.MagicMock:
    context = mock.MagicMock()
    context.__enter__.return_value = cursor
    context.__exit__.return_value = False
    return context


async def _call_middleware(
    tmp_path: Path,
    *,
    tool_name: str,
    user_snapshot: dict[str, object] | Exception,
) -> bool:
    called = False

    async def call_next(_context):
        nonlocal called
        called = True
        return ToolResult(content=[])

    middleware = mcp_remote.RemoteUserMiddleware(
        data_root=tmp_path,
        template_rules_path=mcp_remote._TEMPLATE_RULES_PATH,
    )
    context = MiddlewareContext(
        message=mt.CallToolRequestParams(name=tool_name, arguments={})
    )
    token = AccessToken(
        token="token",
        client_id="client-id",
        scopes=["openid"],
        claims={"sub": "google-sub", "email": "u@example.com", "name": "User"},
    )
    load_patch = (
        mock.patch("finance_cli.mcp_remote._load_user_billing_snapshot", side_effect=user_snapshot)
        if isinstance(user_snapshot, Exception)
        else mock.patch("finance_cli.mcp_remote._load_user_billing_snapshot", return_value=user_snapshot)
    )

    with (
        mock.patch("finance_cli.mcp_remote.get_access_token", return_value=token),
        mock.patch("finance_cli.mcp_remote._resolve_user_id", return_value="42"),
        mock.patch("finance_cli.mcp_remote.provision_user", return_value={"db_path": str(tmp_path / "finance.db")}),
        load_patch,
    ):
        await middleware.on_call_tool(context, call_next)

    return called


@pytest.mark.parametrize("tool_name", ["txn_list", "cat_auto_categorize", "budget_set", "unknown_tool"])
def test_unexpired_trial_allows_any_tool(tmp_path: Path, tool_name: str) -> None:
    called = asyncio.run(
        _call_middleware(
            tmp_path,
            tool_name=tool_name,
            user_snapshot={
                "tier": "trial",
                "trial_ends_at": datetime.now(timezone.utc) + timedelta(days=1),
                "lifetime_deal": False,
            },
        )
    )

    assert called is True


@pytest.mark.parametrize("tool_name", ["txn_list"])
def test_expired_trial_allows_read_only_tools(tmp_path: Path, tool_name: str) -> None:
    called = asyncio.run(
        _call_middleware(
            tmp_path,
            tool_name=tool_name,
            user_snapshot={
                "tier": "trial",
                "trial_ends_at": datetime.now(timezone.utc) - timedelta(days=1),
                "lifetime_deal": False,
            },
        )
    )

    assert called is True


@pytest.mark.parametrize("tool_name", ["cat_auto_categorize", "budget_set"])
def test_expired_trial_blocks_gated_tools(tmp_path: Path, tool_name: str) -> None:
    with pytest.raises(PermissionError, match="Resubscribe"):
        asyncio.run(
            _call_middleware(
                tmp_path,
                tool_name=tool_name,
                user_snapshot={
                    "tier": "trial",
                    "trial_ends_at": datetime.now(timezone.utc) - timedelta(days=1),
                    "lifetime_deal": False,
                },
            )
        )


@pytest.mark.parametrize("tool_name", ["txn_categorize", "export_csv", "rules_add_keyword"])
def test_cancelled_allows_spec_read_only_and_manual_categorization_tools(tmp_path: Path, tool_name: str) -> None:
    assert asyncio.run(
        _call_middleware(
            tmp_path,
            tool_name=tool_name,
            user_snapshot={"tier": "cancelled", "trial_ends_at": None, "lifetime_deal": False},
        )
    )


@pytest.mark.parametrize("tool_name", ["plaid_sync", "cat_auto_categorize", "biz_pl", "interventions_get", "unknown_tool"])
def test_cancelled_blocks_gated_or_unknown_tools(tmp_path: Path, tool_name: str) -> None:
    with pytest.raises(PermissionError, match="Resubscribe"):
        asyncio.run(
            _call_middleware(
                tmp_path,
                tool_name=tool_name,
                user_snapshot={"tier": "cancelled", "trial_ends_at": None, "lifetime_deal": False},
            )
        )


def test_lifetime_allows_unknown_tool(tmp_path: Path) -> None:
    assert asyncio.run(
        _call_middleware(
            tmp_path,
            tool_name="unknown_tool",
            user_snapshot={"tier": "lifetime", "trial_ends_at": None, "lifetime_deal": True},
        )
    )


def test_deleted_user_blocks_any_tool(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="Account is not accessible"):
        asyncio.run(
            _call_middleware(
                tmp_path,
                tool_name="txn_list",
                user_snapshot=PermissionError("Account is not accessible"),
            )
        )


def test_resolve_user_id_insert_applies_trial_cost_cap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [None, None, (11, True)]
    conn.cursor.return_value = _cursor_context(cursor)

    with (
        mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn),
        mock.patch("finance_cli.mcp_remote.provision_user", return_value={"db_path": str(tmp_path / "11" / "finance.db")}) as provision,
        mock.patch("finance_cli.mcp_remote.apply_trial_cost_cap") as apply_cap,
    ):
        user_id = mcp_remote._resolve_user_id(
            "google-sub",
            "u@example.com",
            "User",
            data_root=tmp_path,
            template_rules_path=mcp_remote._TEMPLATE_RULES_PATH,
        )

    assert user_id == "11"
    provision.assert_called_once()
    apply_cap.assert_called_once_with(tmp_path / "11" / "finance.db")


def test_resolve_user_id_conflict_insert_does_not_apply_trial_cost_cap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [None, None, (11, False)]
    conn.cursor.return_value = _cursor_context(cursor)

    with (
        mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn),
        mock.patch("finance_cli.mcp_remote.provision_user") as provision,
        mock.patch("finance_cli.mcp_remote.apply_trial_cost_cap") as apply_cap,
    ):
        user_id = mcp_remote._resolve_user_id(
            "google-sub",
            "u@example.com",
            "User",
            data_root=tmp_path,
            template_rules_path=mcp_remote._TEMPLATE_RULES_PATH,
        )

    assert user_id == "11"
    provision.assert_not_called()
    apply_cap.assert_not_called()
