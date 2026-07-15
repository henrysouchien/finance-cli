from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request
from pathlib import Path
from unittest import mock

import pytest

import finance_cli.mcp_remote as mcp_remote

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _remote_test_env(tmpdir: str) -> dict[str, str]:
    env = os.environ.copy()
    env["FINANCE_CLI_DISABLE_DOTENV"] = "1"
    env["GOOGLE_CLIENT_ID"] = "test-client-id.apps.googleusercontent.com"
    env["GOOGLE_CLIENT_SECRET"] = "test-secret"
    env["FASTMCP_HOME"] = str(Path(tmpdir) / "fastmcp")
    env["FINANCE_CLI_DB"] = str(Path(tmpdir) / "finance.db")
    env["FINANCE_GATEWAY_DATA_ROOT"] = str(Path(tmpdir) / "users")
    pythonpath = [str(_PROJECT_ROOT / "finance-web"), str(_PROJECT_ROOT)]
    if env.get("PYTHONPATH"):
        pythonpath.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(pythonpath)
    return env


def _run_configure_remote_subprocess(source: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _remote_test_env(tmpdir)
        return subprocess.run(
            [sys.executable, "-c", textwrap.dedent(source)],
            capture_output=True,
            cwd=_PROJECT_ROOT,
            env=env,
            text=True,
        )


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_json(url: str) -> dict[str, object]:
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


def _cursor_context(cursor: mock.MagicMock) -> mock.MagicMock:
    context = mock.MagicMock()
    context.__enter__.return_value = cursor
    context.__exit__.return_value = False
    return context


def test_configure_remote_removes_excluded_tools() -> None:
    result = _run_configure_remote_subprocess(
        """\
        import asyncio
        from finance_cli.mcp_remote import REMOTE_EXCLUDED_TOOLS, configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings
        from finance_cli.mcp_server import mcp

        configure_remote(McpRemoteSettings.from_env())
        tools = asyncio.run(mcp.list_tools())
        tool_names = {tool.name for tool in tools}
        leaked = REMOTE_EXCLUDED_TOOLS & tool_names
        assert not leaked, f"Excluded tools still visible: {sorted(leaked)}"
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_prunes_without_fastmcp_remove_tool_deprecation() -> None:
    result = _run_configure_remote_subprocess(
        """\
        import warnings
        from finance_cli.mcp_remote import configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            configure_remote(McpRemoteSettings.from_env())
        messages = [str(item.message) for item in caught]
        assert not any("remove_tool()" in message and "deprecated" in message for message in messages), messages
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_inserts_remote_user_middleware_first() -> None:
    result = _run_configure_remote_subprocess(
        """\
        from finance_cli.mcp_remote import configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings
        from finance_cli.mcp_server import mcp

        configure_remote(McpRemoteSettings.from_env())
        names = [type(middleware).__name__ for middleware in mcp.middleware]
        assert names[:4] == [
            "RemoteUserMiddleware",
            "DereferenceRefsMiddleware",
            "UserContextMiddleware",
            "OperationLogMiddleware",
        ], names
        assert names[4] == "PathSanitizeMiddleware", names
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_registers_google_auth() -> None:
    result = _run_configure_remote_subprocess(
        """\
        from fastmcp.server.auth.providers.google import GoogleProvider
        from finance_cli.mcp_remote import GoogleProviderNoCIMD, configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings
        from finance_cli.mcp_server import mcp

        configure_remote(McpRemoteSettings.from_env())
        assert isinstance(mcp.auth, GoogleProvider)
        assert isinstance(mcp.auth, GoogleProviderNoCIMD)
        assert mcp.auth._cimd_manager is None
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_installs_cashnerd_consent_page() -> None:
    result = _run_configure_remote_subprocess(
        """\
        from fastmcp.server.auth.oauth_proxy import consent
        from finance_cli.mcp_remote import configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings

        configure_remote(McpRemoteSettings.from_env())
        html = consent.create_consent_html(
            client_id="claude-code",
            redirect_uri="http://localhost:5173/callback",
            scopes=[
                "openid",
                "https://www.googleapis.com/auth/userinfo.email",
            ],
            txn_id="txn-123",
            csrf_token="csrf-123",
            client_name="Claude Code",
        )
        assert "Connect CashNerd to your MCP client" in html
        assert "Secure MCP authorization" in html
        assert "Claude Code" in html
        assert "Read your email address" in html
        assert "http://localhost:5173/callback" in html
        assert 'name="action" value="approve"' in html
        assert 'name="action" value="deny"' in html
        assert "Application Access Request" not in html
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_registers_revocation_handler_when_database_url_present() -> (
    None
):
    result = _run_configure_remote_subprocess(
        """\
        import os
        from finance_cli.mcp_remote import configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings
        from finance_cli.plaid_client import _get_revocation_failure_handler

        os.environ["DATABASE_URL"] = "postgresql://db.example/finance"
        configure_remote(McpRemoteSettings.from_env())
        assert _get_revocation_failure_handler() is not None
        """
    )

    assert result.returncode == 0, result.stderr


def test_middleware_resolves_postgres_user_id_from_claims() -> None:
    result = _run_configure_remote_subprocess(
        """\
        import asyncio
        import os
        from pathlib import Path
        from unittest import mock

        import mcp.types as mt
        from fastmcp.server.auth import AccessToken
        from fastmcp.server.middleware import MiddlewareContext
        from fastmcp.tools.tool import ToolResult

        from finance_cli.mcp_remote import RemoteUserMiddleware, _TEMPLATE_RULES_PATH
        from finance_cli.user_context import get_user_context

        seen = {}

        async def call_next(context):
            seen["arguments"] = context.message.arguments
            seen["user_context"] = get_user_context()
            return ToolResult(content=[])

        middleware = RemoteUserMiddleware(
            data_root=Path(os.environ["FINANCE_GATEWAY_DATA_ROOT"]),
            template_rules_path=_TEMPLATE_RULES_PATH,
        )
        context = MiddlewareContext(
            message=mt.CallToolRequestParams(
                name="goal_list",
                arguments={"limit": 5, "_request_id": "req-123"},
            )
        )
        token = AccessToken(
            token="token",
            client_id="app-id",
            scopes=["openid"],
            claims={
                "sub": "12345",
                "email": "user@example.com",
                "name": "Example User",
            },
        )

        with (
            mock.patch("finance_cli.mcp_remote.get_access_token", return_value=token),
            mock.patch("finance_cli.mcp_remote._resolve_user_id", return_value="42") as resolve_user_id,
            mock.patch("finance_cli.mcp_remote.storage_dispatch.storage_mode_for_user", return_value="remote")
            as storage_mode_for_user,
            mock.patch(
                "finance_cli.mcp_remote._load_user_billing_snapshot",
                return_value={"tier": "paid", "trial_ends_at": None, "lifetime_deal": False},
            ),
        ):
            asyncio.run(middleware.on_call_tool(context, call_next))

        expected = str(
            Path(os.environ["FINANCE_GATEWAY_DATA_ROOT"]).resolve() / "42" / "finance.db"
        )
        assert seen["user_context"] is not None
        assert seen["user_context"].db_path == expected
        assert seen["user_context"].expected_user_id == "42"
        assert seen["user_context"].local_mode is False
        assert seen["user_context"].storage_mode == "remote"
        assert seen["arguments"] == {"limit": 5}
        assert "app-id" not in seen["user_context"].db_path
        resolve_user_id.assert_called_once_with(
            "12345",
            "user@example.com",
            "Example User",
            data_root=Path(os.environ["FINANCE_GATEWAY_DATA_ROOT"]),
            template_rules_path=_TEMPLATE_RULES_PATH,
        )
        storage_mode_for_user.assert_called_once_with("42")
        assert get_user_context() is None
        """
    )

    assert result.returncode == 0, result.stderr


def test_middleware_rejects_unknown_sub() -> None:
    result = _run_configure_remote_subprocess(
        """\
        import asyncio
        import os
        from pathlib import Path
        from unittest import mock

        import mcp.types as mt
        import pytest
        from fastmcp.server.auth import AccessToken
        from fastmcp.server.middleware import MiddlewareContext

        from finance_cli.mcp_remote import RemoteUserMiddleware, _TEMPLATE_RULES_PATH
        from finance_cli.user_context import get_user_context

        async def call_next(_context):
            raise AssertionError("call_next should not run")

        middleware = RemoteUserMiddleware(
            data_root=Path(os.environ["FINANCE_GATEWAY_DATA_ROOT"]),
            template_rules_path=_TEMPLATE_RULES_PATH,
        )
        context = MiddlewareContext(
            message=mt.CallToolRequestParams(name="goal_list", arguments={"limit": 5})
        )
        token = AccessToken(
            token="token",
            client_id="app-id",
            scopes=["openid"],
            claims={"sub": "unknown"},
        )

        with (
            mock.patch("finance_cli.mcp_remote.get_access_token", return_value=token),
            mock.patch("finance_cli.mcp_remote._resolve_user_id") as resolve_user_id,
        ):
            with pytest.raises(
                PermissionError,
                match="Google authentication did not return a valid user identity",
            ):
                asyncio.run(middleware.on_call_tool(context, call_next))

        resolve_user_id.assert_not_called()
        assert get_user_context() is None
        """
    )

    assert result.returncode == 0, result.stderr


def test_resolve_user_id_returns_existing_google_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [(42,), (None,)]
    conn.cursor.return_value = _cursor_context(cursor)

    with mock.patch.object(
        mcp_remote.psycopg2, "connect", return_value=conn
    ) as connect:
        assert (
            mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")
            == "42"
        )

    connect.assert_called_once_with("postgresql://db.example/finance")
    assert cursor.execute.call_args_list == [
        mock.call(
            "SELECT id FROM users WHERE google_user_id = %s", ("google-sub-123",)
        ),
        mock.call("SELECT deleted_at FROM users WHERE id = %s", (42,)),
        mock.call(
            """
        UPDATE users
           SET tier = 'trial',
               trial_ends_at = NOW() + INTERVAL '21 days',
               updated_at = NOW()
         WHERE id = %s
           AND tier = 'registered'
           AND trial_ends_at IS NULL
        """,
            ("42",),
        ),
    ]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_resolve_user_id_falls_back_to_email_and_updates_google_user_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [None, (7,), (None,)]
    conn.cursor.return_value = _cursor_context(cursor)

    with mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn):
        assert (
            mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")
            == "7"
        )

    assert cursor.execute.call_args_list == [
        mock.call(
            "SELECT id FROM users WHERE google_user_id = %s", ("google-sub-123",)
        ),
        mock.call("SELECT id FROM users WHERE email = %s", ("user@example.com",)),
        mock.call("SELECT deleted_at FROM users WHERE id = %s", (7,)),
        mock.call(
            "UPDATE users SET google_user_id = %s, updated_at = NOW() WHERE id = %s",
            ("google-sub-123", "7"),
        ),
        mock.call(
            """
        UPDATE users
           SET tier = 'trial',
               trial_ends_at = NOW() + INTERVAL '21 days',
               updated_at = NOW()
         WHERE id = %s
           AND tier = 'registered'
           AND trial_ends_at IS NULL
        """,
            ("7",),
        ),
    ]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_resolve_user_id_creates_new_user_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [None, None, (11, True)]
    conn.cursor.return_value = _cursor_context(cursor)

    with mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn):
        assert (
            mcp_remote._resolve_user_id(
                "google-sub-123", "user@example.com", "Example User"
            )
            == "11"
        )

    assert cursor.execute.call_args_list[0] == mock.call(
        "SELECT id FROM users WHERE google_user_id = %s",
        ("google-sub-123",),
    )
    assert cursor.execute.call_args_list[1] == mock.call(
        "SELECT id FROM users WHERE email = %s",
        ("user@example.com",),
    )
    insert_sql, insert_params = cursor.execute.call_args_list[2].args
    assert "INSERT INTO users" in insert_sql
    assert "'trial'" in insert_sql
    assert "ON CONFLICT (google_user_id) DO UPDATE" in insert_sql
    assert insert_params == ("user@example.com", "Example User", "google-sub-123")
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_resolve_user_id_rejects_deleted_google_user(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [(42,), ("2026-04-16T10:30:00+00:00",)]
    conn.cursor.return_value = _cursor_context(cursor)

    with mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn):
        with pytest.raises(PermissionError, match="Account has been deleted."):
            mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")

    assert cursor.execute.call_args_list == [
        mock.call(
            "SELECT id FROM users WHERE google_user_id = %s", ("google-sub-123",)
        ),
        mock.call("SELECT deleted_at FROM users WHERE id = %s", (42,)),
    ]
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_resolve_user_id_rejects_deleted_email_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.side_effect = [None, (7,), ("2026-04-16T10:30:00+00:00",)]
    conn.cursor.return_value = _cursor_context(cursor)

    with mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn):
        with pytest.raises(PermissionError, match="Account has been deleted."):
            mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")

    assert cursor.execute.call_args_list == [
        mock.call(
            "SELECT id FROM users WHERE google_user_id = %s", ("google-sub-123",)
        ),
        mock.call("SELECT id FROM users WHERE email = %s", ("user@example.com",)),
        mock.call("SELECT deleted_at FROM users WHERE id = %s", (7,)),
    ]
    conn.commit.assert_not_called()
    conn.close.assert_called_once()


def test_resolve_user_id_retries_after_integrity_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example/finance")
    conn = mock.MagicMock()
    first_cursor = mock.MagicMock()
    retry_cursor = mock.MagicMock()
    first_cursor.fetchone.side_effect = [None, None]
    first_cursor.execute.side_effect = [
        None,
        None,
        mcp_remote.psycopg2.IntegrityError(),
    ]
    retry_cursor.fetchone.side_effect = [(99,), (None,)]
    conn.cursor.side_effect = [
        _cursor_context(first_cursor),
        _cursor_context(retry_cursor),
    ]

    with mock.patch.object(mcp_remote.psycopg2, "connect", return_value=conn):
        assert (
            mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")
            == "99"
        )

    conn.rollback.assert_called_once()
    assert retry_cursor.execute.call_args_list == [
        mock.call(
            "SELECT id FROM users WHERE google_user_id = %s", ("google-sub-123",)
        ),
        mock.call("SELECT deleted_at FROM users WHERE id = %s", (99,)),
        mock.call(
            """
        UPDATE users
           SET tier = 'trial',
               trial_ends_at = NOW() + INTERVAL '21 days',
               updated_at = NOW()
         WHERE id = %s
           AND tier = 'registered'
           AND trial_ends_at IS NULL
        """,
            ("99",),
        ),
    ]
    conn.commit.assert_called_once()
    conn.close.assert_called_once()


def test_resolve_user_id_requires_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="DATABASE_URL required for user resolution"):
        mcp_remote._resolve_user_id("google-sub-123", "user@example.com", "Example")


def test_configure_remote_requires_email_and_profile_scopes() -> None:
    result = _run_configure_remote_subprocess(
        """\
        from finance_cli.mcp_remote import configure_remote
        from finance_cli.mcp_remote_config import McpRemoteSettings
        from finance_cli.mcp_server import mcp

        configure_remote(McpRemoteSettings.from_env())
        assert mcp.auth.required_scopes == [
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ]
        """
    )

    assert result.returncode == 0, result.stderr


def test_configure_remote_serves_oauth_metadata() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        env = _remote_test_env(tmpdir)
        port = _find_free_port()
        env["MCP_REMOTE_HOST"] = "127.0.0.1"
        env["MCP_REMOTE_PORT"] = str(port)

        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """\
                    from finance_cli.mcp_remote import main

                    main()
                    """
                ),
            ],
            cwd=_PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            auth_metadata: dict[str, object] | None = None
            deadline = time.time() + 20
            last_error: Exception | None = None
            auth_url = f"http://127.0.0.1:{port}/.well-known/oauth-authorization-server"

            while time.time() < deadline:
                if process.poll() is not None:
                    stdout, stderr = process.communicate()
                    pytest.fail(
                        f"Remote server exited before OAuth metadata was ready.\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
                    )
                try:
                    auth_metadata = _read_json(auth_url)
                    break
                except Exception as exc:  # pragma: no cover - failure path only
                    last_error = exc
                    time.sleep(0.1)

            if auth_metadata is None:
                pytest.fail(
                    f"OAuth metadata endpoint never became ready: {last_error!r}"
                )

            assert (
                auth_metadata["authorization_endpoint"]
                == "https://cashnerd.ai/authorize"
            )
            assert auth_metadata["token_endpoint"] == "https://cashnerd.ai/token"
            assert "client_id_metadata_document_supported" not in auth_metadata
            assert (
                auth_metadata["registration_endpoint"] == "https://cashnerd.ai/register"
            )

            resource_metadata = _read_json(
                f"http://127.0.0.1:{port}/.well-known/oauth-protected-resource/mcp"
            )
            assert resource_metadata["resource"] == "https://cashnerd.ai/mcp"
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
