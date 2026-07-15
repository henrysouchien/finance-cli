from __future__ import annotations

import pytest
from pydantic import ValidationError

from finance_cli.mcp_remote_config import McpRemoteSettings

_ENV_KEYS = [
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "MCP_REMOTE_BASE_URL",
    "FINANCE_GATEWAY_DATA_ROOT",
    "DATABASE_URL",
    "MCP_REMOTE_HOST",
    "MCP_REMOTE_PORT",
]


def _clear_mcp_remote_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")


def test_from_env_reports_missing_google_client_id(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "test-client-secret")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GOOGLE_CLIENT_ID$"):
        McpRemoteSettings.from_env()


def test_from_env_reports_missing_google_client_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "test-client-id")

    with pytest.raises(ValueError, match=r"Missing or invalid config: GOOGLE_CLIENT_SECRET$"):
        McpRemoteSettings.from_env()


def test_from_env_summarizes_multiple_missing_fields_on_one_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_mcp_remote_env(monkeypatch)

    with pytest.raises(ValueError) as exc_info:
        McpRemoteSettings.from_env()

    assert (
        str(exc_info.value)
        == "Missing or invalid config: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET"
    )
    assert "\n" not in str(exc_info.value)


def test_from_env_reports_invalid_port(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    _set_required_env(monkeypatch)
    monkeypatch.setenv("MCP_REMOTE_PORT", "not-a-number")

    with pytest.raises(ValueError, match=r"Missing or invalid config: MCP_REMOTE_PORT$"):
        McpRemoteSettings.from_env()


def test_from_env_reports_out_of_range_port(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    _set_required_env(monkeypatch)
    monkeypatch.setenv("MCP_REMOTE_PORT", "65536")

    with pytest.raises(ValueError, match=r"Missing or invalid config: MCP_REMOTE_PORT$"):
        McpRemoteSettings.from_env()


def test_from_env_reports_negative_port(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    _set_required_env(monkeypatch)
    monkeypatch.setenv("MCP_REMOTE_PORT", "-1")

    with pytest.raises(ValueError, match=r"Missing or invalid config: MCP_REMOTE_PORT$"):
        McpRemoteSettings.from_env()


def test_from_env_uses_defaults_for_optional_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_mcp_remote_env(monkeypatch)
    _set_required_env(monkeypatch)

    settings = McpRemoteSettings.from_env()

    assert settings.base_url == "https://cashnerd.ai"
    assert settings.host == "127.0.0.1"
    assert settings.port == 8003
    assert settings.database_url == ""


def test_mcp_remote_settings_is_frozen() -> None:
    settings = McpRemoteSettings(
        **{
            "GOOGLE_CLIENT_ID": "test-client-id",
            "GOOGLE_CLIENT_SECRET": "test-client-secret",
        }
    )

    with pytest.raises(ValidationError, match="frozen"):
        settings.port = 9000


def test_mcp_remote_settings_does_not_steal_shared_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression test for the 2026-04-20 prod incident: MCP remote bound to
    0.0.0.0:8080 instead of 127.0.0.1:8003 because populate_by_name=True caused
    field-name fallback to pick up the web backend's HOST/PORT from shared .env."""

    monkeypatch.setenv("GOOGLE_CLIENT_ID", "x")
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "y")
    monkeypatch.delenv("MCP_REMOTE_HOST", raising=False)
    monkeypatch.delenv("MCP_REMOTE_PORT", raising=False)
    monkeypatch.setenv("HOST", "0.0.0.0")
    monkeypatch.setenv("PORT", "8080")

    settings = McpRemoteSettings.from_env()

    assert settings.host == "127.0.0.1"
    assert settings.port == 8003
