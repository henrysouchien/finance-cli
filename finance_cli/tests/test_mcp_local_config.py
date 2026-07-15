from __future__ import annotations

import pytest
from pydantic import ValidationError

from finance_cli.mcp_local_config import McpLocalSettings


def test_mcp_local_settings_from_env_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    settings = McpLocalSettings.from_env()

    assert isinstance(settings, McpLocalSettings)


def test_mcp_local_settings_is_frozen() -> None:
    settings = McpLocalSettings()

    with pytest.raises(ValidationError, match="frozen"):
        settings.log_level = "DEBUG"
