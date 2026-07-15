"""Configuration for the local MCP process."""

from __future__ import annotations

import logging

from pydantic import ValidationError
from finance_cli.settings_base import FinanceBaseSettings

logger = logging.getLogger(__name__)


class McpLocalSettings(FinanceBaseSettings):
    """Boot-time validation marker for the local MCP process.

    The security-sensitive server URL stays owned by SyncConfig because it is
    persisted in ~/.cashnerd/config.json and consumed via load_config().
    """

    @classmethod
    def from_env(cls) -> "McpLocalSettings":
        try:
            return cls()
        except ValidationError as exc:
            logger.error("McpLocalSettings validation failed: %s", exc)
            fields = cls._summarize_validation_errors(exc)
            raise ValueError(f"Missing or invalid config: {', '.join(fields)}") from exc

    @classmethod
    def _summarize_validation_errors(cls, exc: ValidationError) -> list[str]:
        fields: list[str] = []
        for error in exc.errors():
            loc = error.get("loc", ())
            if not loc:
                continue
            label = str(loc[0])
            if label not in fields:
                fields.append(label)
        return fields or ["unknown"]
