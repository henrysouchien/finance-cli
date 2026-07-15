"""Centralized logging configuration for finance_cli."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
import sys

from .perf import _request_id_var

_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_RESERVED_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__) | {
    "asctime",
    "message",
}


class StructuredFormatter(logging.Formatter):
    """Format log records as JSON with core and extra fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_FIELDS:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str)


class CorrelationFilter(logging.Filter):
    """Inject request correlation fields onto log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "request_id", None) is None:
            record.request_id = _request_id_var.get(None)
        return True


def setup_logging() -> None:
    """Configure the shared finance_cli logger from environment variables."""
    root = logging.getLogger("finance_cli")

    # Keep setup idempotent across repeated CLI invocations in one process.
    for handler in root.handlers[:]:
        handler.close()
        root.removeHandler(handler)

    root.propagate = False

    raw_level_name = str(os.getenv("FINANCE_CLI_LOG_LEVEL", "WARNING"))
    level_name = raw_level_name.strip().upper()
    if level_name not in _VALID_LEVELS:
        sys.stderr.write(
            f"WARNING: Invalid FINANCE_CLI_LOG_LEVEL={raw_level_name!r}, using WARNING\n"
        )
        level_name = "WARNING"
    root.setLevel(getattr(logging, level_name))

    log_format = str(os.getenv("FINANCE_CLI_LOG_FORMAT", "text")).strip().lower()
    if log_format == "json":
        formatter: logging.Formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    stderr_handler.addFilter(CorrelationFilter())
    root.addHandler(stderr_handler)

    log_file = os.getenv("FINANCE_CLI_LOG_FILE")
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(CorrelationFilter())
            root.addHandler(file_handler)
        except OSError:
            sys.stderr.write(
                f"WARNING: Could not open log file {log_file}, continuing with stderr only\n"
            )


__all__ = ["CorrelationFilter", "StructuredFormatter", "setup_logging"]
