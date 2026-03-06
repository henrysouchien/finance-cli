"""Centralized logging configuration for finance_cli."""

from __future__ import annotations

import logging
import os
import sys

_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


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

    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    root.addHandler(stderr_handler)

    log_file = os.getenv("FINANCE_CLI_LOG_FILE")
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root.addHandler(file_handler)
        except OSError:
            sys.stderr.write(
                f"WARNING: Could not open log file {log_file}, continuing with stderr only\n"
            )


__all__ = ["setup_logging"]
