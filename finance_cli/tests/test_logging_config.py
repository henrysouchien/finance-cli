from __future__ import annotations

import logging
import sys

from finance_cli.logging_config import setup_logging


def _clear_finance_cli_handlers() -> None:
    logger = logging.getLogger("finance_cli")
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def test_setup_logging_defaults_to_warning_stderr(monkeypatch) -> None:
    monkeypatch.delenv("FINANCE_CLI_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FINANCE_CLI_LOG_FILE", raising=False)

    setup_logging()

    logger = logging.getLogger("finance_cli")
    try:
        assert logger.level == logging.WARNING
        assert logger.propagate is False
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0], logging.StreamHandler)
        assert getattr(logger.handlers[0], "stream", None) is sys.stderr
    finally:
        _clear_finance_cli_handlers()


def test_setup_logging_reloads_env_and_resets_handlers(monkeypatch, tmp_path) -> None:
    log_file = tmp_path / "finance-cli.log"

    monkeypatch.setenv("FINANCE_CLI_LOG_LEVEL", "INFO")
    monkeypatch.setenv("FINANCE_CLI_LOG_FILE", str(log_file))
    setup_logging()

    logger = logging.getLogger("finance_cli")
    try:
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 2

        monkeypatch.setenv("FINANCE_CLI_LOG_LEVEL", "ERROR")
        monkeypatch.delenv("FINANCE_CLI_LOG_FILE", raising=False)
        setup_logging()

        assert logger.level == logging.ERROR
        assert len(logger.handlers) == 1
    finally:
        _clear_finance_cli_handlers()


def test_setup_logging_invalid_level_falls_back_to_warning(monkeypatch, capsys) -> None:
    monkeypatch.setenv("FINANCE_CLI_LOG_LEVEL", "bogus")
    monkeypatch.delenv("FINANCE_CLI_LOG_FILE", raising=False)

    setup_logging()

    logger = logging.getLogger("finance_cli")
    try:
        captured = capsys.readouterr()
        assert "Invalid FINANCE_CLI_LOG_LEVEL" in captured.err
        assert logger.level == logging.WARNING
    finally:
        _clear_finance_cli_handlers()


def test_setup_logging_file_handler_failure_warns_and_continues(monkeypatch, capsys) -> None:
    monkeypatch.setenv("FINANCE_CLI_LOG_LEVEL", "INFO")
    monkeypatch.setenv("FINANCE_CLI_LOG_FILE", "/tmp/should-fail.log")

    def _raise_os_error(*_args, **_kwargs):
        raise OSError("permission denied")

    monkeypatch.setattr("finance_cli.logging_config.logging.FileHandler", _raise_os_error)

    setup_logging()

    logger = logging.getLogger("finance_cli")
    try:
        captured = capsys.readouterr()
        assert "Could not open log file" in captured.err
        assert len(logger.handlers) == 1
    finally:
        _clear_finance_cli_handlers()
