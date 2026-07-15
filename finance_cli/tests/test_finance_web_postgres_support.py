from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

FINANCE_WEB_TESTS = Path(__file__).resolve().parents[2] / "finance-web" / "server" / "tests"
if str(FINANCE_WEB_TESTS) not in sys.path:
    sys.path.insert(0, str(FINANCE_WEB_TESTS))

import postgres_support  # noqa: E402


def test_require_postgresql_reports_plugin_and_binary_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(importlib.util, "find_spec", lambda _name: None)
    monkeypatch.setattr(postgres_support.shutil, "which", lambda _name: None)

    with pytest.raises(pytest.skip.Exception) as exc_info:
        postgres_support.require_postgresql(request=object())

    message = str(exc_info.value)
    assert "pytest-postgresql is not installed" in message
    assert "install the repo dev extra" in message
    assert "pg_ctl, initdb, postgres, pg_config" in message


def test_require_postgresql_reports_fixture_executable_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Request:
        def getfixturevalue(self, name: str):
            assert name == "postgresql"
            raise type(
                "ExecutableMissingException",
                (Exception,),
                {"__module__": "pytest_postgresql.executor"},
            )("pg_ctl not found")

    monkeypatch.setattr(importlib.util, "find_spec", lambda _name: object())
    monkeypatch.setattr(
        postgres_support.shutil,
        "which",
        lambda name: "/usr/bin/postgres" if name == "postgres" else None,
    )

    with pytest.raises(pytest.skip.Exception) as exc_info:
        postgres_support.require_postgresql(Request())

    message = str(exc_info.value)
    assert "server binaries are not available" in message
    assert "pg_ctl not found" in message
    assert "pg_ctl, initdb, pg_config" in message
