from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import plan
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    defaults = {"format": "json"}
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_abandon_plan_logs_distinct_signal(db_path: Path) -> None:
    with connect(db_path) as conn:
        plan.handle_create(_ns(month="2026-04"), conn)

        result = plan.handle_abandon(_ns(month="2026-04"), conn)

        event = conn.execute(
            """
            SELECT event, outcome, json_extract(properties, '$.month') AS month
              FROM analytics_events
             WHERE event = 'feature.plan_abandoned'
            """
        ).fetchone()

    assert result["summary"] == {"month": "2026-04", "abandoned": True}
    assert event is not None
    assert event["outcome"] == "abandoned"
    assert event["month"] == "2026-04"
