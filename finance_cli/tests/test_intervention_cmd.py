from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import intervention_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.tests.test_intervention_engine import _seed_account, _seed_credit_liability


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _ns(**kwargs) -> Namespace:
    return Namespace(**kwargs)


def _seed_d1(conn) -> None:
    high = _seed_account(conn, account_type="credit_card", balance_cents=-90_000, institution_name="High")
    mid = _seed_account(conn, account_type="credit_card", balance_cents=-30_000, institution_name="Mid")
    low = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, institution_name="Low")
    _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=3_000)
    _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=500)
    _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=200)


def test_handle_list_keeps_cli_logging_contract(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)

        result = intervention_cmd.handle_list(_ns(surface="agent_prompt"), conn)
        row = conn.execute("SELECT surface FROM intervention_log").fetchone()

    assert result["data"]["surface"] == "agent_prompt"
    assert result["data"]["log_surface"] == "cli"
    assert "cli_report" in result
    assert row["surface"] == "cli"


def test_handle_list_cli_log_does_not_suppress_read_only_surfaces(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)

        listed = intervention_cmd.handle_list(_ns(surface="agent_prompt"), conn)
        read_only = intervention_cmd.handle_get(_ns(surface="agent_prompt"), conn)

    assert listed["data"]["interventions"][0]["pattern_id"] == "D-1"
    assert read_only["data"]["interventions"][0]["pattern_id"] == "D-1"


def test_cli_dismissal_does_not_suppress_read_only_surfaces(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)

        listed = intervention_cmd.handle_list(_ns(surface="agent_prompt"), conn)
        log_id = listed["data"]["interventions"][0]["log_id"]
        intervention_cmd.handle_dismiss(_ns(log_id=log_id), conn)
        read_only = intervention_cmd.handle_get(_ns(surface="agent_prompt"), conn)

    assert read_only["data"]["interventions"][0]["pattern_id"] == "D-1"


def test_handle_get_is_read_only_and_returns_clean_envelope(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)

        result = intervention_cmd.handle_get(_ns(surface="agent_prompt"), conn)
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_log").fetchone()["cnt"]

    assert result["data"]["surface"] == "agent_prompt"
    assert result["data"]["interventions"]
    assert result["summary"]["surface"] == "agent_prompt"
    assert "log_surface" not in result["data"]
    assert "cli_report" not in result
    assert int(count) == 0
