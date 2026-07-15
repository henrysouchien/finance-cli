from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import evaluate_for_surface
from finance_cli.tests.test_intervention_engine import NOW, _seed_account, _seed_credit_liability


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def test_agent_prompt_fire_suppresses_dashboard_during_cooldown(db_path: Path) -> None:
    with connect(db_path) as conn:
        high = _seed_account(conn, account_type="credit_card", balance_cents=-90_000, institution_name="High")
        mid = _seed_account(conn, account_type="credit_card", balance_cents=-30_000, institution_name="Mid")
        low = _seed_account(conn, account_type="credit_card", balance_cents=-5_000, institution_name="Low")
        _seed_credit_liability(conn, account_id=high, apr_purchase=29.99, minimum_payment_cents=3_000)
        _seed_credit_liability(conn, account_id=mid, apr_purchase=19.99, minimum_payment_cents=500)
        _seed_credit_liability(conn, account_id=low, apr_purchase=9.99, minimum_payment_cents=200)

        _, gateway_surface = evaluate_for_surface(
            conn,
            "agent_prompt",
            log_to_surface="agent_prompt",
            now=NOW,
        )
        _, dashboard_surface = evaluate_for_surface(
            conn,
            "dashboard",
            log_to_surface=None,
            now=NOW,
        )

    assert any(item.pattern_id == "D-1" for item in gateway_surface)
    # context.py loads MAX(fired_at) by pattern across all surfaces, so an
    # agent_prompt fire suppresses dashboard candidates during cooldown.
    assert all(item.pattern_id != "D-1" for item in dashboard_surface)
