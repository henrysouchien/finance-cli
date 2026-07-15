from __future__ import annotations

from argparse import Namespace
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.commands import intervention_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.tests.test_intervention_engine import (
    _seed_account,
    _seed_credit_liability,
    _seed_intervention_log,
)


FROZEN_NOW = datetime(2026, 4, 10, 12, 0, 0)


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


def test_handle_expire_transitions_only_old_pending_rows(db_path: Path) -> None:
    with connect(db_path) as conn:
        old_pending = _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-03-01 12:00:00")
        recent_pending = _seed_intervention_log(conn, pattern_id="C-1", fired_at="2026-04-08 12:00:00")
        acted = _seed_intervention_log(
            conn,
            pattern_id="T-2",
            fired_at="2026-03-01 12:00:00",
            user_action="acted",
            acted_at="2026-03-02 12:00:00",
        )

        result = intervention_cmd.handle_expire(_ns(), conn, now=FROZEN_NOW)
        rows = {
            int(row["id"]): (row["user_action"], row["acted_at"])
            for row in conn.execute(
                "SELECT id, user_action, acted_at FROM intervention_log ORDER BY id"
            ).fetchall()
        }

    assert result["data"]["expired"] == 1
    assert rows[old_pending][0] == "ignored"
    assert rows[old_pending][1] is not None
    assert rows[recent_pending][0] == "pending"
    assert rows[acted] == ("acted", "2026-03-02 12:00:00")


def test_expired_rows_still_affect_cooldown(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)
        _seed_intervention_log(conn, pattern_id="D-1", fired_at="2026-04-02 12:00:00", surface="agent_prompt")

        expired = intervention_cmd.handle_expire(_ns(), conn, now=FROZEN_NOW)
        result = run_engine(conn, now=FROZEN_NOW)

    assert expired["data"]["expired"] == 1
    assert result.interventions == ()
