from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from finance_cli.commands import intervention_cmd
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.tests.test_intervention_engine import NOW, _seed_account, _seed_credit_liability


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


def test_mute_blocks_pattern_and_unmute_restores_it(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_d1(conn)
        assert [item.pattern_id for item in run_engine(conn, now=NOW).interventions] == ["D-1"]

        muted = intervention_cmd.handle_mute(_ns(pattern_id="D-1", reason="too noisy"), conn)
        blocked = run_engine(conn, now=NOW).interventions
        unmuted = intervention_cmd.handle_unmute(_ns(pattern_id="D-1"), conn)
        restored = run_engine(conn, now=NOW).interventions

    assert muted["data"]["pattern_id"] == "D-1"
    assert muted["data"]["created"] is True
    assert blocked == ()
    assert unmuted["data"]["deleted"] is True
    assert [item.pattern_id for item in restored] == ["D-1"]


def test_double_mute_is_idempotent_and_unmute_missing_is_noop(db_path: Path) -> None:
    with connect(db_path) as conn:
        first = intervention_cmd.handle_mute(_ns(pattern_id="D-1", reason=""), conn)
        second = intervention_cmd.handle_mute(_ns(pattern_id="D-1", reason="new reason"), conn)
        missing = intervention_cmd.handle_unmute(_ns(pattern_id="T-2"), conn)
        count = conn.execute("SELECT COUNT(*) AS cnt FROM intervention_mutes WHERE pattern_id = 'D-1'").fetchone()["cnt"]

    assert first["data"]["created"] is True
    assert second["data"]["created"] is False
    assert int(count) == 1
    assert missing["data"]["deleted"] is False


def test_handle_mute_validates_pattern_id(db_path: Path) -> None:
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="Unknown intervention pattern: TYPO-1"):
            intervention_cmd.handle_mute(_ns(pattern_id="TYPO-1", reason=""), conn)


def test_build_context_populates_muted_patterns(db_path: Path) -> None:
    with connect(db_path) as conn:
        intervention_cmd.handle_mute(_ns(pattern_id="D-1", reason=""), conn)
        intervention_cmd.handle_mute(_ns(pattern_id="T-2", reason=""), conn)
        ctx = build_context(conn, now=NOW)

    assert ctx.muted_patterns == frozenset({"D-1", "T-2"})
