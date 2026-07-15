from __future__ import annotations

from pathlib import Path
import uuid

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.mcp_server import (
    coach_debt_payoff_artifact_read,
    coach_debt_payoff_artifact_save,
    debt_dashboard,
    debt_simulate,
    skill_state_get,
    skill_state_set,
    txn_list,
)
from finance_cli.skills import load_skill
from finance_cli.user_context import UserContext, reset_user_context, set_user_context


def _seed_account(
    conn,
    *,
    account_type: str,
    balance_current_cents: int,
    institution_name: str,
    account_name: str,
    card_ending: str | None = None,
) -> str:
    account_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            card_ending, balance_current_cents, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (
            account_id,
            institution_name,
            account_name,
            account_type,
            card_ending,
            balance_current_cents,
        ),
    )
    return account_id


def _seed_credit_liability(
    conn,
    *,
    account_id: str,
    apr_purchase: float,
    minimum_payment_cents: int,
) -> str:
    liability_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO liabilities (
            id, account_id, liability_type, is_active,
            apr_purchase, minimum_payment_cents, next_monthly_payment_cents
        ) VALUES (?, ?, 'credit', 1, ?, ?, ?)
        """,
        (liability_id, account_id, apr_purchase, minimum_payment_cents, minimum_payment_cents),
    )
    return liability_id


def _seed_transaction(
    conn,
    *,
    account_id: str,
    amount_cents: int,
    description: str,
    date: str,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, is_active,
            is_reviewed, source, account_id
        ) VALUES (?, ?, ?, ?, 1, 1, 'manual', ?)
        """,
        (uuid.uuid4().hex, date, description, amount_cents, account_id),
    )


@pytest.fixture()
def pilot_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(db_path)

    token = set_user_context(UserContext.from_paths(db_path=db_path))
    try:
        with connect(db_path) as conn:
            checking_id = _seed_account(
                conn,
                account_type="checking",
                balance_current_cents=350_000,
                institution_name="Pilot Bank",
                account_name="Operating Checking",
            )
            card_a_id = _seed_account(
                conn,
                account_type="credit_card",
                balance_current_cents=-180_000,
                institution_name="Pilot Issuer",
                account_name="High APR Card",
                card_ending="1111",
            )
            card_b_id = _seed_account(
                conn,
                account_type="credit_card",
                balance_current_cents=-60_000,
                institution_name="Pilot Store",
                account_name="Store Card",
                card_ending="2222",
            )
            _seed_credit_liability(
                conn,
                account_id=card_a_id,
                apr_purchase=24.99,
                minimum_payment_cents=6_000,
            )
            _seed_credit_liability(
                conn,
                account_id=card_b_id,
                apr_purchase=14.99,
                minimum_payment_cents=2_500,
            )
            _seed_transaction(
                conn,
                account_id=checking_id,
                amount_cents=600_000,
                description="Pilot freelance income",
                date="2026-05-01",
            )
            _seed_transaction(
                conn,
                account_id=checking_id,
                amount_cents=-240_000,
                description="Pilot operating expenses",
                date="2026-05-05",
            )
            _seed_transaction(
                conn,
                account_id=checking_id,
                amount_cents=-50_000,
                description="Pilot extra debt payment",
                date="2026-05-10",
            )
            conn.commit()
        yield tmp_path
    finally:
        reset_user_context(token)


def test_debt_payoff_pilot_deterministic_smoke(pilot_data_dir: Path) -> None:
    """Fresh pilot fixture covers the debt arc's shipped deterministic primitives."""

    skill = load_skill("coach_debt_payoff")
    assert skill["data"]["name"] == "coach_debt_payoff"
    assert "Phase 0: Diagnose" in skill["data"]["content"]

    assert skill_state_get("coach_debt_payoff")["data"]["state"] == {}
    state_result = skill_state_set(
        "coach_debt_payoff",
        {
            "phase": "diagnose",
            "classification": "stressed",
            "cash_flow_surplus_cents": 310_000,
        },
    )
    assert state_result["summary"]["updated"] is True
    assert skill_state_get("coach_debt_payoff")["data"]["state"]["phase"] == "diagnose"

    dashboard = debt_dashboard(sort="apr")
    assert dashboard["summary"]["total_cards"] == 2
    assert dashboard["data"]["total_balance_cents"] == 240_000
    card_ids = [card["card_id"] for card in dashboard["data"]["cards"]]
    assert dashboard["data"]["cards"][0]["apr"] == pytest.approx(24.99)

    transactions = txn_list(date_from="2026-05-01", date_to="2026-05-31", limit=10)
    assert transactions["summary"]["returned"] == 3

    simulation = debt_simulate(extra_dollars=500, strategy="compare")
    assert simulation["data"]["avalanche"]["months_to_payoff"] > 0
    assert simulation["data"]["snowball"]["months_to_payoff"] > 0
    assert simulation["summary"]["strategy"] == "compare"
    assert simulation["data"]["avalanche"]["schedule"][0]["base_extra_cents"] == 50_000

    artifact = coach_debt_payoff_artifact_save(
        action_plan_payload={
            "generated_at": "2026-05-30T12:00:00Z",
            "smart_goal": "Pay $500 per month toward the pilot credit cards.",
            "strategy": {
                "name": "avalanche",
                "why": "The highest APR card has the highest interest pressure.",
            },
            "action_steps": [
                {"step": "Pay all minimums", "timeline": "monthly"},
                {"step": "Send extra cash to the highest APR card", "timeline": "monthly"},
            ],
            "monthly_commitment_cents": 50_000,
            "debts_in_scope": [
                {"id": card_ids[0], "label": "High APR Card", "source": "liability"},
                {"id": card_ids[1], "label": "Store Card", "source": "liability"},
            ],
            "target_debt_free_date": "2026-11-30",
            "monitoring_cadence": "monthly",
            "next_check_in": "2026-06-30",
        },
        dry_run=False,
    )
    assert artifact["summary"]["saved"] is True
    assert Path(artifact["data"]["artifact_path"]).is_relative_to(pilot_data_dir)

    saved = coach_debt_payoff_artifact_read(date=None)
    assert saved["summary"]["found"] is True
    assert saved["data"]["action_plan_payload"]["strategy"]["name"] == "avalanche"
    assert saved["data"]["action_plan_payload"]["monthly_commitment_cents"] == 50_000
