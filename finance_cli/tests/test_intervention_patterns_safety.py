from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import Move, PATTERN_REGISTRY, Priority
from finance_cli.interventions.safety import evaluate_s1_duplicate_charge, evaluate_s2_unfamiliar_vendor_large_charge


NOW = datetime(2026, 6, 20, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_account(conn, account_id: str = "card-1") -> str:
    conn.execute(
        """
        INSERT INTO accounts (
            id, institution_name, account_name, account_type,
            balance_current_cents, is_active
        ) VALUES (?, 'Card Bank', 'Rewards', 'credit_card', -50000, 1)
        """,
        (account_id,),
    )
    conn.commit()
    return account_id


def _seed_txn(
    conn,
    txn_id: str,
    *,
    account_id: str = "card-1",
    txn_date: str = "2026-06-18",
    description: str = "ACME STORE",
    amount_cents: int = -5_000,
    is_active: int = 1,
    is_payment: int = 0,
    is_recurring: int = 0,
    parent_transaction_id: str | None = None,
    category_source: str | None = None,
    category_rule_id: str | None = None,
    use_type: str | None = None,
    is_reviewed: int = 0,
    raw_plaid_json: str | None = None,
    source_category: str | None = None,
) -> str:
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, date, description, amount_cents, is_active,
            is_payment, is_recurring, parent_transaction_id, category_source,
            category_rule_id, use_type, is_reviewed, raw_plaid_json, source_category, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual')
        """,
        (
            txn_id,
            account_id,
            txn_date,
            description,
            amount_cents,
            is_active,
            is_payment,
            is_recurring,
            parent_transaction_id,
            category_source,
            category_rule_id,
            use_type,
            is_reviewed,
            raw_plaid_json,
            source_category,
        ),
    )
    conn.commit()
    return txn_id


def _seed_vendor_memory(
    conn,
    *,
    pattern: str,
    use_type: str = "Any",
    is_enabled: int = 1,
    is_confirmed: int = 1,
) -> str:
    rule_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id, description_pattern, canonical_name, use_type, is_enabled, is_confirmed
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (rule_id, pattern, pattern.title(), use_type, is_enabled, is_confirmed),
    )
    conn.commit()
    return rule_id


def _seed_dispute_workflow(
    conn,
    *,
    transaction_id: str,
    duplicate_transaction_id: str,
    status: str = "active",
) -> None:
    conn.execute(
        """
        INSERT INTO transaction_dispute_workflows (
            id, transaction_id, duplicate_transaction_id, account_id, status,
            dispute_reason, amount_cents, merchant_name, transaction_date,
            duplicate_date, source, snapshot_json, idempotency_key
        ) VALUES (?, ?, ?, 'card-1', ?, 'duplicate_charge', 5000,
                  'ACME STORE', '2026-06-18', '2026-06-14', 'agent', '{}', ?)
        """,
        (
            uuid.uuid4().hex,
            transaction_id,
            duplicate_transaction_id,
            status,
            f"txn_dispute:{transaction_id}:{duplicate_transaction_id}:duplicate_charge",
        ),
    )
    conn.commit()


def test_s1_fires_for_recent_same_vendor_same_amount_duplicate_charge(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14", description="Acme Store 441")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18", description="ACME STORE 441")

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "S-1"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.priority is Priority.HIGH
    assert intervention.dollar_impact_cents == 5_000
    assert "Possible duplicate: ACME STORE 441 charged $50.00" in intervention.headline
    assert intervention.action is not None
    assert intervention.action.tool == "txn_dispute_workflow"
    assert intervention.action.params == {
        "transaction_id": "txn-new",
        "duplicate_transaction_id": "txn-old",
        "dispute_reason": "duplicate_charge",
        "note": "Possible duplicate $50.00 charge at ACME STORE 441 on 2026-06-14 and 2026-06-18.",
    }


def test_s1_is_registered_and_runs_through_engine(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18")

        result = run_engine(conn, now=NOW)

    assert "S-1" in PATTERN_REGISTRY
    assert any(item.pattern_id == "S-1" for item in result.interventions)


def test_s1_ignores_matches_outside_seven_day_duplicate_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-01")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18")

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_allows_exactly_seven_day_duplicate_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-11")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18")

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["transaction_id"] == "txn-new"
    assert intervention.action.params["duplicate_transaction_id"] == "txn-old"


def test_s1_fetches_matching_prior_row_at_recent_window_boundary(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-04-29")
        _seed_txn(conn, "txn-new", txn_date="2026-05-06")

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["transaction_id"] == "txn-new"
    assert intervention.action.params["duplicate_transaction_id"] == "txn-old"


def test_s1_requires_same_account_for_duplicate_pair(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_account(conn, "card-2")
        _seed_txn(conn, "txn-card-1", account_id="card-1", txn_date="2026-06-14")
        _seed_txn(conn, "txn-card-2", account_id="card-2", txn_date="2026-06-18")

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_ignores_low_amount_duplicate_lookalikes(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14", amount_cents=-999)
        _seed_txn(conn, "txn-new", txn_date="2026-06-18", amount_cents=-999)

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_ignores_recurring_duplicate_lookalikes(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14", is_recurring=1)
        _seed_txn(conn, "txn-new", txn_date="2026-06-18", is_recurring=1)

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_suppresses_same_day_paired_airline_ticket_charges(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "ticket-a",
            txn_date="2026-06-18",
            description="UNITED AIRLINES TICKET",
            amount_cents=-45_300,
        )
        _seed_txn(
            conn,
            "ticket-b",
            txn_date="2026-06-18",
            description="UNITED AIRLINES TICKET",
            amount_cents=-45_300,
        )

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_still_flags_airline_duplicate_charge_across_dates(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-old",
            txn_date="2026-06-14",
            description="UNITED AIRLINES TICKET",
            amount_cents=-45_300,
        )
        _seed_txn(
            conn,
            "txn-new",
            txn_date="2026-06-18",
            description="UNITED AIRLINES TICKET",
            amount_cents=-45_300,
        )

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["transaction_id"] == "txn-new"
    assert intervention.action.params["duplicate_transaction_id"] == "txn-old"


def test_s1_ignores_split_children_and_payments(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "parent", txn_date="2026-06-14")
        _seed_txn(
            conn,
            "split-child",
            txn_date="2026-06-18",
            parent_transaction_id="parent",
        )
        _seed_txn(conn, "payment", txn_date="2026-06-18", is_payment=1)

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_suppresses_pairs_with_existing_resolved_dispute_workflow(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18")
        _seed_dispute_workflow(
            conn,
            transaction_id="txn-old",
            duplicate_transaction_id="txn-new",
            status="resolved",
        )

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s1_suppresses_pairs_with_existing_active_dispute_workflow(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-old", txn_date="2026-06-14")
        _seed_txn(conn, "txn-new", txn_date="2026-06-18")
        _seed_dispute_workflow(
            conn,
            transaction_id="txn-old",
            duplicate_transaction_id="txn-new",
        )

        intervention = evaluate_s1_duplicate_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_fires_for_recent_unfamiliar_large_charge(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="CAMERA SHOP 987654",
            amount_cents=-25_000,
            raw_plaid_json=json.dumps(
                {"merchant_name": "Camera Shop", "merchant_entity_id": "merchant-camera"}
            ),
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.pattern_id == "S-2"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.MEDIUM
    assert intervention.dollar_impact_cents == 25_000
    assert intervention.headline == (
        "$250.00 to Camera Shop on 2026-06-18 - first charge from this merchant in your "
        "transaction history. Worth confirming it's legit."
    )
    assert intervention.action is not None
    assert intervention.action.tool == "txn_explain"
    assert intervention.action.params == {"id": "txn-camera"}


def test_s2_is_registered_and_runs_through_engine(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-large", txn_date="2026-06-18", description="UNKNOWN VENDOR", amount_cents=-22_000)

        result = run_engine(conn, now=NOW)

    assert "S-2" in PATTERN_REGISTRY
    assert any(item.pattern_id == "S-2" for item in result.interventions)


def test_s2_suppresses_if_enabled_vendor_memory_matches(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_vendor_memory(conn, pattern="camera shop")
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="CAMERA SHOP 987654",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_does_not_suppress_unconfirmed_vendor_memory_match(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_vendor_memory(conn, pattern="camera shop", is_confirmed=0)
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="CAMERA SHOP 987654",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-camera"}


def test_s2_suppresses_if_vendor_was_seen_before_on_same_account(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-prior", txn_date="2026-05-01", description="Camera Shop", amount_cents=-4_000)
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_suppresses_same_account_descriptor_variants(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-prior",
            txn_date="2026-05-01",
            description="Camera Shop 1234",
            amount_cents=-4_000,
        )
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="Camera Shop 9876",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_suppresses_if_vendor_was_seen_before_on_another_account(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_account(conn, "card-2")
        _seed_txn(conn, "txn-prior", account_id="card-1", txn_date="2026-05-01", description="Camera Shop")
        _seed_txn(
            conn,
            "txn-camera",
            account_id="card-2",
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_accountless_candidate_uses_user_vendor_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_txn(
            conn,
            "txn-prior",
            account_id="card-1",
            txn_date="2026-05-01",
            description="Camera Shop",
            amount_cents=-4_000,
        )
        _seed_txn(
            conn,
            "txn-camera",
            account_id=None,
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_accountless_candidate_suppresses_prior_accountless_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_txn(
            conn,
            "txn-prior",
            account_id=None,
            txn_date="2026-05-01",
            description="Camera Shop",
            amount_cents=-4_000,
        )
        _seed_txn(
            conn,
            "txn-other-account",
            account_id="card-1",
            txn_date="2026-06-18",
            description="Other Unknown",
            amount_cents=-22_000,
        )
        _seed_txn(
            conn,
            "txn-camera",
            account_id=None,
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-other-account"}


def test_s2_suppresses_if_plaid_merchant_entity_was_seen_before(db_path: Path) -> None:
    prior_payload = json.dumps({"merchant_name": "Camera Online", "merchant_entity_id": "entity-camera"})
    new_payload = json.dumps({"merchant_name": "Camera Retail", "merchant_entity_id": "entity-camera"})
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-prior",
            txn_date="2026-05-01",
            description="CAMERA ONLINE",
            amount_cents=-4_000,
            raw_plaid_json=prior_payload,
        )
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="CAMERA RETAIL",
            amount_cents=-25_000,
            raw_plaid_json=new_payload,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_suppresses_cross_account_plaid_merchant_entity_history(db_path: Path) -> None:
    prior_payload = json.dumps({"merchant_name": "Camera Online", "merchant_entity_id": "entity-camera"})
    new_payload = json.dumps({"merchant_name": "Camera Retail", "merchant_entity_id": "entity-camera"})
    with connect(db_path) as conn:
        _seed_account(conn, "card-1")
        _seed_account(conn, "card-2")
        _seed_txn(
            conn,
            "txn-prior",
            account_id="card-1",
            txn_date="2026-05-01",
            description="CAMERA ONLINE",
            amount_cents=-4_000,
            raw_plaid_json=prior_payload,
        )
        _seed_txn(
            conn,
            "txn-camera",
            account_id="card-2",
            txn_date="2026-06-18",
            description="CAMERA RETAIL",
            amount_cents=-25_000,
            raw_plaid_json=new_payload,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_does_not_collapse_short_numbered_distinct_merchants(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-prior",
            txn_date="2026-05-01",
            description="Parking Lot 17",
            amount_cents=-4_000,
        )
        _seed_txn(
            conn,
            "txn-parking",
            txn_date="2026-06-18",
            description="Parking Lot 42",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-parking"}


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("category_source", "user"),
        ("is_reviewed", 1),
    ),
)
def test_s2_suppresses_user_confirmed_transactions(db_path: Path, field: str, value: object) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        kwargs = {field: value}
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
            **kwargs,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_suppresses_confirmed_vendor_memory_categorized_transaction(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        rule_id = _seed_vendor_memory(conn, pattern="camera shop", is_confirmed=1)
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
            category_source="vendor_memory",
            category_rule_id=rule_id,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_does_not_suppress_unconfirmed_vendor_memory_categorized_transaction(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        rule_id = _seed_vendor_memory(conn, pattern="camera shop", is_confirmed=0)
        _seed_txn(
            conn,
            "txn-camera",
            txn_date="2026-06-18",
            description="Camera Shop",
            amount_cents=-25_000,
            category_source="vendor_memory",
            category_rule_id=rule_id,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-camera"}


def test_s2_fires_for_recurring_unfamiliar_large_charge(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-recurring",
            txn_date="2026-06-18",
            description="Unknown Annual Service",
            amount_cents=-25_000,
            is_recurring=1,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-recurring"}


@pytest.mark.parametrize(
    "source_category",
    (
        "TRANSFER_OUT_ACCOUNT_TRANSFER",
        "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT",
    ),
)
def test_s2_suppresses_provider_non_merchant_transfer_categories(
    db_path: Path,
    source_category: str,
) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-transfer",
            txn_date="2026-06-18",
            description="Check 1112",
            amount_cents=-300_000,
            source_category=source_category,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_suppresses_bare_check_number_descriptors_without_provider_category(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-check",
            txn_date="2026-06-18",
            description="Check #1112",
            amount_cents=-300_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_does_not_suppress_check_named_merchants(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(
            conn,
            "txn-check-merchant",
            txn_date="2026-06-18",
            description="Check Cashing Store",
            amount_cents=-25_000,
        )

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-check-merchant"}


@pytest.mark.parametrize(
    "kwargs",
    (
        {"amount_cents": -19_999},
        {"amount_cents": 25_000},
        {"is_payment": 1},
        {"parent_transaction_id": "parent"},
        {"txn_date": "2026-05-01"},
    ),
)
def test_s2_ignores_non_qualifying_large_charge_lookalikes(db_path: Path, kwargs: dict[str, object]) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        if kwargs.get("parent_transaction_id"):
            _seed_txn(conn, "parent", txn_date="2026-06-17", description="Parent", amount_cents=-5_000)
        seed_kwargs = {"txn_date": "2026-06-18", "description": "Camera Shop", "amount_cents": -25_000}
        seed_kwargs.update(kwargs)
        _seed_txn(conn, "txn-camera", **seed_kwargs)

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is None


def test_s2_picks_largest_qualifying_recent_charge(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_account(conn)
        _seed_txn(conn, "txn-small", txn_date="2026-06-18", description="Small Unknown", amount_cents=-21_000)
        _seed_txn(conn, "txn-large", txn_date="2026-06-17", description="Large Unknown", amount_cents=-40_000)

        intervention = evaluate_s2_unfamiliar_vendor_large_charge(conn, build_context(conn, now=NOW))

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {"id": "txn-large"}
