from __future__ import annotations

import uuid
from pathlib import Path

from finance_cli.categorizer import (
    MatchResult,
    _resolve_prefix_match,
    apply_match,
    lookup_category_mapping,
    map_plaid_pfc_to_category,
    match_transaction,
    normalize_description,
)
from finance_cli.db import connect, initialize_database


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_rule(
    conn,
    pattern: str,
    category_id: str,
    use_type: str = "Any",
    priority: int = 0,
    confidence: float = 1.0,
    match_count: int = 0,
    is_confirmed: int = 1,
) -> str:
    rule_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id, description_pattern, category_id, use_type, confidence,
            priority, is_enabled, is_confirmed, match_count
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (rule_id, normalize_description(pattern), category_id, use_type, confidence, priority, is_confirmed, match_count),
    )
    conn.commit()
    return rule_id


def _seed_transaction(conn, txn_id: str, description: str, is_payment: int = 0) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            date,
            description,
            amount_cents,
            source,
            is_active,
            is_payment
        ) VALUES (?, '2025-01-01', ?, -1000, 'manual', 1, ?)
        """,
        (txn_id, description, is_payment),
    )
    conn.commit()


def test_categorization_determinism(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        _seed_rule(conn, "uber", dining_id, use_type="Any", priority=10, confidence=1.0, match_count=3)

        first = match_transaction(conn, "UBER TRIP 123456", use_type="Business")
        second = match_transaction(conn, "UBER TRIP 999999", use_type="Business")

    assert first is not None
    assert second is not None
    assert first.category_id == second.category_id
    assert first.category_source == "auto_prefix"


def test_prefix_tie_break_priority_confidence_match_count(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        groceries_id = _seed_category(conn, "Groceries")

        _seed_rule(conn, "amazon", dining_id, use_type="Any", priority=1, confidence=0.5, match_count=1)
        _seed_rule(conn, "amaz", groceries_id, use_type="Any", priority=9, confidence=0.9, match_count=10)

        result = match_transaction(conn, "amazon market", use_type="Personal")

    assert result is not None
    assert result.category_id == dining_id


def test_ambiguity_guard_for_true_tie() -> None:
    rows = [
        {
            "id": "r1",
            "description_pattern": "uber",
            "category_id": "c1",
            "use_type": "Any",
            "confidence": 0.8,
            "priority": 1,
            "match_count": 5,
        },
        {
            "id": "r2",
            "description_pattern": "uber",
            "category_id": "c2",
            "use_type": "Any",
            "confidence": 0.8,
            "priority": 1,
            "match_count": 5,
        },
    ]

    result = _resolve_prefix_match(rows, "uber ride")
    assert result is not None
    assert result.category_source == "ambiguous"
    assert result.category_id is None


def test_prefix_tie_break_confidence_wins() -> None:
    rows = [
        {
            "id": "r1",
            "description_pattern": "uber",
            "category_id": "c1",
            "use_type": "Any",
            "confidence": 0.6,
            "priority": 5,
            "match_count": 10,
        },
        {
            "id": "r2",
            "description_pattern": "uber",
            "category_id": "c2",
            "use_type": "Any",
            "confidence": 0.9,
            "priority": 5,
            "match_count": 1,
        },
    ]

    result = _resolve_prefix_match(rows, "uber trip")
    assert result is not None
    assert result.category_source == "auto_prefix"
    assert result.category_id == "c2"
    assert result.category_rule_id == "r2"


def test_prefix_tie_break_match_count_wins() -> None:
    rows = [
        {
            "id": "r1",
            "description_pattern": "uber",
            "category_id": "c1",
            "use_type": "Any",
            "confidence": 0.8,
            "priority": 5,
            "match_count": 3,
        },
        {
            "id": "r2",
            "description_pattern": "uber",
            "category_id": "c2",
            "use_type": "Any",
            "confidence": 0.8,
            "priority": 5,
            "match_count": 12,
        },
    ]

    result = _resolve_prefix_match(rows, "uber trip")
    assert result is not None
    assert result.category_source == "auto_prefix"
    assert result.category_id == "c2"
    assert result.category_rule_id == "r2"


def test_unconfirmed_vendor_memory_confidence_is_capped(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        _seed_rule(conn, "uber", dining_id, use_type="Any", is_confirmed=0)

        exact = match_transaction(conn, "UBER", use_type="Personal")
        prefix = match_transaction(conn, "UBER TRIP", use_type="Personal")

    assert exact is not None
    assert exact.category_source == "vendor_memory"
    assert exact.category_confidence == 0.7

    assert prefix is not None
    assert prefix.category_source == "auto_prefix"
    assert prefix.category_confidence == 0.7


def test_match_transaction_payment_keyword_short_circuits_vendor_memory(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        payments_id = _seed_category(conn, "Payments & Transfers")
        dining_id = _seed_category(conn, "Dining")
        _seed_rule(conn, "credit card bill payment", dining_id, use_type="Any")

        result = match_transaction(conn, "BANK OF AMERICA CREDIT CARD BILL PAYMENT", use_type="Personal")

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.category_rule_id is None
    assert result.is_payment is True


def test_match_transaction_auto_pymt_keyword_marks_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        payments_row = conn.execute(
            "SELECT id FROM categories WHERE name = 'Payments & Transfers'"
        ).fetchone()
        payments_id = str(payments_row["id"]) if payments_row else _seed_category(conn, "Payments & Transfers")
        result = match_transaction(conn, "BLOOMINGDALES DES:AUTO PYMT", use_type="Personal")

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.is_payment is True


def test_match_transaction_is_payment_input_short_circuits_before_normalize(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        payments_id = _seed_category(conn, "Payments & Transfers")
        result = match_transaction(conn, "", use_type="Personal", is_payment=True)

    assert result is not None
    assert result.category_id == payments_id
    assert result.category_source == "keyword_rule"
    assert result.is_payment is True


def test_apply_match_sets_is_payment_when_result_marks_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Payments & Transfers")
        txn_id = uuid.uuid4().hex
        _seed_transaction(conn, txn_id, "Payment transaction", is_payment=0)

        applied = apply_match(
            conn,
            txn_id,
            MatchResult(
                category_id=category_id,
                category_source="keyword_rule",
                category_confidence=0.9,
                category_rule_id=None,
                is_payment=True,
            ),
        )
        row = conn.execute("SELECT is_payment FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    assert applied is True
    assert row["is_payment"] == 1


def test_apply_match_clears_is_payment_when_result_is_not_payment(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        txn_id = uuid.uuid4().hex
        _seed_transaction(conn, txn_id, "Already payment", is_payment=1)

        applied = apply_match(
            conn,
            txn_id,
            MatchResult(
                category_id=category_id,
                category_source="keyword_rule",
                category_confidence=0.9,
                category_rule_id=None,
                is_payment=False,
            ),
        )
        row = conn.execute("SELECT is_payment FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    assert applied is True
    assert row["is_payment"] == 0


def test_lookup_category_mapping_exact_match_increments_count(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        conn.execute(
            """
            INSERT INTO category_mappings (
                id, source_category, source, category_id, created_by, match_count, is_enabled
            ) VALUES (?, ?, ?, ?, 'system', 0, 1)
            """,
            (uuid.uuid4().hex, "Restaurant-Restaurant", "plaid", dining_id),
        )
        conn.commit()

        resolved = lookup_category_mapping(conn, "restaurant-restaurant", "plaid")
        row = conn.execute(
            """
            SELECT match_count
              FROM category_mappings
             WHERE lower(source_category) = lower(?) AND source = 'plaid'
            """,
            ("Restaurant-Restaurant",),
        ).fetchone()

    assert resolved == dining_id
    assert row["match_count"] == 1


def test_lookup_category_mapping_uses_universal_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        travel_id = _seed_category(conn, "Travel")
        conn.execute(
            """
            INSERT INTO category_mappings (
                id, source_category, source, category_id, created_by, match_count, is_enabled
            ) VALUES (?, ?, NULL, ?, 'system', 0, 1)
            """,
            (uuid.uuid4().hex, "Airlines", travel_id),
        )
        conn.commit()

        resolved = lookup_category_mapping(conn, "airlines", "csv_import")
        row = conn.execute(
            """
            SELECT match_count
              FROM category_mappings
             WHERE lower(source_category) = lower(?) AND source IS NULL
            """,
            ("Airlines",),
        ).fetchone()

    assert resolved == travel_id
    assert row["match_count"] == 1


def test_map_plaid_pfc_gas_maps_to_transportation() -> None:
    category_name, is_payment = map_plaid_pfc_to_category(
        {"primary": "TRANSPORTATION", "detailed": "TRANSPORTATION_GAS"}
    )
    assert category_name == "Transportation"
    assert is_payment is False
