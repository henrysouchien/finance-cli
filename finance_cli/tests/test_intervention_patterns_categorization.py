from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from finance_cli.categorizer import normalize_description
from finance_cli.db import connect, initialize_database
from finance_cli.intervention_engine import run_engine
from finance_cli.interventions.categorization import (
    evaluate_k1_uncategorized_pileup,
    evaluate_k2_repeated_recategorization,
    evaluate_k3_bulk_memory_offer,
    evaluate_k4_new_merchant_confidence_check,
    evaluate_k5_stale_rule_override,
)
from finance_cli.interventions.context import build_context
from finance_cli.interventions.registry import PATTERN_REGISTRY, Move, Priority

NOW = datetime(2026, 6, 20, 12, 0, 0)


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _category_id(conn, name: str) -> str:
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row is not None:
        return str(row["id"])
    category_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO categories (id, name, is_income, is_system, sort_order)
        VALUES (?, ?, 0, 0, 0)
        """,
        (category_id, name),
    )
    return category_id


def _seed_user_recat(
    conn,
    *,
    description: str = "ACME COFFEE",
    category_name: str = "Coffee",
    txn_date: str,
    category_source: str = "user",
    use_type: str | None = "Personal",
) -> str:
    category_id = _category_id(conn, category_name)
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, category_source,
            category_confidence, use_type, is_active, is_payment, source
        ) VALUES (?, ?, ?, -500, ?, ?, 1.0, ?, 1, 0, 'manual')
        """,
        (txn_id, txn_date, description, category_id, category_source, use_type),
    )
    conn.commit()
    return txn_id


def _seed_auto_categorized(
    conn,
    *,
    description: str,
    category_name: str = "Dining",
    txn_date: str,
    category_source: str = "keyword_rule",
    use_type: str | None = "Personal",
) -> str:
    return _seed_user_recat(
        conn,
        description=description,
        category_name=category_name,
        txn_date=txn_date,
        category_source=category_source,
        use_type=use_type,
    )


def _seed_uncategorized_txn(
    conn,
    *,
    txn_date: str,
    description: str = "MYSTERY MERCHANT",
    is_reviewed: int = 0,
) -> str:
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id, is_active,
            is_reviewed, is_payment, source
        ) VALUES (?, ?, ?, -1200, NULL, 1, ?, 0, 'manual')
        """,
        (txn_id, txn_date, description, is_reviewed),
    )
    conn.commit()
    return txn_id


def _seed_categorized_txn(
    conn,
    *,
    txn_date: str,
    category_name: str,
    category_source: str,
) -> str:
    category_id = _category_id(conn, category_name)
    txn_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (
            id, date, description, amount_cents, category_id,
            category_source, is_active, is_reviewed, is_payment, source
        ) VALUES (?, ?, ?, -1200, ?, ?, 1, 1, 0, 'manual')
        """,
        (txn_id, txn_date, f"{category_name} Merchant", category_id, category_source),
    )
    conn.commit()
    return txn_id


def _seed_import_batch(conn, *, created_at: str) -> str:
    batch_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO import_batches (
            id, source_type, file_path, file_hash_sha256, created_at
        ) VALUES (?, 'csv', ?, ?, ?)
        """,
        (batch_id, f"/tmp/{batch_id}.csv", uuid.uuid4().hex, created_at),
    )
    conn.commit()
    return batch_id


def _seed_vendor_memory(
    conn,
    *,
    pattern: str,
    category_name: str = "Coffee",
    use_type: str = "Any",
    is_enabled: int = 1,
) -> str:
    rule_id = uuid.uuid4().hex
    category_id = _category_id(conn, category_name)
    conn.execute(
        """
        INSERT INTO vendor_memory (
            id, description_pattern, canonical_name, category_id, use_type,
            confidence, priority, is_enabled, is_confirmed, match_count
        ) VALUES (?, ?, ?, ?, ?, 1.0, 0, ?, 1, 0)
        """,
        (rule_id, pattern, pattern.title(), category_id, use_type, is_enabled),
    )
    conn.commit()
    return rule_id


def _seed_k2_happy_path(conn) -> None:
    for day in ("2026-04-01", "2026-05-01", "2026-06-01"):
        _seed_user_recat(conn, txn_date=day)


def _seed_k1_happy_path(conn) -> None:
    for index in range(10):
        _seed_uncategorized_txn(
            conn,
            txn_date=f"2026-06-{index + 1:02d}",
            description=f"MYSTERY MERCHANT {index}",
        )


def _evaluate_k1(conn):
    return evaluate_k1_uncategorized_pileup(conn, build_context(conn, now=NOW))


def _evaluate_k2(conn):
    return evaluate_k2_repeated_recategorization(conn, build_context(conn, now=NOW))


def _evaluate_k3(conn):
    return evaluate_k3_bulk_memory_offer(conn, build_context(conn, now=NOW))


def _evaluate_k4(conn):
    return evaluate_k4_new_merchant_confidence_check(conn, build_context(conn, now=NOW))


def _evaluate_k5(conn):
    return evaluate_k5_stale_rule_override(conn, build_context(conn, now=NOW))


def test_k1_fires_for_ten_uncategorized_transactions(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k1_happy_path(conn)
        for idx, source in enumerate(("vendor_memory", "keyword_rule", "user", "ai")):
            _seed_categorized_txn(
                conn,
                txn_date=f"2026-05-{idx + 1:02d}",
                category_name=f"Category {idx}",
                category_source=source,
            )

        intervention = _evaluate_k1(conn)

    assert intervention is not None
    assert intervention.pattern_id == "K-1"
    assert intervention.move is Move.PRESCRIBE
    assert intervention.tiers == (1, 3)
    assert "10 uncategorized transactions piling up" in intervention.headline
    assert "oldest is 19 days ago" in intervention.headline
    assert (
        "Historically, 75% of categorized transactions came from automatic sources."
        in intervention.detail_bullets
    )
    assert intervention.action is not None
    assert intervention.action.tool == "cat_auto_categorize"
    assert intervention.action.params == {"dry_run": True, "ai": False}


def test_k1_is_registered_and_runs_through_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k1_happy_path(conn)

        result = run_engine(conn, now=NOW)

    assert "K-1" in PATTERN_REGISTRY
    assert any(item.pattern_id == "K-1" for item in result.interventions)
    assert any(
        item.pattern_id == "K-1"
        for item in result.get_for_surface("action_queue")
    )


def test_k1_requires_ten_actionable_uncategorized_transactions(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for index in range(9):
            _seed_uncategorized_txn(conn, txn_date=f"2026-06-{index + 1:02d}")

        intervention = _evaluate_k1(conn)

    assert intervention is None


def test_k1_ignores_reviewed_uncategorized_transactions(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index in range(10):
            _seed_uncategorized_txn(
                conn,
                txn_date=f"2026-06-{index + 1:02d}",
                is_reviewed=1,
            )

        intervention = _evaluate_k1(conn)

    assert intervention is None


def test_k1_suppresses_when_import_batch_is_less_than_one_hour_old(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        _seed_k1_happy_path(conn)
        _seed_import_batch(conn, created_at="2026-06-20 11:30:00")

        intervention = _evaluate_k1(conn)

    assert intervention is None


def test_k1_allows_after_import_settle_window(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k1_happy_path(conn)
        _seed_import_batch(conn, created_at="2026-06-20 10:59:59")

        intervention = _evaluate_k1(conn)

    assert intervention is not None


def test_k1_handles_missing_historical_auto_coverage(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k1_happy_path(conn)

        intervention = _evaluate_k1(conn)

    assert intervention is not None
    assert (
        "No historical automatic categorization coverage yet; the dry-run "
        "preview will show what can be handled."
        in intervention.detail_bullets
    )


def test_k2_fires_for_repeated_same_vendor_user_recategorization(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)

        intervention = _evaluate_k2(conn)

    assert intervention is not None
    assert intervention.pattern_id == "K-2"
    assert intervention.move is Move.PATTERN_CATCH
    assert intervention.tiers == (1,)
    assert intervention.priority is Priority.MEDIUM
    assert "You've recategorized 'ACME COFFEE' 3 times" in intervention.headline
    assert "100% to Coffee" in intervention.headline
    assert "Estimated avoided fixes: about 1/month." in intervention.detail_bullets
    assert intervention.tier4_ladder is None
    assert intervention.action is not None
    assert intervention.action.tool == "cat_memory_add"
    assert intervention.action.params == {
        "pattern": normalize_description("ACME COFFEE"),
        "category": "Coffee",
        "use_type": "Personal",
    }


def test_k2_is_registered_and_runs_through_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)

        result = run_engine(conn, now=NOW)

    assert "K-2" in PATTERN_REGISTRY
    k2 = next(item for item in result.interventions if item.pattern_id == "K-2")
    assert "Estimated avoided fixes: about 1/month." in k2.detail_bullets
    assert k2.tier4_ladder is None
    assert any(
        item.pattern_id == "K-2"
        for item in result.get_for_surface("action_queue")
    )


def test_k2_requires_three_user_recategorizations(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_user_recat(conn, txn_date="2026-05-01")
        _seed_user_recat(conn, txn_date="2026-06-01")

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_ignores_non_user_category_sources(db_path: Path) -> None:
    with connect(db_path) as conn:
        for day in ("2026-04-01", "2026-05-01", "2026-06-01"):
            _seed_user_recat(conn, txn_date=day, category_source="keyword_rule")

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_suppresses_ambiguous_category_history_below_80_percent(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for day in ("2026-03-01", "2026-04-01", "2026-05-01"):
            _seed_user_recat(conn, txn_date=day, category_name="Coffee")
        _seed_user_recat(conn, txn_date="2026-06-01", category_name="Groceries")

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_accepts_exactly_80_percent_category_consistency(db_path: Path) -> None:
    with connect(db_path) as conn:
        for day in ("2026-02-01", "2026-03-01", "2026-04-01", "2026-05-01"):
            _seed_user_recat(conn, txn_date=day, category_name="Coffee")
        _seed_user_recat(conn, txn_date="2026-06-01", category_name="Groceries")

        intervention = _evaluate_k2(conn)

    assert intervention is not None
    assert "80% to Coffee" in intervention.headline
    assert "Most common category: Coffee (4/5 fixes)." in intervention.detail_bullets


def test_k2_suppresses_existing_exact_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            use_type="Personal",
        )

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_suppresses_existing_prefix_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)
        _seed_vendor_memory(conn, pattern="acme", use_type="Any")

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_does_not_reenable_disabled_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            use_type="Personal",
            is_enabled=0,
        )

        intervention = _evaluate_k2(conn)

    assert intervention is None


def test_k2_uses_any_use_type_for_mixed_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_user_recat(conn, txn_date="2026-04-01", use_type="Personal")
        _seed_user_recat(conn, txn_date="2026-05-01", use_type="Business")
        _seed_user_recat(conn, txn_date="2026-06-01", use_type=None)

        intervention = _evaluate_k2(conn)

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["use_type"] == "Any"


def test_k2_uses_any_when_history_has_unknown_use_type(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_user_recat(conn, txn_date="2026-04-01", use_type="Personal")
        _seed_user_recat(conn, txn_date="2026-05-01", use_type="Personal")
        _seed_user_recat(conn, txn_date="2026-06-01", use_type=None)

        intervention = _evaluate_k2(conn)

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["use_type"] == "Any"


def test_k2_does_not_suppress_for_non_applicable_use_type_rule(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_k2_happy_path(conn)
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            use_type="Business",
        )

        intervention = _evaluate_k2(conn)

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params["use_type"] == "Personal"


def test_k3_fires_for_five_recent_user_categorized_transactions(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for index, day in enumerate(("14", "15", "16", "17", "18")):
            _seed_user_recat(
                conn,
                description=f"BULK MERCHANT {index}",
                category_name="Coffee",
                txn_date=f"2026-06-{day}",
            )

        intervention = _evaluate_k3(conn)

    assert intervention is not None
    assert intervention.pattern_id == "K-3"
    assert intervention.move is Move.PRESCRIBE
    assert intervention.tiers == (1, 3)
    assert intervention.priority is Priority.MEDIUM
    assert "5 transactions this week across 5 merchants" in intervention.headline
    assert "Next time I'll handle them automatically." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "cat_memory_add_bulk"
    assert intervention.action.params["dry_run"] is False
    assert intervention.action.params["rules"] == [
        {
            "pattern": normalize_description("BULK MERCHANT 4"),
            "category": "Coffee",
            "use_type": "Personal",
        },
        {
            "pattern": normalize_description("BULK MERCHANT 3"),
            "category": "Coffee",
            "use_type": "Personal",
        },
        {
            "pattern": normalize_description("BULK MERCHANT 2"),
            "category": "Coffee",
            "use_type": "Personal",
        },
        {
            "pattern": normalize_description("BULK MERCHANT 1"),
            "category": "Coffee",
            "use_type": "Personal",
        },
        {
            "pattern": normalize_description("BULK MERCHANT 0"),
            "category": "Coffee",
            "use_type": "Personal",
        },
    ]


def test_k3_is_registered_and_runs_through_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index in range(5):
            _seed_user_recat(
                conn,
                description=f"BULK REGISTERED {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        result = run_engine(conn, now=NOW)

    assert "K-3" in PATTERN_REGISTRY
    assert any(item.pattern_id == "K-3" for item in result.interventions)
    assert any(
        item.pattern_id == "K-3"
        for item in result.get_for_surface("action_queue")
    )


def test_k3_requires_five_actionable_recent_user_categorizations(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for index in range(4):
            _seed_user_recat(
                conn,
                description=f"BULK SHORT {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k3_ignores_old_manual_categorizations(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_user_recat(
            conn,
            description="BULK OLD",
            txn_date="2026-06-13",
        )
        for index in range(4):
            _seed_user_recat(
                conn,
                description=f"BULK RECENT {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k3_skips_merchants_with_existing_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index in range(5):
            _seed_user_recat(
                conn,
                description=f"BULK KNOWN {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("BULK KNOWN 0"),
            use_type="Personal",
        )

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k3_skips_merchants_recently_suggested_by_k2(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index in range(5):
            _seed_user_recat(
                conn,
                description=f"BULK K2 {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        conn.execute(
            """
            INSERT INTO intervention_log (
                pattern_id, fired_at, surface, user_action, headline, payload
            ) VALUES ('K-2', '2026-06-19 12:00:00', 'action_queue', 'pending', ?, ?)
            """,
            (
                "K-2",
                json.dumps(
                    {
                        "action": {
                            "params": {
                                "pattern": normalize_description("BULK K2 0")
                            }
                        }
                    }
                ),
            ),
        )
        conn.commit()

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k3_leaves_repeated_same_merchant_patterns_to_k2(db_path: Path) -> None:
    with connect(db_path) as conn:
        for day in ("2026-06-14", "2026-06-15", "2026-06-16"):
            _seed_user_recat(
                conn,
                description="BULK REPEATED",
                category_name="Coffee",
                txn_date=day,
            )
        _seed_user_recat(
            conn,
            description="BULK OTHER 1",
            category_name="Coffee",
            txn_date="2026-06-17",
        )
        _seed_user_recat(
            conn,
            description="BULK OTHER 2",
            category_name="Coffee",
            txn_date="2026-06-18",
        )

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k3_ignores_ambiguous_same_merchant_category_pairs(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for index in range(4):
            _seed_user_recat(
                conn,
                description=f"BULK STABLE {index}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_user_recat(
            conn,
            description="BULK AMBIGUOUS",
            category_name="Coffee",
            txn_date="2026-06-18",
        )
        _seed_user_recat(
            conn,
            description="BULK AMBIGUOUS",
            category_name="Groceries",
            txn_date="2026-06-19",
        )

        intervention = _evaluate_k3(conn)

    assert intervention is None


def test_k4_fires_for_five_new_auto_categorized_merchants(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D", "E")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO MERCHANT {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        intervention = _evaluate_k4(conn)

    assert intervention is not None
    assert intervention.pattern_id == "K-4"
    assert intervention.move is Move.COACH
    assert intervention.tiers == (1,)
    assert intervention.priority is Priority.LOW
    assert "5 new merchants this week" in intervention.headline
    assert "AUTO MERCHANT E as Dining" in intervention.headline
    assert "Only first-time merchants" in intervention.detail_bullets[0]
    assert intervention.action is not None
    assert intervention.action.tool == "cat_review_new_merchants"
    assert intervention.action.params["dry_run"] is False
    items = intervention.action.params["items"]
    assert len(items) == 5
    assert items[0]["decision"] == "confirm"
    assert items[0]["merchant"] == "AUTO MERCHANT E"
    assert items[0]["pattern"] == normalize_description("AUTO MERCHANT E")
    assert items[0]["category"] == "Dining"
    assert items[0]["use_type"] == "Personal"
    assert len(items[0]["txn_ids"]) == 1


def test_k4_is_registered_and_runs_through_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D", "E")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO REGISTERED {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        result = run_engine(conn, now=NOW)

    assert "K-4" in PATTERN_REGISTRY
    assert any(item.pattern_id == "K-4" for item in result.interventions)
    assert any(
        item.pattern_id == "K-4"
        for item in result.get_for_surface("action_queue")
    )


def test_k4_caps_headline_merchant_list_at_five(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D", "E", "F")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO CAP {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        intervention = _evaluate_k4(conn)

    assert intervention is not None
    assert "6 new merchants this week" in intervention.headline
    assert "and 1 more" in intervention.headline
    assert "AUTO CAP A as Dining" not in intervention.headline
    assert intervention.action is not None
    assert len(intervention.action.params["items"]) == 6


def test_k4_requires_five_new_auto_categorized_merchants(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO SHORT {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )

        intervention = _evaluate_k4(conn)

    assert intervention is None


def test_k4_skips_merchants_with_prior_transaction_history(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_auto_categorized(
            conn,
            description="AUTO PRIOR",
            txn_date="2026-06-10",
        )
        for index, letter in enumerate(("A", "B", "C", "D")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO RECENT {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_auto_categorized(
            conn,
            description="AUTO PRIOR",
            txn_date="2026-06-18",
        )

        intervention = _evaluate_k4(conn)

    assert intervention is None


def test_k4_skips_merchants_with_existing_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D", "E")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO KNOWN {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("AUTO KNOWN A"),
            use_type="Personal",
        )

        intervention = _evaluate_k4(conn)

    assert intervention is None


def test_k4_ignores_non_auto_category_sources(db_path: Path) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO SOURCE {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_auto_categorized(
            conn,
            description="AUTO SOURCE USER",
            txn_date="2026-06-18",
            category_source="user",
        )

        intervention = _evaluate_k4(conn)

    assert intervention is None


def test_k4_skips_inconsistent_auto_categories_for_same_merchant(
    db_path: Path,
) -> None:
    with connect(db_path) as conn:
        for index, letter in enumerate(("A", "B", "C", "D")):
            _seed_auto_categorized(
                conn,
                description=f"AUTO STABLE {letter}",
                txn_date=f"2026-06-{14 + index:02d}",
            )
        _seed_auto_categorized(
            conn,
            description="AUTO MIXED",
            category_name="Dining",
            txn_date="2026-06-18",
        )
        _seed_auto_categorized(
            conn,
            description="AUTO MIXED",
            category_name="Groceries",
            txn_date="2026-06-19",
            category_source="ai",
        )

        intervention = _evaluate_k4(conn)

    assert intervention is None


def test_k5_fires_for_stale_exact_vendor_memory_rule(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-03",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is not None
    assert intervention.pattern_id == "K-5"
    assert intervention.move is Move.WARN
    assert intervention.priority is Priority.MEDIUM
    assert "says Coffee" in intervention.headline
    assert "changing it to Groceries" in intervention.headline
    assert "2 overrides across 2 different dates." in intervention.detail_bullets
    assert intervention.action is not None
    assert intervention.action.tool == "cat_memory_add"
    assert intervention.action.params == {
        "pattern": normalize_description("ACME COFFEE"),
        "category": "Groceries",
        "use_type": "Personal",
    }


def test_k5_is_registered_and_runs_through_action_queue(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-03",
        )

        result = run_engine(conn, now=NOW)

    assert "K-5" in PATTERN_REGISTRY
    assert any(item.pattern_id == "K-5" for item in result.interventions)
    assert any(
        item.pattern_id == "K-5"
        for item in result.get_for_surface("action_queue")
    )


def test_k5_fires_for_stale_prefix_vendor_memory_rule(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern="acme",
            category_name="Coffee",
            use_type="Any",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE 101",
            category_name="Groceries",
            txn_date="2026-06-01",
            use_type=None,
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE 202",
            category_name="Groceries",
            txn_date="2026-06-03",
            use_type=None,
        )

        intervention = _evaluate_k5(conn)

    assert intervention is not None
    assert intervention.action is not None
    assert intervention.action.params == {
        "pattern": "acme",
        "category": "Groceries",
        "use_type": "Any",
    }


def test_k5_requires_two_override_dates(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is None


def test_k5_suppresses_same_day_overrides(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is None


def test_k5_ignores_user_category_matching_vendor_memory(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Coffee",
            txn_date="2026-06-01",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Coffee",
            txn_date="2026-06-03",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is None


def test_k5_ignores_disabled_vendor_memory_rule(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Personal",
            is_enabled=0,
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-03",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is None


def test_k5_ignores_non_applicable_use_type_rule(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_vendor_memory(
            conn,
            pattern=normalize_description("ACME COFFEE"),
            category_name="Coffee",
            use_type="Business",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-01",
            use_type="Personal",
        )
        _seed_user_recat(
            conn,
            description="ACME COFFEE",
            category_name="Groceries",
            txn_date="2026-06-03",
            use_type="Personal",
        )

        intervention = _evaluate_k5(conn)

    assert intervention is None
