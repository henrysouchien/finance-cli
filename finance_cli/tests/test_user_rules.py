from __future__ import annotations

import uuid
from pathlib import Path
import textwrap

import pytest

import finance_cli.user_rules as user_rules_module
from finance_cli.categorizer import MatchResult, apply_match, match_transaction
from finance_cli.db import connect, initialize_database
from finance_cli.user_rules import (
    CANONICAL_CATEGORIES,
    UserRules,
    _empty_rules,
    load_rules,
    match_keyword_rule,
    match_payment_keyword,
    resolve_category_alias,
)


def _write_rules(path: Path, content: str) -> Path:
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    return path


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_transaction(conn, txn_id: str, description: str, amount_cents: int, category_id: str | None = None, use_type: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id,
            date,
            description,
            amount_cents,
            category_id,
            use_type,
            source,
            is_active
        ) VALUES (?, '2025-01-05', ?, ?, ?, ?, 'manual', 1)
        """,
        (txn_id, description, amount_cents, category_id, use_type),
    )
    conn.commit()


def test_keyword_rule_precedence_longest_then_priority_then_file_order(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        keyword_rules:
          - keywords: ["AMZN"]
            category: "Shopping"
            use_type: Personal
            priority: 0
          - keywords: ["AMZN PRIME"]
            category: "Software & Subscriptions"
            use_type: Personal
            priority: 0
          - keywords: ["UBER"]
            category: "Travel"
            use_type: Personal
            priority: 1
          - keywords: ["UBER"]
            category: "Transportation"
            use_type: Personal
            priority: 5
          - keywords: ["LYFT"]
            category: "Travel"
            use_type: Personal
            priority: 2
          - keywords: ["LYFT"]
            category: "Transportation"
            use_type: Personal
            priority: 2
        """,
    )
    rules = load_rules(rules_path)

    longest = match_keyword_rule("Payment AMZN PRIME annual", rules)
    assert longest is not None
    assert longest.category == "Software & Subscriptions"
    assert longest.matched_keyword == "AMZN PRIME"

    priority = match_keyword_rule("UBER TRIP", rules)
    assert priority is not None
    assert priority.category == "Transportation"

    file_order = match_keyword_rule("LYFT RIDE", rules)
    assert file_order is not None
    assert file_order.category == "Travel"
    assert file_order.rule_index == 4


def test_alias_resolution_is_case_insensitive(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        category_aliases:
          "Education & Training": "Professional Fees"
          "Other": null
        """,
    )
    rules = load_rules(rules_path)

    assert resolve_category_alias("Education & Training", rules) == "Professional Fees"
    assert resolve_category_alias("education & training", rules) == "Professional Fees"
    assert resolve_category_alias("other", rules) is None
    assert resolve_category_alias("Travel", rules) == "Travel"


def test_match_payment_keyword_matches_expected_patterns(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        payment_keywords:
          - "CREDIT CARD BILL PAYMENT"
          - "AUTOPAY PAYMENT"
        """,
    )
    rules = load_rules(rules_path)

    assert match_payment_keyword("BANK OF AMERICA CREDIT CARD BILL PAYMENT", rules) is True
    assert match_payment_keyword("CHASE AUTOPAY PAYMENT", rules) is True


def test_match_payment_keyword_rejects_false_positives(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        payment_keywords:
          - "CREDIT CARD BILL PAYMENT"
          - "AUTOPAY PAYMENT"
        """,
    )
    rules = load_rules(rules_path)

    assert match_payment_keyword("ROYALTY PAYMENT FROM DISTRIBUTOR", rules) is False
    assert match_payment_keyword("KARTRA PAYOUT", rules) is False
    assert match_payment_keyword("ATT* BILL PAYMENT", rules) is False
    assert match_payment_keyword("AMAZON MEDIA EU DES:PAYMENT", rules) is False


def test_match_keyword_rule_loom_dot_com_avoids_bloomingdales_false_positive(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        keyword_rules:
          - keywords: ["LOOM.COM"]
            category: "Software & Subscriptions"
            use_type: Business
            priority: 0
        """,
    )
    rules = load_rules(rules_path)

    loom = match_keyword_rule("LOOM.COM SUBSCRIPTION", rules)
    assert loom is not None
    assert loom.matched_keyword == "LOOM.COM"

    bloomingdales = match_keyword_rule("BLOOMINGDALES DES:AUTO PYMT", rules)
    assert bloomingdales is None


def test_canonical_categories_has_expected_size() -> None:
    assert len(CANONICAL_CATEGORIES) == 39


def test_alias_resolution_is_single_pass_no_chaining(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        category_aliases:
          "A": "B"
          "B": "C"
        """,
    )
    rules = load_rules(rules_path)
    assert resolve_category_alias("A", rules) == "B"


def test_match_transaction_keyword_rule_resolves_alias(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        keyword_rules:
          - keywords: ["TUITION"]
            category: "Education & Training"
            use_type: Business
            priority: 0
        category_aliases:
          "Education & Training": "Professional Fees"
        """,
    )
    rules = load_rules(rules_path)
    monkeypatch.setattr("finance_cli.categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        pro_fees_id = _seed_category(conn, "Professional Fees")
        result = match_transaction(conn, "TUITION PAYMENT", use_type="Business")

    assert result is not None
    assert result.category_source == "keyword_rule"
    assert result.category_id == pro_fees_id
    assert result.category_confidence == 0.9


def test_apply_match_split_rule_rounding_and_idempotent(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        split_rules:
          - match:
              category: Rent
            business_pct: 25
            business_category: Rent
            personal_category: Rent
            note: "25% split"
        """,
    )
    rules = load_rules(rules_path)
    monkeypatch.setattr("finance_cli.categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        rent_id = _seed_category(conn, "Rent")
        parent_id = uuid.uuid4().hex
        _seed_transaction(conn, parent_id, "RENT PAYMENT", -1001, category_id=None, use_type="Business")

        applied = apply_match(
            conn,
            parent_id,
            MatchResult(
                category_id=rent_id,
                category_source="plaid",
                category_confidence=0.5,
                category_rule_id=None,
                matched_rule_id=None,
            ),
        )
        assert applied is True

        parent = conn.execute(
            "SELECT is_active, split_group_id FROM transactions WHERE id = ?",
            (parent_id,),
        ).fetchone()
        assert parent["is_active"] == 0
        assert parent["split_group_id"] is not None

        children = conn.execute(
            """
            SELECT amount_cents, use_type, split_pct, category_source
              FROM transactions
             WHERE parent_transaction_id = ?
             ORDER BY use_type ASC
            """,
            (parent_id,),
        ).fetchall()
        assert len(children) == 2
        assert [row["use_type"] for row in children] == ["Business", "Personal"]
        assert [row["amount_cents"] for row in children] == [-250, -751]
        assert [row["split_pct"] for row in children] == [0.25, 0.75]
        assert all(row["category_source"] == "keyword_rule" for row in children)

        apply_match(
            conn,
            parent_id,
            MatchResult(
                category_id=rent_id,
                category_source="plaid",
                category_confidence=0.5,
                category_rule_id=None,
                matched_rule_id=None,
            ),
        )
        child_count = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE parent_transaction_id = ?",
            (parent_id,),
        ).fetchone()["n"]
        assert child_count == 2


def test_category_override_skips_user_and_keyword_rule_sources(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        category_overrides:
          - categories: [Travel]
            force_use_type: Personal
            note: "Travel defaults personal"
        """,
    )
    rules = load_rules(rules_path)
    monkeypatch.setattr("finance_cli.categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        travel_id = _seed_category(conn, "Travel")

        txn_plaid = uuid.uuid4().hex
        txn_user = uuid.uuid4().hex
        txn_keyword = uuid.uuid4().hex
        _seed_transaction(conn, txn_plaid, "Flight", -10000, category_id=None, use_type=None)
        _seed_transaction(conn, txn_user, "Train", -5000, category_id=None, use_type="Business")
        _seed_transaction(conn, txn_keyword, "Taxi", -3000, category_id=None, use_type="Business")

        apply_match(
            conn,
            txn_plaid,
            MatchResult(travel_id, "plaid", 0.5, None, None),
        )
        apply_match(
            conn,
            txn_user,
            MatchResult(travel_id, "user", 1.0, None, None),
        )
        apply_match(
            conn,
            txn_keyword,
            MatchResult(travel_id, "keyword_rule", 0.9, None, None),
        )

        row_plaid = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_plaid,)).fetchone()
        row_user = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_user,)).fetchone()
        row_keyword = conn.execute("SELECT use_type FROM transactions WHERE id = ?", (txn_keyword,)).fetchone()

    assert row_plaid["use_type"] == "Personal"
    assert row_user["use_type"] == "Business"
    assert row_keyword["use_type"] == "Business"


def test_user_rules_ai_parser_default() -> None:
    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
    )
    assert rules.ai_parser == {}
    assert rules.extractors == {}


def test_load_rules_parses_ai_parser_section(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        ai_parser:
          provider: openai
          model: gpt-4o-mini
          confidence_warn: 0.8
        """,
    )
    rules = load_rules(rules_path)
    assert rules.ai_parser["provider"] == "openai"
    assert rules.ai_parser["model"] == "gpt-4o-mini"


def test_load_rules_parses_extractors_section(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        extractors:
          default_backend: azure
          azure:
            endpoint_env: AZURE_DI_ENDPOINT
            api_key_env: AZURE_DI_API_KEY
          bsc:
            api_key_env: BSC_API_KEY
        """,
    )
    rules = load_rules(rules_path)
    assert rules.extractors["default_backend"] == "azure"
    assert rules.extractors["azure"]["endpoint_env"] == "AZURE_DI_ENDPOINT"
    assert rules.extractors["bsc"]["api_key_env"] == "BSC_API_KEY"


def test_load_rules_parses_payment_keywords(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        payment_keywords:
          - "CREDIT CARD BILL PAYMENT"
          - "PAYMENT - THANK"
        """,
    )
    rules = load_rules(rules_path)
    assert rules.payment_keywords == ["CREDIT CARD BILL PAYMENT", "PAYMENT - THANK"]


def test_load_rules_rejects_unknown_default_backend(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        extractors:
          default_backend: unsupported
        """,
    )
    with pytest.raises(ValueError, match="extractors.default_backend"):
        load_rules(rules_path)


def test_empty_rules_has_ai_parser() -> None:
    rules = _empty_rules()
    assert rules.ai_parser == {}
    assert rules.extractors == {}


def test_load_rules_uses_mtime_cache(tmp_path: Path, monkeypatch) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        keyword_rules:
          - keywords: ["AMZN"]
            category: "Shopping"
            use_type: Personal
            priority: 0
        """,
    )

    calls = {"count": 0}
    original_safe_load = user_rules_module.yaml.safe_load

    def _counting_safe_load(payload: str):
        calls["count"] += 1
        return original_safe_load(payload)

    monkeypatch.setattr(user_rules_module.yaml, "safe_load", _counting_safe_load)

    first = load_rules(rules_path)
    second = load_rules(rules_path)

    assert calls["count"] == 1
    assert first is second


def test_load_rules_returns_empty_when_file_removed(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path / "rules.yaml",
        """
        category_aliases:
          "Education & Training": "Professional Fees"
        """,
    )

    first = load_rules(rules_path)
    assert first.category_aliases == {"Education & Training": "Professional Fees"}

    rules_path.unlink()

    second = load_rules(rules_path)
    assert second == _empty_rules()
