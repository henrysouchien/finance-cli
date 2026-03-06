from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from finance_cli.__main__ import main
from finance_cli.ai_categorizer import (
    _available_categories,
    _get_or_create_category_id,
    categorize_batch,
    categorize_uncategorized,
)
from finance_cli.db import connect, initialize_database
from finance_cli.user_rules import UserRules


def _seed_category(conn, name: str) -> str:
    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_transaction(conn, txn_id: str, description: str) -> None:
    conn.execute(
        """
        INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
        VALUES (?, '2025-01-05', ?, -1200, 'manual', 1)
        """,
        (txn_id, description),
    )
    conn.commit()


def test_categorize_batch_uses_transaction_id_for_duplicate_descriptions(monkeypatch) -> None:
    response = json.dumps(
        [
            {"id": "txn_b", "category": "Groceries", "use_type": "Personal", "reasoning": "Food"},
            {"id": "txn_a", "category": "Dining", "use_type": "Business", "reasoning": "Meal"},
        ]
    )
    monkeypatch.setattr(
        "finance_cli.ai_categorizer._request_provider",
        lambda *_: (response, {"input_tokens": 10, "output_tokens": 4}),
    )

    batch = categorize_batch(
        transactions=[
            {"id": "txn_a", "description": "UBER TRIP"},
            {"id": "txn_b", "description": "UBER TRIP"},
        ],
        categories=["Dining", "Groceries"],
        provider="openai",
        model="gpt-test",
    )

    assert [item.transaction_id for item in batch.results] == ["txn_a", "txn_b"]
    assert batch.results[0].category_name == "Dining"
    assert batch.results[1].category_name == "Groceries"
    assert batch.input_tokens == 10
    assert batch.output_tokens == 4


def test_categorize_batch_retries_malformed_json_and_handles_partial_results(monkeypatch) -> None:
    calls = {"count": 0}

    def _fake_request(*_args):
        calls["count"] += 1
        if calls["count"] == 1:
            return ('[{"id":"txn_a","category":"Dining"', {"input_tokens": 11, "output_tokens": 2})
        return (
            json.dumps(
                [
                    {
                        "id": "txn_a",
                        "category": "Dining",
                        "use_type": "Personal",
                        "reasoning": "Meal",
                    }
                ]
            ),
            {"input_tokens": 13, "output_tokens": 3},
        )

    monkeypatch.setattr("finance_cli.ai_categorizer._request_provider", _fake_request)

    batch = categorize_batch(
        transactions=[
            {"id": "txn_a", "description": "Lunch"},
            {"id": "txn_b", "description": "Lunch"},
        ],
        categories=["Dining"],
        provider="claude",
        model="claude-test",
    )

    assert calls["count"] == 2
    assert batch.results[0].transaction_id == "txn_a"
    assert batch.results[0].category_name == "Dining"
    assert batch.results[0].error is None
    assert batch.results[1].transaction_id == "txn_b"
    assert batch.results[1].category_name is None
    assert batch.results[1].error == "missing_result_for_id"
    assert batch.input_tokens == 24
    assert batch.output_tokens == 5


def test_categorize_batch_logs_info(monkeypatch) -> None:
    response = json.dumps(
        [{"id": "txn_a", "category": "Dining", "use_type": "Personal", "reasoning": "Meal"}]
    )
    monkeypatch.setattr(
        "finance_cli.ai_categorizer._request_provider",
        lambda *_: (response, {"input_tokens": 9, "output_tokens": 4}),
    )

    messages: list[str] = []

    class _FakeLogger:
        def info(self, message, *args):
            rendered = str(message) % args if args else str(message)
            messages.append(rendered)

        def warning(self, message, *args):
            rendered = str(message) % args if args else str(message)
            messages.append(rendered)

    monkeypatch.setattr("finance_cli.ai_categorizer.logger", _FakeLogger())
    batch = categorize_batch(
        transactions=[{"id": "txn_a", "description": "Lunch"}],
        categories=["Dining"],
        provider="openai",
        model="gpt-test",
    )

    assert batch.input_tokens == 9
    assert batch.output_tokens == 4
    assert "AI categorize batch starting batch_size=1 provider=openai model=gpt-test" in messages
    assert "AI categorize batch complete batch_size=1 categorized=1 failed=0 input_tokens=9 output_tokens=4" in messages


def test_categorize_batch_retry_accumulates_tokens(monkeypatch) -> None:
    responses = [
        ('[{"id":"txn_a","category":"Dining"', {"input_tokens": 12, "output_tokens": 2}),
        (
            json.dumps([{"id": "txn_a", "category": "Dining", "use_type": "Personal"}]),
            {"input_tokens": 7, "output_tokens": 3},
        ),
    ]

    def _fake_request(*_args):
        return responses.pop(0)

    monkeypatch.setattr("finance_cli.ai_categorizer._request_provider", _fake_request)
    batch = categorize_batch(
        transactions=[{"id": "txn_a", "description": "Lunch"}],
        categories=["Dining"],
        provider="openai",
        model="gpt-test",
    )

    assert batch.input_tokens == 19
    assert batch.output_tokens == 5
    assert batch.results[0].error is None


def test_categorize_batch_failure_reports_tokens(monkeypatch) -> None:
    responses = [
        ('{"not":"an array"}', {"input_tokens": 20, "output_tokens": 4}),
        ('{"still":"bad"}', {"input_tokens": 21, "output_tokens": 5}),
    ]

    def _fake_request(*_args):
        return responses.pop(0)

    monkeypatch.setattr("finance_cli.ai_categorizer._request_provider", _fake_request)
    batch = categorize_batch(
        transactions=[{"id": "txn_a", "description": "Lunch"}],
        categories=["Dining"],
        provider="claude",
        model="claude-test",
    )

    assert batch.input_tokens == 41
    assert batch.output_tokens == 9
    assert len(batch.results) == 1
    assert str(batch.results[0].error).startswith("parse_failed:")


def test_categorize_uncategorized_auto_remember_and_alias_resolution(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={"Food": "Dining"},
        income_sources={},
        ai_categorizer={
            "provider": "openai",
            "model": "gpt-test",
            "batch_size": 10,
            "auto_remember": True,
            "auto_remember_confirmed": False,
            "confidence": 0.7,
            "available_categories": ["Dining"],
        },
        raw={},
    )

    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)
    monkeypatch.setattr(
        "finance_cli.ai_categorizer._request_provider",
        lambda *_: (
            json.dumps(
                [{"id": "txn_1", "category": "Food", "use_type": "Personal", "reasoning": "Restaurant"}]
            ),
            {"input_tokens": 25, "output_tokens": 6},
        ),
    )

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        _seed_transaction(conn, "txn_1", "JOE'S PIZZA")

        report = categorize_uncategorized(conn, limit=10, dry_run=False, provider="openai", batch_size=5)

        txn = conn.execute(
            "SELECT category_id, category_source, category_confidence FROM transactions WHERE id = 'txn_1'"
        ).fetchone()
        memory = conn.execute(
            "SELECT category_id, is_confirmed FROM vendor_memory ORDER BY rowid ASC LIMIT 1"
        ).fetchone()
        ai_log = conn.execute(
            "SELECT provider, model, prompt_hash, category_name FROM ai_categorization_log WHERE transaction_id = 'txn_1'"
        ).fetchone()

    assert report["categorized"] == 1
    assert report["failed"] == 0
    assert report["input_tokens"] == 25
    assert report["output_tokens"] == 6
    assert report["elapsed_ms"] >= 0

    assert txn["category_id"] == dining_id
    assert txn["category_source"] == "ai"
    assert txn["category_confidence"] == 0.7

    assert memory is not None
    assert memory["category_id"] == dining_id
    assert memory["is_confirmed"] == 0

    assert ai_log is not None
    assert ai_log["provider"] == "openai"
    assert ai_log["model"] == "gpt-test"
    assert ai_log["category_name"] == "Dining"
    assert len(str(ai_log["prompt_hash"])) == 64


def test_categorize_uncategorized_requires_provider_when_missing(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
        raw={},
    )
    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        _seed_category(conn, "Dining")
        _seed_transaction(conn, "txn_missing_provider", "UNKNOWN MERCHANT")

        with pytest.raises(ValueError, match="AI provider is required"):
            categorize_uncategorized(conn, limit=10, dry_run=True)


def test_categorize_uncategorized_empty_input(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={"provider": "openai"},
        raw={},
    )
    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        report = categorize_uncategorized(conn, limit=10, dry_run=True)

    assert report["categorized"] == 0
    assert report["failed"] == 0
    assert report["batches"] == 0
    assert report["input_tokens"] == 0
    assert report["output_tokens"] == 0
    assert report["elapsed_ms"] == 0


def test_cat_memory_confirm_command(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        category_id = _seed_category(conn, "Dining")
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, confidence,
                priority, is_enabled, is_confirmed, match_count
            ) VALUES ('rule1', 'coffee', ?, 'Any', 0.7, 0, 1, 0, 0)
            """,
            (category_id,),
        )
        conn.commit()

    code = main(["cat", "memory", "confirm", "rule1"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute("SELECT is_confirmed FROM vendor_memory WHERE id = 'rule1'").fetchone()
    assert row["is_confirmed"] == 1


def test_cat_auto_categorize_with_ai_flags(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_transaction(conn, "txn_ai", "UNKNOWN MERCHANT")

    monkeypatch.setattr(
        "finance_cli.commands.cat.categorize_uncategorized",
        lambda *_args, **_kwargs: {
            "categorized": 1,
            "failed": 0,
            "batches": 1,
            "provider": "openai",
            "model": "gpt-test",
            "input_tokens": 10,
            "output_tokens": 3,
            "elapsed_ms": 12,
        },
    )

    code = main(["cat", "auto-categorize", "--ai", "--provider", "openai", "--batch-size", "5"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["status"] == "success"
    assert payload["data"]["updated"] == 1
    assert payload["data"]["by_source"]["ai"] == 1


def test_cat_auto_categorize_cli_includes_tokens(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_transaction(conn, "txn_ai_cli", "UNKNOWN MERCHANT")

    monkeypatch.setattr(
        "finance_cli.commands.cat.categorize_uncategorized",
        lambda *_args, **_kwargs: {
            "categorized": 1,
            "failed": 2,
            "batches": 3,
            "provider": "openai",
            "model": "gpt-test",
            "input_tokens": 11,
            "output_tokens": 7,
            "elapsed_ms": 45,
        },
    )

    code = main(["cat", "auto-categorize", "--ai", "--provider", "openai", "--format", "cli"])
    output = capsys.readouterr().out
    assert code == 0
    assert "ai: categorized=1 failed=2 batches=3 tokens=in:11/out:7 elapsed=45ms" in output


def test_cat_auto_categorize_ai_requires_provider_when_not_configured(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_transaction(conn, "txn_ai_missing_provider", "UNKNOWN MERCHANT")

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
        raw={},
    )
    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)

    code = main(["cat", "auto-categorize", "--ai"])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert "AI provider is required" in payload["error"]


def test_available_categories_returns_only_canonical(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with connect(db_path) as conn:
        _seed_category(conn, "Dining")
        _seed_category(conn, "Custom Category")
        categories = _available_categories(conn, configured=["Custom Category"])

    assert "Dining" in categories
    assert "Custom Category" not in categories


def test_get_or_create_category_id_rejects_non_canonical(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={},
        income_sources={},
        ai_categorizer={},
        raw={},
    )
    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="Non-canonical category"):
            _get_or_create_category_id(conn, "Not Canonical")


def test_get_or_create_category_id_resolves_alias_before_lookup(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rules = UserRules(
        keyword_rules=[],
        split_rules=[],
        category_overrides=[],
        category_aliases={"Food": "Dining"},
        income_sources={},
        ai_categorizer={},
        raw={},
    )
    monkeypatch.setattr("finance_cli.ai_categorizer.load_rules", lambda: rules)

    with connect(db_path) as conn:
        dining_id = _seed_category(conn, "Dining")
        category_id = _get_or_create_category_id(conn, "Food")

    assert category_id == dining_id
