from __future__ import annotations

import json
import textwrap
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    return json.loads(capsys.readouterr().out)


def test_txn_explain_keyword_rule_path(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    (tmp_path / "rules.yaml").write_text(
        textwrap.dedent(
            """
            keyword_rules:
              - keywords: ["UBER"]
                category: "Travel"
                use_type: Personal
                priority: 0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    txn_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Travel', 0)", (category_id,))
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id,
                category_source, category_confidence, source, source_category
            ) VALUES (?, '2026-02-01', 'UBER TRIP TO OFFICE', -1899, ?, 'keyword_rule', 0.85, 'manual', 'TRANSPORT')
            """,
            (txn_id, category_id),
        )
        conn.commit()

    payload = _run_cli(["txn", "explain", txn_id], capsys)
    assert payload["status"] == "success"
    assert payload["command"] == "txn.explain"
    assert payload["data"]["final_category"] == "Travel"
    assert payload["data"]["category_source"] == "keyword_rule"
    assert payload["data"]["keyword_rule_match"]["matched_keyword"] == "UBER"
    assert payload["data"]["source_category"] == "TRANSPORT"
    assert "Keyword rule matched 'UBER' -> Travel" in payload["cli_report"]


def test_txn_explain_vendor_memory_path(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    txn_id = uuid.uuid4().hex
    rule_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Software & Subscriptions', 0)", (category_id,))
        conn.execute(
            """
            INSERT INTO vendor_memory (
                id, description_pattern, category_id, use_type, is_confirmed, match_count, priority
            ) VALUES (?, 'netflix', ?, 'Any', 1, 7, 2)
            """,
            (rule_id, category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source,
                category_rule_id, category_confidence, source
            ) VALUES (?, '2026-02-03', 'NETFLIX.COM', -1599, ?, 'vendor_memory', ?, 0.99, 'manual')
            """,
            (txn_id, category_id, rule_id),
        )
        conn.commit()

    payload = _run_cli(["txn", "explain", txn_id], capsys)
    assert payload["status"] == "success"
    assert payload["data"]["category_source"] == "vendor_memory"
    assert payload["data"]["vendor_memory_rule"]["id"] == rule_id
    assert payload["data"]["vendor_memory_rule"]["description_pattern"] == "netflix"
    assert payload["data"]["vendor_memory_rule"]["is_confirmed"] is True
    assert payload["data"]["vendor_memory_rule"]["match_count"] == 7


def test_txn_explain_ai_path(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)

    txn_id = uuid.uuid4().hex
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 0)", (category_id,))
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id,
                category_source, category_confidence, source
            ) VALUES (?, '2026-02-04', 'RESTAURANT', -4200, ?, 'ai', 0.77, 'manual')
            """,
            (txn_id, category_id),
        )
        conn.execute(
            """
            INSERT INTO ai_categorization_log (
                id, batch_id, transaction_id, provider, model, category_name, use_type, confidence, reasoning, prompt_hash
            ) VALUES (?, ?, ?, 'openai', 'gpt-4o-mini', 'Dining', 'Personal', 0.77, 'merchant indicates restaurant spend', 'hash1')
            """,
            (uuid.uuid4().hex, uuid.uuid4().hex, txn_id),
        )
        conn.commit()

    payload = _run_cli(["txn", "explain", txn_id], capsys)
    assert payload["status"] == "success"
    assert payload["data"]["category_source"] == "ai"
    assert payload["data"]["ai_reasoning"]["provider"] == "openai"
    assert payload["data"]["ai_reasoning"]["model"] == "gpt-4o-mini"
    assert payload["data"]["ai_reasoning"]["reasoning"] == "merchant indicates restaurant spend"
    assert "AI reasoning: merchant indicates restaurant spend" in payload["cli_report"]
