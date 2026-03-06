from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database


def _run_cli(args: list[str], capsys) -> dict:
    code = main(args)
    assert code == 0
    return json.loads(capsys.readouterr().out)


def _seed_transactions(db_path: Path) -> None:
    with connect(db_path) as conn:
        category_id = uuid.uuid4().hex
        project_id = uuid.uuid4().hex
        conn.execute("INSERT INTO categories (id, name, is_system) VALUES (?, 'Dining', 0)", (category_id,))
        conn.execute("INSERT INTO projects (id, name, is_active) VALUES (?, 'Client A', 1)", (project_id,))
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, source,
                project_id, notes, source_category, dedupe_key, raw_plaid_json, is_active
            ) VALUES (?, '2026-01-03', 'Newest', -3000, ?, 'keyword_rule', 'manual',
                      ?, 'memo', 'FOOD_AND_DRINK', ?, '{"merchant":"Coffee"}', 1)
            """,
            (uuid.uuid4().hex, category_id, project_id, uuid.uuid4().hex),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_id, category_source, source, is_active
            ) VALUES (?, '2026-01-02', 'Middle', -2000, ?, 'user', 'manual', 1)
            """,
            (uuid.uuid4().hex, category_id),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_source, source, is_active
            ) VALUES (?, '2026-01-01', 'Oldest', -1000, NULL, 'manual', 1)
            """,
            (uuid.uuid4().hex,),
        )
        conn.execute(
            """
            INSERT INTO transactions (
                id, date, description, amount_cents, category_source, source, is_active
            ) VALUES (?, '2025-12-31', 'Inactive', -500, 'user', 'manual', 0)
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()


def test_txn_list_default_fields_and_pagination(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _seed_transactions(db_path)

    payload = _run_cli(["txn", "list", "--limit", "2", "--offset", "1"], capsys)
    assert payload["status"] == "success"
    assert payload["command"] == "txn.list"
    assert len(payload["data"]["transactions"]) == 2

    pagination = payload["data"]["pagination"]
    assert pagination == {"total_count": 3, "limit": 2, "offset": 1, "has_more": False}

    row = payload["data"]["transactions"][0]
    assert "id" in row
    assert "date" in row
    assert "description" in row
    assert "source_category" in row
    assert "raw_plaid_json" not in row
    assert "dedupe_key" not in row

    assert payload["summary"]["total_count"] == 3
    assert payload["summary"]["returned"] == 2


def test_txn_list_verbose_includes_internal_fields(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _seed_transactions(db_path)

    payload = _run_cli(["txn", "list", "--verbose", "--limit", "1"], capsys)
    row = payload["data"]["transactions"][0]
    assert payload["data"]["pagination"]["has_more"] is True
    assert row["raw_plaid_json"] is not None
    assert row["dedupe_key"] is not None
    assert "is_active" in row
    assert "removed_at" in row
    assert "created_at" in row
    assert "updated_at" in row
