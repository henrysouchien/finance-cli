from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from finance_cli.commands import txn as txn_cmd
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def conn(tmp_path, monkeypatch):
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    with connect(db_path) as connection:
        yield connection


def _seed_category(conn, name: str) -> str:
    category_id = uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, name),
    )
    conn.commit()
    return category_id


def _seed_txn(conn, *, description: str, category_id: str | None, date: str) -> str:
    txn_id = uuid4().hex
    conn.execute(
        """
        INSERT INTO transactions (id, date, description, amount_cents, category_id, source, is_active)
        VALUES (?, ?, ?, ?, ?, 'manual', 1)
        """,
        (txn_id, date, description, -1200, category_id),
    )
    conn.commit()
    return txn_id


def test_handle_search_filters_by_category_in_fts_path(conn) -> None:
    groceries_id = _seed_category(conn, "Groceries")
    dining_id = _seed_category(conn, "Dining")
    groceries_txn = _seed_txn(conn, description="STORE RUN", category_id=groceries_id, date="2026-03-02")
    _seed_txn(conn, description="STORE LUNCH", category_id=dining_id, date="2026-03-01")

    result = txn_cmd.handle_search(SimpleNamespace(query="STORE", category="Groceries"), conn)

    assert result["data"]["query"] == "STORE"
    assert [row["id"] for row in result["data"]["transactions"]] == [groceries_txn]
    assert [row["category_name"] for row in result["data"]["transactions"]] == ["Groceries"]


def test_handle_search_without_category_returns_all_matches(conn) -> None:
    groceries_id = _seed_category(conn, "Groceries")
    dining_id = _seed_category(conn, "Dining")
    _seed_txn(conn, description="STORE RUN", category_id=groceries_id, date="2026-03-02")
    _seed_txn(conn, description="STORE LUNCH", category_id=dining_id, date="2026-03-01")

    result = txn_cmd.handle_search(SimpleNamespace(query="STORE", category=None), conn)

    assert [row["category_name"] for row in result["data"]["transactions"]] == ["Groceries", "Dining"]


def test_handle_search_with_missing_category_returns_no_results(conn) -> None:
    groceries_id = _seed_category(conn, "Groceries")
    _seed_txn(conn, description="STORE RUN", category_id=groceries_id, date="2026-03-02")

    result = txn_cmd.handle_search(SimpleNamespace(query="STORE", category="Nonexistent"), conn)

    assert result["data"]["transactions"] == []
    assert result["summary"]["total_transactions"] == 0


def test_handle_search_filters_by_category_in_like_fallback(conn) -> None:
    groceries_id = _seed_category(conn, "Groceries")
    dining_id = _seed_category(conn, "Dining")
    groceries_txn = _seed_txn(conn, description="STORE RUN", category_id=groceries_id, date="2026-03-02")
    _seed_txn(conn, description="STORE LUNCH", category_id=dining_id, date="2026-03-01")

    result = txn_cmd.handle_search(SimpleNamespace(query='"STORE', category="Groceries"), conn)

    assert [row["id"] for row in result["data"]["transactions"]] == [groceries_txn]
    assert [row["category_name"] for row in result["data"]["transactions"]] == ["Groceries"]
