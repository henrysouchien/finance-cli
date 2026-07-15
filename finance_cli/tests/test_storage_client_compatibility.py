from __future__ import annotations

import sqlite3
from collections.abc import Callable

import pytest


@pytest.fixture(params=["sqlite3", "storage_client"], ids=["sqlite3", "storage_client"])
def backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture()
def connection_factory(backend: str, request: pytest.FixtureRequest) -> Callable[[], object]:
    def make_connection():
        if backend == "sqlite3":
            return sqlite3.connect(":memory:")
        storage_factory = request.getfixturevalue("storage_connection_factory")
        return storage_factory()

    return make_connection


def test_execute_fetch_methods_and_iteration(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany("INSERT INTO items (name) VALUES (?)", [("alpha",), ("beta",), ("gamma",)])

        cursor = conn.execute("SELECT id, name FROM items ORDER BY id")
        assert cursor.fetchone() == (1, "alpha")
        assert cursor.fetchmany(1) == [(2, "beta")]
        assert cursor.fetchall() == [(3, "gamma")]

        iter_cursor = conn.execute("SELECT name FROM items ORDER BY id")
        assert list(iter_cursor) == [("alpha",), ("beta",), ("gamma",)]
    finally:
        conn.close()


def test_executemany_rowcount(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE many_items (id INTEGER PRIMARY KEY, name TEXT)")
        cursor = conn.executemany(
            "INSERT INTO many_items (name) VALUES (:name)",
            [{"name": "alpha"}, {"name": "beta"}, {"name": "gamma"}],
        )

        assert cursor.rowcount == 3
        assert conn.execute("SELECT COUNT(*) FROM many_items").fetchone() == (3,)
    finally:
        conn.close()


def test_executescript_multi_statement_init(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.executescript(
            """
            CREATE TABLE script_items (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO script_items (name) VALUES ('alpha');
            INSERT INTO script_items (name) VALUES ('beta');
            """
        )

        assert conn.execute("SELECT name FROM script_items ORDER BY id").fetchall() == [
            ("alpha",),
            ("beta",),
        ]
    finally:
        conn.close()


def test_commit_rollback_begin_and_savepoints(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE txn_items (id INTEGER PRIMARY KEY, name TEXT)")

        conn.execute("BEGIN")
        assert conn.in_transaction
        conn.execute("INSERT INTO txn_items (name) VALUES (?)", ("rolled_back",))
        conn.rollback()
        assert not conn.in_transaction
        assert conn.execute("SELECT COUNT(*) FROM txn_items").fetchone() == (0,)

        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO txn_items (name) VALUES (?)", ("kept",))
        conn.execute("SAVEPOINT s")
        conn.execute("INSERT INTO txn_items (name) VALUES (?)", ("discarded",))
        conn.execute("ROLLBACK TO SAVEPOINT s")
        conn.execute("RELEASE s")
        assert conn.in_transaction
        conn.commit()

        assert conn.execute("SELECT name FROM txn_items ORDER BY id").fetchall() == [("kept",)]
        assert not conn.in_transaction
    finally:
        conn.close()


def test_lastrowid_insert_zero_and_non_insert_normalization(backend: str, connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE ids (id INTEGER PRIMARY KEY, name TEXT)")

        first = conn.execute("INSERT INTO ids (name) VALUES (?)", ("alpha",))
        assert first.lastrowid == 1

        zero = conn.execute("INSERT INTO ids (id, name) VALUES (0, ?)", ("zero",))
        assert zero.lastrowid == 0

        select_cursor = conn.execute("SELECT name FROM ids WHERE id = 0")
        assert select_cursor.fetchone() == ("zero",)
        if backend == "storage_client":
            assert select_cursor.lastrowid is None
    finally:
        conn.close()


def test_in_transaction_toggles_for_implicit_dml(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE implicit_txn (id INTEGER PRIMARY KEY, name TEXT)")
        assert not conn.in_transaction

        conn.execute("INSERT INTO implicit_txn (name) VALUES (?)", ("alpha",))
        assert conn.in_transaction

        conn.rollback()
        assert not conn.in_transaction
        assert conn.execute("SELECT COUNT(*) FROM implicit_txn").fetchone() == (0,)
    finally:
        conn.close()


def test_row_factory_sqlite_row(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE row_items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO row_items (name) VALUES (?)", ("alpha",))
        conn.row_factory = sqlite3.Row

        row = conn.execute("SELECT id AS Id, name FROM row_items").fetchone()

        assert row[0] == 1
        assert row["Id"] == 1
        assert row["id"] == 1
        assert row["name"] == "alpha"
        assert list(row) == [1, "alpha"]
        assert list(row.keys()) == ["Id", "name"]
    finally:
        conn.close()


def test_context_manager_commits_rolls_back_and_keeps_connection_open(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE context_items (id INTEGER PRIMARY KEY, name TEXT)")

        with conn:
            conn.execute("INSERT INTO context_items (name) VALUES (?)", ("committed",))

        with pytest.raises(RuntimeError):
            with conn:
                conn.execute("INSERT INTO context_items (name) VALUES (?)", ("rolled_back",))
                raise RuntimeError("trigger rollback")

        assert conn.execute("SELECT name FROM context_items ORDER BY id").fetchall() == [("committed",)]
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()


def test_complex_sql_forms(connection_factory) -> None:
    conn = connection_factory()
    try:
        conn.execute("CREATE TABLE complex_items (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        conn.execute("CREATE TABLE complex_audit (item_id INTEGER, item_name TEXT)")
        conn.execute(
            """
            CREATE TRIGGER complex_items_ai AFTER INSERT ON complex_items
            BEGIN
                INSERT INTO complex_audit (item_id, item_name) VALUES (NEW.id, NEW.name);
            END
            """
        )
        conn.execute(
            """
            INSERT INTO complex_items (id, name) VALUES (1, 'alpha')
            ON CONFLICT(id) DO UPDATE SET name = excluded.name
            """
        )
        conn.execute(
            """
            INSERT INTO complex_items (id, name) VALUES (1, 'alpha-updated')
            ON CONFLICT(id) DO UPDATE SET name = excluded.name
            """
        )

        recursive = conn.execute(
            """
            WITH RECURSIVE cte(x) AS (
                SELECT 1
                UNION ALL
                SELECT x + 1 FROM cte WHERE x < 3
            )
            SELECT x FROM cte ORDER BY x
            """
        ).fetchall()

        assert recursive == [(1,), (2,), (3,)]
        assert conn.execute("SELECT name FROM complex_items WHERE id = 1").fetchone() == ("alpha-updated",)
        assert conn.execute("SELECT item_name FROM complex_audit").fetchall() == [("alpha",)]
    finally:
        conn.close()


def test_cursor_description(connection_factory) -> None:
    conn = connection_factory()
    try:
        cursor = conn.execute("SELECT 1 AS one, 'two' AS two")

        assert cursor.description == (
            ("one", None, None, None, None, None, None),
            ("two", None, None, None, None, None, None),
        )
    finally:
        conn.close()
