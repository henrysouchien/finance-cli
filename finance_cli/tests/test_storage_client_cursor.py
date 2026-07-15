from __future__ import annotations

import sqlite3

import pytest


def test_arraysize_default_is_one(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        cursor = conn.cursor()
        assert cursor.arraysize == 1
    finally:
        conn.close()


def test_fetchmany_respects_arraysize_and_explicit_size(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        conn.execute("CREATE TABLE fetch_test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.executemany(
            "INSERT INTO fetch_test (name) VALUES (?)",
            [("alpha",), ("beta",), ("gamma",)],
        )

        cursor = conn.execute("SELECT name FROM fetch_test ORDER BY id")
        assert cursor.fetchmany() == [("alpha",)]

        cursor = conn.execute("SELECT name FROM fetch_test ORDER BY id")
        cursor.arraysize = 2
        assert cursor.fetchmany() == [("alpha",), ("beta",)]

        cursor = conn.execute("SELECT name FROM fetch_test ORDER BY id")
        assert cursor.fetchmany(2) == [("alpha",), ("beta",)]
        assert cursor.fetchall() == [("gamma",)]
    finally:
        conn.close()


def test_cursor_close_prevents_future_operations(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        cursor = conn.execute("SELECT 1")
        cursor.close()

        with pytest.raises(sqlite3.ProgrammingError):
            cursor.fetchone()
        with pytest.raises(sqlite3.ProgrammingError):
            cursor.execute("SELECT 1")
    finally:
        conn.close()


def test_description_matches_column_names(storage_connection_factory) -> None:
    conn = storage_connection_factory()
    try:
        cursor = conn.execute("SELECT 1 AS one, 'two' AS two")

        assert cursor.description == (
            ("one", None, None, None, None, None, None),
            ("two", None, None, None, None, None, None),
        )
    finally:
        conn.close()


def test_compat_row_factory(storage_connection_factory) -> None:
    from finance_cli.db import COMPAT_ROW_FACTORY

    conn = storage_connection_factory()
    try:
        conn.row_factory = COMPAT_ROW_FACTORY
        row = conn.execute("SELECT 7 AS amount, 'alpha' AS name").fetchone()

        assert row[0] == 7
        assert row["amount"] == 7
        assert row.get("name") == "alpha"
        assert row.keys() == ("amount", "name")
    finally:
        conn.close()
