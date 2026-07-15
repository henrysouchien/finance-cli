from __future__ import annotations

import sqlite3
from pathlib import Path

from finance_cli.db import initialize_database
from finance_cli.sync_protocol import (
    CHANGELOG_TABLES,
    DOWNSTREAM_ONLY_TABLES,
    NON_REPLICATED_WRITABLE_TABLES,
    READ_ONLY_TABLES,
    REPLICATED_TABLES,
    SECRET_COLUMNS,
    SERVER_ONLY_TABLES,
    SYNCABLE_TABLES,
)


def test_sync_protocol_table_sets_match_schema_and_changelog(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }

    assert SYNCABLE_TABLES == REPLICATED_TABLES
    assert CHANGELOG_TABLES == REPLICATED_TABLES | DOWNSTREAM_ONLY_TABLES
    assert REPLICATED_TABLES.isdisjoint(NON_REPLICATED_WRITABLE_TABLES)
    assert REPLICATED_TABLES.isdisjoint(DOWNSTREAM_ONLY_TABLES)
    assert DOWNSTREAM_ONLY_TABLES.isdisjoint(NON_REPLICATED_WRITABLE_TABLES)
    assert DOWNSTREAM_ONLY_TABLES.isdisjoint(READ_ONLY_TABLES)
    assert REPLICATED_TABLES.isdisjoint(READ_ONLY_TABLES)
    assert NON_REPLICATED_WRITABLE_TABLES.isdisjoint(READ_ONLY_TABLES)
    assert not SERVER_ONLY_TABLES

    assert REPLICATED_TABLES <= table_names
    assert DOWNSTREAM_ONLY_TABLES <= table_names
    assert CHANGELOG_TABLES <= table_names
    assert NON_REPLICATED_WRITABLE_TABLES <= table_names
    assert READ_ONLY_TABLES <= table_names


def test_secret_columns_reference_real_tables_and_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        for table_name, secret_columns in SECRET_COLUMNS.items():
            assert table_name in REPLICATED_TABLES
            column_names = {
                str(row[1])
                for row in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            }
            assert set(secret_columns) <= column_names
