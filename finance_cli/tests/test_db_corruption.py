from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from finance_cli.db import connect, initialize_database


def test_connect_reports_corrupt_wal_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "off")
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    wal_path = db_path.with_name(f"{db_path.name}-wal")
    shm_path = db_path.with_name(f"{db_path.name}-shm")
    assert wal_path.exists()
    assert shm_path.exists()

    wal_path.write_bytes(b"not-a-real-wal")

    with pytest.raises(sqlite3.OperationalError) as excinfo:
        connect(db_path)

    message = str(excinfo.value)
    assert "WAL sidecar appears corrupt" in message
    assert str(db_path) in message
    assert str(wal_path) in message
    assert str(shm_path) in message
    assert "Original SQLite error: disk I/O error" in message
