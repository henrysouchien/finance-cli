from __future__ import annotations

import uuid
from pathlib import Path

import pytest

import finance_cli.db as db_module
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import TenantMismatchError


@pytest.fixture()
def encrypted_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "require")
    monkeypatch.setattr(db_module.db_keys, "get_user_db_key", lambda _user_id, **_kwargs: b"\x11" * 32)
    path = tmp_path / "users" / "alice" / "finance.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def test_encrypted_round_trip(encrypted_db_path: Path) -> None:
    initialize_database(encrypted_db_path)

    with connect(encrypted_db_path) as conn:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, 'alice')"
        )
        conn.execute(
            """
            INSERT INTO categories (id, name, is_system)
            VALUES (?, 'Test Category', 0)
            """,
            (uuid.uuid4().hex,),
        )
        conn.commit()

    with connect(encrypted_db_path, expected_user_id="alice") as conn:
        marker = conn.execute(
            "SELECT user_id FROM tenant_marker WHERE singleton = 1"
        ).fetchone()
        category = conn.execute(
            "SELECT name FROM categories WHERE name = 'Test Category'"
        ).fetchone()

    assert marker is not None
    assert marker["user_id"] == "alice"
    assert category is not None
    assert category["name"] == "Test Category"


def test_wrong_dek_rejected(encrypted_db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    initialize_database(encrypted_db_path)

    with connect(encrypted_db_path) as conn:
        conn.execute(
            "INSERT INTO tenant_marker (singleton, user_id) VALUES (1, 'alice')"
        )
        conn.commit()

    monkeypatch.setattr(db_module.db_keys, "get_user_db_key", lambda _user_id, **_kwargs: b"\x22" * 32)

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(encrypted_db_path, expected_user_id="alice")

    assert excinfo.value.reason == "crypto_failure"


def test_provision_mode_rejects_plaintext_user_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "provision")
    monkeypatch.setenv("FINANCE_WEB_DATA_ROOT", str(tmp_path / "users"))
    monkeypatch.setattr(db_module.db_keys, "get_user_db_key", lambda _user_id, **_kwargs: b"\x11" * 32)
    path = tmp_path / "users" / "alice" / "finance.db"
    path.parent.mkdir(parents=True)
    with db_module.sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE tenant_marker (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), user_id TEXT NOT NULL)"
        )
        conn.execute("INSERT INTO tenant_marker VALUES (1, 'alice')")

    with pytest.raises(TenantMismatchError) as excinfo:
        connect(path, expected_user_id="alice")

    assert excinfo.value.reason == "crypto_failure"
