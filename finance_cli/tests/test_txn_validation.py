"""Transaction input validation tests."""
from __future__ import annotations

import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from finance_cli.__main__ import main
from finance_cli.commands import txn
from finance_cli.db import connect, initialize_database
from finance_cli.exceptions import ValidationError


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _add_args(**kwargs) -> SimpleNamespace:
    defaults = dict(
        date="2026-03-26",
        description="Test",
        amount=25.0,
        category=None,
        account_id=None,
        idempotency_key=None,
        dry_run=False,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _edit_args(tid, **kwargs) -> SimpleNamespace:
    defaults = dict(id=tid, description=None, amount=None, date=None, notes=None, dry_run=False)
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _deactivate_args(tid, **kwargs) -> SimpleNamespace:
    defaults = dict(id=tid, dry_run=False)
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _seed_txn(conn, desc="Test txn", amount_cents=-2500) -> str:
    tid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO transactions (id, date, description, amount_cents, source, is_active)"
        " VALUES (?, '2026-03-26', ?, ?, 'manual', 1)",
        (tid, desc, amount_cents),
    )
    conn.commit()
    return tid


def test_add_rejects_empty_description(db_path):
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="description"):
            txn.handle_add(_add_args(description=""), conn)


def test_add_rejects_whitespace_description(db_path):
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="description"):
            txn.handle_add(_add_args(description="   "), conn)


def test_add_rejects_none_description(db_path):
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="description"):
            txn.handle_add(_add_args(description=None), conn)


def test_add_rejects_zero_amount(db_path):
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="zero"):
            txn.handle_add(_add_args(amount=0), conn)


def test_add_rejects_subcent_rounding_to_zero(db_path):
    """0.004 rounds to 0 cents — should be rejected."""
    with connect(db_path) as conn:
        with pytest.raises(ValidationError, match="zero"):
            txn.handle_add(_add_args(amount=0.004), conn)


def test_add_rejection_leaves_db_unchanged(db_path):
    """Rejected handle_add should not insert any row."""
    with connect(db_path) as conn:
        before = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
        try:
            txn.handle_add(_add_args(description=""), conn)
        except ValidationError:
            pass
        after = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
        assert after == before


def test_edit_rejects_empty_description(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        with pytest.raises(ValidationError, match="description"):
            txn.handle_edit(_edit_args(tid, description=""), conn)


def test_edit_rejects_whitespace_description(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        with pytest.raises(ValidationError, match="description"):
            txn.handle_edit(_edit_args(tid, description="   "), conn)


def test_edit_rejects_zero_amount(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        with pytest.raises(ValidationError, match="zero"):
            txn.handle_edit(_edit_args(tid, amount=0), conn)


def test_edit_rejects_subcent_rounding_to_zero(db_path):
    """0.004 rounds to 0 cents — should be rejected via edit too."""
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        with pytest.raises(ValidationError, match="zero"):
            txn.handle_edit(_edit_args(tid, amount=0.004), conn)


def test_edit_rejection_leaves_db_unchanged(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn, desc="Original", amount_cents=-5000)
        try:
            txn.handle_edit(_edit_args(tid, description=""), conn)
        except ValidationError:
            pass
        row = conn.execute("SELECT description, amount_cents FROM transactions WHERE id = ?", (tid,)).fetchone()
        assert row["description"] == "Original"
        assert row["amount_cents"] == -5000


def test_deactivate_soft_deletes_transaction(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        result = txn.handle_deactivate(_deactivate_args(tid), conn)
        row = conn.execute(
            "SELECT is_active, removed_at FROM transactions WHERE id = ?",
            (tid,),
        ).fetchone()

    assert result["data"]["deactivated"] is True
    assert result["summary"]["deactivated_count"] == 1
    assert row["is_active"] == 0
    assert row["removed_at"] is not None


def test_deactivate_is_idempotent_for_inactive_transaction(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        txn.handle_deactivate(_deactivate_args(tid), conn)
        result = txn.handle_deactivate(_deactivate_args(tid), conn)

    assert result["data"]["deactivated"] is False
    assert result["summary"]["deactivated_count"] == 0
    assert "already inactive" in result["cli_report"]


def test_deactivate_dry_run_rolls_back(db_path):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)
        result = txn.handle_deactivate(_deactivate_args(tid, dry_run=True), conn)
        row = conn.execute(
            "SELECT is_active, removed_at FROM transactions WHERE id = ?",
            (tid,),
        ).fetchone()

    assert result["data"]["deactivated"] is True
    assert result["data"]["dry_run"] is True
    assert row["is_active"] == 1
    assert row["removed_at"] is None


def test_cli_txn_deactivate_command(db_path, capsys):
    with connect(db_path) as conn:
        tid = _seed_txn(conn)

    code = main(["txn", "deactivate", tid, "--format", "json"])
    payload = capsys.readouterr().out

    assert code == 0
    assert '"command": "txn.deactivate"' in payload
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT is_active, removed_at FROM transactions WHERE id = ?",
            (tid,),
        ).fetchone()
    assert row["is_active"] == 0
    assert row["removed_at"] is not None
