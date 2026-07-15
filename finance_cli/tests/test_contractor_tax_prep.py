from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli import contractor_tax_prep
from finance_cli.db import connect, initialize_database


@pytest.fixture()
def db_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(path))
    monkeypatch.setenv("FINANCE_CLI_DISABLE_DOTENV", "1")
    initialize_database(path)
    return path


def _seed_contractor(
    conn,
    *,
    contractor_id: str,
    name: str,
    tin_last4: str | None = None,
    entity_type: str = "individual",
    is_active: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO contractors (id, name, tin_last4, entity_type, is_active)
        VALUES (?, ?, ?, ?, ?)
        """,
        (contractor_id, name, tin_last4, entity_type, is_active),
    )


def _seed_transaction(conn, *, txn_id: str, amount_cents: int, txn_date: str = "2026-05-01") -> None:
    conn.execute(
        """
        INSERT INTO transactions (id, date, description, amount_cents, source, is_active, use_type)
        VALUES (?, ?, 'Contractor payment', ?, 'manual', 1, 'Business')
        """,
        (txn_id, txn_date, amount_cents),
    )


def _link_payment(
    conn,
    *,
    payment_id: str,
    contractor_id: str,
    txn_id: str,
    tax_year: int = 2026,
    paid_via_card: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO contractor_payments (
            id, contractor_id, transaction_id, tax_year, paid_via_card
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (payment_id, contractor_id, txn_id, tax_year, paid_via_card),
    )


def test_flag_contractor_january_prep_is_idempotent_and_snapshots_totals(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_contractor(conn, contractor_id="contractor-1", name="Jane Doe")
        _seed_transaction(conn, txn_id="txn-non-card", amount_cents=-55_000)
        _seed_transaction(conn, txn_id="txn-card", amount_cents=-10_000)
        _link_payment(conn, payment_id="pay-1", contractor_id="contractor-1", txn_id="txn-non-card")
        _link_payment(
            conn,
            payment_id="pay-2",
            contractor_id="contractor-1",
            txn_id="txn-card",
            paid_via_card=1,
        )
        conn.commit()

        first = contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id="contractor-1",
            tax_year=2026,
        )
        second = contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id="contractor-1",
            tax_year=2026,
            reason="W-9 follow-up needed before January.",
            source="user",
        )
        rows = conn.execute("SELECT id, status, reason, source FROM contractor_tax_prep_flags").fetchall()

    snapshot = first["data"]["payment_snapshot"]
    assert first["summary"]["flagged"] == 1
    assert first["summary"]["approaching_1099_threshold"] is True
    assert first["summary"]["requires_1099"] is False
    assert snapshot["payment_count"] == 2
    assert snapshot["total_paid_cents"] == 65_000
    assert snapshot["non_card_paid_cents"] == 55_000
    assert snapshot["card_paid_cents"] == 10_000
    assert snapshot["w9_collection_recommended"] is True
    assert second["data"]["flag"]["id"] == first["data"]["flag"]["id"]
    assert len(rows) == 1
    assert rows[0]["status"] == "active"
    assert rows[0]["reason"] == "W-9 follow-up needed before January."
    assert rows[0]["source"] == "user"


def test_flag_contractor_january_prep_dry_run_does_not_write(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_contractor(conn, contractor_id="contractor-1", name="Jane Doe", tin_last4="1234")
        conn.commit()

        result = contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id="contractor-1",
            tax_year=2026,
            dry_run=True,
        )
        row_count = conn.execute("SELECT COUNT(*) AS n FROM contractor_tax_prep_flags").fetchone()["n"]

    assert result["summary"]["flagged"] == 0
    assert result["data"]["dry_run"] is True
    assert result["data"]["contractor"]["tin_on_file"] is True
    assert row_count == 0


def test_flag_contractor_january_prep_validation(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_contractor(conn, contractor_id="inactive", name="Inactive", is_active=0)
        conn.commit()

        with pytest.raises(ValueError, match="contractor not found"):
            contractor_tax_prep.flag_contractor_january_prep(conn, contractor_id="missing")
        with pytest.raises(ValueError, match="contractor must be active"):
            contractor_tax_prep.flag_contractor_january_prep(conn, contractor_id="inactive")
        with pytest.raises(ValueError, match="tax_year must be in YYYY format"):
            contractor_tax_prep.flag_contractor_january_prep(conn, contractor_id="inactive", tax_year="2026-Q1")
        with pytest.raises(ValueError, match="source must be one of"):
            contractor_tax_prep.flag_contractor_january_prep(conn, contractor_id="inactive", source="chat")


def test_list_contractor_january_prep_flags_filters_status_and_year(db_path: Path) -> None:
    with connect(db_path) as conn:
        _seed_contractor(conn, contractor_id="contractor-1", name="Jane Doe")
        _seed_contractor(conn, contractor_id="contractor-2", name="Sam LLC", entity_type="llc")
        conn.commit()
        contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id="contractor-1",
            tax_year=2026,
        )
        contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id="contractor-2",
            tax_year=2026,
        )
        conn.execute(
            "UPDATE contractor_tax_prep_flags SET status = 'cancelled' WHERE contractor_id = 'contractor-1'"
        )
        conn.commit()

        active = contractor_tax_prep.list_contractor_january_prep_flags(conn, tax_year=2026)
        all_flags = contractor_tax_prep.list_contractor_january_prep_flags(
            conn,
            tax_year=2026,
            status="all",
        )

    assert active["summary"] == {"count": 1, "tax_year": 2026, "status": "active"}
    assert active["data"]["flags"][0]["contractor_id"] == "contractor-2"
    assert all_flags["summary"] == {"count": 2, "tax_year": 2026, "status": "all"}


def test_contractor_tax_prep_tools_are_classified() -> None:
    from finance_cli import mcp_server as _mcp_server  # noqa: F401
    from finance_cli.gateway import tools as gateway_tools
    from finance_cli.sync.tool_classification import DB_WRITE_TOOLS, NO_SYNC_TOOLS

    assert "contractor_january_prep_flags_list" in gateway_tools.READ_ONLY_TOOLS
    assert "contractor_january_prep_flags_list" not in gateway_tools.BRIDGE_TOOLS
    assert "contractor_january_prep_flags_list" in NO_SYNC_TOOLS
    assert "flag_contractor_january_prep" in gateway_tools.APPROVAL_REQUIRED_TOOLS
    assert "flag_contractor_january_prep" in DB_WRITE_TOOLS
