from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database
from finance_cli.importers import _validate_institution_name, import_csv, import_normalized_rows
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path


def _insert_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str = "credit_card",
    card_ending: str | None = None,
    plaid_account_id: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, card_ending, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, 1)
        """,
        (
            account_id,
            plaid_account_id,
            institution_name,
            f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
        ),
    )


def test_import_normalized_rows_unknown_name_blocks_before_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Unknown Credit Union",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="not in CANONICAL_NAMES"):
            import_normalized_rows(conn, rows, "Unknown Credit Union")
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]

    assert txn_count == 0
    assert account_count == 0


def test_import_csv_unknown_name_blocks(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    csv_path = _write_csv(
        tmp_path / "unknown.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Unknown Credit Union,Business,Travel,false
        """,
    )

    with connect(db_path) as conn:
        with pytest.raises(ValueError, match="Unknown Credit Union"):
            import_csv(conn, csv_path, source_name="Unknown Credit Union")


def test_import_normalized_rows_bypass_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Unknown Credit Union",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        report = import_normalized_rows(conn, rows, "Unknown Credit Union", validate_name=False)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert report.inserted == 1
    assert txn_count == 1


def test_validate_institution_name_ai_and_empty_warns(caplog) -> None:
    parent_logger = logging.getLogger("finance_cli")
    parent_logger.propagate = True
    try:
        with caplog.at_level(logging.WARNING):
            _validate_institution_name("AI:gpt-4o")
            _validate_institution_name("AI")
            _validate_institution_name("")
        warn_records = [
            r for r in caplog.records
            if "No real institution name derived" in r.message
        ]
        assert len(warn_records) == 3
    finally:
        parent_logger.propagate = False


def test_pdf_import_bypass_allows_synthetic_ai_source(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    extracted = ExtractResult(
        transactions=[
            {"date": "2026-02-10", "description": "A", "amount_cents": -100, "source": "AI"},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        result = import_extracted_statement(conn, extracted, pdf_path, "ai:claude", validate_name=False)

    assert result["inserted"] == 1


def test_equivalence_gap_logs_warning(tmp_path: Path, caplog) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Flight",
            "Amount": "-88.40",
            "Card Ending": "8894",
            "Account Type": "credit_card",
            "Source": "Bank of America",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id="plaid_merrill_8894",
            institution_name="Merrill",
            account_type="credit_card",
            card_ending="8894",
            plaid_account_id="plaid_plaid_merrill_8894",
        )
        logger = logging.getLogger("finance_cli.importers")
        logger.addHandler(caplog.handler)
        try:
            caplog.set_level(logging.WARNING)
            import_normalized_rows(conn, rows, "Bank of America")
        finally:
            logger.removeHandler(caplog.handler)

    assert "INSTITUTION_EQUIV_GAP" in caplog.text


def test_dedup_audit_names_reports_issues(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    with connect(db_path) as conn:
        _insert_account(
            conn,
            account_id="plaid_merrill_8894",
            institution_name="Merrill",
            account_type="credit_card",
            card_ending="8894",
            plaid_account_id="plaid_plaid_merrill_8894",
        )
        _insert_account(
            conn,
            account_id="hash_bofa_8894",
            institution_name="Bank of America",
            account_type="credit_card",
            card_ending="8894",
        )
        _insert_account(
            conn,
            account_id="plaid_chase_1234",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
            plaid_account_id="plaid_plaid_chase_1234",
        )
        _insert_account(
            conn,
            account_id="hash_chase_bank",
            institution_name="Chase Bank",
            account_type="credit_card",
            card_ending="5678",
        )
        _insert_account(
            conn,
            account_id="hash_unknown",
            institution_name="Unknown Credit Union",
            account_type="checking",
        )
        conn.execute(
            """
            INSERT INTO plaid_items (id, plaid_item_id, institution_name, status)
            VALUES (?, ?, ?, 'active')
            """,
            ("item_unknown", "item_unknown", "Unknown Institution"),
        )
        conn.commit()

    code = main(["dedup", "audit-names"])
    payload = json.loads(capsys.readouterr().out)
    issue_types = {item["type"] for item in payload["data"]["issues"]}

    assert code == 0
    assert payload["status"] == "success"
    assert {"unmapped_name", "unmapped_plaid_item", "similar_unaliased", "equivalence_gap", "orphaned_account"} <= issue_types


def test_dedup_audit_names_clean_db(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(["dedup", "audit-names"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["issues"] == []
    assert payload["summary"]["total_issues"] == 0
