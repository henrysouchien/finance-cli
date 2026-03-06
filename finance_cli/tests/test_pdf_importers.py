from __future__ import annotations

from pathlib import Path

from finance_cli.db import connect, initialize_database
from finance_cli.importers.pdf import ExtractResult, extract_transactions, import_pdf_statement


def test_extract_result_structure(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    sample_text = "\n".join(
        [
            "Statement Period 01/01/2025 - 01/31/2025",
            "01/03 COFFEE SHOP 10.00",
            "01/05 BOOK STORE 20.00",
            "Total Charges -$30.00",
        ]
    )
    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: sample_text)

    result = extract_transactions(pdf_path, "chase_credit")

    assert isinstance(result, ExtractResult)
    assert isinstance(result.transactions, list)
    assert len(result.transactions) == 2
    assert set(result.transactions[0].keys()) >= {"date", "description", "amount_cents", "source"}
    assert result.statement_total_cents == -3000
    assert result.extracted_total_cents == -3000


def test_import_pdf_statement_dedupe_by_file_hash(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    pdf_path = tmp_path / "dup.pdf"
    pdf_path.write_bytes(b"same-file")

    fake_extract = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -1000, "source": "Chase Credit"},
            {"date": "2025-01-02", "description": "B", "amount_cents": -2000, "source": "Chase Credit"},
        ],
        statement_total_cents=-3000,
        extracted_total_cents=-3000,
        reconciled=True,
        warnings=[],
    )
    monkeypatch.setattr("finance_cli.importers.pdf.extract_transactions", lambda *_: fake_extract)

    with connect(db_path) as conn:
        first = import_pdf_statement(conn, pdf_path=pdf_path, bank="chase_credit", dry_run=False)
        second = import_pdf_statement(conn, pdf_path=pdf_path, bank="chase_credit", dry_run=False)

        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["inserted"] == 2
    assert first["already_imported"] is False
    assert second["already_imported"] is True
    assert txn_count == 2
    assert batch_count == 1


def test_reconciliation_pass_and_fail(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    pass_text = "\n".join(
        [
            "Statement Period 01/01/2025 - 01/31/2025",
            "01/03 COFFEE SHOP 10.00",
            "01/05 BOOK STORE 20.00",
            "Total Charges -$30.00",
        ]
    )
    fail_text = "\n".join(
        [
            "Statement Period 01/01/2025 - 01/31/2025",
            "01/03 COFFEE SHOP 10.00",
            "01/05 BOOK STORE 20.00",
            "Total Charges -$25.00",
        ]
    )

    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: pass_text)
    matched = extract_transactions(pdf_path, "chase_credit")

    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: fail_text)
    mismatch = extract_transactions(pdf_path, "chase_credit")

    assert matched.reconciled is True
    assert mismatch.reconciled is False
    assert mismatch.warnings


def test_extract_apple_card_ach_deposit_is_positive(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    sample_text = "\n".join(
        [
            "01/26/2026 ACH Deposit Internet transfer from account ending in 6451 $500.00",
            "01/28/2026 ACH Deposit Internet transfer from account ending in 6451 $100.00",
            "01/29/2026 APPLE.COM/BILL ONE APPLE PARK WAY CUPERTINO 95014 CA USA $9.99",
        ]
    )
    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: sample_text)

    result = extract_transactions(pdf_path, "apple")
    amounts = [row["amount_cents"] for row in result.transactions]

    assert 50000 in amounts
    assert 10000 in amounts
    assert -999 in amounts


def test_extract_apple_card_keeps_large_purchase(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "statement.pdf"
    pdf_path.write_bytes(b"dummy")

    sample_text = "01/30/2026 ELECTRONICS STORE SAN FRANCISCO CA USA $750.00"
    monkeypatch.setattr("finance_cli.importers.pdf._extract_pdf_text", lambda _: sample_text)

    result = extract_transactions(pdf_path, "apple")

    assert len(result.transactions) == 1
    assert result.transactions[0]["amount_cents"] == -75000
