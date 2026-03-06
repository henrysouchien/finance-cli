from __future__ import annotations

import json
import uuid
from pathlib import Path

from finance_cli.ai_statement_parser import AIParseResult, ai_result_to_extract_result
from finance_cli.db import connect, initialize_database
from finance_cli.importers import _account_id_for_source, import_normalized_rows
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement
from finance_cli.ingest_validation import validate_ai_parse


def _parsed_payload(
    *,
    institution: str,
    account_label: str = "Card",
    statement_card_ending: str | None = None,
    txn_card_ending: str | None = None,
) -> dict[str, object]:
    statement: dict[str, object] = {
        "institution": institution,
        "account_label": account_label,
        "statement_period_start": "2025-01-01",
        "statement_period_end": "2025-01-31",
        "new_balance": 110.00,
        "total_charges": 10.00,
        "total_payments": None,
        "currency": "USD",
    }
    if statement_card_ending is not None:
        statement["card_ending"] = statement_card_ending
    return {
        "statement": statement,
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE SHOP",
                "amount": -10.00,
                "card_ending": txn_card_ending,
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            }
        ],
        "extraction_meta": {
            "model": "fake-model",
            "prompt_version": "v2",
            "notes": None,
            "expected_transaction_count": 1,
        },
    }


def _extract_single_txn(parsed: dict[str, object]) -> dict[str, object]:
    result = AIParseResult(
        raw_json=json.dumps(parsed),
        parsed=parsed,
        validation=validate_ai_parse(parsed),
        provider="claude",
        model="claude-test",
        prompt_version="v2",
        prompt_hash="a" * 64,
    )
    extracted = ai_result_to_extract_result(result)
    return extracted.transactions[0]


def _write_pdf(path: Path, name: str) -> Path:
    pdf_path = path / name
    pdf_path.write_bytes(name.encode("utf-8"))
    return pdf_path


def _insert_account(conn, account_id: str) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, card_ending)
        VALUES (?, 'Manual', 'Manual Account', 'credit_card', '9999')
        """,
        (account_id,),
    )


def test_apple_card_canonicalized() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Apple"))
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_apple_card_variant_canonicalized() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Apple Card Inc"))
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_barclays_canonicalized() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Barclays US", statement_card_ending="8024"))
    assert txn["source"] == "Barclays"
    assert txn["card_ending"] == "8024"


def test_unknown_institution_passthrough() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Unknown Bank Co."))
    assert txn["source"] == "Unknown Bank Co."
    assert txn["card_ending"] is None


def test_canonical_card_ending_overrides_extracted() -> None:
    txn = _extract_single_txn(
        _parsed_payload(
            institution="Apple Card",
            statement_card_ending="1234",
            txn_card_ending="5678",
        )
    )
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_punctuation_stripped_in_canonicalization() -> None:
    txn_apple = _extract_single_txn(_parsed_payload(institution="Apple Card Inc."))
    txn_bloom = _extract_single_txn(_parsed_payload(institution="Bloomingdale's", statement_card_ending="4321"))
    assert txn_apple["source"] == "Apple Card"
    assert txn_bloom["source"] == "Bloomingdale's"


def test_whitespace_variants_canonicalized() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="  Apple   Card  "))
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_statement_level_card_ending_used() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Citi", statement_card_ending="1234", txn_card_ending=None))
    assert txn["card_ending"] == "1234"


def test_statement_level_beats_per_txn() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Citi", statement_card_ending="9999", txn_card_ending="1111"))
    assert txn["card_ending"] == "9999"


def test_account_label_last4_fallback() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Citi", account_label="Travel Card 4455"))
    assert txn["card_ending"] == "4455"


def test_masked_account_label() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Citi", account_label="Mastercard **** 8024"))
    assert txn["card_ending"] == "8024"


def test_account_id_derived_when_none(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "derived.pdf")

    extracted = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Citi", "card_ending": "1234"},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        txn = conn.execute("SELECT account_id FROM transactions LIMIT 1").fetchone()
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]

    assert txn["account_id"] == _account_id_for_source("Citi", "1234")
    assert account_count == 1


def test_account_id_override_respected(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "override.pdf")

    extracted = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Citi", "card_ending": "1111"},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
    )
    forced_account_id = "manual_account_override"

    with connect(db_path) as conn:
        _insert_account(conn, forced_account_id)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude", account_id=forced_account_id)
        txn = conn.execute("SELECT account_id FROM transactions LIMIT 1").fetchone()
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]

    assert txn["account_id"] == forced_account_id
    assert account_count == 1


def test_derived_account_matches_csv_apple(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "apple.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "Apple",
            "Source": "Apple Card",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Apple Card",
                "card_ending": "Apple",
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "Apple Card", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_derived_account_matches_csv_barclays(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "barclays.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "8024",
            "Source": "Barclays",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Barclays",
                "card_ending": "8024",
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "Barclays", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_derived_account_matches_csv_chase_credit(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "chase-credit.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "0368",
            "Source": "Chase Credit",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Chase Credit",
                "card_ending": "0368",
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "Chase Credit", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_derived_account_matches_csv_bofa_checking(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "boa-checking.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "",
            "Source": "BofA Checking",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Bank of America",
                "card_ending": None,
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "BofA Checking", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_derived_account_matches_csv_bofa_credit_from_bank_of_america(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "bofa-credit.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "1234",
            "Source": "BofA Credit",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Bank of America",
                "card_ending": "1234",
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "BofA Credit", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_account_id_for_source_canonicalizes_bofa_aliases() -> None:
    assert _account_id_for_source("BofA Checking", "") == _account_id_for_source("Bank of America", "")
    assert _account_id_for_source("BofA Credit", "1234") == _account_id_for_source(
        "Bank of America", "1234"
    )


def test_derived_account_matches_csv_amex(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "amex.pdf")

    rows = [
        {
            "Date": "2025-01-01",
            "Description": "Coffee",
            "Amount": "-1.00",
            "Card Ending": "Amex",
            "Source": "Amex",
            "Is Payment": "false",
        }
    ]
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-02",
                "description": "Book",
                "amount_cents": -200,
                "source": "Amex",
                "card_ending": "Amex",
            }
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_normalized_rows(conn, rows, "Amex", validate_name=False)
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        csv_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import'"
        ).fetchone()["account_id"]
        pdf_account = conn.execute(
            "SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import'"
        ).fetchone()["account_id"]

    assert csv_account == pdf_account


def test_mixed_card_endings_uses_most_common(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    first_pdf = _write_pdf(tmp_path, "mixed-a.pdf")
    second_pdf = _write_pdf(tmp_path, "mixed-b.pdf")

    extracted_tie_with_statement = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Barclays", "card_ending": "1111"},
            {"date": "2025-01-02", "description": "B", "amount_cents": -100, "source": "Barclays", "card_ending": "2222"},
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
        statement_card_ending="2222",
    )
    extracted_tie_without_statement = ExtractResult(
        transactions=[
            {"date": "2025-01-03", "description": "C", "amount_cents": -100, "source": "Barclays", "card_ending": "1111"},
            {"date": "2025-01-04", "description": "D", "amount_cents": -100, "source": "Barclays", "card_ending": "2222"},
        ],
        statement_total_cents=-200,
        extracted_total_cents=-200,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, extracted_tie_with_statement, first_pdf, "ai:claude")
        first_account = conn.execute(
            "SELECT account_id FROM transactions WHERE description = 'A'"
        ).fetchone()["account_id"]

        import_extracted_statement(conn, extracted_tie_without_statement, second_pdf, "ai:claude")
        second_account = conn.execute(
            "SELECT account_id FROM transactions WHERE description = 'C'"
        ).fetchone()["account_id"]

    assert first_account == _account_id_for_source("Barclays", "2222")
    assert second_account == _account_id_for_source("Barclays", "1111")


def test_dry_run_no_account_created(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, f"dry-run-{uuid.uuid4().hex}.pdf")

    extracted = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Citi", "card_ending": "1234"},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        result = import_extracted_statement(conn, extracted, pdf_path, "ai:claude", dry_run=True)
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert result["inserted"] == 1
    assert account_count == 0
    assert txn_count == 0


def test_goldman_sachs_canonicalized_to_apple_card() -> None:
    """Apple Card statements often say 'Goldman Sachs' as the issuing bank."""
    txn = _extract_single_txn(_parsed_payload(institution="Goldman Sachs"))
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_goldman_sachs_bank_usa_canonicalized() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="Goldman Sachs Bank USA"))
    assert txn["source"] == "Apple Card"
    assert txn["card_ending"] == "Apple"


def test_american_express_canonicalized_to_american_express() -> None:
    txn = _extract_single_txn(_parsed_payload(institution="American Express"))
    assert txn["source"] == "American Express"
    assert txn["card_ending"] is None


def test_statement_account_type_propagated() -> None:
    """AI parser account_type field propagates through ExtractResult."""
    parsed = _parsed_payload(institution="Citi", statement_card_ending="1234")
    parsed["statement"]["account_type"] = "credit_card"
    result = AIParseResult(
        raw_json=json.dumps(parsed),
        parsed=parsed,
        validation=validate_ai_parse(parsed),
        provider="claude",
        model="claude-test",
        prompt_version="v2",
        prompt_hash="a" * 64,
    )
    extracted = ai_result_to_extract_result(result)
    assert extracted.statement_account_type == "credit_card"


def test_statement_account_type_none_when_missing() -> None:
    """Missing account_type in parsed JSON results in None."""
    parsed = _parsed_payload(institution="Citi")
    # account_type not set in statement
    result = AIParseResult(
        raw_json=json.dumps(parsed),
        parsed=parsed,
        validation=validate_ai_parse(parsed),
        provider="claude",
        model="claude-test",
        prompt_version="v2",
        prompt_hash="a" * 64,
    )
    extracted = ai_result_to_extract_result(result)
    assert extracted.statement_account_type is None


def test_pdf_account_type_used_for_credit_card_without_card_ending(tmp_path: Path) -> None:
    """IMPORT-001: credit card PDF with no card_ending should use statement_account_type
    instead of falling back to 'checking' heuristic."""
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "no-card-ending-cc.pdf")

    extracted = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Citi", "card_ending": None},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
        statement_account_type="credit_card",
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        account = conn.execute(
            "SELECT account_type FROM accounts LIMIT 1"
        ).fetchone()

    assert account["account_type"] == "credit_card"


def test_pdf_account_type_checking_explicit(tmp_path: Path) -> None:
    """Explicit checking account_type is respected even with card_ending present."""
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = _write_pdf(tmp_path, "checking-explicit.pdf")

    extracted = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -100, "source": "Citi", "card_ending": "9999"},
        ],
        statement_total_cents=-100,
        extracted_total_cents=-100,
        reconciled=True,
        warnings=[],
        statement_account_type="checking",
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, extracted, pdf_path, "ai:claude")
        account = conn.execute(
            "SELECT account_type FROM accounts LIMIT 1"
        ).fetchone()

    assert account["account_type"] == "checking"
