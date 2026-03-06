from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from finance_cli.db import connect, initialize_database
from finance_cli.importers import (
    detect_csv_institution,
    import_csv,
    import_normalized_rows,
    normalize_csv,
    supported_institutions,
)


APPLE_HEADER = (
    "Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By\n"
)


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(dedent(content).lstrip("\n"), encoding="utf-8")
    return path


def _apple_csv_row(
    date: str = "02/17/2026",
    description: str = "DELTA 1030",
    merchant: str = "DELTA",
    category: str = "Airlines",
    txn_type: str = "Purchase",
    amount: str = "88.40",
) -> str:
    return f'{date},{date},"{description}","{merchant}","{category}","{txn_type}","{amount}","Henry"\n'


CHASE_CREDIT_HEADER = "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"


def _chase_credit_csv_row(
    date: str = "02/17/2026",
    post_date: str = "02/18/2026",
    description: str = "UBER   *TRIP",
    category: str = "Travel",
    txn_type: str = "Sale",
    amount: str = "-29.53",
    memo: str = "",
) -> str:
    return f"{date},{post_date},{description},{category},{txn_type},{amount},{memo}\n"


AMEX_HEADER = (
    "Date,Description,Amount,Extended Details,Appears On Your Statement As,"
    "Address,City/State,Zip Code,Country,Reference,Category\n"
)


def _amex_csv_row(
    date: str = "01/15/2026",
    description: str = "ANDYS DELI",
    amount: str = "10.82",
    category: str = "Merchandise & Supplies-Groceries",
    reference: str = "'320260010013666733'",
) -> str:
    return f'{date},"{description}",{amount},"","","","","","",{reference},{category}\n'


BOFA_CHECKING_PREAMBLE = (
    'Description,,Summary Amt.\n'
    'Beginning balance as of 01/01/2026,,"5,000.00"\n'
    'Total credits,,"1,000.00"\n'
    'Total debits,,"-500.00"\n'
    'Ending balance as of 01/31/2026,,"5,500.00"\n'
    "\n"
    "Date,Description,Amount,Running Bal.\n"
)


def _bofa_checking_row(
    date: str = "01/15/2026",
    description: str = "PAYPAL DES:INST XFER",
    amount: str = "-32.00",
    running_bal: str = "4,968.00",
) -> str:
    return f'{date},"{description}","{amount}","{running_bal}"\n'


def test_apple_card_purchase_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(txn_type="Purchase", amount="20.00"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Amount"] == "-20.00"
    assert result.rows[0]["Is Payment"] == "false"
    assert result.rows[0]["Account Type"] == "credit_card"


def test_apple_card_payment_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(txn_type="Payment", amount="-100.00", category="Payment"),
    )

    result = normalize_csv(csv_path, "apple")

    assert result.rows[0]["Amount"] == "100.00"
    assert result.rows[0]["Is Payment"] == "true"


def test_apple_card_interest_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(txn_type="Interest", amount="117.18", category="Interest"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Amount"] == "-117.18"
    assert result.rows[0]["Is Payment"] == "false"


def test_apple_card_credit_refund_not_payment(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(txn_type="Credit", amount="-0.36", category="Other"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Amount"] == "0.36"
    assert result.rows[0]["Is Payment"] == "false"


def test_apple_card_debit_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(txn_type="Debit", amount="-1.25", category="Other"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Amount"] == "1.25"
    assert result.rows[0]["Is Payment"] == "false"


def test_apple_card_category_preserved(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(category="Transportation"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Category"] == "Transportation"


def test_apple_card_merchant_fallback(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(description="", merchant="Fallback Merchant"),
    )

    result = normalize_csv(csv_path, "apple_card")

    assert result.rows[0]["Description"] == "Fallback Merchant"


def test_barclays_finds_header_row(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024
        Account Balance as of February 18 2026:    $9855.18

        Some extra line before header
        Transaction Date,Description,Category,Amount
        02/13/2026,"Spotify","DEBIT",-11.99
        """,
    )

    result = normalize_csv(csv_path, "barclays")

    assert result.raw_row_count == 1
    assert result.rows[0]["Description"] == "Spotify"
    assert result.rows[0]["Account Type"] == "credit_card"


def test_barclays_parses_card_ending(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024

        Transaction Date,Description,Category,Amount
        02/13/2026,"Spotify","DEBIT",-11.99
        """,
    )

    result = normalize_csv(csv_path, "barclays")

    assert result.rows[0]["Card Ending"] == "8024"


def test_barclays_sign_preserved(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024

        Transaction Date,Description,Category,Amount
        02/13/2026,"Spotify","DEBIT",-11.99
        02/05/2026,"Payment Received","CREDIT",327.51
        """,
    )

    result = normalize_csv(csv_path, "barclays")

    assert result.rows[0]["Amount"] == "-11.99"
    assert result.rows[1]["Amount"] == "327.51"


def test_barclays_payment_detection(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024

        Transaction Date,Description,Category,Amount
        02/05/2026,"PAYMENT RECEIVED","CREDIT",327.51
        """,
    )

    result = normalize_csv(csv_path, "barclays")

    assert result.rows[0]["Is Payment"] == "true"


def test_barclays_credit_not_payment(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024

        Transaction Date,Description,Category,Amount
        02/05/2026,"CASH BACK STMT CREDIT","CREDIT",5.00
        """,
    )

    result = normalize_csv(csv_path, "barclays")

    assert result.rows[0]["Is Payment"] == "false"


def test_chase_credit_sale_sign_preserved(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase1234_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER + _chase_credit_csv_row(txn_type="Sale", amount="-29.53"),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Amount"] == "-29.53"
    assert result.rows[0]["Is Payment"] == "false"
    assert result.rows[0]["Account Type"] == "credit_card"


def test_chase_credit_payment_detected(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase1234_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER
        + _chase_credit_csv_row(
            description="Payment Thank You - Web", category="", txn_type="Payment", amount="500.00"
        ),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Amount"] == "500.00"
    assert result.rows[0]["Is Payment"] == "true"


def test_chase_credit_fee_not_payment(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase1234_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER
        + _chase_credit_csv_row(
            description="PURCHASE INTEREST CHARGE",
            category="Fees & Adjustments",
            txn_type="Fee",
            amount="-22.88",
        ),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Amount"] == "-22.88"
    assert result.rows[0]["Is Payment"] == "false"


def test_chase_credit_card_ending_from_filename(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase0368_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER + _chase_credit_csv_row(),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Card Ending"] == "0368"


def test_chase_credit_card_ending_fallback(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "chase_export.csv",
        CHASE_CREDIT_HEADER + _chase_credit_csv_row(),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Card Ending"] == ""


def test_chase_credit_category_pass_through(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase1234_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER
        + _chase_credit_csv_row(category="Travel")
        + _chase_credit_csv_row(
            description="Payment Thank You - Web", category="", txn_type="Payment", amount="40.00"
        ),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Category"] == "Travel"
    assert "Category" not in result.rows[1]


def test_chase_credit_return_not_payment(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase1234_Activity20250101_20251231_20260219.CSV",
        CHASE_CREDIT_HEADER
        + _chase_credit_csv_row(
            description="ADOBE  *800-833-6687", category="Shopping", txn_type="Return", amount="37.55"
        ),
    )
    result = normalize_csv(csv_path, "chase_credit")
    assert result.rows[0]["Amount"] == "37.55"
    assert result.rows[0]["Is Payment"] == "false"


def test_detect_chase_credit(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "Chase0368_Activity.CSV",
        CHASE_CREDIT_HEADER + _chase_credit_csv_row(),
    )
    assert detect_csv_institution(csv_path) == "chase_credit"


def test_amex_expense_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER + _amex_csv_row(amount="10.82"),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Amount"] == "-10.82"
    assert result.rows[0]["Is Payment"] == "false"
    assert result.rows[0]["Account Type"] == "credit_card"


def test_amex_payment_detected_and_sign_negated(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER
        + _amex_csv_row(
            description="AUTOPAY PAYMENT - THANK YOU",
            amount="-853.21",
            category="",
            reference="'320253630927875670'",
        ),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Amount"] == "853.21"
    assert result.rows[0]["Is Payment"] == "true"


def test_amex_online_payment_detected(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER
        + _amex_csv_row(
            description="ONLINE PAYMENT - THANK YOU",
            amount="-5830.00",
            category="",
        ),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Is Payment"] == "true"


def test_amex_credit_adjustment_not_payment(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER
        + _amex_csv_row(
            description="CR ADJ FOR FINANCE CHARGE",
            amount="-2.93",
            category="Fees & Adjustments-Fees & Adjustments",
        ),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Amount"] == "2.93"
    assert result.rows[0]["Is Payment"] == "false"


def test_amex_category_preserved(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER + _amex_csv_row(category="Transportation-Rail Services"),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Category"] == "Transportation-Rail Services"


def test_amex_reference_mapped_to_transaction_id(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER + _amex_csv_row(reference="'320260010013666733'"),
    )
    result = normalize_csv(csv_path, "amex")
    assert result.rows[0]["Transaction ID"] == "320260010013666733"


def test_detect_amex(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER + _amex_csv_row(),
    )
    assert detect_csv_institution(csv_path) == "amex"


def test_amex_multiline_fields_parsed(tmp_path: Path) -> None:
    row_with_multiline = (
        '01/15/2026,"ANDYS DELI",10.82,'
        '"10156320260 2129890648\nANDYS DELI\nNEW YORK\nNY",'
        '"ANDYS DELI","106 7TH AVE SOUTH","NEW YORK\nNY",'
        "10014,UNITED STATES,"
        "'320260010013666733',Merchandise & Supplies-Groceries\n"
    )
    csv_path = _write_csv(
        tmp_path / "amex.csv",
        AMEX_HEADER + row_with_multiline,
    )
    result = normalize_csv(csv_path, "amex")
    assert result.raw_row_count == 1
    assert len(result.rows) == 1
    assert result.rows[0]["Amount"] == "-10.82"
    assert result.rows[0]["Description"] == "ANDYS DELI"


def test_bofa_checking_finds_header_row(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "bofa.csv",
        BOFA_CHECKING_PREAMBLE + _bofa_checking_row(),
    )
    result = normalize_csv(csv_path, "bofa_checking")
    assert result.raw_row_count == 1
    assert result.rows[0]["Description"] == "PAYPAL DES:INST XFER"


def test_bofa_checking_skips_beginning_balance(tmp_path: Path) -> None:
    content = (
        BOFA_CHECKING_PREAMBLE
        + '01/01/2026,Beginning balance as of 01/01/2026,,"5,000.00"\n'
        + _bofa_checking_row(date="01/15/2026", amount="-50.00")
    )
    csv_path = _write_csv(tmp_path / "bofa.csv", content)
    result = normalize_csv(csv_path, "bofa_checking")
    assert len(result.rows) == 1
    assert result.skipped_row_count == 1
    assert result.rows[0]["Amount"] == "-50.00"


def test_bofa_checking_sign_preserved(tmp_path: Path) -> None:
    content = (
        BOFA_CHECKING_PREAMBLE
        + _bofa_checking_row(description="Check 1139", amount="-2,900.00")
        + _bofa_checking_row(description="Zelle from Mom", amount="1,000.00")
    )
    csv_path = _write_csv(tmp_path / "bofa.csv", content)
    result = normalize_csv(csv_path, "bofa_checking")
    assert result.rows[0]["Amount"] == "-2900.00"
    assert result.rows[1]["Amount"] == "1000.00"


def test_bofa_checking_is_payment_always_false(tmp_path: Path) -> None:
    content = (
        BOFA_CHECKING_PREAMBLE
        + _bofa_checking_row(description="BANK OF AMERICA CREDIT CARD Bill Payment", amount="-108.00")
        + _bofa_checking_row(description="Zelle payment from SOMEONE", amount="200.00")
    )
    csv_path = _write_csv(tmp_path / "bofa.csv", content)
    result = normalize_csv(csv_path, "bofa_checking")
    assert all(r["Is Payment"] == "false" for r in result.rows)


def test_bofa_checking_source_and_card_ending(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "bofa.csv",
        BOFA_CHECKING_PREAMBLE + _bofa_checking_row(),
    )
    result = normalize_csv(csv_path, "bofa_checking")
    assert result.rows[0]["Card Ending"] == ""
    assert result.rows[0]["Source"] == "Bank of America"
    assert result.rows[0]["Account Type"] == "checking"
    assert result.source_name == "Bank of America"


def test_detect_bofa_checking(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "bofa.csv",
        BOFA_CHECKING_PREAMBLE + _bofa_checking_row(),
    )
    assert detect_csv_institution(csv_path) == "bofa_checking"


def test_bofa_checking_comma_amounts(tmp_path: Path) -> None:
    content = (
        BOFA_CHECKING_PREAMBLE
        + _bofa_checking_row(description="Large deposit", amount="3,000.00")
        + _bofa_checking_row(description="Large payment", amount="-2,900.00")
    )
    csv_path = _write_csv(tmp_path / "bofa.csv", content)
    result = normalize_csv(csv_path, "bofa_checking")
    assert result.rows[0]["Amount"] == "3000.00"
    assert result.rows[1]["Amount"] == "-2900.00"


def test_bofa_checking_warns_missing_card_ending(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "bofa.csv",
        BOFA_CHECKING_PREAMBLE + _bofa_checking_row(),
    )
    result = normalize_csv(csv_path, "bofa_checking")

    expected = "No card ending available for BofA Checking"
    assert sum(1 for warning in result.warnings if expected in warning) == 1


def test_bofa_checking_no_warning_when_empty(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "bofa.csv",
        BOFA_CHECKING_PREAMBLE,
    )
    result = normalize_csv(csv_path, "bofa_checking")

    assert result.raw_row_count == 0
    assert all("No card ending available for BofA Checking" not in warning for warning in result.warnings)


def test_unsupported_institution_raises(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "sample.csv",
        APPLE_HEADER + _apple_csv_row(),
    )

    with pytest.raises(ValueError, match="unsupported institution"):
        normalize_csv(csv_path, "wells_fargo")


def test_file_not_found_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        normalize_csv(tmp_path / "missing.csv", "apple_card")


def test_supported_institutions_list() -> None:
    assert supported_institutions() == ["american_express", "amex", "apple", "apple_card", "barclays", "bofa_checking", "chase_credit"]


def test_detect_apple_card(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(),
    )

    assert detect_csv_institution(csv_path) == "apple_card"


def test_detect_barclays(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "barclays.csv",
        """
        Barclays Bank Delaware
        Account Number: XXXXXXXXXXXX8024

        Transaction Date,Description,Category,Amount
        02/13/2026,"Spotify","DEBIT",-11.99
        """,
    )

    assert detect_csv_institution(csv_path) == "barclays"


def test_detect_unknown_returns_none(tmp_path: Path) -> None:
    csv_path = _write_csv(
        tmp_path / "unknown.csv",
        """
        date,description,amount
        2026-02-13,Spotify,-11.99
        """,
    )

    assert detect_csv_institution(csv_path) is None


def test_detect_missing_file_returns_none(tmp_path: Path) -> None:
    assert detect_csv_institution(tmp_path / "missing.csv") is None


def test_normalize_then_import(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER
        + _apple_csv_row(date="02/10/2026", txn_type="Purchase", amount="12.50", category="Dining")
        + _apple_csv_row(
            date="02/11/2026",
            txn_type="Payment",
            amount="-100.00",
            category="Payment",
            description="ACH DEPOSIT",
            merchant="ACH DEPOSIT",
        ),
    )
    normalized = normalize_csv(csv_path, "apple_card")

    with connect(db_path) as conn:
        report = import_normalized_rows(conn, normalized.rows, normalized.source_name, dry_run=False)
        amount_rows = conn.execute(
            "SELECT amount_cents, is_payment FROM transactions ORDER BY date, description"
        ).fetchall()

    assert report.inserted == 2
    assert report.skipped_duplicates == 0
    assert report.errors == 0
    assert [row["amount_cents"] for row in amount_rows] == [-1250, 10000]
    assert [row["is_payment"] for row in amount_rows] == [0, 1]


def test_normalize_import_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "apple.csv",
        APPLE_HEADER + _apple_csv_row(date="02/10/2026", amount="12.50"),
    )
    normalized = normalize_csv(csv_path, "apple_card")

    with connect(db_path) as conn:
        first = import_normalized_rows(conn, normalized.rows, normalized.source_name, dry_run=False)
        second = import_normalized_rows(conn, normalized.rows, normalized.source_name, dry_run=False)
        count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert first.inserted == 1
    assert first.skipped_duplicates == 0
    assert second.inserted == 0
    assert second.skipped_duplicates == 1
    assert count == 1


def test_import_normalized_rows_dry_run_has_no_side_effects(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Test Source",
            "Category": "Travel",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        report = import_normalized_rows(conn, rows, "Test Source", dry_run=True, validate_name=False)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        category_count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]

    assert report.inserted == 1
    assert report.skipped_duplicates == 0
    assert txn_count == 0
    assert account_count == 0
    assert category_count == 0


def test_import_csv_dry_run_has_no_side_effects(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "canonical.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Chase,Business,Travel,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=True)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        account_count = conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()["n"]
        category_count = conn.execute("SELECT COUNT(*) AS n FROM categories").fetchone()["n"]

    assert report.inserted == 1
    assert report.skipped_duplicates == 0
    assert txn_count == 0
    assert account_count == 0
    assert category_count == 0


def test_import_csv_writes_import_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "canonical.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Chase,Business,Travel,false
        2026-02-11,COFFEE SHOP,-5.00,1234,Chase,Personal,Dining,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        batch = conn.execute(
            """
            SELECT source_type, file_path, bank_parser, extracted_count, imported_count, skipped_count,
                   reconcile_status, statement_total_cents, extracted_total_cents, file_hash_sha256
              FROM import_batches
            """
        ).fetchone()

    assert report.inserted == 2
    assert report.skipped_duplicates == 0
    assert batch["source_type"] == "csv"
    assert batch["file_path"] == str(csv_path)
    assert batch["bank_parser"] == "Chase Credit"
    assert batch["extracted_count"] == 2
    assert batch["imported_count"] == 2
    assert batch["skipped_count"] == 0
    assert batch["reconcile_status"] == "no_totals"
    assert batch["statement_total_cents"] is None
    assert batch["extracted_total_cents"] is None
    assert len(batch["file_hash_sha256"]) == 64


def test_import_csv_file_hash_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "canonical.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Chase,Business,Travel,false
        2026-02-11,COFFEE SHOP,-5.00,1234,Chase,Personal,Dining,false
        """,
    )

    with connect(db_path) as conn:
        first = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        second = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=False)
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first.inserted == 2
    assert first.skipped_duplicates == 0
    assert second.inserted == 0
    assert second.skipped_duplicates == 2
    assert batch_count == 1


def test_import_csv_dry_run_no_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    csv_path = _write_csv(
        tmp_path / "canonical.csv",
        """
        Date,Description,Amount,Card Ending,Source,Use Type,Category,Is Payment
        2026-02-10,UBER TRIP,-12.50,1234,Chase,Business,Travel,false
        """,
    )

    with connect(db_path) as conn:
        report = import_csv(conn, csv_path, source_name="Chase Credit", dry_run=True)
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert report.inserted == 1
    assert report.skipped_duplicates == 0
    assert batch_count == 0


def test_import_normalized_rows_writes_import_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    file_path = _write_csv(tmp_path / "source.csv", APPLE_HEADER + _apple_csv_row())
    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Test Source",
            "Category": "Travel",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        report = import_normalized_rows(
            conn,
            rows,
            "Test Source",
            dry_run=False,
            file_path=file_path,
            validate_name=False,
        )
        batch = conn.execute(
            """
            SELECT source_type, file_path, bank_parser, extracted_count, imported_count, skipped_count, reconcile_status
              FROM import_batches
            """
        ).fetchone()

    assert report.inserted == 1
    assert report.skipped_duplicates == 0
    assert batch["source_type"] == "csv"
    assert batch["file_path"] == str(file_path)
    assert batch["bank_parser"] == "Test Source"
    assert batch["extracted_count"] == 1
    assert batch["imported_count"] == 1
    assert batch["skipped_count"] == 0
    assert batch["reconcile_status"] == "no_totals"


def test_import_normalized_rows_file_hash_dedup(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    file_path = _write_csv(tmp_path / "source.csv", APPLE_HEADER + _apple_csv_row())
    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test A",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Test Source",
            "Category": "Travel",
            "Is Payment": "false",
        },
        {
            "Date": "2026-02-11",
            "Description": "Test B",
            "Amount": "-6.25",
            "Card Ending": "1234",
            "Source": "Test Source",
            "Category": "Dining",
            "Is Payment": "false",
        },
    ]

    with connect(db_path) as conn:
        first = import_normalized_rows(conn, rows, "Test Source", dry_run=False, file_path=file_path, validate_name=False)
        second = import_normalized_rows(conn, rows, "Test Source", dry_run=False, file_path=file_path, validate_name=False)
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first.inserted == 2
    assert first.skipped_duplicates == 0
    assert second.inserted == 0
    assert second.skipped_duplicates == 2
    assert batch_count == 1


def test_import_normalized_rows_no_file_path_no_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)

    rows = [
        {
            "Date": "2026-02-10",
            "Description": "Test",
            "Amount": "-12.50",
            "Card Ending": "1234",
            "Source": "Test Source",
            "Category": "Travel",
            "Is Payment": "false",
        }
    ]

    with connect(db_path) as conn:
        report = import_normalized_rows(conn, rows, "Test Source", dry_run=False, validate_name=False)
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert report.inserted == 1
    assert report.skipped_duplicates == 0
    assert batch_count == 0
