from __future__ import annotations

import json
from pathlib import Path

from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database
from finance_cli.dedup import find_cross_format_duplicates
from finance_cli.importers import _account_id_for_source, import_normalized_rows


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _insert_plaid_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str,
    card_ending: str | None,
    is_active: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (
            id, plaid_account_id, institution_name, account_name, account_type, card_ending, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            account_id,
            f"plaid_{account_id}",
            institution_name,
            f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
            is_active,
        ),
    )


def _insert_hash_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str,
    card_ending: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, card_ending)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            account_id,
            institution_name,
            f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
        ),
    )


def _insert_txn(
    conn,
    *,
    txn_id: str,
    account_id: str,
    date: str,
    amount_cents: int,
    description: str,
    source: str,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, dedupe_key, date, description, amount_cents, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (txn_id, account_id, f"dedupe:{txn_id}", date, description, amount_cents, source),
    )


def test_alias_created_exact_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_barclays_8024",
            institution_name="Barclays - Cards",
            account_type="credit_card",
            card_ending="8024",
        )
        rows = [
            {
                "Date": "2026-02-18",
                "Description": "SPOTIFY",
                "Amount": "-11.99",
                "Card Ending": "8024",
                "Account Type": "credit_card",
                "Source": "Barclays",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, rows, source_name="Barclays", validate_name=False)
        hash_account_id = _account_id_for_source("Barclays", "8024")
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert alias is not None
    assert alias["canonical_id"] == "plaid_barclays_8024"


def test_alias_created_with_institution_equivalence_on_card_ending(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_merrill_8894",
            institution_name="Merrill",
            account_type="credit_card",
            card_ending="8894",
        )
        rows = [
            {
                "Date": "2026-02-18",
                "Description": "FLIGHT",
                "Amount": "-88.40",
                "Card Ending": "8894",
                "Account Type": "credit_card",
                "Source": "Bank of America",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, rows, source_name="Bank of America", validate_name=False)
        hash_account_id = _account_id_for_source("Bank of America", "8894")
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert alias is not None
    assert alias["canonical_id"] == "plaid_merrill_8894"


def test_no_alias_with_institution_equivalence_when_only_account_type_matches(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_merrill_checking",
            institution_name="Merrill",
            account_type="checking",
            card_ending=None,
        )
        rows = [
            {
                "Date": "2026-02-18",
                "Description": "COFFEE",
                "Amount": "-4.20",
                "Card Ending": "",
                "Account Type": "checking",
                "Source": "Bank of America",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, rows, source_name="Bank of America", validate_name=False)
        hash_account_id = _account_id_for_source("Bank of America", "")
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert alias is None


def test_alias_created_with_type_fallback_when_card_ending_not_numeric(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_amex_main",
            institution_name="American Express",
            account_type="credit_card",
            card_ending="9001",
        )
        rows = [
            {
                "Date": "2026-02-18",
                "Description": "DINNER",
                "Amount": "-50.00",
                "Card Ending": "Amex",
                "Account Type": "credit_card",
                "Source": "Amex",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, rows, source_name="Amex", validate_name=False)
        hash_account_id = _account_id_for_source("Amex", "Amex")
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert alias is not None
    assert alias["canonical_id"] == "plaid_amex_main"


def test_no_alias_when_type_fallback_is_ambiguous(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_chase_1",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="1234",
        )
        _insert_plaid_account(
            conn,
            account_id="plaid_chase_2",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="5678",
        )
        rows = [
            {
                "Date": "2026-02-18",
                "Description": "MARKET",
                "Amount": "-10.00",
                "Card Ending": "Card",
                "Account Type": "credit_card",
                "Source": "Chase",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, rows, source_name="Chase", validate_name=False)
        hash_account_id = _account_id_for_source("Chase", "Card")
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert alias is None


def test_stale_alias_persists_when_plaid_account_deactivates(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_chase_0368",
            institution_name="Chase",
            account_type="credit_card",
            card_ending="0368",
            is_active=1,
        )
        first_rows = [
            {
                "Date": "2026-02-18",
                "Description": "ONE",
                "Amount": "-20.00",
                "Card Ending": "0368",
                "Account Type": "credit_card",
                "Source": "Chase",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, first_rows, source_name="Chase", validate_name=False)
        hash_account_id = _account_id_for_source("Chase", "0368")
        assert conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone() is not None

        conn.execute("UPDATE accounts SET is_active = 0 WHERE id = 'plaid_chase_0368'")
        second_rows = [
            {
                "Date": "2026-02-19",
                "Description": "TWO",
                "Amount": "-30.00",
                "Card Ending": "0368",
                "Account Type": "credit_card",
                "Source": "Chase",
                "Is Payment": "false",
            }
        ]
        import_normalized_rows(conn, second_rows, source_name="Chase", validate_name=False)
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()
        second_txn_account = conn.execute(
            "SELECT account_id FROM transactions WHERE description = 'TWO'"
        ).fetchone()

    assert alias is not None
    assert alias["canonical_id"] == "plaid_chase_0368"
    assert second_txn_account is not None
    assert second_txn_account["account_id"] == "plaid_chase_0368"


def test_dedup_resolves_aliases_for_grouping_and_filter(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_shared",
            institution_name="Barclays - Cards",
            account_type="credit_card",
            card_ending="8024",
        )
        hash_account_id = _account_id_for_source("Barclays", "8024")
        _insert_hash_account(
            conn,
            account_id=hash_account_id,
            institution_name="Barclays",
            account_type="credit_card",
            card_ending="8024",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES (?, ?)",
            (hash_account_id, "plaid_shared"),
        )
        _insert_txn(
            conn,
            txn_id="csv_txn",
            account_id=hash_account_id,
            date="2026-02-18",
            amount_cents=-1199,
            description="SPOTIFY",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="plaid_txn",
            account_id="plaid_shared",
            date="2026-02-18",
            amount_cents=-1199,
            description="SPOTIFY",
            source="plaid",
        )

        all_report = find_cross_format_duplicates(conn)
        canonical_filtered = find_cross_format_duplicates(conn, account_id="plaid_shared")
        hash_filtered = find_cross_format_duplicates(conn, account_id=hash_account_id)

    assert len(all_report.matches) == 1
    assert len(canonical_filtered.matches) == 1
    assert len(hash_filtered.matches) == 1
    assert all_report.matches[0].keep_id == "csv_txn"
    assert all_report.matches[0].remove_id == "plaid_txn"


def test_account_name_is_clean_for_new_hash_accounts(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        import_normalized_rows(
            conn,
            [
                {
                    "Date": "2026-02-18",
                    "Description": "APPLE PURCHASE",
                    "Amount": "-2.00",
                    "Card Ending": "Apple",
                    "Account Type": "credit_card",
                    "Source": "Apple Card",
                    "Is Payment": "false",
                }
            ],
            source_name="Apple Card",
            validate_name=False,
        )
        import_normalized_rows(
            conn,
            [
                {
                    "Date": "2026-02-18",
                    "Description": "CHASE PURCHASE",
                    "Amount": "-3.00",
                    "Card Ending": "0368",
                    "Account Type": "credit_card",
                    "Source": "Chase",
                    "Is Payment": "false",
                },
            ],
            source_name="Chase",
            validate_name=False,
        )
        apple_id = _account_id_for_source("Apple Card", "Apple")
        chase_id = _account_id_for_source("Chase", "0368")
        apple_name = conn.execute(
            "SELECT account_name FROM accounts WHERE id = ?",
            (apple_id,),
        ).fetchone()["account_name"]
        chase_name = conn.execute(
            "SELECT account_name FROM accounts WHERE id = ?",
            (chase_id,),
        ).fetchone()["account_name"]

    assert apple_name == "Apple Card"
    assert chase_name == "Chase 0368"


def test_account_id_hash_regression_stable() -> None:
    assert _account_id_for_source("Apple Card", "Apple") == "ebd916058bbc04f803c40585"
    assert _account_id_for_source("BofA Checking", "") == _account_id_for_source(
        "Bank of America",
        "",
    )


def test_backfill_aliases_command_dry_run_and_commit(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    with connect(db_path) as conn:
        _insert_plaid_account(
            conn,
            account_id="plaid_barclays_8024",
            institution_name="Barclays - Cards",
            account_type="credit_card",
            card_ending="8024",
        )
        hash_account_id = _account_id_for_source("Barclays", "8024")
        _insert_hash_account(
            conn,
            account_id=hash_account_id,
            institution_name="Barclays",
            account_type="credit_card",
            card_ending="8024",
        )
        conn.commit()

    dry_run_code = main(["dedup", "backfill-aliases"])
    dry_run_payload = json.loads(capsys.readouterr().out)
    with connect(db_path) as conn:
        dry_run_alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    commit_code = main(["dedup", "backfill-aliases", "--commit"])
    commit_payload = json.loads(capsys.readouterr().out)
    with connect(db_path) as conn:
        commit_alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

    assert dry_run_code == 0
    assert dry_run_payload["status"] == "success"
    assert dry_run_payload["data"]["dry_run"] is True
    assert dry_run_payload["data"]["aliased"] == 1
    assert dry_run_alias is None

    assert commit_code == 0
    assert commit_payload["status"] == "success"
    assert commit_payload["data"]["dry_run"] is False
    assert commit_payload["data"]["aliased"] == 1
    assert commit_alias is not None
    assert commit_alias["canonical_id"] == "plaid_barclays_8024"
