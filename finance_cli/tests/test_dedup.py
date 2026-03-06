from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path

import finance_cli.commands.dedup_cmd as dedup_cmd_module
from finance_cli.__main__ import main
from finance_cli.db import connect, initialize_database
from finance_cli.dedup import apply_dedup, find_cross_format_duplicates


def _setup_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _insert_account(conn, account_id: str, card_ending: str = "1234") -> None:
    conn.execute(
        """
        INSERT INTO accounts (id, institution_name, account_name, account_type, card_ending)
        VALUES (?, 'Test Bank', 'Test Card', 'credit_card', ?)
        """,
        (account_id, card_ending),
    )


def _insert_named_account(
    conn,
    *,
    account_id: str,
    institution_name: str,
    account_type: str,
    card_ending: str | None = None,
    plaid_account_id: str | None = None,
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
            plaid_account_id,
            institution_name,
            f"{institution_name} {card_ending or ''}".strip(),
            account_type,
            card_ending,
            is_active,
        ),
    )


def _insert_txn(
    conn,
    *,
    txn_id: str,
    account_id: str | None,
    date: str,
    amount_cents: int,
    description: str,
    source: str,
    is_active: int = 1,
    notes: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions (
            id, account_id, dedupe_key, date, description, amount_cents, source, is_active, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            txn_id,
            account_id,
            f"dedupe:{txn_id}",
            date,
            description,
            amount_cents,
            source,
            is_active,
            notes,
        ),
    )


def test_exact_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS SEATTLE", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS SEATTLE", source="pdf_import")

        report = find_cross_format_duplicates(conn)
        removed = apply_dedup(conn, report)
        row = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert len(report.matches) == 1
    assert report.elapsed_ms >= 0
    assert report.matches[0].match_type == "exact"
    assert report.matches[0].keep_id == "csv_1"
    assert report.matches[0].remove_id == "pdf_1"
    assert removed == 1
    assert row["is_active"] == 0


def test_substring_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="csv_1",
            account_id="acct_a",
            date="2025-01-01",
            amount_cents=-500,
            description="STARBUCKS DOWNTOWN SEATTLE",
            source="csv_import",
        )
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].match_type == "substring"


def test_key_only_fallback_unambiguous(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="UBER", source="pdf_import")
        report = find_cross_format_duplicates(conn)
        removed = apply_dedup(conn, report, exclude_match_types={"key_only"})
        row = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert len(report.matches) == 1
    assert report.matches[0].match_type == "key_only"
    assert report.matches[0].keep_id == "csv_1"
    assert report.matches[0].remove_id == "pdf_1"
    assert removed == 0
    assert row["is_active"] == 1


def test_key_only_included_when_opted_in(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="UBER", source="pdf_import")
        report = find_cross_format_duplicates(conn)
        removed = apply_dedup(conn, report)
        row = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert len(report.matches) == 1
    assert report.matches[0].match_type == "key_only"
    assert removed == 1
    assert row["is_active"] == 0


def test_no_match_different_description_when_ambiguous(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="csv_2", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="MCDONALDS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="UBER", source="pdf_import")
        _insert_txn(conn, txn_id="pdf_2", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="LYFT", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_multiple_same_day_amount(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_a", account_id="acct_a", date="2025-01-01", amount_cents=-999, description="STARBUCKS DOWNTOWN", source="csv_import")
        _insert_txn(conn, txn_id="csv_b", account_id="acct_a", date="2025-01-01", amount_cents=-999, description="MCDONALDS AIRPORT", source="csv_import")
        _insert_txn(conn, txn_id="pdf_a", account_id="acct_a", date="2025-01-01", amount_cents=-999, description="STARBUCKS", source="pdf_import")
        _insert_txn(conn, txn_id="pdf_b", account_id="acct_a", date="2025-01-01", amount_cents=-999, description="MCDONALDS", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 2
    pairs = {(item.keep_id, item.remove_id) for item in report.matches}
    assert pairs == {("csv_a", "pdf_a"), ("csv_b", "pdf_b")}


def test_mixed_safe_and_key_only_commit(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_exact", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_exact", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="NETFLIX", source="pdf_import")
        _insert_txn(conn, txn_id="csv_key", account_id="acct_a", date="2025-01-02", amount_cents=-700, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_key", account_id="acct_a", date="2025-01-02", amount_cents=-700, description="UBER", source="pdf_import")

        report = find_cross_format_duplicates(conn)
        removed = apply_dedup(conn, report, exclude_match_types={"key_only"})
        row_exact = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_exact'").fetchone()
        row_key = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_key'").fetchone()

    assert len(report.matches) == 2
    assert removed == 1
    assert row_exact["is_active"] == 0
    assert row_key["is_active"] == 1


def test_exact_preferred_over_substring(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS SEATTLE", source="csv_import")
        _insert_txn(conn, txn_id="a_pdf_sub", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS", source="pdf_import")
        _insert_txn(conn, txn_id="z_pdf_exact", account_id="acct_a", date="2025-01-01", amount_cents=-500, description="STARBUCKS SEATTLE", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].remove_id == "z_pdf_exact"
    assert report.matches[0].match_type == "exact"


def test_preference_csv_over_pdf(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert report.matches[0].sources == ("csv_import", "pdf_import")


def test_preference_csv_over_plaid(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="csv_import")
        _insert_txn(conn, txn_id="plaid_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="plaid")
        report = find_cross_format_duplicates(conn)

    assert report.matches[0].sources == ("csv_import", "plaid")


def test_preference_plaid_over_pdf(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="plaid_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="plaid")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert report.matches[0].sources == ("plaid", "pdf_import")


def test_preference_manual_over_pdf(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="manual_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="manual")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-400, description="TARGET", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].sources == ("manual", "pdf_import")


def test_null_account_id_excluded(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_txn(conn, txn_id="csv_1", account_id=None, date="2025-01-01", amount_cents=-400, description="TARGET", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id=None, date="2025-01-01", amount_cents=-400, description="TARGET", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_empty_description_skipped(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="1234567890", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="9876543210", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_zero_amount_transactions(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=0, description="CARD AUTH", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=0, description="CARD AUTH", source="pdf_import")
        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1


def test_dry_run_no_changes(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="pdf_import")

    code = main(["dedup", "cross-format"])
    payload = json.loads(capsys.readouterr().out)

    with connect(db_path) as conn:
        states = conn.execute("SELECT id, is_active FROM transactions ORDER BY id").fetchall()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["removed"] == 0
    assert payload["data"]["elapsed_ms"] >= 0
    assert [row["is_active"] for row in states] == [1, 1]


def test_cross_format_cli_report_includes_elapsed(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="pdf_import")

    code = main(["dedup", "cross-format", "--format", "cli"])
    output = capsys.readouterr().out
    assert code == 0
    assert "Cross-format duplicates (dry-run): 1 match(es), removed=0 elapsed=" in output


def test_cross_format_no_match_includes_elapsed(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(["dedup", "cross-format", "--format", "cli"])
    output = capsys.readouterr().out
    assert code == 0
    assert "No cross-format duplicates found (dry-run) elapsed=" in output


def test_commit_skips_key_only_by_default(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="UBER", source="pdf_import")

    code = main(["dedup", "cross-format", "--commit"])
    payload = json.loads(capsys.readouterr().out)

    with connect(db_path) as conn:
        state = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["removed"] == 0
    assert payload["summary"]["key_only_count"] == 1
    assert state["is_active"] == 1


def test_commit_include_key_only_removes_match(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="STARBUCKS", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="UBER", source="pdf_import")

    code = main(["dedup", "cross-format", "--commit", "--include-key-only"])
    payload = json.loads(capsys.readouterr().out)

    with connect(db_path) as conn:
        state = conn.execute("SELECT is_active FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["removed"] == 1
    assert payload["summary"]["key_only_count"] == 1
    assert Path(payload["data"]["backup_path"]).exists()
    assert state["is_active"] == 0


def test_idempotent_rerun(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="pdf_import")

        first = find_cross_format_duplicates(conn)
        first_removed = apply_dedup(conn, first)
        second = find_cross_format_duplicates(conn)
        second_removed = apply_dedup(conn, second)

    assert len(first.matches) == 1
    assert first_removed == 1
    assert len(second.matches) == 0
    assert second_removed == 0


def test_scope_by_account(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_account(conn, "acct_b")
        _insert_txn(conn, txn_id="csv_a", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="HULU", source="csv_import")
        _insert_txn(conn, txn_id="pdf_a", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="HULU", source="pdf_import")
        _insert_txn(conn, txn_id="csv_b", account_id="acct_b", date="2025-01-01", amount_cents=-100, description="HULU", source="csv_import")
        _insert_txn(conn, txn_id="pdf_b", account_id="acct_b", date="2025-01-01", amount_cents=-100, description="HULU", source="pdf_import")
        report = find_cross_format_duplicates(conn, account_id="acct_a")

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "csv_a"


def test_scope_by_date_range(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_old", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="HULU", source="csv_import")
        _insert_txn(conn, txn_id="pdf_old", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="HULU", source="pdf_import")
        _insert_txn(conn, txn_id="csv_new", account_id="acct_a", date="2025-02-01", amount_cents=-100, description="HULU", source="csv_import")
        _insert_txn(conn, txn_id="pdf_new", account_id="acct_a", date="2025-02-01", amount_cents=-100, description="HULU", source="pdf_import")
        report = find_cross_format_duplicates(conn, date_from="2025-02-01", date_to="2025-02-28")

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "csv_new"


def test_scope_by_date_range_mixed_formats(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_in", account_id="acct_a", date="01/28/2026", amount_cents=-999, description="APPLE BILL", source="csv_import")
        _insert_txn(conn, txn_id="pdf_in", account_id="acct_a", date="2026-01-28", amount_cents=-999, description="APPLE BILL", source="pdf_import")
        _insert_txn(conn, txn_id="csv_out", account_id="acct_a", date="01/05/2026", amount_cents=-999, description="APPLE BILL", source="csv_import")
        _insert_txn(conn, txn_id="pdf_out", account_id="acct_a", date="2026-01-05", amount_cents=-999, description="APPLE BILL", source="pdf_import")

        report = find_cross_format_duplicates(conn, date_from="2026-01-20", date_to="2026-01-31")

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "csv_in"
    assert report.matches[0].remove_id == "pdf_in"


def test_import_batches_untouched(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="pdf_import")
        batch_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO import_batches (
                id, source_type, file_path, file_hash_sha256, bank_parser, extracted_count, imported_count, skipped_count, reconcile_status
            ) VALUES (?, 'pdf', '/tmp/file.pdf', ?, 'ai:claude', 2, 2, 0, 'matched')
            """,
            (batch_id, "hash-for-batch"),
        )
        before = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
        report = find_cross_format_duplicates(conn)
        apply_dedup(conn, report)
        after = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
        persisted = conn.execute("SELECT id FROM import_batches WHERE id = ?", (batch_id,)).fetchone()

    assert before == 1
    assert after == 1
    assert persisted["id"] == batch_id


def test_removed_at_and_notes_set(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-01", amount_cents=-100, description="NETFLIX", source="pdf_import")

        report = find_cross_format_duplicates(conn)
        apply_dedup(conn, report)
        row = conn.execute("SELECT removed_at, notes FROM transactions WHERE id = 'pdf_1'").fetchone()

    assert row["removed_at"] is not None
    assert "cross-format-dedup: kept csv_1 (csv_import)" in str(row["notes"])


def test_date_format_mismatch(tmp_path: Path) -> None:
    """CSV stores MM/DD/YYYY, PDF stores YYYY-MM-DD — dedup should still match."""
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="01/28/2026", amount_cents=-999, description="STARBUCKS SEATTLE", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2026-01-28", amount_cents=-999, description="STARBUCKS SEATTLE", source="pdf_import")

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "csv_1"
    assert report.matches[0].remove_id == "pdf_1"
    assert report.matches[0].date == "2026-01-28"


def test_date_format_mixed_batch(tmp_path: Path) -> None:
    """Multiple transactions with mixed date formats all match correctly."""
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        # Two CSV (MM/DD/YYYY) and two PDF (YYYY-MM-DD) on different dates
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="01/10/2026", amount_cents=-1327, description="UBER TRIP", source="csv_import")
        _insert_txn(conn, txn_id="csv_2", account_id="acct_a", date="01/17/2026", amount_cents=-11500, description="ALASKA AIRLINES", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2026-01-10", amount_cents=-1327, description="UBER TRIP", source="pdf_import")
        _insert_txn(conn, txn_id="pdf_2", account_id="acct_a", date="2026-01-17", amount_cents=-11500, description="ALASKA AIRLINES", source="pdf_import")

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 2
    ids_removed = {m.remove_id for m in report.matches}
    assert ids_removed == {"pdf_1", "pdf_2"}


def test_review_key_only_returns_enriched_matches(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="acct_review",
            institution_name="Merrill",
            account_type="credit_card",
            card_ending="9932",
            plaid_account_id="plaid_acct_review",
        )
        _insert_txn(
            conn,
            txn_id="plaid_keep_txn",
            account_id="acct_review",
            date="2025-11-24",
            amount_cents=-9145,
            description="Brooklyn Fare Greenwich",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_remove_txn",
            account_id="acct_review",
            date="2025-11-24",
            amount_cents=-9145,
            description="BROOKLYN FARE GREENWIC NEW YORK, NY",
            source="pdf_import",
        )
        conn.commit()

    code = main(["dedup", "review-key-only"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["total_key_only"] == 1
    assert payload["data"]["skipped"] == 0
    assert len(payload["data"]["matches"]) == 1
    match = payload["data"]["matches"][0]
    assert match["date"] == "2025-11-24"
    assert match["amount_cents"] == -9145
    assert match["amount"] == "-91.45"
    assert match["keep"]["source"] == "plaid"
    assert match["remove"]["source"] == "pdf_import"
    assert match["keep"]["description"] == "Brooklyn Fare Greenwich"
    assert match["remove"]["description"] == "BROOKLYN FARE GREENWIC NEW YORK, NY"
    assert match["keep"]["institution"] == "Merrill"
    assert match["keep"]["account"] == "Merrill 9932"


def test_review_key_only_filters_non_key_only(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="csv_exact",
            account_id="acct_a",
            date="2025-11-01",
            amount_cents=-1234,
            description="NETFLIX",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="pdf_exact",
            account_id="acct_a",
            date="2025-11-01",
            amount_cents=-1234,
            description="NETFLIX",
            source="pdf_import",
        )
        _insert_txn(
            conn,
            txn_id="csv_key",
            account_id="acct_a",
            date="2025-11-02",
            amount_cents=-4321,
            description="FOOD COURT",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="pdf_key",
            account_id="acct_a",
            date="2025-11-02",
            amount_cents=-4321,
            description="RESTAURANT",
            source="pdf_import",
        )
        conn.commit()

    code = main(["dedup", "review-key-only"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["total_key_only"] == 1
    assert len(payload["data"]["matches"]) == 1
    assert payload["data"]["matches"][0]["date"] == "2025-11-02"
    assert payload["data"]["matches"][0]["amount_cents"] == -4321


def test_review_key_only_empty(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(["dedup", "review-key-only", "--format", "cli"])
    output = capsys.readouterr().out

    assert code == 0
    assert "No key-only matches" in output


def test_review_key_only_cli_report_format(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    keep_id = "plaid_keep_123456789"
    remove_id = "pdf_remove_987654321"
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id=keep_id,
            account_id="acct_a",
            date="2025-11-24",
            amount_cents=-5424,
            description="Salt Charcoal",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id=remove_id,
            account_id="acct_a",
            date="2025-11-24",
            amount_cents=-5424,
            description="SALT + CHARCOAL 5455877-8144102         NY",
            source="csv_import",
        )
        conn.commit()

    code = main(["dedup", "review-key-only", "--format", "cli"])
    output = capsys.readouterr().out

    assert code == 0
    assert "KEEP" in output
    assert "REMOVE" in output
    assert "Salt Charcoal" in output
    assert "SALT + CHARCOAL 5455877-8144102         NY" in output
    assert f"({keep_id[:8]})" in output
    assert f"({remove_id[:8]})" in output


def test_review_key_only_account_id_filter(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_account(conn, "acct_b")
        _insert_txn(conn, txn_id="csv_a", account_id="acct_a", date="2025-12-01", amount_cents=-1000, description="FOO", source="csv_import")
        _insert_txn(conn, txn_id="pdf_a", account_id="acct_a", date="2025-12-01", amount_cents=-1000, description="BAR", source="pdf_import")
        _insert_txn(conn, txn_id="csv_b", account_id="acct_b", date="2025-12-01", amount_cents=-2000, description="ALPHA", source="csv_import")
        _insert_txn(conn, txn_id="pdf_b", account_id="acct_b", date="2025-12-01", amount_cents=-2000, description="BETA", source="pdf_import")
        conn.commit()

    code = main(["dedup", "review-key-only", "--account-id", "acct_a"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["total_key_only"] == 1
    assert len(payload["data"]["matches"]) == 1
    assert payload["data"]["filters"]["account_id"] == "acct_a"
    assert payload["data"]["matches"][0]["keep"]["id"] == "csv_a"


def test_review_key_only_date_filters(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_old", account_id="acct_a", date="2025-11-10", amount_cents=-1000, description="OLD A", source="csv_import")
        _insert_txn(conn, txn_id="pdf_old", account_id="acct_a", date="2025-11-10", amount_cents=-1000, description="OLD B", source="pdf_import")
        _insert_txn(conn, txn_id="csv_new", account_id="acct_a", date="2025-12-10", amount_cents=-2000, description="NEW A", source="csv_import")
        _insert_txn(conn, txn_id="pdf_new", account_id="acct_a", date="2025-12-10", amount_cents=-2000, description="NEW B", source="pdf_import")
        conn.commit()

    code = main(["dedup", "review-key-only", "--from", "2025-12-01", "--to", "2025-12-31"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["total_key_only"] == 1
    assert len(payload["data"]["matches"]) == 1
    assert payload["data"]["matches"][0]["date"] == "2025-12-10"


def test_review_key_only_cli_footer_echoes_filters(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-01-15", amount_cents=-1000, description="FOO", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-01-15", amount_cents=-1000, description="BAR", source="pdf_import")
        conn.commit()

    code = main(
        [
            "dedup",
            "review-key-only",
            "--account-id",
            "acct_a",
            "--from",
            "2025-01-01",
            "--to",
            "2025-01-31",
            "--format",
            "cli",
        ]
    )
    output = capsys.readouterr().out

    assert code == 0
    assert (
        "To apply: dedup cross-format --account-id acct_a --from 2025-01-01 --to 2025-01-31 --include-key-only --commit"
        in output
    )


def test_review_key_only_cli_truncation(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        for idx in range(55):
            amount = -10000 - idx
            _insert_txn(
                conn,
                txn_id=f"csv_{idx}",
                account_id="acct_a",
                date="2025-12-15",
                amount_cents=amount,
                description=f"CSV DESC {idx}",
                source="csv_import",
            )
            _insert_txn(
                conn,
                txn_id=f"pdf_{idx}",
                account_id="acct_a",
                date="2025-12-15",
                amount_cents=amount,
                description=f"PDF DESC {idx}",
                source="pdf_import",
            )
        conn.commit()

    code = main(["dedup", "review-key-only", "--format", "cli"])
    output = capsys.readouterr().out

    assert code == 0
    assert "Key-only matches: 55 pending review" in output
    assert output.count("KEEP   [") == 50
    assert "... 5 more (use --format json for full output)" in output


def test_review_key_only_skips_missing_enrichment(tmp_path: Path, monkeypatch, capsys, caplog) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-12-20", amount_cents=-1000, description="FOO", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-12-20", amount_cents=-1000, description="BAR", source="pdf_import")
        conn.commit()

    original = dedup_cmd_module.find_cross_format_duplicates

    def _find_then_delete(conn, account_id=None, date_from=None, date_to=None):
        report = original(conn, account_id=account_id, date_from=date_from, date_to=date_to)
        if report.matches:
            conn.execute("DELETE FROM transactions WHERE id = ?", (report.matches[0].remove_id,))
            conn.commit()
        return report

    monkeypatch.setattr(dedup_cmd_module, "find_cross_format_duplicates", _find_then_delete)
    logger = logging.getLogger("finance_cli.commands.dedup_cmd")
    logger.addHandler(caplog.handler)
    caplog.set_level(logging.WARNING)

    try:
        code = main(["dedup", "review-key-only"])
        payload = json.loads(capsys.readouterr().out)
    finally:
        logger.removeHandler(caplog.handler)

    assert code == 0
    assert payload["data"]["total_key_only"] == 1
    assert payload["data"]["skipped"] == 1
    assert payload["summary"]["skipped"] == 1
    assert payload["data"]["matches"] == []
    assert "Skipping key-only review match due to missing transaction enrichment" in caplog.text


def test_review_key_only_json_contract(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-12-21", amount_cents=-1999, description="DINNER", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-12-21", amount_cents=-1999, description="RESTAURANT", source="pdf_import")
        conn.commit()

    code = main(["dedup", "review-key-only"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["summary"] == {"total_key_only": 1, "skipped": 0}
    assert payload["data"]["total_key_only"] == 1
    assert payload["data"]["skipped"] == 0
    assert len(payload["data"]["matches"]) == 1
    match = payload["data"]["matches"][0]
    assert {"date", "amount_cents", "amount", "keep", "remove"} <= set(match.keys())
    assert isinstance(match["amount_cents"], int)
    assert isinstance(match["amount"], str)
    assert {"id", "source", "description", "institution", "account", "account_type"} <= set(match["keep"].keys())
    assert {"id", "source", "description", "institution", "account", "account_type"} <= set(match["remove"].keys())


def test_review_key_only_chunked_enrichment(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        for idx in range(260):
            amount = -50000 - idx
            _insert_txn(
                conn,
                txn_id=f"csv_chunk_{idx}",
                account_id="acct_a",
                date="2025-12-25",
                amount_cents=amount,
                description=f"CSV MATCH {idx}",
                source="csv_import",
            )
            _insert_txn(
                conn,
                txn_id=f"pdf_chunk_{idx}",
                account_id="acct_a",
                date="2025-12-25",
                amount_cents=amount,
                description=f"PDF MATCH {idx}",
                source="pdf_import",
            )
        conn.commit()

    code = main(["dedup", "review-key-only"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["total_key_only"] == 260
    assert len(payload["data"]["matches"]) == 260
    assert payload["data"]["skipped"] == 0
    assert any(item["remove"]["id"] == "pdf_chunk_259" for item in payload["data"]["matches"])


def test_review_key_only_inverted_date_range(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(conn, txn_id="csv_1", account_id="acct_a", date="2025-12-22", amount_cents=-2222, description="FOO", source="csv_import")
        _insert_txn(conn, txn_id="pdf_1", account_id="acct_a", date="2025-12-22", amount_cents=-2222, description="BAR", source="pdf_import")
        conn.commit()

    code = main(["dedup", "review-key-only", "--from", "2026-01-01", "--to", "2025-01-01"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["total_key_only"] == 0
    assert payload["data"]["matches"] == []
    assert payload["data"]["skipped"] == 0
    assert payload["summary"] == {"total_key_only": 0, "skipped": 0}


def test_create_alias_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_checking",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill_checking",
            institution_name="Merrill",
            account_type="checking",
            card_ending="6451",
            plaid_account_id="plaid_ext_merrill",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_bofa_checking", "--to", "plaid_merrill_checking"])
    payload = json.loads(capsys.readouterr().out)
    with connect(db_path) as conn:
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = 'hash_bofa_checking'"
        ).fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert alias is None


def test_create_alias_commit(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_checking",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill_checking",
            institution_name="Merrill",
            account_type="checking",
            card_ending="6451",
            plaid_account_id="plaid_ext_merrill",
        )
        conn.commit()

    code = main(
        ["dedup", "create-alias", "--from", "hash_bofa_checking", "--to", "plaid_merrill_checking", "--commit"]
    )
    payload = json.loads(capsys.readouterr().out)
    with connect(db_path) as conn:
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = 'hash_bofa_checking'"
        ).fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is False
    assert payload["data"]["no_op"] is False
    assert alias is not None
    assert alias["canonical_id"] == "plaid_merrill_checking"


def test_create_alias_commit_rewrites_transactions_and_subscriptions(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_checking",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill_checking",
            institution_name="Merrill",
            account_type="checking",
            card_ending="6451",
            plaid_account_id="plaid_ext_merrill",
        )
        _insert_txn(
            conn,
            txn_id="csv_hash_txn",
            account_id="hash_bofa_checking",
            date="2026-02-18",
            amount_cents=-1200,
            description="NETFLIX",
            source="csv_import",
        )
        conn.execute(
            """
            INSERT INTO subscriptions (
                id, vendor_name, category_id, amount_cents, frequency, next_expected, account_id, is_active, use_type, is_auto_detected
            ) VALUES
                ('sub_auto_hash', 'Netflix', NULL, 1200, 'monthly', '2026-03-18', 'hash_bofa_checking', 1, 'Personal', 1),
                ('sub_manual_canonical', 'Netflix', NULL, 1300, 'monthly', '2026-03-18', 'plaid_merrill_checking', 1, 'Personal', 0),
                ('sub_auto_hash_hulu', 'Hulu', NULL, 900, 'monthly', '2026-03-18', 'hash_bofa_checking', 1, 'Personal', 1)
            """
        )
        conn.commit()

    code = main(
        ["dedup", "create-alias", "--from", "hash_bofa_checking", "--to", "plaid_merrill_checking", "--commit"]
    )
    payload = json.loads(capsys.readouterr().out)

    with connect(db_path) as conn:
        txn_accounts = conn.execute(
            "SELECT account_id FROM transactions WHERE id = 'csv_hash_txn'"
        ).fetchone()
        netflix_subs = conn.execute(
            """
            SELECT id, is_auto_detected, account_id
              FROM subscriptions
             WHERE vendor_name = 'Netflix' AND frequency = 'monthly'
             ORDER BY id
            """
        ).fetchall()
        hulu_sub = conn.execute(
            """
            SELECT account_id
              FROM subscriptions
             WHERE id = 'sub_auto_hash_hulu'
            """
        ).fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert txn_accounts["account_id"] == "plaid_merrill_checking"
    assert len(netflix_subs) == 1
    assert netflix_subs[0]["id"] == "sub_manual_canonical"
    assert netflix_subs[0]["is_auto_detected"] == 0
    assert netflix_subs[0]["account_id"] == "plaid_merrill_checking"
    assert hulu_sub["account_id"] == "plaid_merrill_checking"


def test_create_alias_replaces_existing(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_checking",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_old",
            institution_name="Merrill",
            account_type="checking",
            card_ending="1111",
            plaid_account_id="plaid_old_ext",
        )
        _insert_named_account(
            conn,
            account_id="plaid_new",
            institution_name="Merrill",
            account_type="checking",
            card_ending="2222",
            plaid_account_id="plaid_new_ext",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES ('hash_bofa_checking', 'plaid_old')"
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_bofa_checking", "--to", "plaid_new", "--commit"])
    payload = json.loads(capsys.readouterr().out)
    with connect(db_path) as conn:
        alias = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = 'hash_bofa_checking'"
        ).fetchone()

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["replaced_alias"]["canonical_id"] == "plaid_old"
    assert alias is not None
    assert alias["canonical_id"] == "plaid_new"


def test_create_alias_noop_same_target(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_checking",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_target",
            institution_name="Merrill",
            account_type="checking",
            card_ending="6451",
            plaid_account_id="plaid_target_ext",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES ('hash_bofa_checking', 'plaid_target')"
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_bofa_checking", "--to", "plaid_target"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["no_op"] is True


def test_create_alias_rejects_self_link(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_same",
            institution_name="Bank of America",
            account_type="checking",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_same", "--to", "hash_same"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "--from and --to must be different" in payload["error"]


def test_create_alias_rejects_nonexistent_from(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="plaid_target",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_target_ext",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "missing_hash", "--to", "plaid_target"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "Source account 'missing_hash' not found" in payload["error"]


def test_create_alias_rejects_nonexistent_to(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_source",
            institution_name="Bank of America",
            account_type="checking",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_source", "--to", "missing_plaid"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "Target account 'missing_plaid' not found" in payload["error"]


def test_create_alias_rejects_plaid_as_from(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="plaid_from",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_from_ext",
        )
        _insert_named_account(
            conn,
            account_id="plaid_to",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_to_ext",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "plaid_from", "--to", "plaid_to"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "must be a hash account" in payload["error"]


def test_create_alias_rejects_hash_as_to(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_from",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="hash_to",
            institution_name="Bank of America",
            account_type="checking",
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_from", "--to", "hash_to"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "must be a Plaid account" in payload["error"]


def test_create_alias_rejects_inactive_to(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_from",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_inactive",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_inactive_ext",
            is_active=0,
        )
        conn.commit()

    code = main(["dedup", "create-alias", "--from", "hash_from", "--to", "plaid_inactive"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "error"
    assert "is inactive" in payload["error"]


def test_suggest_aliases_finds_equivalent_institution(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill",
            institution_name="Merrill",
            account_type="checking",
            card_ending="6451",
            plaid_account_id="plaid_merrill_ext",
        )
        _insert_txn(
            conn,
            txn_id="txn_hash",
            account_id="hash_bofa",
            date="2026-01-10",
            amount_cents=-500,
            description="Checking txn",
            source="csv_import",
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    suggestions = payload["data"]["suggestions"]
    assert len(suggestions) == 1
    assert suggestions[0]["hash_account_id"] == "hash_bofa"
    assert suggestions[0]["candidates"][0]["plaid_account_id"] == "plaid_merrill"
    assert "equivalent_institution_same_type" in suggestions[0]["candidates"][0]["reasons"]


def test_suggest_aliases_skips_hash_with_card_ending_for_cross_institution(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa_card",
            institution_name="Bank of America",
            account_type="checking",
            card_ending="0001",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill",
            institution_name="Merrill",
            account_type="checking",
            card_ending="9999",
            plaid_account_id="plaid_merrill_ext",
        )
        _insert_txn(
            conn,
            txn_id="txn_hash",
            account_id="hash_bofa_card",
            date="2026-01-10",
            amount_cents=-500,
            description="Checking txn",
            source="csv_import",
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["suggestions"] == []


def test_suggest_aliases_skips_already_aliased(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_merrill_ext",
        )
        conn.execute(
            "INSERT INTO account_aliases (hash_account_id, canonical_id) VALUES ('hash_bofa', 'plaid_merrill')"
        )
        _insert_txn(
            conn,
            txn_id="txn_hash",
            account_id="hash_bofa",
            date="2026-01-10",
            amount_cents=-500,
            description="Checking txn",
            source="csv_import",
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["suggestions"] == []


def test_suggest_aliases_empty_when_all_matched(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="plaid_only",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_only_ext",
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["suggestions"] == []


def test_suggest_aliases_includes_txn_counts(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_bofa",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_merrill_ext",
        )
        _insert_txn(
            conn,
            txn_id="hash_active",
            account_id="hash_bofa",
            date="2026-01-10",
            amount_cents=-500,
            description="Active hash txn",
            source="csv_import",
            is_active=1,
        )
        _insert_txn(
            conn,
            txn_id="hash_inactive",
            account_id="hash_bofa",
            date="2026-01-11",
            amount_cents=-500,
            description="Inactive hash txn",
            source="csv_import",
            is_active=0,
        )
        _insert_txn(
            conn,
            txn_id="plaid_active_1",
            account_id="plaid_merrill",
            date="2026-01-10",
            amount_cents=-500,
            description="Active plaid txn 1",
            source="plaid",
            is_active=1,
        )
        _insert_txn(
            conn,
            txn_id="plaid_active_2",
            account_id="plaid_merrill",
            date="2026-01-11",
            amount_cents=-500,
            description="Active plaid txn 2",
            source="plaid",
            is_active=1,
        )
        _insert_txn(
            conn,
            txn_id="plaid_inactive",
            account_id="plaid_merrill",
            date="2026-01-12",
            amount_cents=-500,
            description="Inactive plaid txn",
            source="plaid",
            is_active=0,
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    suggestion = payload["data"]["suggestions"][0]
    assert suggestion["txn_count"] == 1
    assert suggestion["candidates"][0]["txn_count"] == 2


def test_suggest_aliases_deterministic_ordering(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_b",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="hash_a",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_2",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_2_ext",
        )
        _insert_named_account(
            conn,
            account_id="plaid_1",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_1_ext",
        )
        _insert_txn(
            conn,
            txn_id="txn_hash_a",
            account_id="hash_a",
            date="2026-01-10",
            amount_cents=-500,
            description="Hash A",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="txn_hash_b",
            account_id="hash_b",
            date="2026-01-11",
            amount_cents=-500,
            description="Hash B",
            source="csv_import",
        )
        conn.commit()

    first_code = main(["dedup", "suggest-aliases"])
    first_payload = json.loads(capsys.readouterr().out)
    second_code = main(["dedup", "suggest-aliases"])
    second_payload = json.loads(capsys.readouterr().out)

    assert first_code == 0
    assert second_code == 0
    assert [item["hash_account_id"] for item in first_payload["data"]["suggestions"]] == ["hash_a", "hash_b"]
    assert [item["hash_account_id"] for item in second_payload["data"]["suggestions"]] == ["hash_a", "hash_b"]
    assert [c["plaid_account_id"] for c in first_payload["data"]["suggestions"][0]["candidates"]] == ["plaid_1", "plaid_2"]


def test_suggest_aliases_filters_accounts_without_active_transactions(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_inactive_only",
            institution_name="Bank of America",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_merrill",
            institution_name="Merrill",
            account_type="checking",
            plaid_account_id="plaid_merrill_ext",
        )
        _insert_txn(
            conn,
            txn_id="txn_hash_inactive",
            account_id="hash_inactive_only",
            date="2026-01-10",
            amount_cents=-500,
            description="Inactive only",
            source="csv_import",
            is_active=0,
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["data"]["scanned_hash_accounts"] == 0
    assert payload["data"]["suggestions"] == []


def test_suggest_aliases_income_account_no_candidates(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="hash_income",
            institution_name="Payroll Provider",
            account_type="checking",
        )
        _insert_named_account(
            conn,
            account_id="plaid_chase",
            institution_name="Chase",
            account_type="checking",
            plaid_account_id="plaid_chase_ext",
        )
        _insert_txn(
            conn,
            txn_id="income_txn",
            account_id="hash_income",
            date="2026-01-10",
            amount_cents=500000,
            description="PAYROLL DEPOSIT",
            source="csv_import",
        )
        conn.commit()

    code = main(["dedup", "suggest-aliases"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["scanned_hash_accounts"] == 1
    assert payload["data"]["suggestions"] == []


def test_detect_equivalences_finds_overlap(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="acct_citi",
            institution_name="Citibank",
            account_type="credit_card",
            card_ending="1234",
        )
        _insert_named_account(
            conn,
            account_id="acct_bloom",
            institution_name="Bloomingdale's",
            account_type="credit_card",
            card_ending="1234",
        )
        _insert_txn(
            conn,
            txn_id="citi_txn_1",
            account_id="acct_citi",
            date="2026-01-10",
            amount_cents=-500,
            description="Overlap 1",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="bloom_txn_1",
            account_id="acct_bloom",
            date="2026-01-10",
            amount_cents=-500,
            description="Overlap 1",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="citi_txn_2",
            account_id="acct_citi",
            date="2026-01-11",
            amount_cents=-700,
            description="Overlap 2",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="bloom_txn_2",
            account_id="acct_bloom",
            date="2026-01-11",
            amount_cents=-700,
            description="Overlap 2",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="citi_txn_3",
            account_id="acct_citi",
            date="2026-01-12",
            amount_cents=-900,
            description="Overlap 3",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="bloom_txn_3",
            account_id="acct_bloom",
            date="2026-01-12",
            amount_cents=-900,
            description="Overlap 3",
            source="plaid",
        )
        conn.commit()

    code = main(["dedup", "detect-equivalences"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["command"] == "dedup.detect-equivalences"
    assert payload["summary"]["candidate_count"] == 1
    candidate = payload["data"]["candidates"][0]
    assert {candidate["institution_a"], candidate["institution_b"]} == {"Citi", "Bloomingdale's"}
    assert candidate["card_ending"] == "1234"
    assert candidate["overlap_count"] == 3


def test_detect_equivalences_skips_existing(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path)
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    with connect(db_path) as conn:
        _insert_named_account(
            conn,
            account_id="acct_bofa",
            institution_name="Bank of America",
            account_type="credit_card",
            card_ending="8894",
        )
        _insert_named_account(
            conn,
            account_id="acct_merrill",
            institution_name="Merrill",
            account_type="credit_card",
            card_ending="8894",
        )
        _insert_txn(
            conn,
            txn_id="bofa_txn_1",
            account_id="acct_bofa",
            date="2026-01-10",
            amount_cents=-100,
            description="Overlap 1",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="merrill_txn_1",
            account_id="acct_merrill",
            date="2026-01-10",
            amount_cents=-100,
            description="Overlap 1",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="bofa_txn_2",
            account_id="acct_bofa",
            date="2026-01-11",
            amount_cents=-200,
            description="Overlap 2",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="merrill_txn_2",
            account_id="acct_merrill",
            date="2026-01-11",
            amount_cents=-200,
            description="Overlap 2",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="bofa_txn_3",
            account_id="acct_bofa",
            date="2026-01-12",
            amount_cents=-300,
            description="Overlap 3",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="merrill_txn_3",
            account_id="acct_merrill",
            date="2026-01-12",
            amount_cents=-300,
            description="Overlap 3",
            source="plaid",
        )
        conn.commit()

    code = main(["dedup", "detect-equivalences"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "success"
    assert payload["summary"]["candidate_count"] == 0
    assert payload["data"]["candidates"] == []


def test_fuzzy_date_plaid_pdf_substring_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_regus",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-71300,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_regus",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-71300,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "plaid_regus"
    assert report.matches[0].remove_id == "pdf_regus"
    assert report.matches[0].match_type == "substring"


def test_fuzzy_date_exact_description(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_exact",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-9123,
            description="ALASKA AIRLINES TICKET",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_exact",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-9123,
            description="ALASKA AIRLINES TICKET",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].match_type == "exact"
    assert report.matches[0].keep_id == "plaid_exact"
    assert report.matches[0].remove_id == "pdf_exact"


def test_fuzzy_date_no_match_different_description(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_a",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-5000,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_b",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-5000,
            description="WHOLE FOODS MARKET",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_fuzzy_date_no_match_2_day_gap(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_far",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-5000,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_far",
            account_id="acct_a",
            date="2025-01-18",
            amount_cents=-5000,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_fuzzy_date_same_source_no_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_1",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-3300,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="plaid_2",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-3300,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="plaid",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_fuzzy_date_coexists_with_exact(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="csv_exact",
            account_id="acct_a",
            date="2025-01-15",
            amount_cents=-1000,
            description="NETFLIX SUBSCRIPTION",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="pdf_exact",
            account_id="acct_a",
            date="2025-01-15",
            amount_cents=-1000,
            description="NETFLIX SUBSCRIPTION",
            source="pdf_import",
        )
        _insert_txn(
            conn,
            txn_id="plaid_fuzzy",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-71300,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_fuzzy",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-71300,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 2
    pairs = {(match.keep_id, match.remove_id) for match in report.matches}
    assert pairs == {("csv_exact", "pdf_exact"), ("plaid_fuzzy", "pdf_fuzzy")}


def test_fuzzy_date_short_description_no_match(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_short",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-4100,
            description="REGUS",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_short",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-4100,
            description="REGUS TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_fuzzy_date_non_iso_date_no_crash(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_bad_date",
            account_id="acct_a",
            date="not-a-date",
            amount_cents=-5000,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_good_date",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-5000,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 0


def test_fuzzy_date_with_date_range_filter(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="plaid_boundary",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-4500,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_boundary",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-4500,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(
            conn,
            date_from="2025-01-19",
            date_to="2025-01-20",
        )

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "plaid_boundary"
    assert report.matches[0].remove_id == "pdf_boundary"


def test_fuzzy_date_short_desc_in_merged_group_skipped(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="a_plaid_short",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-6000,
            description="REGUS",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="b_plaid_long",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-6000,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_long",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-6000,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 1
    assert report.matches[0].keep_id == "b_plaid_long"
    assert report.matches[0].remove_id == "pdf_long"


def test_fuzzy_date_no_contradictory_keep_remove(tmp_path: Path) -> None:
    db_path = _setup_db(tmp_path)
    with connect(db_path) as conn:
        _insert_account(conn, "acct_a")
        _insert_txn(
            conn,
            txn_id="csv_exact",
            account_id="acct_a",
            date="2025-01-10",
            amount_cents=-1000,
            description="NETFLIX SUBSCRIPTION",
            source="csv_import",
        )
        _insert_txn(
            conn,
            txn_id="pdf_exact",
            account_id="acct_a",
            date="2025-01-10",
            amount_cents=-1000,
            description="NETFLIX SUBSCRIPTION",
            source="pdf_import",
        )
        _insert_txn(
            conn,
            txn_id="a_plaid_short",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-6000,
            description="REGUS",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="b_plaid_long",
            account_id="acct_a",
            date="2025-01-20",
            amount_cents=-6000,
            description="REGUS MANAGEMENT GROUP",
            source="plaid",
        )
        _insert_txn(
            conn,
            txn_id="pdf_long",
            account_id="acct_a",
            date="2025-01-19",
            amount_cents=-6000,
            description="REGUS MANAGEMENT GROUP BCIWGPLC TX",
            source="pdf_import",
        )

        report = find_cross_format_duplicates(conn)

    assert len(report.matches) == 2
    keep_ids = {match.keep_id for match in report.matches}
    remove_ids = {match.remove_id for match in report.matches}
    assert keep_ids.isdisjoint(remove_ids)
