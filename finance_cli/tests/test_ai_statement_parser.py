from __future__ import annotations

import json
from pathlib import Path

import pytest

from finance_cli.ai_statement_parser import (
    AIParseResult,
    _build_parse_prompt,
    _extract_json_object,
    ai_parse_statement,
    ai_result_to_extract_result,
)
from finance_cli.db import connect, initialize_database
from finance_cli.importers.pdf import ExtractResult, import_extracted_statement
from finance_cli.ingest_validation import validate_ai_parse


def _valid_parsed_payload() -> dict[str, object]:
    return {
        "statement": {
            "institution": "Citi",
            "account_label": "Card",
            "account_type": "credit_card",
            "statement_period_start": "2025-01-01",
            "statement_period_end": "2025-01-31",
            "new_balance": 130.00,
            "currency": "USD",
        },
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE SHOP",
                "amount": -10.00,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            },
            {
                "date": "2025-01-05",
                "description": "BOOK STORE",
                "amount": -20.00,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.92,
                "evidence": None,
            },
        ],
        "extraction_meta": {
            "model": "fake-model",
            "prompt_version": "v1",
            "notes": None,
            "expected_transaction_count": 2,
        },
    }


def _make_result(parsed: dict[str, object]) -> AIParseResult:
    return AIParseResult(
        raw_json=json.dumps(parsed),
        parsed=parsed,
        validation=validate_ai_parse(parsed),
        provider="claude",
        model="claude-test",
        prompt_version="v1",
        prompt_hash="a" * 64,
    )


def _sample_extract_result() -> ExtractResult:
    return ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "A", "amount_cents": -1000, "source": "AI"},
            {"date": "2025-01-02", "description": "B", "amount_cents": -2000, "source": "AI"},
        ],
        extracted_total_cents=-3000,
        reconciled=False,
        warnings=[],
    )


def test_build_prompt_includes_json_schema() -> None:
    system_prompt, user_prompt, prompt_hash = _build_parse_prompt("hello")
    assert "strict JSON only" in system_prompt
    assert '"statement"' in system_prompt
    assert '"transactions"' in system_prompt
    assert '"expected_transaction_count"' in system_prompt
    assert "negative=expense/outflow" in system_prompt
    assert '"new_balance"' in system_prompt
    assert '"apr_purchase"' in system_prompt
    assert '"apr_balance_transfer"' in system_prompt
    assert '"apr_cash_advance"' in system_prompt
    assert '"statement_total"' not in system_prompt
    assert '"total_charges"' not in system_prompt
    assert '"total_payments"' not in system_prompt
    assert "APR fields are credit-card only" in system_prompt
    assert "Do NOT stop after the first page of transactions" in system_prompt
    assert "confidence must be a numeric score in [0.0, 1.0]" in system_prompt
    assert "Do not assign 0.0 to all rows by default." in system_prompt
    assert user_prompt.startswith("Extract all transactions from this statement:")
    assert len(prompt_hash) == 64


def test_prompt_hash_deterministic() -> None:
    one = _build_parse_prompt("same")
    two = _build_parse_prompt("same")
    assert one[2] == two[2]


def test_extract_json_object_clean() -> None:
    payload = _extract_json_object('{"statement": {}, "transactions": []}')
    assert isinstance(payload, dict)
    assert "transactions" in payload


def test_extract_json_object_markdown_fences() -> None:
    payload = _extract_json_object("```json\n{\"statement\": {}, \"transactions\": []}\n```")
    assert payload["statement"] == {}


def test_extract_json_object_invalid_raises() -> None:
    with pytest.raises(ValueError):
        _extract_json_object("not json")


def test_parse_success(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    parsed = _valid_parsed_payload()

    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "statement text")
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", lambda *_: json.dumps(parsed))

    result = ai_parse_statement(pdf_path, provider="openai", model="gpt-test")
    assert result.provider == "openai"
    assert result.model == "gpt-test"
    assert result.validation.passed is True


def test_parse_normalizes_mmddyyyy_dates_before_validation(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    parsed = _valid_parsed_payload()
    parsed["statement"]["statement_period_start"] = "01/01/2025"
    parsed["statement"]["statement_period_end"] = "01/31/2025"
    parsed["transactions"][0]["date"] = "01/03/2025"
    parsed["transactions"][1]["date"] = "01/05/2025"

    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "statement text")
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", lambda *_: json.dumps(parsed))

    result = ai_parse_statement(pdf_path, provider="openai", model="gpt-test")
    assert result.validation.passed is True
    assert result.parsed["statement"]["statement_period_start"] == "2025-01-01"
    assert result.parsed["statement"]["statement_period_end"] == "2025-01-31"
    assert result.parsed["transactions"][0]["date"] == "2025-01-03"
    assert result.parsed["transactions"][1]["date"] == "2025-01-05"


def test_parse_retry_on_malformed(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    parsed = _valid_parsed_payload()
    calls = {"count": 0}

    def _fake_send(*_args):
        calls["count"] += 1
        if calls["count"] == 1:
            return "{bad json"
        return json.dumps(parsed)

    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "statement text")
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", _fake_send)

    result = ai_parse_statement(pdf_path, provider="openai")
    assert result.validation.passed is True
    assert calls["count"] == 2


def test_parse_both_retries_fail(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "statement text")
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", lambda *_: "bad")
    with pytest.raises(ValueError):
        ai_parse_statement(pdf_path, provider="openai")


def test_text_too_long_raises(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "x" * 20)
    with pytest.raises(ValueError):
        ai_parse_statement(pdf_path, provider="openai", max_text_chars=10)


def test_parse_requires_provider(tmp_path: Path) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    with pytest.raises(ValueError, match="AI provider is required"):
        ai_parse_statement(pdf_path)


def test_parse_accumulates_token_usage_across_retries(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")
    parsed = _valid_parsed_payload()
    calls = {"count": 0}

    def _fake_send(*_args):
        calls["count"] += 1
        if calls["count"] == 1:
            return "{bad json", {"input_tokens": 10, "output_tokens": 2}
        return json.dumps(parsed), {"input_tokens": 20, "output_tokens": 4}

    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", lambda *_: "statement text")
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", _fake_send)

    result = ai_parse_statement(pdf_path, provider="openai")
    assert result.validation.passed is True
    assert result.input_tokens == 30
    assert result.output_tokens == 6
    assert result.elapsed_ms >= 0


def test_ai_result_to_extract_result_amounts() -> None:
    parsed = _valid_parsed_payload()
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.extracted_total_cents == -3000
    assert extracted.transactions[0]["amount_cents"] == -1000


def test_ai_result_to_extract_result_no_totals() -> None:
    parsed = _valid_parsed_payload()
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.reconciled is False
    assert extracted.total_charges_cents is None
    assert extracted.total_payments_cents is None


def test_ai_result_to_extract_result_extracts_apr_fields() -> None:
    parsed = _valid_parsed_payload()
    parsed["statement"]["apr_purchase"] = 24.99
    parsed["statement"]["apr_balance_transfer"] = 19.99
    parsed["statement"]["apr_cash_advance"] = 29.99
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.apr_purchase == 24.99
    assert extracted.apr_balance_transfer == 19.99
    assert extracted.apr_cash_advance == 29.99


def test_ai_result_to_extract_result_rejects_invalid_apr_values() -> None:
    parsed = _valid_parsed_payload()
    parsed["statement"]["apr_purchase"] = -1
    parsed["statement"]["apr_balance_transfer"] = 120
    parsed["statement"]["apr_cash_advance"] = "bad"
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.apr_purchase is None
    assert extracted.apr_balance_transfer is None
    assert extracted.apr_cash_advance is None


def test_ai_result_to_extract_result_preserves_zero_apr() -> None:
    parsed = _valid_parsed_payload()
    parsed["statement"]["apr_purchase"] = 0.0
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.apr_purchase == 0.0


def test_ai_result_to_extract_result_always_no_totals() -> None:
    """AI path ignores statement_total even if present in response."""
    parsed = _valid_parsed_payload()
    parsed["statement"]["statement_total"] = -30.00
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result)
    assert extracted.statement_total_cents is None
    assert extracted.extracted_total_cents == -3000
    assert extracted.reconciled is False


def test_ai_result_to_extract_result_require_reconciled_raises_on_no_totals() -> None:
    parsed = _valid_parsed_payload()
    result = _make_result(parsed)
    with pytest.raises(ValueError, match="require_reconciled=True"):
        ai_result_to_extract_result(result, require_reconciled=True)


def test_ai_result_to_extract_result_blocks_on_failed_validation() -> None:
    parsed = _valid_parsed_payload()
    parsed["transactions"][0]["amount"] = "bad"
    result = _make_result(parsed)
    with pytest.raises(ValueError):
        ai_result_to_extract_result(result)


def test_ai_result_to_extract_result_blocks_partial_by_default() -> None:
    parsed = _valid_parsed_payload()
    parsed["transactions"][0]["confidence"] = 0.55
    result = _make_result(parsed)
    with pytest.raises(ValueError):
        ai_result_to_extract_result(result)


def test_ai_result_to_extract_result_skips_blocked_rows_with_allow_partial() -> None:
    parsed = _valid_parsed_payload()
    parsed["transactions"][0]["confidence"] = 0.55
    result = _make_result(parsed)
    extracted = ai_result_to_extract_result(result, allow_partial=True)
    assert len(extracted.transactions) == 1
    assert extracted.extracted_total_cents == -2000




def test_import_extracted_statement_writes_ai_audit_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    with connect(db_path) as conn:
        import_extracted_statement(
            conn,
            extracted=_sample_extract_result(),
            file_path=pdf_path,
            bank_parser="ai:claude",
            validate_name=False,
            ai_raw_output_json='{"ok":true}',
            ai_validation_json='{"passed":true}',
            ai_model="claude-test",
            ai_prompt_version="v1",
            ai_prompt_hash="b" * 64,
        )
        row = conn.execute(
            """
            SELECT ai_raw_output_json, ai_validation_json, ai_model, ai_prompt_version, ai_prompt_hash
              FROM import_batches
             ORDER BY created_at DESC
             LIMIT 1
            """
        ).fetchone()

    assert row["ai_raw_output_json"] == '{"ok":true}'
    assert row["ai_validation_json"] == '{"passed":true}'
    assert row["ai_model"] == "claude-test"
    assert row["ai_prompt_version"] == "v1"
    assert row["ai_prompt_hash"] == "b" * 64


def test_import_extracted_statement_idempotent_by_file_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    with connect(db_path) as conn:
        first = import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)
        second = import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]

    assert first["already_imported"] is False
    assert second["already_imported"] is True
    assert txn_count == 2


def test_import_extracted_statement_allows_same_file_hash_across_backends(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    with connect(db_path) as conn:
        first = import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)
        second = import_extracted_statement(
            conn,
            _sample_extract_result(),
            pdf_path,
            "azure:prebuilt-bankStatement.us",
            validate_name=False,
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["already_imported"] is False
    assert second["already_imported"] is False
    assert txn_count == 4
    assert batch_count == 2


def test_import_extracted_statement_replace_existing_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    replacement = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "C", "amount_cents": -500, "source": "AI"},
        ],
        statement_total_cents=-500,
        extracted_total_cents=-500,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        first = import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)
        second = import_extracted_statement(
            conn,
            replacement,
            pdf_path,
            "ai:claude",
            replace_existing_hash=True,
            validate_name=False,
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["inserted"] == 2
    assert second["inserted"] == 1
    assert txn_count == 1
    assert batch_count == 1


def test_import_extracted_statement_replace_scoped_to_backend(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    replacement = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "C", "amount_cents": -500, "source": "AI"},
        ],
        statement_total_cents=-500,
        extracted_total_cents=-500,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)
        import_extracted_statement(
            conn,
            _sample_extract_result(),
            pdf_path,
            "azure:prebuilt-bankStatement.us",
            validate_name=False,
        )
        replaced = import_extracted_statement(
            conn,
            replacement,
            pdf_path,
            "ai:claude",
            replace_existing_hash=True,
            validate_name=False,
        )
        ai_txn_count = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE dedupe_key LIKE 'pdf:ai:%'"
        ).fetchone()["n"]
        azure_txn_count = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE dedupe_key LIKE 'pdf:azure:%'"
        ).fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert replaced["inserted"] == 1
    assert ai_txn_count == 1
    assert azure_txn_count == 2
    assert batch_count == 2


def test_import_extracted_statement_dedupes_by_content_hash_across_files(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    first_pdf = tmp_path / "stmt-a.pdf"
    second_pdf = tmp_path / "stmt-b.pdf"
    first_pdf.write_bytes(b"file-a")
    second_pdf.write_bytes(b"file-b")

    with connect(db_path) as conn:
        first = import_extracted_statement(
            conn,
            _sample_extract_result(),
            first_pdf,
            "ai:claude",
            validate_name=False,
            content_text="same statement text",
        )
        second = import_extracted_statement(
            conn,
            _sample_extract_result(),
            second_pdf,
            "ai:claude",
            validate_name=False,
            content_text="same statement text",
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["already_imported"] is False
    assert second["already_imported"] is True
    assert txn_count == 2
    assert batch_count == 1


def test_import_extracted_statement_content_hash_is_backend_scoped(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    first_pdf = tmp_path / "stmt-a.pdf"
    second_pdf = tmp_path / "stmt-b.pdf"
    first_pdf.write_bytes(b"file-a")
    second_pdf.write_bytes(b"file-b")

    with connect(db_path) as conn:
        first = import_extracted_statement(
            conn,
            _sample_extract_result(),
            first_pdf,
            "ai:claude",
            validate_name=False,
            content_text="same statement text",
        )
        second = import_extracted_statement(
            conn,
            _sample_extract_result(),
            second_pdf,
            "azure:prebuilt-bankStatement.us",
            validate_name=False,
            content_text="same statement text",
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["already_imported"] is False
    assert second["already_imported"] is False
    assert txn_count == 4
    assert batch_count == 2


def test_import_extracted_statement_replace_blocks_different_file_same_content(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    first_pdf = tmp_path / "stmt-a.pdf"
    second_pdf = tmp_path / "stmt-b.pdf"
    first_pdf.write_bytes(b"file-a")
    second_pdf.write_bytes(b"file-b")

    replacement = ExtractResult(
        transactions=[
            {"date": "2025-01-01", "description": "C", "amount_cents": -500, "source": "AI"},
        ],
        statement_total_cents=-500,
        extracted_total_cents=-500,
        reconciled=True,
        warnings=[],
    )

    with connect(db_path) as conn:
        first = import_extracted_statement(
            conn,
            _sample_extract_result(),
            first_pdf,
            "ai:claude",
            validate_name=False,
            content_text="same statement text",
        )
        second = import_extracted_statement(
            conn,
            replacement,
            second_pdf,
            "ai:claude",
            replace_existing_hash=True,
            validate_name=False,
            content_text="same statement text",
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert first["already_imported"] is False
    assert second["already_imported"] is True
    assert second["inserted"] == 0
    assert txn_count == 2
    assert batch_count == 1


def test_import_extracted_statement_dry_run(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    with connect(db_path) as conn:
        result = import_extracted_statement(
            conn,
            _sample_extract_result(),
            pdf_path,
            "ai:claude",
            dry_run=True,
            validate_name=False,
        )
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]

    assert result["already_imported"] is False
    assert result["inserted"] == 2
    assert txn_count == 0
    assert batch_count == 0


def test_import_extracted_statement_return_shape(tmp_path: Path) -> None:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"same")

    with connect(db_path) as conn:
        result = import_extracted_statement(conn, _sample_extract_result(), pdf_path, "ai:claude", validate_name=False)

    assert set(result.keys()) == {
        "file",
        "bank",
        "already_imported",
        "inserted",
        "skipped_duplicates",
        "extracted_count",
        "statement_total_cents",
        "extracted_total_cents",
        "total_charges_cents",
        "total_payments_cents",
        "new_balance_cents",
        "reconciled",
        "reconcile_status",
        "warnings",
        "file_hash",
    }
