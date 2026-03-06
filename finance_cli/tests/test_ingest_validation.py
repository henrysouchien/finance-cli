from __future__ import annotations

from copy import deepcopy

from finance_cli.importers.pdf import ExtractResult
from finance_cli.ingest_validation import validate_ai_parse, validate_extract_result


def _valid_payload() -> dict[str, object]:
    return {
        "statement": {
            "institution": "Citi",
            "account_label": "Card",
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


def _has_error(report, *, gate: str, field: str | None = None) -> bool:
    return any(item.gate == gate and (field is None or item.field == field) for item in report.errors)


def _has_warning(report, *, gate: str, field: str | None = None) -> bool:
    return any(item.gate == gate and (field is None or item.field == field) for item in report.warnings)


def test_valid_minimal_json_passes() -> None:
    report = validate_ai_parse(_valid_payload())
    assert report.passed is True
    assert report.reconcile_status == "no_totals"
    assert report.errors == []
    assert report.blocked_row_indices == []


def test_missing_transactions_key_fails() -> None:
    payload = _valid_payload()
    payload.pop("transactions")
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="schema", field="transactions")


def test_expected_transaction_count_missing_warns() -> None:
    payload = _valid_payload()
    payload["extraction_meta"].pop("expected_transaction_count")
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="schema", field="extraction_meta.expected_transaction_count")


def test_expected_transaction_count_invalid_warns() -> None:
    payload = _valid_payload()
    payload["extraction_meta"]["expected_transaction_count"] = -1
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="schema", field="extraction_meta.expected_transaction_count")


def test_transactions_not_array_fails() -> None:
    payload = _valid_payload()
    payload["transactions"] = {}
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="schema", field="transactions")


def test_transaction_missing_required_field_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0].pop("description")
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="schema", field="description")


def test_empty_transactions_array_warns() -> None:
    payload = _valid_payload()
    payload["transactions"] = []
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="schema", field="transactions")


def test_non_usd_currency_fails() -> None:
    payload = _valid_payload()
    payload["statement"]["currency"] = "EUR"
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="schema", field="statement.currency")


def test_invalid_date_format_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["date"] = "01/15/2025"
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="field", field="date")


def test_non_numeric_amount_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["amount"] = "x"
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="field", field="amount")


def test_empty_description_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["description"] = "    "
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="field", field="description")


def test_zero_amount_warns() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["amount"] = 0
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="field", field="amount")


def test_null_confidence_warns() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = None
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="confidence", field="confidence")
    assert 0 in report.blocked_row_indices


def test_nan_infinity_amount_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["amount"] = float("inf")
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="field", field="amount")


def test_boolean_amount_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["amount"] = True
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="field", field="amount")


def test_date_outside_statement_period_warns() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["date"] = "2025-05-01"
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="semantic", field="date")


def test_large_amount_warns() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["amount"] = -60_000.0
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="semantic", field="amount")


def test_duplicate_rows_flagged() -> None:
    payload = _valid_payload()
    payload["transactions"].append(deepcopy(payload["transactions"][0]))
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="semantic")


def test_reconciliation_always_no_totals() -> None:
    report = validate_ai_parse(_valid_payload())
    assert report.reconcile_status == "no_totals"
    assert report.total_charges_cents is None
    assert report.total_payments_cents is None
    assert report.extracted_total_cents == -3000


def test_low_confidence_warns() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = 0.75
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert _has_warning(report, gate="confidence")
    assert report.blocked_row_indices == []


def test_very_low_confidence_blocks() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = 0.55
    report = validate_ai_parse(payload)
    assert report.passed is True
    assert 0 in report.blocked_row_indices


def test_all_rows_blocked_is_error() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = 0.1
    payload["transactions"][1]["confidence"] = 0.2
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="confidence")


def test_confidence_non_numeric_string_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = "high"
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="confidence", field="confidence")


def test_confidence_out_of_range_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = 1.5
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="confidence", field="confidence")


def test_confidence_boolean_fails() -> None:
    payload = _valid_payload()
    payload["transactions"][0]["confidence"] = True
    report = validate_ai_parse(payload)
    assert report.passed is False
    assert _has_error(report, gate="confidence", field="confidence")


def test_fully_valid_passes_all_gates() -> None:
    report = validate_ai_parse(_valid_payload())
    assert report.passed is True
    assert report.errors == []
    assert report.reconcile_status == "no_totals"
    assert report.low_confidence_count == 0


def test_validate_extract_result_errors_and_warnings() -> None:
    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-01",
                "description": "",
                "amount_cents": 0,
                "source": "",
            },
            {
                "date": "01/02/2025",
                "description": "Coffee",
                "amount_cents": "bad",
                "source": "Citi",
            },
        ],
        statement_total_cents=None,
        extracted_total_cents=0,
        reconciled=False,
        warnings=[],
    )

    errors, warnings = validate_extract_result(extracted)
    assert any("invalid or missing date" in error for error in errors)
    assert any("expected int" in error for error in errors)
    assert any("empty description" in warning for warning in warnings)
    assert any("empty source/institution" in warning for warning in warnings)
    assert any("zero amount" in warning for warning in warnings)


def test_validate_extract_result_empty_list_is_error() -> None:
    extracted = ExtractResult(
        transactions=[],
        statement_total_cents=None,
        extracted_total_cents=0,
        reconciled=False,
        warnings=[],
    )
    errors, warnings = validate_extract_result(extracted)
    assert errors == ["No transactions extracted"]
    assert warnings == []
