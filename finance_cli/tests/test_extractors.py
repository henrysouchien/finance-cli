from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from finance_cli.ai_statement_parser import AIParseResult
from finance_cli.extractors import ExtractOptions, get_extractor, normalize_date, parse_amount_to_cents
from finance_cli.extractors.ai_extractor import AIExtractor, _reconcile_status_from_extract as _ai_reconcile_status
from finance_cli.extractors.azure_extractor import AzureExtractor, _reconcile_status_from_extract as _azure_reconcile_status
from finance_cli.extractors.bsc_extractor import BSCExtractor
from finance_cli.importers.pdf import ExtractResult
from finance_cli.ingest_validation import validate_ai_parse


def test_parse_amount_to_cents_variants() -> None:
    assert parse_amount_to_cents("-5.00") == -500
    assert parse_amount_to_cents("($123.45)") == -12345
    assert parse_amount_to_cents("$1,234.56") == 123456
    assert parse_amount_to_cents(5.0) == 500
    assert parse_amount_to_cents("(0.50)") == -50


def test_parse_amount_to_cents_debit_credit() -> None:
    assert parse_amount_to_cents(debit="12.34") == -1234
    assert parse_amount_to_cents(credit="12.34") == 1234
    with pytest.raises(ValueError):
        parse_amount_to_cents(debit="1", credit="2")


def test_normalize_date_variants() -> None:
    assert normalize_date("01/15/26") == "2026-01-15"
    assert normalize_date("12/31/99") == "1999-12-31"
    assert normalize_date("2026-01-15") == "2026-01-15"
    assert normalize_date("1/5/26") == "2026-01-05"


def test_get_extractor_unknown_backend() -> None:
    with pytest.raises(ValueError):
        get_extractor("unknown", {})


def test_ai_extractor_wraps_ai_metadata(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    parsed = {
        "statement": {
            "institution": "Citi",
            "account_label": "Card",
            "statement_period_start": "2025-01-01",
            "statement_period_end": "2025-01-31",
            "new_balance": 110.0,
            "total_charges": 10.0,
            "total_payments": None,
            "currency": "USD",
        },
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE",
                "amount": -10.0,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            }
        ],
        "extraction_meta": {"expected_transaction_count": 1},
    }
    validation = validate_ai_parse(parsed)

    def _fake_ai_parse_statement(*_args, **_kwargs):
        return AIParseResult(
            raw_json=json.dumps(parsed),
            parsed=parsed,
            validation=validation,
            provider="openai",
            model="gpt-test",
            prompt_version="v1",
            prompt_hash="a" * 64,
            input_tokens=17,
            output_tokens=9,
            elapsed_ms=123,
            extracted_text="statement text",
        )

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-03",
                "description": "COFFEE",
                "amount_cents": -1000,
                "card_ending": "1234",
                "source": "Citi",
            }
        ],
        extracted_total_cents=-1000,
        reconciled=True,
        warnings=[],
        total_charges_cents=1000,
    )

    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_parse_statement", _fake_ai_parse_statement)
    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_result_to_extract_result", lambda *_a, **_k: extracted)

    extractor = AIExtractor({"provider": "openai", "model": "gpt-test"})
    output = extractor.extract(pdf_path, ExtractOptions())

    assert output.meta.backend == "ai"
    assert output.meta.bank_parser_label == "ai:gpt-test"
    assert output.meta.provider == "openai"
    assert output.meta.model_version == "gpt-test"
    assert output.meta.reconcile_status == "matched"
    assert output.meta.content_text == "statement text"
    assert output.meta.raw_api_response
    assert output.meta.validation_summary
    assert output.meta.input_tokens == 17
    assert output.meta.output_tokens == 9
    assert output.meta.elapsed_ms == 123
    assert output.result.transactions[0]["amount_cents"] == -1000


def test_ai_extractor_forwards_require_reconciled(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    parsed = {
        "statement": {"institution": "Citi", "currency": "USD"},
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE",
                "amount": -10.0,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            }
        ],
        "extraction_meta": {"expected_transaction_count": 1},
    }
    validation = validate_ai_parse(parsed)

    def _fake_ai_parse_statement(*_args, **_kwargs):
        return AIParseResult(
            raw_json=json.dumps(parsed),
            parsed=parsed,
            validation=validation,
            provider="openai",
            model="gpt-test",
            prompt_version="v1",
            prompt_hash="a" * 64,
            extracted_text="statement text",
        )

    captured: dict[str, Any] = {}

    def _fake_to_extract(_result, *, allow_partial: bool, require_reconciled: bool):
        captured["allow_partial"] = allow_partial
        captured["require_reconciled"] = require_reconciled
        return ExtractResult(
            transactions=[
                {
                    "date": "2025-01-03",
                    "description": "COFFEE",
                    "amount_cents": -1000,
                    "card_ending": "1234",
                    "source": "Citi",
                }
            ],
            extracted_total_cents=-1000,
            reconciled=False,
            warnings=[],
        )

    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_parse_statement", _fake_ai_parse_statement)
    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_result_to_extract_result", _fake_to_extract)

    extractor = AIExtractor({"provider": "openai", "model": "gpt-test"})
    extractor.extract(pdf_path, ExtractOptions(require_reconciled=True))
    assert captured == {"allow_partial": False, "require_reconciled": True}


def test_ai_extractor_reconcile_status_always_no_totals(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    parsed = {
        "statement": {
            "institution": "Citi",
            "statement_total": -30.0,
            "currency": "USD",
        },
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE",
                "amount": -10.0,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            },
            {
                "date": "2025-01-05",
                "description": "BOOK",
                "amount": -20.0,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.92,
                "evidence": None,
            },
        ],
        "extraction_meta": {"expected_transaction_count": 2},
    }
    validation = validate_ai_parse(parsed)

    def _fake_ai_parse_statement(*_args, **_kwargs):
        return AIParseResult(
            raw_json=json.dumps(parsed),
            parsed=parsed,
            validation=validation,
            provider="openai",
            model="gpt-test",
            prompt_version="v1",
            prompt_hash="a" * 64,
            extracted_text="statement text",
        )

    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_parse_statement", _fake_ai_parse_statement)

    extractor = AIExtractor({"provider": "openai", "model": "gpt-test"})
    output = extractor.extract(pdf_path, ExtractOptions())
    assert output.meta.reconcile_status == "no_totals"


class _FakePoller:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def result(self) -> Any:
        return self._payload


class _FakeAzureClient:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def begin_analyze_document(self, *_args, **_kwargs) -> _FakePoller:
        return _FakePoller(self._payload)


class _FakeAzureCredential:
    def __init__(self, key: str) -> None:
        self.key = key


def test_azure_extractor_maps_transactions(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    payload = {
        "documents": [
            {
                "fields": {
                    "BankName": "chase",
                    "AccountNumber": "****1234",
                    "TotalDeposits": "$100.00",
                    "TotalWithdrawals": "$30.00",
                    "Transactions": [
                        {
                            "Date": "2026-01-01",
                            "Description": "Coffee",
                            "Amount": "5.00",
                            "Type": "debit",
                        },
                        {
                            "Date": "1/2/26",
                            "Description": "Payment",
                            "Amount": "10.00",
                            "Type": "credit",
                        },
                    ],
                }
            }
        ]
    }

    monkeypatch.setenv("AZURE_DI_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("AZURE_DI_API_KEY", "secret")
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.DocumentIntelligenceClient", lambda **_k: _FakeAzureClient(payload))
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.AzureKeyCredential", _FakeAzureCredential)

    extractor = AzureExtractor({})
    output = extractor.extract(pdf_path, ExtractOptions())

    assert output.meta.backend == "azure"
    assert output.meta.bank_parser_label == "azure:prebuilt-bankStatement.us"
    assert output.result.statement_total_cents == 7000
    assert output.result.transactions[0]["source"] == "Chase"
    assert output.result.transactions[0]["card_ending"] == "1234"
    assert output.result.transactions[0]["amount_cents"] == -500
    assert output.result.transactions[1]["amount_cents"] == 1000


def test_azure_extractor_uses_institution_hint(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    payload = {
        "documents": [
            {
                "fields": {
                    "Transactions": [
                        {
                            "Date": "2026-01-01",
                            "Description": "Coffee",
                            "Amount": "5.00",
                            "Type": "debit",
                        }
                    ]
                }
            }
        ]
    }

    monkeypatch.setenv("AZURE_DI_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("AZURE_DI_API_KEY", "secret")
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.DocumentIntelligenceClient", lambda **_k: _FakeAzureClient(payload))
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.AzureKeyCredential", _FakeAzureCredential)

    extractor = AzureExtractor({})
    output = extractor.extract(pdf_path, ExtractOptions(institution_hint="Citi", card_ending_hint="7777"))
    assert output.result.transactions[0]["source"] == "Citi"
    assert output.result.transactions[0]["card_ending"] == "7777"


def test_azure_extractor_missing_institution_errors(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    payload = {"documents": [{"fields": {"Transactions": []}}]}

    monkeypatch.setenv("AZURE_DI_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("AZURE_DI_API_KEY", "secret")
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.DocumentIntelligenceClient", lambda **_k: _FakeAzureClient(payload))
    monkeypatch.setattr("finance_cli.extractors.azure_extractor.AzureKeyCredential", _FakeAzureCredential)

    extractor = AzureExtractor({})
    with pytest.raises(ValueError, match="Could not determine institution"):
        extractor.extract(pdf_path, ExtractOptions())


class _FakeHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_bsc_extractor_normal_flow(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "stmt.pdf"
    pdf_path.write_bytes(b"dummy")

    responses = iter(
        [
            _FakeHTTPResponse('{"uuid":"u1"}'),
            _FakeHTTPResponse('[{"status":"COMPLETED"}]'),
            _FakeHTTPResponse(
                '[{"normalised":[{"date":"01/15/26","description":"Coffee","amount":"-5.00"},'
                '{"date":"01/16/26","description":"NEW BALANCE","amount":"100.00"}]}]'
            ),
        ]
    )

    monkeypatch.setenv("BSC_API_KEY", "secret")
    monkeypatch.setattr("urllib.request.urlopen", lambda *_a, **_k: next(responses))

    extractor = BSCExtractor({"poll_interval_seconds": 0.01, "poll_max_seconds": 1})
    output = extractor.extract(
        pdf_path,
        ExtractOptions(institution_hint="Chase", card_ending_hint="1234"),
    )

    assert output.meta.backend == "bsc"
    assert output.meta.bank_parser_label == "bsc:api"
    assert output.result.statement_total_cents is None
    assert output.result.transactions == [
        {
            "date": "2026-01-15",
            "description": "Coffee",
            "amount_cents": -500,
            "card_ending": "1234",
            "source": "Chase",
        }
    ]


@pytest.mark.parametrize(
    ("helper", "extracted", "expected"),
    [
        (
            _ai_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=True,
                warnings=[],
                statement_total_cents=-100,
                total_charges_cents=100,
            ),
            "matched",
        ),
        (
            _ai_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=False,
                warnings=[],
                statement_total_cents=-100,
                total_payments_cents=50,
            ),
            "mismatch",
        ),
        (
            _ai_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=True,
                warnings=[],
                statement_total_cents=-100,
            ),
            "no_totals",
        ),
        (
            _ai_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=False,
                warnings=[],
            ),
            "no_totals",
        ),
        (
            _azure_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=True,
                warnings=[],
                statement_total_cents=-100,
                total_charges_cents=100,
            ),
            "matched",
        ),
        (
            _azure_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=False,
                warnings=[],
                statement_total_cents=-100,
                total_payments_cents=50,
            ),
            "mismatch",
        ),
        (
            _azure_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=True,
                warnings=[],
                statement_total_cents=-100,
            ),
            "matched",
        ),
        (
            _azure_reconcile_status,
            ExtractResult(
                transactions=[],
                extracted_total_cents=0,
                reconciled=False,
                warnings=[],
            ),
            "no_totals",
        ),
    ],
)
def test_reconcile_status_fallback_order(helper, extracted: ExtractResult, expected: str) -> None:
    assert helper(extracted) == expected
