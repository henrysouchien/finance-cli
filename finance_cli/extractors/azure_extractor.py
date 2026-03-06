"""Azure Document Intelligence extractor adapter.

NOT CURRENTLY IN USE. Kept as a reference implementation for wiring in a new
LLM-based or cloud extraction service. The extractor protocol (StatementExtractor)
and factory (get_extractor) are ready — just register a new backend name.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

from ..importers.pdf import ExtractResult
from ..institution_names import canonicalize as canonicalize_institution_name
from . import ExtractOptions, ExtractorMeta, ExtractorOutput, normalize_date, parse_amount_to_cents

logger = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.core.credentials import AzureKeyCredential
except Exception:  # pragma: no cover - optional dependency
    DocumentIntelligenceClient = None  # type: ignore[assignment]
    AzureKeyCredential = None  # type: ignore[assignment]

_LAST4_RE = re.compile(r"(\d{4})(?!.*\d)")


class AzureExtractor:
    name = "azure"

    def __init__(self, config: dict[str, Any]) -> None:
        cfg = config if isinstance(config, dict) else {}
        endpoint_env = str(cfg.get("endpoint_env") or "AZURE_DI_ENDPOINT")
        api_key_env = str(cfg.get("api_key_env") or "AZURE_DI_API_KEY")
        model_id = str(cfg.get("model_id") or "prebuilt-bankStatement.us")

        endpoint = os.getenv(endpoint_env)
        api_key = os.getenv(api_key_env)

        if DocumentIntelligenceClient is None or AzureKeyCredential is None:
            raise RuntimeError(
                "azure-ai-documentintelligence is not installed. Install it to use --backend azure."
            )
        if not endpoint:
            raise ValueError(f"Missing Azure endpoint env var: {endpoint_env}")
        if not api_key:
            raise ValueError(f"Missing Azure API key env var: {api_key_env}")

        self.model_id = model_id
        self.client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))

    def extract(self, pdf_path: Path, options: ExtractOptions) -> ExtractorOutput:
        started = time.perf_counter()
        with Path(pdf_path).open("rb") as f:
            poller = self.client.begin_analyze_document(
                self.model_id,
                body=f,
                content_type="application/pdf",
            )
        azure_result = poller.result()

        extracted = self._convert_transactions(azure_result, options)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        meta = ExtractorMeta(
            backend="azure",
            bank_parser_label=f"azure:{self.model_id}",
            provider="azure",
            model_version=self.model_id,
            reconcile_status=_reconcile_status_from_extract(extracted),
            raw_api_response=self._serialize_raw(azure_result),
            elapsed_ms=elapsed_ms,
        )
        return ExtractorOutput(result=extracted, meta=meta)

    def _convert_transactions(self, azure_result: Any, options: ExtractOptions) -> ExtractResult:
        warnings: list[str] = []
        documents = _get_documents(azure_result)
        if not documents:
            raise ValueError("Azure response did not include any documents")
        if len(documents) > 1:
            logger.warning("Azure response included %s documents; using first only", len(documents))

        doc = documents[0]
        fields = _get_fields(doc)

        institution = _resolve_institution(fields, options)
        card_ending = _resolve_card_ending(fields, options, warnings)

        transactions: list[dict[str, object]] = []
        for index, row in enumerate(_get_transaction_rows(doc, fields)):
            mapped = _map_transaction_row(row, institution, card_ending, warnings, index)
            if mapped is not None:
                transactions.append(mapped)

        statement_total_cents = _compute_statement_total_cents(fields)
        extracted_total_cents = sum(int(txn["amount_cents"]) for txn in transactions)
        reconciled = (
            statement_total_cents is not None and abs(statement_total_cents - extracted_total_cents) <= 1
        )
        return ExtractResult(
            transactions=transactions,
            statement_total_cents=statement_total_cents,
            extracted_total_cents=extracted_total_cents,
            reconciled=reconciled,
            warnings=warnings,
            statement_card_ending=card_ending,
        )

    def _serialize_raw(self, azure_result: Any) -> str:
        if hasattr(azure_result, "as_dict"):
            try:
                return json.dumps(azure_result.as_dict(), ensure_ascii=True)
            except Exception:
                pass
        if isinstance(azure_result, dict):
            return json.dumps(azure_result, ensure_ascii=True)
        return json.dumps(str(azure_result), ensure_ascii=True)


def _get_documents(azure_result: Any) -> list[Any]:
    if isinstance(azure_result, dict):
        documents = azure_result.get("documents")
        return documents if isinstance(documents, list) else []
    documents = getattr(azure_result, "documents", None)
    return documents if isinstance(documents, list) else []


def _get_fields(document: Any) -> dict[str, Any]:
    if isinstance(document, dict):
        fields = document.get("fields")
        return fields if isinstance(fields, dict) else {}
    fields = getattr(document, "fields", None)
    return fields if isinstance(fields, dict) else {}


def _field_lookup(fields: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in fields:
            return fields[name]
    lower_map = {str(key).lower(): value for key, value in fields.items()}
    for name in names:
        probe = lower_map.get(name.lower())
        if probe is not None:
            return probe
    return None


def _field_value(field: Any) -> Any:
    if field is None:
        return None
    if isinstance(field, dict):
        if "value" in field:
            return field.get("value")
        if "content" in field:
            return field.get("content")
        if "amount" in field:
            return field.get("amount")
        return field

    value = getattr(field, "value", None)
    if value is not None:
        return value

    for attr in (
        "content",
        "value_string",
        "value_number",
        "value_currency",
        "value_date",
        "value_array",
        "value_object",
    ):
        probe = getattr(field, attr, None)
        if probe is not None:
            return probe

    if hasattr(field, "as_dict"):
        try:
            return field.as_dict()
        except Exception:
            return None

    return field


def _as_string(value: Any) -> str:
    probe = _field_value(value)
    if probe is None:
        return ""
    if isinstance(probe, dict):
        inner = probe.get("content")
        if inner is None:
            inner = probe.get("value")
        if inner is None:
            inner = probe.get("amount")
        return str(inner or "").strip()
    return str(probe).strip()


def _resolve_institution(fields: dict[str, Any], options: ExtractOptions) -> str:
    bank_name = _as_string(_field_lookup(fields, "BankName", "bankName", "InstitutionName"))
    if bank_name:
        return canonicalize_institution_name(bank_name)

    hint = str(options.institution_hint or "").strip()
    if hint:
        return canonicalize_institution_name(hint)

    raise ValueError("Could not determine institution. Use --institution flag.")


def _resolve_card_ending(fields: dict[str, Any], options: ExtractOptions, warnings: list[str]) -> str | None:
    account_number = _as_string(_field_lookup(fields, "AccountNumber", "accountNumber"))
    if account_number:
        match = _LAST4_RE.search(account_number)
        if match:
            return match.group(1)

    hint = str(options.card_ending_hint or "").strip()
    if hint:
        return hint

    warnings.append(
        "No card ending available. Account identity may merge with other accounts for this institution. "
        "Use --card-ending to specify."
    )
    return None


def _get_transaction_rows(document: Any, fields: dict[str, Any]) -> list[Any]:
    field = _field_lookup(fields, "Transactions", "transactions")
    value = _field_value(field)
    if isinstance(value, list):
        return value

    if isinstance(document, dict):
        direct = document.get("transactions")
        if isinstance(direct, list):
            return direct
    else:
        direct = getattr(document, "transactions", None)
        if isinstance(direct, list):
            return direct

    return []


def _row_to_mapping(row: Any) -> dict[str, Any]:
    value = _field_value(row)
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        try:
            as_dict = value.as_dict()
            if isinstance(as_dict, dict):
                return as_dict
        except Exception:
            return {}
    return {}


def _amount_from_currency_object(value: Any) -> Any:
    probe = _field_value(value)
    if isinstance(probe, dict):
        if "amount" in probe:
            return probe.get("amount")
        nested = probe.get("value")
        if isinstance(nested, dict) and "amount" in nested:
            return nested.get("amount")
    return probe


def _map_transaction_row(
    row: Any,
    institution: str,
    card_ending: str | None,
    warnings: list[str],
    index: int,
) -> dict[str, object] | None:
    mapping = _row_to_mapping(row)
    if not mapping:
        warnings.append(f"row {index}: could not read transaction object")
        return None

    date_raw = _as_string(
        _first_present(
            mapping,
            "Date",
            "date",
            "TransactionDate",
            "PostedDate",
            "postDate",
        )
    )
    if not date_raw:
        warnings.append(f"row {index}: missing date; skipped")
        return None

    try:
        date_iso = normalize_date(date_raw)
    except ValueError:
        warnings.append(f"row {index}: invalid date '{date_raw}'; skipped")
        return None

    description = _as_string(
        _first_present(
            mapping,
            "Description",
            "description",
            "Merchant",
            "merchant",
            "Name",
            "name",
            "Payee",
            "payee",
        )
    )

    debit_raw = _first_present(mapping, "DebitAmount", "debitAmount")
    credit_raw = _first_present(mapping, "CreditAmount", "creditAmount")
    amount_raw = _first_present(mapping, "Amount", "amount")

    try:
        if debit_raw is not None or credit_raw is not None:
            amount_cents = parse_amount_to_cents(
                debit=_amount_from_currency_object(debit_raw),
                credit=_amount_from_currency_object(credit_raw),
            )
        else:
            amount_value = _amount_from_currency_object(amount_raw)
            amount_cents = parse_amount_to_cents(amount_value)
            kind_raw = _as_string(_first_present(mapping, "Type", "type", "kind", "TransactionType"))
            if kind_raw:
                kind = kind_raw.strip().lower()
                if any(token in kind for token in ("debit", "withdrawal", "charge", "purchase")):
                    amount_cents = -abs(amount_cents)
                elif any(token in kind for token in ("credit", "deposit", "payment", "refund")):
                    amount_cents = abs(amount_cents)
    except ValueError:
        warnings.append(f"row {index}: invalid amount; skipped")
        return None

    return {
        "date": date_iso,
        "description": description,
        "amount_cents": amount_cents,
        "card_ending": card_ending,
        "source": institution,
    }


def _first_present(mapping: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in mapping:
            return mapping[name]
    lower_map = {str(key).lower(): value for key, value in mapping.items()}
    for name in names:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def _compute_statement_total_cents(fields: dict[str, Any]) -> int | None:
    deposits_raw = _field_lookup(fields, "TotalDeposits", "totalDeposits")
    withdrawals_raw = _field_lookup(fields, "TotalWithdrawals", "totalWithdrawals")

    if deposits_raw is None and withdrawals_raw is None:
        return None

    deposits = 0
    withdrawals = 0
    if deposits_raw is not None:
        deposits = abs(parse_amount_to_cents(_amount_from_currency_object(deposits_raw)))
    if withdrawals_raw is not None:
        withdrawals = abs(parse_amount_to_cents(_amount_from_currency_object(withdrawals_raw)))
    return deposits - withdrawals


def _reconcile_status_from_extract(extracted: ExtractResult) -> str:
    if extracted.total_charges_cents is not None or extracted.total_payments_cents is not None:
        return "matched" if extracted.reconciled else "mismatch"
    if extracted.statement_total_cents is not None:
        return "matched" if extracted.reconciled else "mismatch"
    return "no_totals"


__all__ = ["AzureExtractor"]
