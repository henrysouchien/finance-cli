"""AI-driven PDF statement parsing with validation and conversion helpers."""

from __future__ import annotations

import decimal
import hashlib
import json
import logging
import math
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .importers.pdf import ExtractResult, extract_pdf_text
from .institution_names import canonicalize as canonicalize_institution_name
from .ingest_validation import ValidationReport, validate_ai_parse
from .models import dollars_to_cents, normalize_date

logger = logging.getLogger(__name__)

PROMPT_VERSION = "v9"
DEFAULT_MAX_TEXT_CHARS = 100_000
DEFAULT_MAX_TOKENS = 16384
DEFAULT_TIMEOUT = 120

_CARD_ENDING_OVERRIDES: dict[str, str] = {
    "Apple Card": "Apple",
}
_LAST4_RE = re.compile(r"(\d{4})(?!.*\d)")


@dataclass(frozen=True)
class AIParseResult:
    raw_json: str
    parsed: dict[str, Any]
    validation: ValidationReport
    provider: str
    model: str
    prompt_version: str
    prompt_hash: str
    input_tokens: int = 0
    output_tokens: int = 0
    elapsed_ms: int = 0
    extracted_text: str = ""


def _default_model(provider: str) -> str:
    if provider == "openai":
        return "gpt-4o-mini"
    if provider == "claude":
        return "claude-sonnet-4-5-20250929"
    raise ValueError(f"Unsupported AI provider '{provider}'")


def _to_usage_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _usage_from_openai(body: dict[str, Any]) -> dict[str, int]:
    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    return {
        "input_tokens": _to_usage_int(usage_map.get("prompt_tokens")),
        "output_tokens": _to_usage_int(usage_map.get("completion_tokens")),
    }


def _usage_from_claude(body: dict[str, Any]) -> dict[str, int]:
    usage = body.get("usage")
    usage_map = usage if isinstance(usage, dict) else {}
    return {
        "input_tokens": _to_usage_int(usage_map.get("input_tokens")),
        "output_tokens": _to_usage_int(usage_map.get("output_tokens")),
    }


def _send_parse_request(
    provider: str,
    system_prompt: str,
    user_prompt: str,
    model: str,
    max_tokens: int = DEFAULT_MAX_TOKENS,
) -> tuple[str, dict[str, int]]:
    if provider == "openai":
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY is not set")
        payload = {
            "model": model,
            "temperature": 0,
            "max_tokens": max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"OpenAI API error: {detail or exc.reason}") from exc

        choices = body.get("choices") or []
        if not choices:
            raise RuntimeError("OpenAI API returned no choices")
        usage = _usage_from_openai(body)
        message = choices[0].get("message") or {}
        content = message.get("content")
        if isinstance(content, str):
            return content, usage
        if isinstance(content, list):
            parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
            return "".join(parts), usage
        raise RuntimeError("OpenAI API returned unexpected content format")

    if provider == "claude":
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        payload = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [
                {"role": "user", "content": user_prompt},
            ],
        }
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Anthropic API error: {detail or exc.reason}") from exc

        content = body.get("content") or []
        usage = _usage_from_claude(body)
        if isinstance(content, list):
            parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
            if parts:
                return "".join(parts), usage
        raise RuntimeError("Anthropic API returned unexpected content format")

    raise ValueError(f"Unsupported AI provider '{provider}'")


def _canonicalize_institution(raw_institution: str) -> tuple[str, str | None]:
    stripped = str(raw_institution or "").strip()
    if not stripped:
        return "", None

    canonical_name = canonicalize_institution_name(stripped)
    card_ending = _CARD_ENDING_OVERRIDES.get(canonical_name)
    return canonical_name, card_ending


def _build_parse_prompt(pdf_text: str) -> tuple[str, str, str]:
    schema = {
        "statement": {
            "institution": "string|null",
            "account_label": "string|null",
            "card_ending": "string|null",
            "account_type": "credit_card|checking|savings|null",
            "statement_period_start": "YYYY-MM-DD|null",
            "statement_period_end": "YYYY-MM-DD|null",
            "new_balance": "number|null",
            "apr_purchase": "number|null",
            "apr_balance_transfer": "number|null",
            "apr_cash_advance": "number|null",
            "currency": "USD|null",
        },
        "transactions": [
            {
                "date": "YYYY-MM-DD",
                "description": "string",
                "amount": -12.34,
                "card_ending": "string|null",
                "transaction_id": "string|null",
                "confidence": 0.85,
                "evidence": "string|null",
            }
        ],
        "extraction_meta": {
            "model": "string|null",
            "prompt_version": "string|null",
            "notes": "string|null",
            "expected_transaction_count": "number|null",
        },
    }
    system_prompt = (
        "You are a financial statement extractor.\n"
        "Return strict JSON only. No markdown, no code fences, no commentary.\n"
        "Extract only values present in the statement text. Use null when uncertain.\n"
        "Use this schema exactly (do not add keys):\n"
        f"{json.dumps(schema, ensure_ascii=True)}\n"
        "Amount sign convention: negative=expense/outflow, positive=payment/refund/inflow.\n"
        "new_balance: the ending balance or 'New Balance' shown on the statement. "
        "For credit cards this is the amount owed. For checking/savings this is the ending account balance. "
        "Extract the number exactly as shown, always positive, and do not apply sign convention.\n"
        "If new_balance is not explicitly shown on the statement, set to null.\n"
        "APR fields are credit-card only: apr_purchase, apr_balance_transfer, apr_cash_advance.\n"
        "Extract APR percentages from account summary, interest charge, or rate sections when present.\n"
        "Use decimal numbers without a percent sign (example: 24.99), and set APR fields to null for checking/savings.\n"
        "If debit/credit columns exist, combine into one signed amount.\n"
        "Include ALL line items from EVERY section of the statement — purchases, payments, "
        "refunds, interest charges, fees, cash advances, balance transfers, and adjustments.\n"
        "Credit card statements typically have separate sections for each category. "
        "Extract from ALL of them, not just the first section you encounter.\n"
        "Checking/savings statements may list deposits and withdrawals in separate columns or sections — "
        "extract both.\n"
        "Do NOT stop after the first page of transactions — continue through the entire document.\n"
        "Exclude non-transaction rows: subtotals, running balances, previous balance, "
        "new balance, minimum payment due, credit limit, payment coupon lines, and summary totals. "
        "Only extract posted transaction line items.\n"
        "In extraction_meta.expected_transaction_count, report the total number of transaction "
        "line items you can identify in the statement text, even if you cannot extract all of them. "
        "This helps detect incomplete extraction.\n"
        "All numeric fields must be JSON numbers, never strings.\n"
        "For each transaction row, confidence must be a numeric score in [0.0, 1.0] that reflects extraction certainty.\n"
        "Calibrate confidence by evidence strength:\n"
        "- 0.90-1.00: date, description, and amount are explicit and unambiguous\n"
        "- 0.70-0.89: minor ambiguity but likely correct\n"
        "- 0.40-0.69: notable ambiguity (layout noise, uncertain token mapping)\n"
        "- 0.00-0.39: weak evidence; include only if the row is still extractable\n"
        "Do not assign 0.0 to all rows by default."
    )
    user_prompt = "Extract all transactions from this statement:\n\n" + pdf_text
    prompt_hash = hashlib.sha256(f"{system_prompt}\n{user_prompt}".encode("utf-8")).hexdigest()
    return system_prompt, user_prompt, prompt_hash


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    payload = raw_text.strip()
    if not payload:
        raise ValueError("empty LLM response")

    def _coerce(decoded: Any) -> dict[str, Any]:
        if isinstance(decoded, dict):
            return decoded
        raise ValueError("response is not a JSON object")

    try:
        return _coerce(json.loads(payload))
    except (ValueError, json.JSONDecodeError):
        pass

    if payload.startswith("```"):
        lines = payload.splitlines()
        if lines and lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        fenced = "\n".join(lines).strip()
        if fenced:
            try:
                return _coerce(json.loads(fenced))
            except (ValueError, json.JSONDecodeError):
                pass

    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("could not locate JSON object in response")

    try:
        return _coerce(json.loads(payload[start : end + 1]))
    except json.JSONDecodeError as exc:
        raise ValueError("malformed JSON response") from exc


def _normalize_ai_payload_dates(parsed: dict[str, Any]) -> int:
    """Normalize non-ISO dates in-place. Returns count of dates converted."""
    converted = 0
    statement = parsed.get("statement")
    if isinstance(statement, dict):
        for key in ("statement_period_start", "statement_period_end"):
            raw_value = statement.get(key)
            if isinstance(raw_value, str):
                normed = normalize_date(raw_value)
                if normed != raw_value.strip():
                    converted += 1
                statement[key] = normed

    transactions = parsed.get("transactions")
    if not isinstance(transactions, list):
        return converted

    for row in transactions:
        if not isinstance(row, dict):
            continue
        raw_date = row.get("date")
        if isinstance(raw_date, str):
            normed = normalize_date(raw_date)
            if normed != raw_date.strip():
                converted += 1
            row["date"] = normed

    return converted


def ai_parse_statement(
    pdf_path: Path,
    *,
    provider: str | None = None,
    model: str | None = None,
    max_text_chars: int = DEFAULT_MAX_TEXT_CHARS,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    **validation_kwargs: Any,
) -> AIParseResult:
    if max_text_chars <= 0:
        raise ValueError("max_text_chars must be > 0")
    if max_tokens <= 0:
        raise ValueError("max_tokens must be > 0")

    provider_name = str(provider or "").strip().lower()
    if not provider_name:
        raise ValueError("AI provider is required; set --provider or ai_parser.provider in rules.yaml")

    model_name = model or _default_model(provider_name)
    text = extract_pdf_text(Path(pdf_path))
    if len(text) > max_text_chars:
        logger.warning(
            "Extracted text exceeds limit file=%s text_len=%s max_text_chars=%s",
            pdf_path,
            len(text),
            max_text_chars,
        )
        raise ValueError(
            f"extracted PDF text length {len(text)} exceeds max_text_chars={max_text_chars}; increase limit or split statement"
        )
    logger.info(
        "Sending AI parse request file=%s provider=%s model=%s text_len=%s max_tokens=%s",
        pdf_path,
        provider_name,
        model_name,
        len(text),
        max_tokens,
    )

    started_at = time.perf_counter()
    system_prompt, user_prompt, prompt_hash = _build_parse_prompt(text)

    parsed: dict[str, Any] | None = None
    parse_error: str | None = None
    raw_response: str = ""
    total_input_tokens = 0
    total_output_tokens = 0
    for attempt in range(2):
        response = _send_parse_request(provider_name, system_prompt, user_prompt, model_name, max_tokens)
        if isinstance(response, tuple) and len(response) == 2:
            raw_response, usage = response
            total_input_tokens += _to_usage_int(usage.get("input_tokens"))
            total_output_tokens += _to_usage_int(usage.get("output_tokens"))
        else:
            raw_response = str(response)
        try:
            parsed = _extract_json_object(raw_response)
            parse_error = None
            break
        except ValueError as exc:
            parse_error = str(exc)
            if attempt == 0:
                logger.warning(
                    "AI parse response JSON decode failed; retrying file=%s provider=%s model=%s error=%s",
                    pdf_path,
                    provider_name,
                    model_name,
                    parse_error,
                )

    if parsed is None:
        # Detect likely output truncation — response ends mid-JSON
        hint = ""
        stripped = raw_response.rstrip()
        if stripped and not stripped.endswith("}"):
            hint = (
                f" (response appears truncated at {len(stripped)} chars — "
                f"try increasing --max-tokens above {max_tokens})"
            )
        raise ValueError(f"failed to parse model output as JSON object: {parse_error}{hint}")

    dates_converted = _normalize_ai_payload_dates(parsed)
    if dates_converted:
        logger.warning("AI returned %d non-ISO date(s) — normalized before validation", dates_converted)
    validation = validate_ai_parse(parsed, **validation_kwargs)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "AI parse validation complete file=%s passed=%s errors=%s warnings=%s reconcile_status=%s",
        pdf_path,
        validation.passed,
        len(validation.errors),
        len(validation.warnings),
        validation.reconcile_status,
    )
    logger.info(
        "AI parse complete file=%s provider=%s model=%s input_tokens=%s output_tokens=%s elapsed_ms=%s",
        pdf_path,
        provider_name,
        model_name,
        total_input_tokens,
        total_output_tokens,
        elapsed_ms,
    )
    return AIParseResult(
        raw_json=raw_response,
        parsed=parsed,
        validation=validation,
        provider=provider_name,
        model=model_name,
        prompt_version=PROMPT_VERSION,
        prompt_hash=prompt_hash,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        elapsed_ms=elapsed_ms,
        extracted_text=text,
    )


def _format_validation_warning(item: Any) -> str:
    prefix = f"[{item.gate}]"
    if item.row_index is not None:
        prefix += f" row={item.row_index}"
    if item.field:
        prefix += f" field={item.field}"
    return f"{prefix} {item.message}"


def _safe_dollars_to_cents(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return dollars_to_cents(Decimal(str(value)))
    except (TypeError, ValueError, decimal.InvalidOperation):
        logger.warning("Invalid numeric value for %s: %r", field_name, value)
        return None


def _safe_apr_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        logger.warning("Invalid APR value for %s: %r", field_name, value)
        return None
    try:
        apr_value = float(value)
    except (TypeError, ValueError):
        logger.warning("Invalid APR value for %s: %r", field_name, value)
        return None
    if not math.isfinite(apr_value) or apr_value < 0 or apr_value > 100:
        logger.warning("APR out of range for %s: %r", field_name, value)
        return None
    return apr_value


def _coerce_expected_txn_count(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def ai_result_to_extract_result(
    result: AIParseResult,
    *,
    allow_partial: bool = False,
    require_reconciled: bool = False,
) -> ExtractResult:
    if not result.validation.passed:
        raise ValueError("cannot convert AI parse result with validation errors")

    blocked_rows = sorted(set(result.validation.blocked_row_indices))
    if blocked_rows:
        logger.warning(
            "AI parse produced blocked rows count=%s rows=%s allow_partial=%s",
            len(blocked_rows),
            blocked_rows,
            allow_partial,
        )
    if blocked_rows and not allow_partial:
        raise ValueError(
            "validation report includes blocked rows; set allow_partial=True to import only unblocked rows"
        )

    parsed_statement = result.parsed.get("statement")
    statement = parsed_statement if isinstance(parsed_statement, dict) else {}
    parsed_transactions = result.parsed.get("transactions")
    if not isinstance(parsed_transactions, list):
        raise ValueError("parsed payload is missing transactions array")

    raw_institution = str(statement.get("institution") or "").strip()
    if raw_institution:
        source, canonical_card_ending = _canonicalize_institution(raw_institution)
        if not source:
            source = raw_institution
    else:
        source = f"AI:{result.model}"
        canonical_card_ending = None

    statement_card_ending_raw = statement.get("card_ending")
    statement_card_ending = str(statement_card_ending_raw).strip() if statement_card_ending_raw is not None else ""
    if not statement_card_ending:
        account_label = str(statement.get("account_label") or "")
        last4_match = _LAST4_RE.search(account_label)
        if last4_match:
            statement_card_ending = last4_match.group(1)
    statement_card_ending = statement_card_ending or None

    statement_account_type_raw = statement.get("account_type")
    statement_account_type = (
        str(statement_account_type_raw).strip() if statement_account_type_raw is not None else None
    )

    blocked_set = set(blocked_rows)
    transactions: list[dict[str, object]] = []
    for idx, raw_row in enumerate(parsed_transactions):
        if idx in blocked_set:
            continue
        if not isinstance(raw_row, dict):
            continue
        raw_amount_cents = dollars_to_cents(Decimal(str(raw_row.get("amount"))))
        description = str(raw_row.get("description") or "").strip()
        amount_cents = raw_amount_cents
        per_txn_card_ending_raw = raw_row.get("card_ending")
        per_txn_card_ending = (
            str(per_txn_card_ending_raw).strip() if per_txn_card_ending_raw is not None else None
        )
        card_ending = canonical_card_ending or statement_card_ending or per_txn_card_ending
        transactions.append(
            {
                "date": str(raw_row.get("date") or ""),
                "description": description,
                "amount_cents": amount_cents,
                "card_ending": card_ending or None,
                "source": source,
            }
        )

    # Compute normalized total from stored (sign-corrected) amounts.
    extracted_total_cents = sum(int(t["amount_cents"]) for t in transactions)
    reconciled = False
    reconcile_status = "no_totals"
    if require_reconciled:
        raise ValueError(
            f"reconciliation status is '{reconcile_status}' and require_reconciled=True"
        )

    new_balance_cents = _safe_dollars_to_cents(statement.get("new_balance"), "new_balance")
    period_start_raw = statement.get("statement_period_start")
    period_end_raw = statement.get("statement_period_end")
    statement_period_start = str(period_start_raw).strip() if isinstance(period_start_raw, str) else None
    statement_period_end = str(period_end_raw).strip() if isinstance(period_end_raw, str) else None
    statement_period_start = statement_period_start or None
    statement_period_end = statement_period_end or None
    currency_raw = statement.get("currency")
    currency = str(currency_raw).strip().upper() if isinstance(currency_raw, str) else None
    currency = currency or None
    apr_purchase = _safe_apr_float(statement.get("apr_purchase"), "apr_purchase")
    apr_balance_transfer = _safe_apr_float(statement.get("apr_balance_transfer"), "apr_balance_transfer")
    apr_cash_advance = _safe_apr_float(statement.get("apr_cash_advance"), "apr_cash_advance")

    parsed_meta = result.parsed.get("extraction_meta")
    raw_expected_count = parsed_meta.get("expected_transaction_count") if isinstance(parsed_meta, dict) else None
    expected_transaction_count = _coerce_expected_txn_count(raw_expected_count)

    warnings = [_format_validation_warning(item) for item in result.validation.warnings]

    return ExtractResult(
        transactions=transactions,
        extracted_total_cents=extracted_total_cents,
        reconciled=reconciled,
        warnings=warnings,
        statement_card_ending=statement_card_ending,
        statement_account_type=statement_account_type,
        statement_total_cents=None,
        new_balance_cents=new_balance_cents,
        total_charges_cents=None,
        total_payments_cents=None,
        statement_period_start=statement_period_start,
        statement_period_end=statement_period_end,
        currency=currency,
        apr_purchase=apr_purchase,
        apr_balance_transfer=apr_balance_transfer,
        apr_cash_advance=apr_cash_advance,
        expected_transaction_count=expected_transaction_count,
    )


__all__ = [
    "AIParseResult",
    "PROMPT_VERSION",
    "DEFAULT_MAX_TEXT_CHARS",
    "DEFAULT_MAX_TOKENS",
    "DEFAULT_TIMEOUT",
    "ai_parse_statement",
    "ai_result_to_extract_result",
    "_build_parse_prompt",
    "_extract_json_object",
    "_send_parse_request",
]
