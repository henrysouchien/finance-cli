"""Validation gates for AI statement parsing output and generic extract results."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .importers.pdf import ExtractResult
from .models import dollars_to_cents

_RECONCILIATION_TOLERANCE_CENTS = 1


@dataclass(frozen=True)
class FieldError:
    gate: str
    level: str
    row_index: int | None
    field: str | None
    message: str


@dataclass
class ValidationReport:
    passed: bool
    errors: list[FieldError]
    warnings: list[FieldError]
    reconcile_status: str | None
    total_charges_cents: int | None
    total_payments_cents: int | None
    new_balance_cents: int | None
    extracted_total_cents: int | None
    transaction_count: int
    low_confidence_count: int
    blocked_row_indices: list[int]

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "errors": [asdict(item) for item in self.errors],
            "warnings": [asdict(item) for item in self.warnings],
            "reconcile_status": self.reconcile_status,
            "total_charges_cents": self.total_charges_cents,
            "total_payments_cents": self.total_payments_cents,
            "new_balance_cents": self.new_balance_cents,
            "extracted_total_cents": self.extracted_total_cents,
            "transaction_count": self.transaction_count,
            "low_confidence_count": self.low_confidence_count,
            "blocked_row_indices": list(self.blocked_row_indices),
        }


def _make_error(gate: str, row_index: int | None, field: str | None, message: str) -> FieldError:
    return FieldError(gate=gate, level="error", row_index=row_index, field=field, message=message)


def _make_warning(gate: str, row_index: int | None, field: str | None, message: str) -> FieldError:
    return FieldError(gate=gate, level="warning", row_index=row_index, field=field, message=message)


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _is_numeric_finite(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


def _description_has_visible_content(value: object) -> bool:
    if not isinstance(value, str):
        return False
    probe = value.strip()
    if not probe:
        return False
    return any(ch.isprintable() and not ch.isspace() for ch in probe)


def _gate_schema(data: Any) -> tuple[dict[str, Any], list[dict[str, Any]], list[FieldError], list[FieldError]]:
    errors: list[FieldError] = []
    warnings: list[FieldError] = []
    statement: dict[str, Any] = {}
    transactions: list[dict[str, Any]] = []

    if not isinstance(data, dict):
        errors.append(_make_error("schema", None, None, "root payload must be a JSON object"))
        return statement, transactions, errors, warnings

    raw_statement = data.get("statement")
    if "statement" not in data:
        errors.append(_make_error("schema", None, "statement", "missing required key 'statement'"))
    elif not isinstance(raw_statement, dict):
        errors.append(_make_error("schema", None, "statement", "'statement' must be an object"))
    else:
        statement = raw_statement

    raw_transactions = data.get("transactions")
    if "transactions" not in data:
        errors.append(_make_error("schema", None, "transactions", "missing required key 'transactions'"))
    elif not isinstance(raw_transactions, list):
        errors.append(_make_error("schema", None, "transactions", "'transactions' must be an array"))
    else:
        if len(raw_transactions) == 0:
            warnings.append(_make_warning("schema", None, "transactions", "transactions array is empty"))
        for idx, row in enumerate(raw_transactions):
            if not isinstance(row, dict):
                errors.append(_make_error("schema", idx, None, "transaction row must be an object"))
                continue

            for field in ("date", "description", "amount"):
                if field not in row:
                    errors.append(
                        _make_error("schema", idx, field, f"transaction row missing required field '{field}'")
                    )
            transactions.append(row)

    extraction_meta = data.get("extraction_meta")
    if "extraction_meta" in data and extraction_meta is not None:
        if not isinstance(extraction_meta, dict):
            errors.append(_make_error("schema", None, "extraction_meta", "'extraction_meta' must be an object"))
            extraction_meta = None

    expected_transaction_count = (
        extraction_meta.get("expected_transaction_count") if isinstance(extraction_meta, dict) else None
    )
    if expected_transaction_count is None:
        warnings.append(
            _make_warning(
                "schema",
                None,
                "extraction_meta.expected_transaction_count",
                "expected_transaction_count is missing",
            )
        )
    else:
        invalid_expected_count = False
        if isinstance(expected_transaction_count, bool):
            invalid_expected_count = True
        elif isinstance(expected_transaction_count, int):
            invalid_expected_count = expected_transaction_count < 0
        elif isinstance(expected_transaction_count, float):
            invalid_expected_count = (
                (not math.isfinite(expected_transaction_count))
                or (not expected_transaction_count.is_integer())
                or expected_transaction_count < 0
            )
        else:
            invalid_expected_count = True

        if invalid_expected_count:
            warnings.append(
                _make_warning(
                    "schema",
                    None,
                    "extraction_meta.expected_transaction_count",
                    "expected_transaction_count should be an integer >= 0",
                )
            )

    currency = statement.get("currency")
    if currency is not None and str(currency).strip().upper() != "USD":
        errors.append(
            _make_error(
                "schema",
                None,
                "statement.currency",
                "only USD statements are currently supported",
            )
        )

    return statement, transactions, errors, warnings


def _gate_field(statement: dict[str, Any], transactions: list[dict[str, Any]]) -> tuple[list[FieldError], list[FieldError]]:
    errors: list[FieldError] = []
    warnings: list[FieldError] = []

    for idx, row in enumerate(transactions):
        if "date" in row and _parse_iso_date(row.get("date")) is None:
            errors.append(_make_error("field", idx, "date", "date must be ISO format YYYY-MM-DD"))

        if "description" in row and not _description_has_visible_content(row.get("description")):
            errors.append(_make_error("field", idx, "description", "description must contain printable text"))

        if "amount" in row:
            amount = row.get("amount")
            if isinstance(amount, bool):
                errors.append(_make_error("field", idx, "amount", "amount must be numeric, not boolean"))
            elif not isinstance(amount, (int, float)):
                errors.append(_make_error("field", idx, "amount", "amount must be numeric"))
            elif not math.isfinite(float(amount)):
                errors.append(_make_error("field", idx, "amount", "amount must be finite"))
            elif float(amount) == 0:
                warnings.append(_make_warning("field", idx, "amount", "amount is zero"))

    for field_name in ("new_balance",):
        value = statement.get(field_name)
        if value is None:
            continue
        if isinstance(value, bool):
            errors.append(
                _make_error(
                    "field",
                    None,
                    f"statement.{field_name}",
                    f"{field_name} must be numeric, not boolean",
                )
            )
        elif not isinstance(value, (int, float)):
            errors.append(
                _make_error("field", None, f"statement.{field_name}", f"{field_name} must be numeric")
            )
        elif not math.isfinite(float(value)):
            errors.append(
                _make_error("field", None, f"statement.{field_name}", f"{field_name} must be finite")
            )

    return errors, warnings


def _gate_semantic(
    statement: dict[str, Any],
    transactions: list[dict[str, Any]],
    *,
    date_drift_days: int,
    max_amount_warn: float,
) -> tuple[list[FieldError], list[FieldError]]:
    warnings: list[FieldError] = []
    errors: list[FieldError] = []

    period_start = _parse_iso_date(statement.get("statement_period_start"))
    period_end = _parse_iso_date(statement.get("statement_period_end"))
    lower_bound = None
    upper_bound = None
    if period_start is not None and period_end is not None:
        lower_bound = period_start - timedelta(days=date_drift_days)
        upper_bound = period_end + timedelta(days=date_drift_days)

    seen_rows: dict[tuple[str, str, str], int] = {}
    for idx, row in enumerate(transactions):
        parsed_date = _parse_iso_date(row.get("date"))
        if parsed_date is not None and lower_bound is not None and upper_bound is not None:
            if parsed_date < lower_bound or parsed_date > upper_bound:
                warnings.append(
                    _make_warning(
                        "semantic",
                        idx,
                        "date",
                        "transaction date is outside the statement period drift window",
                    )
                )

        amount = row.get("amount")
        if _is_numeric_finite(amount) and abs(float(amount)) > max_amount_warn:
            warnings.append(
                _make_warning("semantic", idx, "amount", f"amount exceeds warning threshold {max_amount_warn:g}")
            )

        key_date = row.get("date")
        key_desc = row.get("description")
        if _is_numeric_finite(amount) and isinstance(key_date, str) and isinstance(key_desc, str):
            key = (key_date, key_desc.strip(), str(Decimal(str(amount))))
            if key in seen_rows:
                warnings.append(
                    _make_warning(
                        "semantic",
                        idx,
                        None,
                        f"duplicate transaction row (same date/description/amount as row {seen_rows[key]})",
                    )
                )
            else:
                seen_rows[key] = idx

    return errors, warnings


def _gate_reconciliation(
    statement: dict[str, Any],
    transactions: list[dict[str, Any]],
    *,
    require_reconciled: bool,
) -> tuple[str, int | None, int | None, int | None, int | None, list[FieldError], list[FieldError]]:
    errors: list[FieldError] = []
    warnings: list[FieldError] = []
    new_balance = statement.get("new_balance")

    try:
        new_balance_cents = dollars_to_cents(Decimal(str(new_balance))) if new_balance is not None else None
    except (InvalidOperation, ValueError):
        new_balance_cents = None

    extracted_total_cents = 0
    for row in transactions:
        amount = row.get("amount")
        if not _is_numeric_finite(amount):
            continue
        try:
            amount_cents = dollars_to_cents(Decimal(str(amount)))
        except (InvalidOperation, ValueError):
            continue
        extracted_total_cents += amount_cents

    return "no_totals", None, None, new_balance_cents, extracted_total_cents, errors, warnings


def _gate_confidence(
    transactions: list[dict[str, Any]],
    *,
    warn_threshold: float,
    block_threshold: float,
) -> tuple[list[FieldError], list[FieldError], list[int], int]:
    errors: list[FieldError] = []
    warnings: list[FieldError] = []
    blocked_rows: list[int] = []
    low_confidence_count = 0

    normalized_rows: list[tuple[int, str, float | None]] = []
    for idx, row in enumerate(transactions):
        raw = row.get("confidence")
        if raw is None:
            normalized_rows.append((idx, "missing", None))
            continue

        if isinstance(raw, bool):
            errors.append(_make_error("confidence", idx, "confidence", "confidence must be numeric, not boolean"))
            continue

        try:
            confidence = float(raw)
        except (TypeError, ValueError):
            errors.append(_make_error("confidence", idx, "confidence", "confidence must be numeric"))
            continue

        if not math.isfinite(confidence):
            errors.append(_make_error("confidence", idx, "confidence", "confidence must be finite"))
            continue
        if confidence < 0 or confidence > 1:
            errors.append(_make_error("confidence", idx, "confidence", "confidence must be in range [0, 1]"))
            continue

        normalized_rows.append((idx, "value", confidence))

    all_missing_or_zero = (
        bool(transactions)
        and len(normalized_rows) == len(transactions)
        and all((kind == "missing") or (value == 0.0) for _idx, kind, value in normalized_rows)
    )

    if all_missing_or_zero:
        warnings.append(
            _make_warning(
                "confidence",
                None,
                "confidence",
                "all confidence values are missing/null/0.0; treated as unknown and not block-enforced",
            )
        )

    for idx, kind, value in normalized_rows:
        if kind == "missing":
            low_confidence_count += 1
            warnings.append(_make_warning("confidence", idx, "confidence", "missing confidence; treated as 0.0"))
            if all_missing_or_zero:
                continue
            confidence = 0.0
        else:
            confidence = float(value or 0.0)
            if confidence < warn_threshold:
                low_confidence_count += 1

        if all_missing_or_zero:
            if kind == "value" and confidence == 0.0:
                warnings.append(
                    _make_warning("confidence", idx, "confidence", "confidence is 0.0 and treated as unknown")
                )
            continue

        if confidence < block_threshold:
            blocked_rows.append(idx)
            warnings.append(
                _make_warning(
                    "confidence",
                    idx,
                    "confidence",
                    f"confidence {confidence:.3f} is below block threshold {block_threshold:.3f}",
                )
            )
        elif confidence < warn_threshold:
            warnings.append(
                _make_warning(
                    "confidence",
                    idx,
                    "confidence",
                    f"confidence {confidence:.3f} is below warning threshold {warn_threshold:.3f}",
                )
            )

    if transactions and len(blocked_rows) == len(transactions):
        errors.append(
            _make_error(
                "confidence",
                None,
                "confidence",
                "all transactions are below block threshold; nothing is eligible for import",
            )
        )

    return errors, warnings, blocked_rows, low_confidence_count


def validate_ai_parse(
    data: Any,
    *,
    date_drift_days: int = 45,
    max_amount_warn: float = 50_000.0,
    confidence_warn: float = 0.80,
    confidence_block: float = 0.60,
    require_reconciled: bool = False,
) -> ValidationReport:
    if date_drift_days < 0:
        raise ValueError("date_drift_days must be >= 0")
    if max_amount_warn <= 0:
        raise ValueError("max_amount_warn must be > 0")
    if not (0 <= confidence_block <= confidence_warn <= 1):
        raise ValueError("confidence thresholds must satisfy 0 <= block <= warn <= 1")

    statement, transactions, schema_errors, schema_warnings = _gate_schema(data)
    field_errors, field_warnings = _gate_field(statement, transactions)
    semantic_errors, semantic_warnings = _gate_semantic(
        statement,
        transactions,
        date_drift_days=date_drift_days,
        max_amount_warn=max_amount_warn,
    )
    (
        reconcile_status,
        total_charges_cents,
        total_payments_cents,
        new_balance_cents,
        extracted_total_cents,
        reconciliation_errors,
        reconciliation_warnings,
    ) = _gate_reconciliation(
        statement,
        transactions,
        require_reconciled=require_reconciled,
    )
    confidence_errors, confidence_warnings, blocked_rows, low_confidence_count = _gate_confidence(
        transactions,
        warn_threshold=confidence_warn,
        block_threshold=confidence_block,
    )

    errors = schema_errors + field_errors + semantic_errors + reconciliation_errors + confidence_errors
    warnings = schema_warnings + field_warnings + semantic_warnings + reconciliation_warnings + confidence_warnings

    return ValidationReport(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        reconcile_status=reconcile_status,
        total_charges_cents=total_charges_cents,
        total_payments_cents=total_payments_cents,
        new_balance_cents=new_balance_cents,
        extracted_total_cents=extracted_total_cents,
        transaction_count=len(transactions),
        low_confidence_count=low_confidence_count,
        blocked_row_indices=blocked_rows,
    )


def validate_extract_result(extracted: ExtractResult) -> tuple[list[str], list[str]]:
    """Universal validation for ExtractResult values.

    Returns:
        (errors, warnings) where errors are fatal.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not extracted.transactions:
        errors.append("No transactions extracted")
        return errors, warnings

    for index, txn in enumerate(extracted.transactions):
        date_value = txn.get("date")
        try:
            date.fromisoformat(str(date_value))
        except (TypeError, ValueError):
            errors.append(f"row {index}: invalid or missing date '{date_value}'")

        amount_value = txn.get("amount_cents")
        if not isinstance(amount_value, int):
            errors.append(
                f"row {index}: amount_cents is {type(amount_value).__name__}, expected int: {amount_value!r}"
            )

        if not str(txn.get("description", "")).strip():
            warnings.append(f"row {index}: empty description")

        if not str(txn.get("source", "")).strip():
            warnings.append(f"row {index}: empty source/institution")

        if isinstance(amount_value, int) and amount_value == 0:
            warnings.append(f"row {index}: zero amount")

    return errors, warnings


__all__ = [
    "FieldError",
    "ValidationReport",
    "_RECONCILIATION_TOLERANCE_CENTS",
    "validate_extract_result",
    "validate_ai_parse",
]
