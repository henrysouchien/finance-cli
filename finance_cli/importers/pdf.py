"""PDF statement importers.

Production PDF imports go through the AI parser (ai_statement_parser.py).
Legacy regex extractors for chase_credit and apple_card are retained for
test coverage; all other bank-specific extractors were archived to
_legacy_pdf_extractors.py during CQ-006 cleanup.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ..categorizer import match_transaction
from ..models import dollars_to_cents

logger = logging.getLogger(__name__)


@dataclass
class ExtractResult:
    transactions: list[dict[str, object]]
    extracted_total_cents: int
    reconciled: bool
    warnings: list[str]
    statement_card_ending: str | None = None
    statement_account_type: str | None = None
    statement_total_cents: int | None = None
    new_balance_cents: int | None = None
    total_charges_cents: int | None = None
    total_payments_cents: int | None = None
    statement_period_start: str | None = None
    statement_period_end: str | None = None
    currency: str | None = None
    apr_purchase: float | None = None
    apr_balance_transfer: float | None = None
    apr_cash_advance: float | None = None
    expected_transaction_count: int | None = None


_AMOUNT_TOKEN_RE = re.compile(r"-?\$?\(?\d[\d,]*\.\d{2}\)?")
_MMDD_RE = re.compile(r"^(\d{2}/\d{2})(?:/(\d{2,4}))?$")
_STATEMENT_RANGE_RE = re.compile(r"(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:-|to|through)\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.IGNORECASE)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _backend_from_bank_parser(bank_parser: str) -> str:
    probe = str(bank_parser or "").strip().lower()
    if not probe:
        return "unknown"
    return probe.split(":", 1)[0]


def _bank_parser_matches_clause(backend: str) -> tuple[str, str]:
    return backend, f"{backend}:%"


def _parse_amount_to_cents(value: str) -> int:
    cleaned = value.strip().replace("$", "").replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return dollars_to_cents(Decimal(cleaned))
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount token '{value}'") from exc


def _normalize_credit_sign(amount_cents: int, description: str) -> int:
    desc = description.lower()
    is_credit_like = any(token in desc for token in ["payment", "refund", "credit", "reversal", "deposit"])
    if is_credit_like:
        return abs(amount_cents)
    return -abs(amount_cents)


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        import pdfplumber  # type: ignore
    except Exception as exc:  # pragma: no cover - dependency may be absent in some envs
        raise RuntimeError("pdfplumber is required for PDF imports") from exc

    chunks: list[str] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            text = page.extract_text() or ""
            if text.strip():
                chunks.append(text)

    combined = "\n".join(chunks).strip()
    if not combined:
        logger.warning("PDF text extraction produced empty text file=%s pages=%s", pdf_path, page_count)
        raise ValueError("No text could be extracted from PDF; OCR/scanned PDFs are not supported")
    logger.info("PDF text extraction complete file=%s pages=%s chars=%s", pdf_path, page_count, len(combined))
    return combined


def _extract_pdf_text(pdf_path: Path) -> str:
    # Backward-compatible private alias used by legacy parser helpers and tests.
    return extract_pdf_text(pdf_path)


def _infer_statement_year(text: str) -> int:
    range_match = _STATEMENT_RANGE_RE.search(text)
    if range_match:
        end_token = range_match.group(2)
        parsed = datetime.strptime(end_token, "%m/%d/%Y") if len(end_token.split("/")[-1]) == 4 else datetime.strptime(end_token, "%m/%d/%y")
        return parsed.year

    date_match = re.search(r"\b\d{1,2}/\d{1,2}/(\d{2,4})\b", text)
    if date_match:
        year = int(date_match.group(1))
        if year < 100:
            year += 2000
        return year

    return datetime.utcnow().year


def _to_iso_date(token: str, default_year: int) -> str | None:
    probe = token.strip()
    if not probe:
        return None

    if re.match(r"^\d{2}/\d{2}/\d{4}$", probe):
        return datetime.strptime(probe, "%m/%d/%Y").date().isoformat()
    if re.match(r"^\d{2}/\d{2}/\d{2}$", probe):
        return datetime.strptime(probe, "%m/%d/%y").date().isoformat()

    mmdd = _MMDD_RE.match(probe)
    if not mmdd:
        return None

    month = int(mmdd.group(1).split("/")[0])
    day = int(mmdd.group(1).split("/")[1])
    year_group = mmdd.group(2)
    year = default_year
    if year_group:
        year = int(year_group)
        if year < 100:
            year += 2000

    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return None


def _extract_statement_total_cents(text: str) -> int | None:
    patterns = [
        r"(?:New Balance|Ending Balance|Statement Balance)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)",
        r"(?:Total Debits|Total Withdrawals|Total Charges)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)",
        r"(?:Amount Due)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)
        if not matches:
            continue
        token = matches[-1]
        try:
            return _parse_amount_to_cents(token)
        except ValueError:
            continue
    return None


def _finalize_result(transactions: list[dict[str, object]], statement_total_cents: int | None, warnings: list[str]) -> ExtractResult:
    extracted_total = sum(int(row["amount_cents"]) for row in transactions)
    reconciled = statement_total_cents is not None and abs(statement_total_cents - extracted_total) <= 1
    if statement_total_cents is not None and not reconciled:
        warnings.append(
            f"statement total {statement_total_cents} does not match extracted total {extracted_total}"
        )
    return ExtractResult(
        transactions=transactions,
        extracted_total_cents=extracted_total,
        reconciled=reconciled,
        warnings=warnings,
        statement_total_cents=statement_total_cents,
    )


def _extract_common_mmdd_lines(text: str, source: str, credit_mode: bool, default_year: int, card_ending: str | None = None) -> list[dict[str, object]]:
    transactions: list[dict[str, object]] = []
    patterns = [
        re.compile(r"^(\d{2}/\d{2})(?:/\d{2,4})?\s+\d{2}/\d{2}\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$"),
        re.compile(r"^(\d{2}/\d{2})(?:/\d{2,4})?\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$"),
    ]

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        matched = None
        for pattern in patterns:
            matched = pattern.match(line)
            if matched:
                break
        if not matched:
            continue

        date_token, description, amount_token = matched.groups()
        iso_date = _to_iso_date(date_token, default_year)
        if not iso_date:
            continue

        amount_cents = _parse_amount_to_cents(amount_token)
        if credit_mode:
            amount_cents = _normalize_credit_sign(amount_cents, description)

        transactions.append(
            {
                "date": iso_date,
                "description": description.strip(),
                "amount_cents": amount_cents,
                "card_ending": card_ending,
                "source": source,
            }
        )

    return transactions


def _extract_chase_credit(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)
    year = _infer_statement_year(text)
    transactions = _extract_common_mmdd_lines(text, source="Chase Credit", credit_mode=True, default_year=year)
    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def _extract_apple_card(pdf_path: Path) -> ExtractResult:
    text = _extract_pdf_text(pdf_path)

    transactions: list[dict[str, object]] = []
    carry_date: str | None = None

    standard_line_re = re.compile(r"^(\d{2}/\d{2}/\d{4})\s+(.+?)\s+(-?\$?\(?\d[\d,]*\.\d{2}\)?)$")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        standard_match = standard_line_re.match(line)
        if standard_match:
            date_token, description, amount_token = standard_match.groups()
            iso_date = _to_iso_date(date_token, datetime.utcnow().year)
            if not iso_date:
                continue
            carry_date = iso_date

            amount_cents = _normalize_credit_sign(_parse_amount_to_cents(amount_token), description)
            transactions.append(
                {
                    "date": iso_date,
                    "description": description.strip(),
                    "amount_cents": amount_cents,
                    "card_ending": "Apple",
                    "source": "Apple Card",
                }
            )
            continue

        if "installment" in line.lower() and "$" in line and carry_date:
            amount_match = _AMOUNT_TOKEN_RE.search(line)
            if not amount_match:
                continue
            amount_cents = -abs(_parse_amount_to_cents(amount_match.group(0)))
            transactions.append(
                {
                    "date": carry_date,
                    "description": "Monthly Installment Payment",
                    "amount_cents": amount_cents,
                    "card_ending": "Apple",
                    "source": "Apple Card",
                }
            )

    return _finalize_result(transactions, _extract_statement_total_cents(text), [])


def extract_transactions(pdf_path: Path, bank: str) -> ExtractResult:
    """Extract transactions from a bank PDF statement."""
    parser_map = {
        "chase_credit": _extract_chase_credit,
        "apple": _extract_apple_card,
        "apple_card": _extract_apple_card,
    }

    parser = parser_map.get(bank)
    if not parser:
        supported = ", ".join(sorted(parser_map))
        raise ValueError(f"Unsupported bank parser '{bank}'. Supported: {supported}")

    return parser(Path(pdf_path))


def import_pdf_statement(
    conn: sqlite3.Connection,
    pdf_path: Path,
    bank: str,
    account_id: str | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    pdf_path = Path(pdf_path)
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF statement not found: {pdf_path}")

    file_hash = _sha256_file(pdf_path)
    backend = _backend_from_bank_parser(bank)
    parser_exact, parser_like = _bank_parser_matches_clause(backend)
    existing_batch = conn.execute(
        """
        SELECT id
          FROM import_batches
         WHERE file_hash_sha256 = ?
           AND (bank_parser = ? OR bank_parser LIKE ?)
        """,
        (file_hash, parser_exact, parser_like),
    ).fetchone()
    if existing_batch:
        return {
            "file": str(pdf_path),
            "bank": bank,
            "already_imported": True,
            "inserted": 0,
            "skipped_duplicates": 0,
            "extracted_count": 0,
            "reconciled": None,
            "warnings": ["file hash already imported"],
            "file_hash": file_hash,
        }

    extracted = extract_transactions(pdf_path, bank)
    return import_extracted_statement(
        conn,
        extracted=extracted,
        file_path=pdf_path,
        bank_parser=bank,
        account_id=account_id,
        dry_run=dry_run,
    )


def import_extracted_statement(
    conn: sqlite3.Connection,
    extracted: ExtractResult,
    file_path: Path,
    bank_parser: str,
    account_id: str | None = None,
    dry_run: bool = False,
    replace_existing_hash: bool = False,
    validate_name: bool = True,
    content_text: str | None = None,
    ai_raw_output_json: str | None = None,
    ai_validation_json: str | None = None,
    ai_model: str | None = None,
    ai_prompt_version: str | None = None,
    ai_prompt_hash: str | None = None,
    auto_commit: bool = True,
) -> dict[str, object]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"PDF statement not found: {file_path}")

    file_hash = _sha256_file(file_path)
    backend = _backend_from_bank_parser(bank_parser)
    parser_exact, parser_like = _bank_parser_matches_clause(backend)
    existing_batch = conn.execute(
        """
        SELECT id
          FROM import_batches
         WHERE file_hash_sha256 = ?
           AND (bank_parser = ? OR bank_parser LIKE ?)
        """,
        (file_hash, parser_exact, parser_like),
    ).fetchone()
    if existing_batch and not replace_existing_hash:
        logger.info("PDF file already imported file=%s bank=%s file_hash=%s", file_path, bank_parser, file_hash)
        return {
            "file": str(file_path),
            "bank": bank_parser,
            "already_imported": True,
            "inserted": 0,
            "skipped_duplicates": 0,
            "extracted_count": 0,
            "reconciled": None,
            "warnings": ["file hash already imported"],
            "file_hash": file_hash,
        }

    content_hash: str | None = None
    if content_text is not None:
        text_payload = str(content_text).strip()
        if text_payload:
            content_hash = _sha256_text(text_payload)
    else:
        try:
            content_hash = _sha256_text(extract_pdf_text(file_path))
        except Exception as exc:
            logger.debug("Skipping PDF content hash file=%s error=%s", file_path, exc)

    if content_hash:
        content_matches = conn.execute(
            """
            SELECT id, file_hash_sha256
              FROM import_batches
             WHERE source_type = 'pdf'
               AND content_hash_sha256 = ?
               AND (bank_parser = ? OR bank_parser LIKE ?)
            """,
            (content_hash, parser_exact, parser_like),
        ).fetchall()
        same_content_different_file = any(
            str(row["file_hash_sha256"] or "") != file_hash for row in content_matches
        )
        if content_matches and (not replace_existing_hash or same_content_different_file):
            logger.info(
                "PDF content already imported file=%s bank=%s file_hash=%s content_hash=%s replace=%s",
                file_path,
                bank_parser,
                file_hash,
                content_hash,
                replace_existing_hash,
            )
            return {
                "file": str(file_path),
                "bank": bank_parser,
                "already_imported": True,
                "inserted": 0,
                "skipped_duplicates": 0,
                "extracted_count": 0,
                "reconciled": None,
                "warnings": ["content hash already imported"],
                "file_hash": file_hash,
            }

    effective_account_id = account_id
    source_name = ""
    derived_card_ending: str | None = None
    if effective_account_id is None:
        for txn in extracted.transactions:
            candidate = str(txn.get("source") or "").strip()
            if candidate:
                source_name = candidate
                break

        if validate_name:
            from . import _validate_institution_name

            _validate_institution_name(source_name)

        statement_card_ending = str(extracted.statement_card_ending or "").strip() or None
        card_counts: Counter[str] = Counter()
        for txn in extracted.transactions:
            card_ending_raw = txn.get("card_ending")
            card_ending = str(card_ending_raw).strip() if card_ending_raw is not None else ""
            if card_ending:
                card_counts[card_ending] += 1

        if card_counts:
            max_count = max(card_counts.values())
            top_choices = sorted(value for value, count in card_counts.items() if count == max_count)
            if statement_card_ending in top_choices:
                derived_card_ending = statement_card_ending
            else:
                derived_card_ending = top_choices[0]
        elif statement_card_ending:
            derived_card_ending = statement_card_ending

        if source_name:
            if dry_run:
                from . import _account_id_for_source

                effective_account_id = _account_id_for_source(source_name, derived_card_ending or "")
            else:
                from . import _get_or_create_account

                effective_account_id, _ = _get_or_create_account(
                    conn,
                    source_name,
                    derived_card_ending or "",
                    cli_source_type="pdf_import",
                    account_type=extracted.statement_account_type,
                )

    replacement_keys: set[str] = set()
    if replace_existing_hash:
        logger.info("PDF replace mode active file=%s bank=%s file_hash=%s", file_path, bank_parser, file_hash)
        rows = conn.execute(
            "SELECT dedupe_key FROM transactions WHERE dedupe_key LIKE ?",
            (f"pdf:{backend}:{file_hash}:%",),
        ).fetchall()
        replacement_keys = {str(row["dedupe_key"]) for row in rows if row["dedupe_key"]}
        if existing_batch and not dry_run:
            conn.execute(
                "DELETE FROM transactions WHERE dedupe_key LIKE ?",
                (f"pdf:{backend}:{file_hash}:%",),
            )
            conn.execute(
                """
                DELETE FROM import_batches
                 WHERE file_hash_sha256 = ?
                   AND (bank_parser = ? OR bank_parser LIKE ?)
                """,
                (file_hash, parser_exact, parser_like),
            )

    currency = str(extracted.currency or "").strip().upper() or None
    if not dry_run and currency and effective_account_id:
        conn.execute(
            "UPDATE accounts SET iso_currency_code = ? WHERE id = ? AND iso_currency_code IS NULL",
            (currency, effective_account_id),
        )

    inserted = 0
    skipped_duplicates = 0

    for idx, txn in enumerate(extracted.transactions):
        dedupe_key = f"pdf:{backend}:{file_hash}:{idx}"
        txn_date = str(txn.get("date") or "")
        description = str(txn.get("description") or "")
        amount_cents = int(txn.get("amount_cents") or 0)
        category_id = None
        category_source = None
        category_confidence = None
        category_rule_id = None
        raw_is_payment = txn.get("is_payment")
        is_payment = 0
        if isinstance(raw_is_payment, str):
            is_payment = 1 if raw_is_payment.strip().lower() in {"1", "true", "yes", "y"} else 0
        elif raw_is_payment is not None:
            is_payment = 1 if bool(raw_is_payment) else 0

        if not dry_run:
            result = match_transaction(
                conn,
                description,
                use_type=None,
                is_payment=bool(is_payment),
            )
            if result and result.category_id:
                category_id = result.category_id
                category_source = result.category_source
                category_confidence = result.category_confidence
                category_rule_id = result.category_rule_id
            if result is not None and result.category_source != "ambiguous":
                is_payment = 1 if result.is_payment else 0

        params = (
            uuid.uuid4().hex,
            effective_account_id,
            dedupe_key,
            txn_date,
            description,
            amount_cents,
            category_id,
            category_source,
            category_confidence,
            category_rule_id,
            is_payment,
            "pdf_import",
        )

        if dry_run:
            exists = conn.execute("SELECT 1 FROM transactions WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
            if exists and dedupe_key not in replacement_keys:
                skipped_duplicates += 1
            else:
                inserted += 1
            continue

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO transactions (
                id,
                account_id,
                dedupe_key,
                date,
                description,
                amount_cents,
                category_id,
                category_source,
                category_confidence,
                category_rule_id,
                is_payment,
                source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        if cursor.rowcount == 0:
            skipped_duplicates += 1
        else:
            inserted += 1

    reconcile_status = "no_totals"
    if extracted.total_charges_cents is not None or extracted.total_payments_cents is not None:
        reconcile_status = "matched" if extracted.reconciled else "mismatch"
    elif extracted.statement_total_cents is not None:
        reconcile_status = "matched" if extracted.reconciled else "mismatch"

    statement_period = None
    if extracted.statement_period_start and extracted.statement_period_end:
        statement_period = f"{extracted.statement_period_start}..{extracted.statement_period_end}"

    if not dry_run:
        if extracted.new_balance_cents is not None and effective_account_id and extracted.statement_period_end:
            conn.execute(
                """
                INSERT INTO balance_snapshots (
                    id, account_id, balance_current_cents, source, snapshot_date
                ) VALUES (?, ?, ?, 'manual', ?)
                ON CONFLICT(account_id, snapshot_date, source) DO UPDATE SET
                    balance_current_cents = excluded.balance_current_cents
                """,
                (
                    uuid.uuid4().hex,
                    effective_account_id,
                    extracted.new_balance_cents,
                    extracted.statement_period_end,
                ),
            )
            conn.execute(
                "UPDATE accounts SET balance_current_cents = ?, balance_updated_at = datetime('now') WHERE id = ?",
                (extracted.new_balance_cents, effective_account_id),
            )

        statement_account_type = str(extracted.statement_account_type or "").strip().lower()
        is_credit_statement = statement_account_type in {"credit_card", "credit card", "credit"}
        has_apr_data = any(
            apr_value is not None
            for apr_value in (
                extracted.apr_purchase,
                extracted.apr_balance_transfer,
                extracted.apr_cash_advance,
            )
        )
        if has_apr_data and effective_account_id and is_credit_statement:
            conn.execute(
                """
                INSERT INTO liabilities (
                    id,
                    account_id,
                    liability_type,
                    is_active,
                    last_statement_balance_cents,
                    last_statement_issue_date,
                    apr_purchase,
                    apr_balance_transfer,
                    apr_cash_advance,
                    updated_at
                ) VALUES (?, ?, 'credit', 1, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(account_id, liability_type) DO UPDATE SET
                    is_active = 1,
                    last_statement_balance_cents = COALESCE(excluded.last_statement_balance_cents, last_statement_balance_cents),
                    last_statement_issue_date = COALESCE(excluded.last_statement_issue_date, last_statement_issue_date),
                    apr_purchase = COALESCE(excluded.apr_purchase, apr_purchase),
                    apr_balance_transfer = COALESCE(excluded.apr_balance_transfer, apr_balance_transfer),
                    apr_cash_advance = COALESCE(excluded.apr_cash_advance, apr_cash_advance),
                    updated_at = datetime('now')
                """,
                (
                    uuid.uuid4().hex,
                    effective_account_id,
                    extracted.new_balance_cents,
                    extracted.statement_period_end,
                    extracted.apr_purchase,
                    extracted.apr_balance_transfer,
                    extracted.apr_cash_advance,
                ),
            )

        conn.execute(
            """
            INSERT INTO import_batches (
                id,
                source_type,
                file_path,
                file_hash_sha256,
                content_hash_sha256,
                bank_parser,
                statement_period,
                extracted_count,
                imported_count,
                skipped_count,
                reconcile_status,
                statement_total_cents,
                extracted_total_cents,
                total_charges_cents,
                total_payments_cents,
                new_balance_cents,
                expected_transaction_count,
                ai_raw_output_json,
                ai_validation_json,
                ai_model,
                ai_prompt_version,
                ai_prompt_hash
            ) VALUES (?, 'pdf', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                uuid.uuid4().hex,
                str(file_path),
                file_hash,
                content_hash,
                bank_parser,
                statement_period,
                len(extracted.transactions),
                inserted,
                skipped_duplicates,
                reconcile_status,
                extracted.statement_total_cents,
                extracted.extracted_total_cents,
                extracted.total_charges_cents,
                extracted.total_payments_cents,
                extracted.new_balance_cents,
                extracted.expected_transaction_count,
                ai_raw_output_json,
                ai_validation_json,
                ai_model,
                ai_prompt_version,
                ai_prompt_hash,
            ),
        )
        if auto_commit:
            conn.commit()

    logger.info(
        "PDF statement import complete file=%s bank=%s inserted=%s skipped_duplicates=%s extracted_count=%s dry_run=%s",
        file_path,
        bank_parser,
        inserted,
        skipped_duplicates,
        len(extracted.transactions),
        dry_run,
    )
    return {
        "file": str(file_path),
        "bank": bank_parser,
        "already_imported": False,
        "inserted": inserted,
        "skipped_duplicates": skipped_duplicates,
        "extracted_count": len(extracted.transactions),
        "statement_total_cents": extracted.statement_total_cents,
        "extracted_total_cents": extracted.extracted_total_cents,
        "total_charges_cents": extracted.total_charges_cents,
        "total_payments_cents": extracted.total_payments_cents,
        "new_balance_cents": extracted.new_balance_cents,
        "reconciled": extracted.reconciled,
        "reconcile_status": reconcile_status,
        "warnings": extracted.warnings,
        "file_hash": file_hash,
    }


__all__ = [
    "ExtractResult",
    "extract_pdf_text",
    "extract_transactions",
    "import_extracted_statement",
    "import_pdf_statement",
]
