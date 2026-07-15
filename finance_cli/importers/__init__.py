"""Importers for transaction and vendor-memory CSVs plus income-source imports."""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import sqlite3
import uuid
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from ..categorizer import normalize_description, match_transaction
from ..institution_names import canonicalize as canonicalize_institution_name
from ..institution_names import is_known, user_registry_path
from ..models import dollars_to_cents, normalize_date
from ..user_rules import UserRules, _empty_rules, load_rules, resolve_category_alias

logger = logging.getLogger(__name__)


@dataclass
class ImportReport:
    inserted: int = 0
    skipped_duplicates: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "inserted": self.inserted,
            "skipped_duplicates": self.skipped_duplicates,
            "errors": self.errors,
        }


def _sha256(parts: list[str]) -> str:
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            return str(row[key]).strip()
    return ""


def _to_bool(value: str) -> int:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return 1
    return 0


def _parse_amount_to_cents(value: str) -> int:
    # CSV import convention: negative values represent expenses, positive values represent income.
    cleaned = value.strip().replace("$", "").replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        dec = Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount '{value}'") from exc
    return dollars_to_cents(dec)


_ALLOWED_ACCOUNT_TYPES = {"checking", "savings", "credit_card", "investment", "loan"}
_INSTITUTION_EQUIVALENTS: dict[str, list[str]] = {
    "Bank of America": ["Merrill"],
    "Merrill": ["Bank of America"],
}


def _normalize_account_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in _ALLOWED_ACCOUNT_TYPES:
        return normalized
    return None


def _scrub_card_ending(card_ending: str | None) -> str | None:
    value = str(card_ending or "").strip()
    if value and re.fullmatch(r"\d{4}", value):
        return value
    return None


def _source_feed_identity(cli_source: str, row: dict[str, str]) -> str:
    row_source = _row_value(row, "Source", "source")
    return canonicalize_institution_name(row_source or cli_source)


def _validate_institution_name(cli_source: str) -> None:
    source = str(cli_source or "").strip()
    if not source or source.upper().startswith("AI"):
        logger.warning("No real institution name derived - account may need manual review source=%s", source)
        return
    if is_known(source):
        return
    raise ValueError(
        f"Institution '{source}' not in the institution registry.\n"
        f"Add a mapping to finance_cli/institution_names.py or {user_registry_path()} before importing."
    )


def _account_id_for_source(cli_source: str, card_ending: str) -> str:
    canonical_source = canonicalize_institution_name(cli_source)
    digest = _sha256(["account", canonical_source, card_ending or "none"])
    return digest[:24]


def _clean_account_name(institution_name: str, card_ending: str) -> str:
    scrubbed = _scrub_card_ending(card_ending)
    if scrubbed:
        return f"{institution_name} {scrubbed}"
    return institution_name


def _find_plaid_account(
    conn: sqlite3.Connection,
    institution_name: str,
    card_ending: str | None,
    account_type: str | None,
) -> str | None:
    canonical_institution = canonicalize_institution_name(institution_name)
    plaid_accounts = conn.execute(
        """
        SELECT id, institution_name, card_ending, account_type
          FROM accounts
         WHERE plaid_account_id IS NOT NULL
           AND is_active = 1
        """
    ).fetchall()

    if card_ending:
        matches = [
            row
            for row in plaid_accounts
            if canonicalize_institution_name(str(row["institution_name"] or "")) == canonical_institution
            and str(row["card_ending"] or "") == card_ending
        ]
        if len(matches) == 1:
            return str(matches[0]["id"])
        if not matches:
            equivalent_names = _INSTITUTION_EQUIVALENTS.get(canonical_institution, [])
            if equivalent_names:
                equivalent_set = set(equivalent_names)
                equivalent_matches = [
                    row
                    for row in plaid_accounts
                    if canonicalize_institution_name(str(row["institution_name"] or "")) in equivalent_set
                    and str(row["card_ending"] or "") == card_ending
                ]
                if len(equivalent_matches) == 1:
                    return str(equivalent_matches[0]["id"])

    if account_type:
        matches = [
            row
            for row in plaid_accounts
            if canonicalize_institution_name(str(row["institution_name"] or "")) == canonical_institution
            and str(row["account_type"] or "") == account_type
        ]
        if len(matches) == 1:
            return str(matches[0]["id"])

    return None


def _upsert_account_alias(conn: sqlite3.Connection, *, hash_account_id: str, canonical_id: str) -> None:
    existing = conn.execute(
        "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
        (hash_account_id,),
    ).fetchone()
    if existing and str(existing["canonical_id"]) == canonical_id:
        return

    conn.execute(
        """
        INSERT INTO account_aliases (hash_account_id, canonical_id)
        VALUES (?, ?)
        ON CONFLICT(hash_account_id) DO UPDATE SET
            canonical_id = excluded.canonical_id,
            created_at = datetime('now')
        """,
        (hash_account_id, canonical_id),
    )
    conn.execute(
        "UPDATE account_aliases SET canonical_id = ? WHERE canonical_id = ?",
        (canonical_id, hash_account_id),
    )
    conn.execute(
        "UPDATE transactions SET account_id = ?, updated_at = datetime('now') WHERE account_id = ?",
        (canonical_id, hash_account_id),
    )
    conn.execute(
        "UPDATE subscriptions SET account_id = ? WHERE account_id = ?",
        (canonical_id, hash_account_id),
    )
    conn.execute(
        """
        DELETE FROM subscriptions
         WHERE account_id = ?
           AND rowid IN (
               SELECT rowid
                 FROM (
                     SELECT rowid,
                            ROW_NUMBER() OVER (
                                PARTITION BY vendor_name, frequency, account_id
                                ORDER BY is_auto_detected ASC, rowid ASC
                            ) AS rn
                       FROM subscriptions
                      WHERE account_id = ?
                 ) ranked
                WHERE rn > 1
           )
        """,
        (canonical_id, canonical_id),
    )


def upsert_account_alias(conn: sqlite3.Connection, *, hash_account_id: str, canonical_id: str) -> None:
    _upsert_account_alias(
        conn,
        hash_account_id=hash_account_id,
        canonical_id=canonical_id,
    )


def _check_equivalence_gap(
    conn: sqlite3.Connection,
    canonical_source: str,
    card_ending: str,
    account_id: str,
) -> None:
    equivalent_names = _INSTITUTION_EQUIVALENTS.get(canonical_source, [])
    normalized_card_ending = _scrub_card_ending(card_ending)
    if not equivalent_names or not normalized_card_ending:
        return

    placeholders = ", ".join("?" for _ in equivalent_names)
    rows = conn.execute(
        f"""
        SELECT a.id, a.institution_name
          FROM accounts a
         WHERE a.plaid_account_id IS NOT NULL
           AND a.institution_name IN ({placeholders})
           AND a.card_ending = ?
           AND a.is_active = 1
           AND NOT EXISTS (
               SELECT 1
                 FROM account_aliases aa
                WHERE aa.canonical_id = a.id
                  AND aa.hash_account_id = ?
           )
        """,
        (*equivalent_names, normalized_card_ending, account_id),
    ).fetchall()
    if not rows:
        return

    logger.warning(
        "INSTITUTION_EQUIV_GAP: new '%s' (card_ending=%s) has equivalent '%s' with Plaid account but no alias - possible missing account_aliases entry",
        canonical_source,
        normalized_card_ending,
        str(rows[0]["institution_name"] or equivalent_names[0]),
    )


def _get_or_create_account(
    conn: sqlite3.Connection,
    cli_source: str,
    card_ending: str,
    *,
    cli_source_type: str = "csv_import",
    account_type: str | None = None,
) -> tuple[str, str]:
    canonical_source = canonicalize_institution_name(cli_source)
    account_id = _account_id_for_source(cli_source, card_ending)
    existing = conn.execute(
        "SELECT id, account_type FROM accounts WHERE id = ?",
        (account_id,),
    ).fetchone()
    existing_type = _normalize_account_type(str(existing["account_type"] or "")) if existing else None
    effective_type = _normalize_account_type(account_type) or existing_type or (
        "credit_card" if card_ending else "checking"
    )

    if not existing:
        account_name = _clean_account_name(canonical_source, card_ending)
        conn.execute(
            """
            INSERT INTO accounts (id, institution_name, account_name, account_type, card_ending, source)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                canonical_source,
                account_name,
                effective_type,
                _scrub_card_ending(card_ending),
                cli_source_type,
            ),
        )
        logger.info(
            "Created account id=%s source=%s type=%s card_ending=%s",
            account_id,
            canonical_source,
            effective_type,
            card_ending or "",
        )
        _check_equivalence_gap(conn, canonical_source, card_ending, account_id)
    plaid_id = _find_plaid_account(
        conn,
        canonical_source,
        _scrub_card_ending(card_ending),
        effective_type,
    )
    if plaid_id and plaid_id != account_id:
        _upsert_account_alias(conn, hash_account_id=account_id, canonical_id=plaid_id)
        logger.info("Linked account alias hash_account_id=%s canonical_id=%s", account_id, plaid_id)
        return plaid_id, account_id

    existing_alias = conn.execute(
        "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
        (account_id,),
    ).fetchone()
    if existing_alias:
        canonical_id = str(existing_alias["canonical_id"])
        if canonical_id and canonical_id != account_id:
            return canonical_id, account_id

    return account_id, account_id


def backfill_account_aliases(conn: sqlite3.Connection, *, dry_run: bool = False) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT id, institution_name, card_ending, account_type
          FROM accounts
         WHERE plaid_account_id IS NULL
        """
    ).fetchall()

    report = {
        "scanned": len(rows),
        "aliased": 0,
        "removed": 0,
        "unchanged": 0,
    }

    for row in rows:
        hash_account_id = str(row["id"])
        institution_name = canonicalize_institution_name(str(row["institution_name"] or ""))
        card_ending = _scrub_card_ending(str(row["card_ending"] or ""))
        account_type = _normalize_account_type(str(row["account_type"] or ""))
        plaid_id = _find_plaid_account(conn, institution_name, card_ending, account_type)

        existing = conn.execute(
            "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
            (hash_account_id,),
        ).fetchone()

        if plaid_id and plaid_id != hash_account_id:
            if existing and str(existing["canonical_id"]) == plaid_id:
                report["unchanged"] += 1
                continue
            report["aliased"] += 1
            if not dry_run:
                _upsert_account_alias(conn, hash_account_id=hash_account_id, canonical_id=plaid_id)
            continue

        report["unchanged"] += 1

    return report


def _get_or_create_category(
    conn: sqlite3.Connection,
    category_name: str,
    rules: UserRules | None = None,
    rules_path: Path | None = None,
) -> str | None:
    effective_rules = rules
    if effective_rules is None:
        try:
            effective_rules = load_rules(path=rules_path)
        except ValueError:
            effective_rules = _empty_rules()

    resolved = resolve_category_alias(category_name, effective_rules)
    if resolved is None:
        return None

    row = conn.execute(
        "SELECT id FROM categories WHERE lower(name) = lower(?)",
        (resolved,),
    ).fetchone()
    if row:
        return row["id"]

    category_id = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO categories (id, name, is_system) VALUES (?, ?, 0)",
        (category_id, resolved),
    )
    return category_id


def _count_existing_csv_matches(
    conn: sqlite3.Connection,
    account_id: str,
    date_value: str,
    amount_cents: int,
    normalized_desc: str,
) -> int:
    """Count active csv_import transactions matching a content signature.

    Fetches candidates by (account_id, amount_cents, source) from DB, then
    filters by normalized date and normalized description in Python. Date
    normalization is done in Python because legacy rows may use non-ISO formats.

    Split children are excluded because they intentionally duplicate the parent
    row's metadata and are not same-source reimport duplicates.
    """
    rows = conn.execute(
        """
        SELECT date, description
          FROM transactions
         WHERE account_id = ?
           AND amount_cents = ?
           AND source = 'csv_import'
           AND is_active = 1
           AND parent_transaction_id IS NULL
        """,
        (account_id, amount_cents),
    ).fetchall()
    norm_date = normalize_date(date_value)
    return sum(
        1
        for row in rows
        if normalize_date(str(row["date"])) == norm_date
        and normalize_description(str(row["description"])) == normalized_desc
    )


def _resolve_guard_account_id(
    conn: sqlite3.Connection,
    effective_account_id: str,
    dry_run: bool,
) -> str:
    """Resolve account IDs for same-source guard DB queries."""
    if not dry_run:
        return effective_account_id
    alias_row = conn.execute(
        "SELECT canonical_id FROM account_aliases WHERE hash_account_id = ?",
        (effective_account_id,),
    ).fetchone()
    return str(alias_row["canonical_id"]) if alias_row else effective_account_id


def _import_row_iter(
    conn: sqlite3.Connection,
    row_iter: Iterable[dict[str, str]],
    source_name: str,
    dry_run: bool = False,
    rules_path: Path | None = None,
) -> ImportReport:
    report = ImportReport()
    occurrences: dict[str, int] = defaultdict(int)
    _raw_existing: dict[tuple[str, str, int, str], int] = {}
    _batch_ordinal: dict[tuple[str, str, int, str], int] = {}
    _dedupe_consumed: dict[tuple[str, str, int, str], int] = {}
    rules_broken = False
    try:
        load_rules(path=rules_path)
    except Exception as exc:
        logger.warning("rules.yaml invalid; skipping categorization pipeline during CSV import: %s", exc)
        rules_broken = True

    for row_idx, raw_row in enumerate(row_iter, start=1):
        row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}
        try:
            date_value = normalize_date(_row_value(row, "Date", "date"))
            description = _row_value(row, "Description", "description")
            amount_cents = _parse_amount_to_cents(_row_value(row, "Amount", "amount"))
            card_ending = _row_value(row, "Card Ending", "card_ending", "CardEnding")
            account_type = _normalize_account_type(
                _row_value(row, "Account Type", "account_type") or None
            )
            use_type = _row_value(row, "Use Type", "use_type") or None
            category_name = _row_value(row, "Category", "category") or None
            external_ref = _row_value(row, "Transaction ID", "transaction_id", "id")
            is_payment = _to_bool(_row_value(row, "Is Payment", "is_payment"))

            if dry_run:
                effective_account_id = _account_id_for_source(source_name, card_ending)
                hash_account_id = effective_account_id
            else:
                effective_account_id, hash_account_id = _get_or_create_account(
                    conn,
                    source_name,
                    card_ending,
                    account_type=account_type,
                )
            source_feed = _source_feed_identity(source_name, row)

            base_fingerprint = _sha256(
                [
                    source_feed,
                    hash_account_id,
                    date_value,
                    normalize_description(description),
                    str(amount_cents),
                    card_ending or "null",
                    external_ref or "null",
                ]
            )
            occurrences[base_fingerprint] += 1
            duplicate_ordinal = occurrences[base_fingerprint]
            dedupe_key = _sha256([base_fingerprint, str(duplicate_ordinal)])

            existing_dk = conn.execute(
                "SELECT 1 FROM transactions WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            norm_desc = normalize_description(description)
            guard_acct = _resolve_guard_account_id(conn, effective_account_id, dry_run)
            content_key = (guard_acct, date_value, amount_cents, norm_desc)

            if existing_dk:
                dk_row = conn.execute(
                    """
                    SELECT 1
                      FROM transactions
                     WHERE dedupe_key = ?
                       AND is_active = 1
                       AND source = 'csv_import'
                       AND parent_transaction_id IS NULL
                    """,
                    (dedupe_key,),
                ).fetchone()
                if dk_row:
                    _dedupe_consumed[content_key] = _dedupe_consumed.get(content_key, 0) + 1
                report.skipped_duplicates += 1
                logger.debug(
                    "Skipped exact dedupe_key match source=%s row_index=%s dedupe_key=%s qualified=%s",
                    source_name,
                    row_idx,
                    dedupe_key[:16],
                    bool(dk_row),
                )
                continue

            if content_key not in _raw_existing:
                _raw_existing[content_key] = _count_existing_csv_matches(
                    conn,
                    guard_acct,
                    date_value,
                    amount_cents,
                    norm_desc,
                )
            _batch_ordinal[content_key] = _batch_ordinal.get(content_key, 0) + 1
            effective_existing = max(
                0,
                _raw_existing[content_key] - _dedupe_consumed.get(content_key, 0),
            )
            if _batch_ordinal[content_key] <= effective_existing:
                report.skipped_duplicates += 1
                logger.debug(
                    "Skipped same-source duplicate source=%s row_index=%s batch_ordinal=%s "
                    "effective_existing=%s (raw=%s consumed=%s)",
                    source_name,
                    row_idx,
                    _batch_ordinal[content_key],
                    effective_existing,
                    _raw_existing[content_key],
                    _dedupe_consumed.get(content_key, 0),
                )
                continue

            category_id = None
            source_category = category_name
            category_source = None
            category_confidence = None
            category_rule_id = None

            if not dry_run and not rules_broken:
                try:
                    match_kwargs: dict[str, Any] = {}
                    if rules_path is not None:
                        match_kwargs["rules_path"] = rules_path
                    result = match_transaction(
                        conn,
                        description,
                        use_type,
                        source_category=source_category,
                        is_payment=bool(is_payment),
                        **match_kwargs,
                    )
                except Exception as exc:
                    logger.warning(
                        "match_transaction() failed for %s row %d: %s",
                        source_name,
                        row_idx,
                        exc,
                    )
                    rules_broken = True
                    result = None

                if result and result.category_id:
                    category_id = result.category_id
                    category_source = result.category_source
                    category_confidence = result.category_confidence
                    category_rule_id = result.category_rule_id
                if result is not None and result.category_source != "ambiguous":
                    is_payment = 1 if result.is_payment else 0

            txn_id = uuid.uuid4().hex
            params = (
                txn_id,
                effective_account_id,
                dedupe_key,
                date_value,
                description,
                amount_cents,
                category_id,
                source_category,
                category_source,
                category_confidence,
                category_rule_id,
                use_type if use_type in {"Business", "Personal"} else None,
                is_payment,
                "csv_import",
            )

            if dry_run:
                existing = conn.execute(
                    "SELECT 1 FROM transactions WHERE dedupe_key = ?",
                    (dedupe_key,),
                ).fetchone()
                if existing:
                    report.skipped_duplicates += 1
                    logger.debug(
                        "Skipped duplicate row source=%s row_index=%s dedupe_key=%s",
                        source_name,
                        row_idx,
                        dedupe_key,
                    )
                else:
                    report.inserted += 1
                    logger.debug(
                        "Dry-run insert row source=%s row_index=%s dedupe_key=%s",
                        source_name,
                        row_idx,
                        dedupe_key,
                    )
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
                    source_category,
                    category_source,
                    category_confidence,
                    category_rule_id,
                    use_type,
                    is_payment,
                    source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                params,
            )

            if cursor.rowcount == 0:
                report.skipped_duplicates += 1
                logger.debug(
                    "Skipped duplicate row source=%s row_index=%s dedupe_key=%s",
                    source_name,
                    row_idx,
                    dedupe_key,
                )
            else:
                report.inserted += 1
                logger.debug(
                    "Inserted transaction source=%s row_index=%s txn_id=%s dedupe_key=%s",
                    source_name,
                    row_idx,
                    txn_id,
                    dedupe_key,
                )
        except Exception as exc:
            report.errors += 1
            logger.warning(
                "Failed to import row source=%s row_index=%s error=%s",
                source_name,
                row_idx,
                exc,
            )

    return report


def import_csv(
    conn: sqlite3.Connection,
    file_path: str | Path,
    source_name: str,
    dry_run: bool = False,
    validate_name: bool = True,
    rules_path: Path | None = None,
) -> ImportReport:
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"CSV not found: {file_path}")
    if validate_name:
        _validate_institution_name(source_name)

    file_hash = _sha256_file(file_path)
    existing_batch = None
    if not dry_run:
        existing_batch = conn.execute(
            "SELECT id FROM import_batches WHERE file_hash_sha256 = ?",
            (file_hash,),
        ).fetchone()

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        report = _import_row_iter(
            conn,
            reader,
            source_name=source_name,
            dry_run=dry_run,
            rules_path=rules_path,
        )

    if not dry_run:
        if not existing_batch:
            conn.execute(
                """
                INSERT INTO import_batches (
                    id,
                    source_type,
                    file_path,
                    file_hash_sha256,
                    bank_parser,
                    extracted_count,
                    imported_count,
                    skipped_count,
                    reconcile_status,
                    statement_total_cents,
                    extracted_total_cents
                ) VALUES (?, 'csv', ?, ?, ?, ?, ?, ?, 'no_totals', NULL, NULL)
                """,
                (
                    uuid.uuid4().hex,
                    str(file_path),
                    file_hash,
                    source_name,
                    report.inserted + report.skipped_duplicates + report.errors,
                    report.inserted,
                    report.skipped_duplicates,
                ),
            )
        conn.commit()

    return report


def import_normalized_rows(
    conn: sqlite3.Connection,
    rows: list[dict[str, str]],
    source_name: str,
    dry_run: bool = False,
    *,
    file_path: str | Path | None = None,
    validate_name: bool = True,
    auto_commit: bool = True,
    rules_path: Path | None = None,
) -> ImportReport:
    if validate_name:
        _validate_institution_name(source_name)

    normalized_file_path: Path | None = None
    file_hash: str | None = None
    existing_batch = None

    if file_path is not None:
        normalized_file_path = Path(file_path)
        if not normalized_file_path.exists():
            raise FileNotFoundError(f"CSV not found: {normalized_file_path}")
        file_hash = _sha256_file(normalized_file_path)
        if not dry_run:
            existing_batch = conn.execute(
                "SELECT id FROM import_batches WHERE file_hash_sha256 = ?",
                (file_hash,),
            ).fetchone()
            if existing_batch:
                logger.info(
                    "CSV file already imported file=%s source=%s file_hash=%s",
                    normalized_file_path,
                    source_name,
                    file_hash,
                )

    report = _import_row_iter(
        conn,
        rows,
        source_name=source_name,
        dry_run=dry_run,
        rules_path=rules_path,
    )

    if not dry_run:
        if normalized_file_path is not None and file_hash and not existing_batch:
            conn.execute(
                """
                INSERT INTO import_batches (
                    id,
                    source_type,
                    file_path,
                    file_hash_sha256,
                    bank_parser,
                    extracted_count,
                    imported_count,
                    skipped_count,
                    reconcile_status,
                    statement_total_cents,
                    extracted_total_cents
                ) VALUES (?, 'csv', ?, ?, ?, ?, ?, ?, 'no_totals', NULL, NULL)
                """,
                (
                    uuid.uuid4().hex,
                    str(normalized_file_path),
                    file_hash,
                    source_name,
                    report.inserted + report.skipped_duplicates + report.errors,
                    report.inserted,
                    report.skipped_duplicates,
                ),
            )
        if auto_commit:
            conn.commit()

    logger.info(
        "CSV normalized import complete source=%s inserted=%s skipped_duplicates=%s errors=%s dry_run=%s",
        source_name,
        report.inserted,
        report.skipped_duplicates,
        report.errors,
        dry_run,
    )

    return report


def import_income_csv(
    conn: sqlite3.Connection,
    file_path: str | Path,
    source_name: str,
    rules: UserRules | None = None,
    dry_run: bool = False,
    rules_path: Path | None = None,
) -> ImportReport:
    report = ImportReport()
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Income CSV not found: {file_path}")

    if rules is None:
        rules = load_rules(path=rules_path)

    source_cfg = rules.income_sources.get(source_name)
    if not source_cfg:
        raise ValueError(f"income source '{source_name}' not configured in rules.yaml")

    columns = source_cfg.get("csv_columns") or {}
    if not isinstance(columns, dict):
        raise ValueError(f"income_sources.{source_name}.csv_columns must be a mapping")

    date_col = str(columns.get("date") or "").strip()
    amount_col = str(columns.get("amount") or "").strip()
    description_col_raw = columns.get("description")
    description_col = str(description_col_raw).strip() if description_col_raw else ""

    if not date_col or not amount_col:
        raise ValueError(f"income_sources.{source_name}.csv_columns must define date and amount")

    platform = str(source_cfg.get("platform") or source_name).strip() or source_name
    default_description = str(source_cfg.get("default_description") or platform).strip() or platform
    configured_use_type = str(source_cfg.get("use_type") or "").strip()
    use_type = configured_use_type if configured_use_type in {"Business", "Personal"} else None
    category_name = str(source_cfg.get("category") or "").strip()

    file_hash = _sha256_file(file_path)
    if not dry_run:
        existing_batch = conn.execute(
            "SELECT id FROM import_batches WHERE file_hash_sha256 = ?",
            (file_hash,),
        ).fetchone()
        if existing_batch:
            report.skipped_duplicates = 1
            return report

    category_id = None
    if category_name:
        if dry_run:
            existing_category = conn.execute(
                "SELECT id FROM categories WHERE name = ?",
                (category_name,),
            ).fetchone()
            category_id = existing_category["id"] if existing_category else "dryrun_category"
        else:
            category_id = _get_or_create_category(conn, category_name, rules_path=rules_path)

    effective_account_id = _account_id_for_source(platform, "")
    if not dry_run:
        effective_account_id, _ = _get_or_create_account(conn, platform, "")

    occurrences: dict[str, int] = defaultdict(int)
    inc_raw_existing: dict[tuple[str, str, int, str], int] = {}
    inc_batch_ordinal: dict[tuple[str, str, int, str], int] = {}
    inc_dedupe_consumed: dict[tuple[str, str, int, str], int] = {}

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)

        for row_idx, raw_row in enumerate(reader, start=1):
            row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}
            try:
                date_value = normalize_date(_row_value(row, date_col, date_col.lower()))
                raw_amount = _row_value(row, amount_col, amount_col.lower())
                description = _row_value(row, description_col, description_col.lower()) if description_col else ""
                if not description:
                    description = default_description

                amount_cents = abs(_parse_amount_to_cents(raw_amount))

                base_fingerprint = _sha256(
                    [
                        "income_csv",
                        source_name,
                        date_value,
                        normalize_description(description),
                        str(amount_cents),
                    ]
                )
                occurrences[base_fingerprint] += 1
                dedupe_key = _sha256([base_fingerprint, str(occurrences[base_fingerprint])])

                inc_existing_dk = conn.execute(
                    "SELECT 1 FROM transactions WHERE dedupe_key = ?",
                    (dedupe_key,),
                ).fetchone()
                inc_norm_desc = normalize_description(description)
                inc_guard_acct = _resolve_guard_account_id(conn, effective_account_id, dry_run)
                inc_content_key = (inc_guard_acct, date_value, amount_cents, inc_norm_desc)

                if inc_existing_dk:
                    inc_dk_row = conn.execute(
                        """
                        SELECT 1
                          FROM transactions
                         WHERE dedupe_key = ?
                           AND is_active = 1
                           AND source = 'csv_import'
                           AND parent_transaction_id IS NULL
                        """,
                        (dedupe_key,),
                    ).fetchone()
                    if inc_dk_row:
                        inc_dedupe_consumed[inc_content_key] = inc_dedupe_consumed.get(inc_content_key, 0) + 1
                    report.skipped_duplicates += 1
                    logger.debug(
                        "Skipped exact income dedupe_key match source=%s row_index=%s dedupe_key=%s qualified=%s",
                        source_name,
                        row_idx,
                        dedupe_key[:16],
                        bool(inc_dk_row),
                    )
                    continue

                if inc_content_key not in inc_raw_existing:
                    inc_raw_existing[inc_content_key] = _count_existing_csv_matches(
                        conn,
                        inc_guard_acct,
                        date_value,
                        amount_cents,
                        inc_norm_desc,
                    )
                inc_batch_ordinal[inc_content_key] = inc_batch_ordinal.get(inc_content_key, 0) + 1
                inc_effective = max(
                    0,
                    inc_raw_existing[inc_content_key] - inc_dedupe_consumed.get(inc_content_key, 0),
                )
                if inc_batch_ordinal[inc_content_key] <= inc_effective:
                    report.skipped_duplicates += 1
                    logger.debug(
                        "Skipped same-source income duplicate source=%s row_index=%s batch_ordinal=%s "
                        "effective_existing=%s (raw=%s consumed=%s)",
                        source_name,
                        row_idx,
                        inc_batch_ordinal[inc_content_key],
                        inc_effective,
                        inc_raw_existing[inc_content_key],
                        inc_dedupe_consumed.get(inc_content_key, 0),
                    )
                    continue

                if dry_run:
                    existing = conn.execute(
                        "SELECT 1 FROM transactions WHERE dedupe_key = ?",
                        (dedupe_key,),
                    ).fetchone()
                    if existing:
                        report.skipped_duplicates += 1
                    else:
                        report.inserted += 1
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
                        use_type,
                        source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'keyword_rule', 0.9, ?, 'csv_import')
                    """,
                    (
                        uuid.uuid4().hex,
                        effective_account_id,
                        dedupe_key,
                        date_value,
                        description,
                        amount_cents,
                        category_id,
                        use_type,
                    ),
                )

                if cursor.rowcount == 0:
                    report.skipped_duplicates += 1
                else:
                    report.inserted += 1
            except Exception as exc:
                report.errors += 1
                logger.warning(
                    "Failed income CSV row import source=%s error=%s",
                    source_name,
                    exc,
                )

    if dry_run:
        conn.rollback()
        return report

    conn.execute(
        """
        INSERT INTO import_batches (
            id,
            source_type,
            file_path,
            file_hash_sha256,
            bank_parser,
            extracted_count,
            imported_count,
            skipped_count,
            reconcile_status,
            statement_total_cents,
            extracted_total_cents
        ) VALUES (?, 'income_csv', ?, ?, ?, ?, ?, ?, 'no_totals', NULL, NULL)
        """,
        (
            uuid.uuid4().hex,
            str(file_path),
            file_hash,
            source_name,
            report.inserted + report.skipped_duplicates + report.errors,
            report.inserted,
            report.skipped_duplicates,
        ),
    )
    conn.commit()
    return report


def import_vendor_memory_csv(
    conn: sqlite3.Connection,
    file_path: str | Path,
    dry_run: bool = False,
) -> dict[str, int]:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Vendor memory CSV not found: {file_path}")

    inserted = 0
    updated = 0
    errors = 0

    with file_path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for raw_row in reader:
            row = {str(k).strip(): str(v).strip() for k, v in raw_row.items() if k is not None}
            try:
                raw_pattern = _row_value(row, "Description", "description", "Pattern", "pattern")
                if not raw_pattern:
                    continue
                pattern = normalize_description(raw_pattern)

                use_type = _row_value(row, "Use Type", "use_type")
                if use_type not in {"Business", "Personal"}:
                    use_type = "Any"

                category_name = _row_value(row, "Category", "category")
                category_id = _get_or_create_category(conn, category_name) if category_name else None
                canonical = _row_value(row, "Canonical Name", "canonical_name") or None

                existing = conn.execute(
                    "SELECT id FROM vendor_memory WHERE description_pattern = ? AND use_type = ?",
                    (pattern, use_type),
                ).fetchone()
                if existing:
                    if not dry_run:
                        conn.execute(
                            """
                            UPDATE vendor_memory
                               SET canonical_name = ?,
                                   category_id = ?,
                                   confidence = 1.0,
                                   is_enabled = 1,
                                   is_confirmed = 1
                             WHERE id = ?
                            """,
                            (canonical, category_id, existing["id"]),
                        )
                    updated += 1
                else:
                    if not dry_run:
                        conn.execute(
                            """
                            INSERT INTO vendor_memory (
                                id,
                                description_pattern,
                                canonical_name,
                                category_id,
                                use_type,
                                confidence,
                                priority,
                                is_enabled,
                                is_confirmed,
                                match_count
                            ) VALUES (?, ?, ?, ?, ?, 1.0, 0, 1, 1, 0)
                            """,
                            (uuid.uuid4().hex, pattern, canonical, category_id, use_type),
                        )
                    inserted += 1
            except Exception as exc:
                errors += 1
                logger.warning(
                    "Failed vendor-memory CSV row import file=%s error=%s",
                    file_path,
                    exc,
                )

    if not dry_run:
        conn.commit()

    return {"inserted": inserted, "updated": updated, "errors": errors}


from .csv_normalizers import NormalizeResult, detect_csv_institution, normalize_csv, supported_institutions  # noqa: E402
from .pdf import ExtractResult, extract_transactions, import_pdf_statement  # noqa: E402

__all__ = [
    "ExtractResult",
    "ImportReport",
    "NormalizeResult",
    "backfill_account_aliases",
    "detect_csv_institution",
    "extract_transactions",
    "import_csv",
    "import_income_csv",
    "import_normalized_rows",
    "import_pdf_statement",
    "import_vendor_memory_csv",
    "normalize_csv",
    "supported_institutions",
    "upsert_account_alias",
]
