"""Portable preferences export/import helpers."""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import sqlite3
import tarfile
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from finance_cli import __version__
from finance_cli.config import ensure_data_dir
from finance_cli.user_rules import invalidate_rules_cache, resolve_rules_path

log = logging.getLogger(__name__)

_ARCHIVE_SUFFIX = ".tar.gz"
_SPECIAL_EXPORT_KEYS = frozenset({"category_name", "account_institution", "account_card_ending", "account_name"})

# Tables to export in bundle order.
EXPORT_TABLES: list[dict[str, Any]] = [
    {
        "table": "vendor_memory",
        "category_col": "category_id",
        "columns": [
            "description_pattern",
            "canonical_name",
            "use_type",
            "confidence",
            "priority",
            "is_enabled",
            "is_confirmed",
            "match_count",
        ],
    },
    {
        "table": "budgets",
        "category_col": "category_id",
        "columns": ["period", "amount_cents", "effective_from", "effective_to", "use_type"],
    },
    {
        "table": "biz_section_budgets",
        "category_col": None,
        "columns": ["pl_section", "amount_cents", "period", "effective_from", "effective_to"],
    },
    {
        "table": "goals",
        "category_col": None,
        "columns": [
            "name",
            "metric",
            "target_cents",
            "target_pct",
            "starting_cents",
            "starting_pct",
            "direction",
            "deadline",
            "is_active",
        ],
    },
    {
        "table": "subscriptions",
        "category_col": "category_id",
        "columns": [
            "vendor_name",
            "amount_cents",
            "frequency",
            "next_expected",
            "account_id",
            "is_active",
            "use_type",
            "sub_type",
            "is_auto_detected",
        ],
        "has_account_id": True,
    },
    {
        "table": "category_mappings",
        "category_col": "category_id",
        "columns": ["source_category", "source", "created_by", "confidence", "match_count", "is_enabled"],
    },
    {
        "table": "pl_section_map",
        "category_col": "category_id",
        "columns": ["pl_section", "display_order"],
    },
    {
        "table": "schedule_c_map",
        "category_col": "category_id",
        "columns": ["schedule_c_line", "line_number", "deduction_pct", "tax_year", "notes"],
    },
    {
        "table": "mileage_rates",
        "category_col": None,
        "columns": ["tax_year", "rate_cents"],
        "natural_pk": ["tax_year"],
    },
    {
        "table": "settings",
        "category_col": None,
        "columns": ["key", "value"],
        "natural_pk": ["key"],
    },
    {
        "table": "provider_routing",
        "category_col": None,
        "columns": ["institution_name", "provider"],
        "natural_pk": ["institution_name"],
    },
    {
        "table": "tax_config",
        "category_col": None,
        "columns": ["tax_year", "config_key", "config_value"],
        "natural_pk": ["tax_year", "config_key"],
    },
    {
        "table": "user_strategy_preferences",
        "category_col": None,
        "columns": ["domain", "strategy", "rationale", "source", "evidence_json"],
        "natural_pk": ["domain"],
    },
    {
        "table": "account_alert_rules",
        "category_col": None,
        "columns": [
            "id",
            "rule_type",
            "account_id",
            "threshold_cents",
            "channel",
            "label",
            "status",
            "cooldown_hours",
            "payload_json",
            "idempotency_key",
        ],
        "natural_pk": ["idempotency_key"],
    },
]


@dataclass
class ExportResult:
    bundle_path: Path
    bundle_size: int
    table_counts: dict[str, int]
    file_count: int
    categories_referenced: list[str]


@dataclass
class ImportResult:
    dry_run: bool
    mode: str
    tables_imported: dict[str, int]
    tables_skipped: dict[str, int]
    categories_resolved: dict[str, str]
    categories_missing: list[str]
    categories_created: list[str]
    accounts_resolved: int
    accounts_unresolved: int
    files_copied: list[str]
    warnings: list[str]


@dataclass
class ValidationResult:
    valid: bool
    manifest: dict[str, Any] | None
    errors: list[str]
    warnings: list[str]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_data_dir(data_dir: Path | None = None) -> Path:
    return data_dir.expanduser().resolve() if data_dir is not None else ensure_data_dir()


def _normalize_rules_path(rules_path: Path | None = None, *, data_dir: Path | None = None) -> Path:
    if rules_path is not None:
        return rules_path.expanduser().resolve()
    if data_dir is not None:
        return (data_dir / "rules.yaml").expanduser().resolve()
    return resolve_rules_path()


def _bundle_name_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def _resolve_bundle_destination(destination: Path | None, *, data_dir: Path) -> Path:
    default_name = f"preferences_{_bundle_name_timestamp()}{_ARCHIVE_SUFFIX}"
    if destination is None:
        target = data_dir / default_name
    else:
        resolved = destination.expanduser().resolve()
        if resolved.is_dir() or not resolved.suffix:
            target = resolved / default_name
        else:
            target = resolved

    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        return target

    if target.name.endswith(_ARCHIVE_SUFFIX):
        stem = target.name[: -len(_ARCHIVE_SUFFIX)]
        suffix = _ARCHIVE_SUFFIX
    else:
        stem = target.stem
        suffix = target.suffix

    counter = 1
    while True:
        candidate = target.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _row_get(row: Any, key: str, index: int) -> Any:
    if row is None:
        return None
    if isinstance(row, sqlite3.Row):
        try:
            return row[key]
        except Exception:
            return row[index]
    if isinstance(row, dict):
        return row.get(key)
    return row[index]


def _append_warning_once(warnings: list[str], message: str) -> None:
    if message not in warnings:
        warnings.append(message)


def _get_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return []
    return [str(row[1]) for row in rows]


def _get_migration_version(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
    except sqlite3.OperationalError:
        return 0
    return int((_row_get(row, "MAX(version)", 0) if row is not None else 0) or 0)


def _file_entries(root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for file_path in sorted(path for path in root.rglob("*") if path.is_file() and path.name != "manifest.json"):
        entries.append(
            {
                "path": file_path.relative_to(root).as_posix(),
                "sha256": _sha256(file_path),
                "size_bytes": int(file_path.stat().st_size),
            }
        )
    return entries


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str, sort_keys=True) + "\n")


def _read_tar_member(tar: tarfile.TarFile, member_name: str) -> bytes:
    extracted = tar.extractfile(member_name)
    if extracted is None:
        raise ValueError(f"Archive member missing or unreadable: {member_name}")
    return extracted.read()


def _parse_jsonl(data: bytes) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    text = data.decode("utf-8")
    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        candidate = raw_line.strip()
        if not candidate:
            continue
        payload = json.loads(candidate)
        if not isinstance(payload, dict):
            raise ValueError(f"JSONL row {line_no} must be an object")
        rows.append(payload)
    return rows


def _resolve_category_name(conn: sqlite3.Connection, category_id: str) -> str | None:
    row = conn.execute("SELECT name FROM categories WHERE id = ?", (category_id,)).fetchone()
    return str(_row_get(row, "name", 0)) if row is not None else None


def _resolve_category_id(conn: sqlite3.Connection, category_name: str) -> str | None:
    row = conn.execute(
        "SELECT id FROM categories WHERE name = ? COLLATE NOCASE",
        (category_name,),
    ).fetchone()
    return str(_row_get(row, "id", 0)) if row is not None else None


def _resolve_account(
    conn: sqlite3.Connection,
    institution: str,
    card_ending: str | None,
    account_name: str | None,
) -> str | None:
    if card_ending:
        row = conn.execute(
            "SELECT id FROM accounts WHERE institution_name = ? AND card_ending = ? AND is_active = 1",
            (institution, card_ending),
        ).fetchone()
        if row is not None:
            return str(_row_get(row, "id", 0))
    if account_name:
        row = conn.execute(
            "SELECT id FROM accounts WHERE institution_name = ? AND account_name = ? AND is_active = 1",
            (institution, account_name),
        ).fetchone()
        if row is not None:
            return str(_row_get(row, "id", 0))
    return None


def _export_table(conn: sqlite3.Connection, table_config: dict[str, Any]) -> tuple[list[dict[str, Any]], set[str]]:
    table = str(table_config["table"])
    columns = list(table_config["columns"])
    category_col = table_config.get("category_col")
    has_account_id = bool(table_config.get("has_account_id"))
    existing_cols = _get_table_columns(conn, table)
    if not existing_cols:
        return [], set()

    selected_cols = [column for column in columns if column in existing_cols]
    if category_col and category_col in existing_cols:
        selected_cols.append(category_col)

    if has_account_id and "account_id" in existing_cols and "account_id" not in selected_cols:
        selected_cols.append("account_id")

    if not selected_cols:
        return [], set()

    order_cols = ", ".join(selected_cols)
    rows = conn.execute(f"SELECT {order_cols} FROM {table} ORDER BY {order_cols}").fetchall()
    exported_rows: list[dict[str, Any]] = []
    categories_referenced: set[str] = set()

    for row in rows:
        payload = {column: _row_get(row, column, idx) for idx, column in enumerate(selected_cols)}

        if category_col and category_col in payload:
            category_id = payload.pop(category_col)
            if category_id:
                category_name = _resolve_category_name(conn, str(category_id))
                payload["category_name"] = category_name
                if category_name:
                    categories_referenced.add(category_name)
            else:
                payload["category_name"] = None

        if has_account_id and "account_id" in payload:
            account_id = payload.pop("account_id")
            if account_id:
                account_row = conn.execute(
                    "SELECT institution_name, card_ending, account_name FROM accounts WHERE id = ?",
                    (account_id,),
                ).fetchone()
                payload["account_institution"] = _row_get(account_row, "institution_name", 0)
                payload["account_card_ending"] = _row_get(account_row, "card_ending", 1)
                payload["account_name"] = _row_get(account_row, "account_name", 2)
            else:
                payload["account_institution"] = None
                payload["account_card_ending"] = None
                payload["account_name"] = None

        exported_rows.append(payload)

    return exported_rows, categories_referenced


def _export_account_flags(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    account_cols = set(_get_table_columns(conn, "accounts"))
    if not account_cols or "is_business" not in account_cols:
        return []

    select_cols = ["institution_name", "is_business"]
    if "card_ending" in account_cols:
        select_cols.append("card_ending")
    if "account_name" in account_cols:
        select_cols.append("account_name")

    where_parts = ["is_business = 1"]
    if "is_active" in account_cols:
        where_parts.append("is_active = 1")

    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM accounts WHERE {' AND '.join(where_parts)} "
        f"ORDER BY institution_name, COALESCE(card_ending, ''), COALESCE(account_name, '')"
    ).fetchall()
    exported: list[dict[str, Any]] = []
    for row in rows:
        exported.append(
            {
                "institution_name": _row_get(row, "institution_name", 0),
                "card_ending": _row_get(row, "card_ending", 1 if "card_ending" in select_cols else 0),
                "account_name": _row_get(
                    row,
                    "account_name",
                    select_cols.index("account_name") if "account_name" in select_cols else 0,
                ),
                "is_business": _row_get(row, "is_business", select_cols.index("is_business")),
            }
        )
    return exported


def export_preferences(
    conn: sqlite3.Connection,
    *,
    destination: Path | None = None,
    data_dir: Path | None = None,
    rules_path: Path | None = None,
) -> ExportResult:
    resolved_data_dir = _normalize_data_dir(data_dir)
    resolved_rules_path = _normalize_rules_path(rules_path, data_dir=resolved_data_dir)
    bundle_path = _resolve_bundle_destination(destination, data_dir=resolved_data_dir)

    with tempfile.TemporaryDirectory(prefix="finance_preferences_") as tmpdir:
        staging_dir = Path(tmpdir)
        table_counts: dict[str, int] = {}
        referenced_categories: set[str] = set()

        for table_config in EXPORT_TABLES:
            table = str(table_config["table"])
            try:
                rows, categories = _export_table(conn, table_config)
            except sqlite3.OperationalError:
                log.warning("Skipping preferences export for missing table %s", table, exc_info=True)
                rows, categories = [], set()

            referenced_categories.update(categories)
            table_counts[table] = len(rows)
            _write_jsonl(staging_dir / f"{table}.jsonl", rows)

        try:
            account_flags = _export_account_flags(conn)
        except sqlite3.OperationalError:
            log.warning("Skipping preferences export for account business flags", exc_info=True)
            account_flags = []
        table_counts["account_business_flags"] = len(account_flags)
        _write_jsonl(staging_dir / "account_business_flags.jsonl", account_flags)

        file_count = 0
        if resolved_rules_path.exists():
            shutil.copy2(resolved_rules_path, staging_dir / "rules.yaml")
            file_count += 1

        agent_memory_path = resolved_data_dir / "agent_memory.md"
        if agent_memory_path.exists():
            shutil.copy2(agent_memory_path, staging_dir / "agent_memory.md")
            file_count += 1

        sessions_dir = resolved_data_dir / "sessions"
        if sessions_dir.is_dir():
            shutil.copytree(sessions_dir, staging_dir / "sessions", dirs_exist_ok=True)
            file_count += sum(1 for path in (staging_dir / "sessions").rglob("*") if path.is_file())

        file_entries = _file_entries(staging_dir)
        tables_manifest = {
            f"{table_config['table']}": {
                "row_count": table_counts[str(table_config["table"])],
                "sha256": _sha256(staging_dir / f"{table_config['table']}.jsonl"),
            }
            for table_config in EXPORT_TABLES
        }
        tables_manifest["account_business_flags"] = {
            "row_count": table_counts["account_business_flags"],
            "sha256": _sha256(staging_dir / "account_business_flags.jsonl"),
        }
        manifest = {
            "version": 1,
            "created_at": _utc_now_iso(),
            "finance_cli_version": __version__,
            "migration_version": _get_migration_version(conn),
            "tables": tables_manifest,
            "files": [entry for entry in file_entries if not entry["path"].endswith(".jsonl")],
            "categories_referenced": sorted(referenced_categories),
        }
        (staging_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with tarfile.open(bundle_path, "w:gz") as tar:
            for path in sorted(candidate for candidate in staging_dir.rglob("*") if candidate.is_file()):
                tar.add(path, arcname=path.relative_to(staging_dir).as_posix())

    return ExportResult(
        bundle_path=bundle_path,
        bundle_size=int(bundle_path.stat().st_size),
        table_counts=table_counts,
        file_count=file_count,
        categories_referenced=sorted(referenced_categories),
    )


def validate_bundle(
    bundle_path: Path,
    conn: sqlite3.Connection,
) -> ValidationResult:
    if not bundle_path.exists():
        return ValidationResult(valid=False, manifest=None, errors=["File not found"], warnings=[])

    try:
        with tarfile.open(bundle_path, "r:gz") as tar:
            names = set(tar.getnames())
            if "manifest.json" not in names:
                return ValidationResult(valid=False, manifest=None, errors=["Missing manifest.json"], warnings=[])
            try:
                manifest = json.loads(_read_tar_member(tar, "manifest.json").decode("utf-8"))
            except json.JSONDecodeError as exc:
                return ValidationResult(
                    valid=False,
                    manifest=None,
                    errors=[f"Invalid manifest.json: {exc}"],
                    warnings=[],
                )

            errors: list[str] = []
            warnings: list[str] = []

            if int(manifest.get("version", 0) or 0) > 1:
                warnings.append(f"Bundle version {manifest['version']} is newer than supported (1)")

            for file_entry in manifest.get("files", []):
                if not isinstance(file_entry, dict):
                    continue
                member_name = str(file_entry.get("path", "")).strip()
                if not member_name:
                    continue
                if member_name not in names:
                    errors.append(f"File listed in manifest but missing from archive: {member_name}")
                    continue
                actual_sha = _sha256_bytes(_read_tar_member(tar, member_name))
                if actual_sha != str(file_entry.get("sha256", "")):
                    errors.append(f"Checksum mismatch for {member_name}")

            for table_name, table_meta in (manifest.get("tables") or {}).items():
                if not isinstance(table_meta, dict):
                    continue
                jsonl_name = f"{table_name}.jsonl"
                if jsonl_name not in names:
                    errors.append(f"Table {table_name} listed in manifest but {jsonl_name} missing from archive")
                    continue
                actual_sha = _sha256_bytes(_read_tar_member(tar, jsonl_name))
                if actual_sha != str(table_meta.get("sha256", "")):
                    errors.append(f"Checksum mismatch for {jsonl_name}")

            if "account_business_flags.jsonl" in names:
                try:
                    for row in _parse_jsonl(_read_tar_member(tar, "account_business_flags.jsonl")):
                        account_id = _resolve_account(
                            conn,
                            str(row.get("institution_name") or ""),
                            row.get("card_ending"),
                            row.get("account_name"),
                        )
                        if not account_id:
                            warnings.append(
                                f"Account not found in target DB: {row.get('institution_name')} "
                                f"ending {row.get('card_ending')}"
                            )
                except (ValueError, json.JSONDecodeError) as exc:
                    errors.append(f"Invalid account_business_flags.jsonl: {exc}")
    except (tarfile.TarError, OSError) as exc:
        return ValidationResult(valid=False, manifest=None, errors=[f"Invalid tar.gz: {exc}"], warnings=[])

    bundle_migration = int(manifest.get("migration_version", 0) or 0)
    local_migration = _get_migration_version(conn)
    if bundle_migration > local_migration:
        warnings.append(
            f"Bundle from newer schema (migration {bundle_migration}) than target DB ({local_migration}) "
            "best-effort import, unknown fields will be skipped"
        )
    elif bundle_migration < local_migration:
        warnings.append(
            f"Bundle from older schema (migration {bundle_migration}) than target DB ({local_migration}) "
            "missing fields will use defaults"
        )

    for category_name in manifest.get("categories_referenced", []):
        if isinstance(category_name, str) and category_name and not _resolve_category_id(conn, category_name):
            warnings.append(f"Category not found in target DB: {category_name}")

    return ValidationResult(valid=not errors, manifest=manifest, errors=errors, warnings=warnings)


def _find_other_parent_id(conn: sqlite3.Connection, category_cols: set[str]) -> str | None:
    if "parent_id" in category_cols:
        row = conn.execute(
            "SELECT id FROM categories WHERE name = ? COLLATE NOCASE AND parent_id IS NULL ORDER BY rowid ASC LIMIT 1",
            ("Other",),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id FROM categories WHERE name = ? COLLATE NOCASE ORDER BY rowid ASC LIMIT 1",
            ("Other",),
        ).fetchone()
    return str(_row_get(row, "id", 0)) if row is not None else None


def _insert_generic_row(
    conn: sqlite3.Connection,
    table: str,
    row: dict[str, Any],
    table_columns_order: list[str],
) -> None:
    insert_columns = [column for column in table_columns_order if column in row]
    placeholders = ", ".join("?" for _ in insert_columns)
    sql = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders})"
    conn.execute(sql, tuple(row[column] for column in insert_columns))


def _create_missing_category(conn: sqlite3.Connection, name: str) -> str:
    existing = _resolve_category_id(conn, name)
    if existing:
        return existing

    category_columns_order = _get_table_columns(conn, "categories")
    category_cols = set(category_columns_order)
    if not category_cols:
        raise sqlite3.OperationalError("categories table not found")

    other_id = _find_other_parent_id(conn, category_cols)
    if other_id is None:
        other_id = uuid.uuid4().hex
        other_row: dict[str, Any] = {"id": other_id, "name": "Other"}
        if "parent_id" in category_cols:
            other_row["parent_id"] = None
        if "level" in category_cols:
            other_row["level"] = 0
        if "is_income" in category_cols:
            other_row["is_income"] = 0
        if "is_system" in category_cols:
            other_row["is_system"] = 0
        if "sort_order" in category_cols:
            other_row["sort_order"] = 0
        _insert_generic_row(conn, "categories", other_row, category_columns_order)

    category_id = uuid.uuid4().hex
    category_row: dict[str, Any] = {"id": category_id, "name": name}
    if "parent_id" in category_cols:
        category_row["parent_id"] = other_id
    if "level" in category_cols:
        category_row["level"] = 1
    if "is_income" in category_cols:
        category_row["is_income"] = 0
    if "is_system" in category_cols:
        category_row["is_system"] = 0
    if "sort_order" in category_cols:
        category_row["sort_order"] = 0
    _insert_generic_row(conn, "categories", category_row, category_columns_order)
    return category_id


def _prepare_import_row(
    conn: sqlite3.Connection,
    table_config: dict[str, Any],
    row: dict[str, Any],
    *,
    cat_map: dict[str, str],
    warnings: list[str],
) -> tuple[dict[str, Any] | None, int, int]:
    prepared = {key: value for key, value in row.items() if key not in _SPECIAL_EXPORT_KEYS}
    category_col = table_config.get("category_col")
    table = str(table_config["table"])

    if category_col:
        category_name = row.get("category_name")
        prepared.pop(category_col, None)
        if category_name:
            category_id = cat_map.get(str(category_name))
            if not category_id:
                _append_warning_once(warnings, f"Category not found in target DB: {category_name} (table {table})")
                return None, 0, 0
            prepared[str(category_col)] = category_id
        else:
            prepared[str(category_col)] = None

    if table_config.get("has_account_id"):
        institution = str(row.get("account_institution") or "")
        card_ending = row.get("account_card_ending")
        account_name = row.get("account_name")
        prepared.pop("account_id", None)
        if institution or card_ending or account_name:
            account_id = _resolve_account(conn, institution, card_ending, account_name)
            if account_id:
                prepared["account_id"] = account_id
            else:
                prepared["account_id"] = None
                _append_warning_once(
                    warnings,
                    f"Subscription account not found in target DB: {institution or '?'} ending {card_ending}",
                )
        else:
            prepared["account_id"] = None

    return prepared, 0, 0


def _exists_query(
    conn: sqlite3.Connection,
    table: str,
    values: dict[str, Any],
    *,
    nocase_columns: set[str] | None = None,
    coalesce_blank_columns: set[str] | None = None,
) -> bool:
    clauses: list[str] = []
    params: list[Any] = []
    nocase_columns = nocase_columns or set()
    coalesce_blank_columns = coalesce_blank_columns or set()

    for column, value in values.items():
        if column in coalesce_blank_columns:
            clauses.append(f"COALESCE({column}, '') = ?")
            params.append(value or "")
            continue
        if value is None:
            clauses.append(f"{column} IS NULL")
            continue
        if column in nocase_columns:
            clauses.append(f"{column} = ? COLLATE NOCASE")
        else:
            clauses.append(f"{column} = ?")
        params.append(value)

    if not clauses:
        return False

    row = conn.execute(f"SELECT 1 FROM {table} WHERE {' AND '.join(clauses)} LIMIT 1", tuple(params)).fetchone()
    return row is not None


def _row_conflicts(
    conn: sqlite3.Connection,
    table_config: dict[str, Any],
    row: dict[str, Any],
    table_cols: set[str],
) -> bool:
    table = str(table_config["table"])
    if table == "vendor_memory":
        values = {"description_pattern": row.get("description_pattern")}
        if "use_type" in table_cols:
            values["use_type"] = row.get("use_type", "Any")
        return _exists_query(conn, table, values)

    if table == "budgets":
        if "category_id" not in row or "period" not in row or "effective_from" not in row:
            return False
        if "use_type" in table_cols:
            sql = (
                "SELECT 1 FROM budgets WHERE category_id = ? AND period = ? AND use_type = ? "
                "AND (effective_to IS NULL OR effective_to >= ?) "
                "AND effective_from <= COALESCE(?, '9999-12-31') LIMIT 1"
            )
            params = (
                row.get("category_id"),
                row.get("period"),
                row.get("use_type", "Personal"),
                row.get("effective_from"),
                row.get("effective_to"),
            )
        else:
            sql = (
                "SELECT 1 FROM budgets WHERE category_id = ? AND period = ? "
                "AND (effective_to IS NULL OR effective_to >= ?) "
                "AND effective_from <= COALESCE(?, '9999-12-31') LIMIT 1"
            )
            params = (
                row.get("category_id"),
                row.get("period"),
                row.get("effective_from"),
                row.get("effective_to"),
            )
        return conn.execute(sql, params).fetchone() is not None

    if table == "biz_section_budgets":
        values = {key: row.get(key) for key in ("pl_section", "effective_from") if key in table_cols}
        return _exists_query(conn, table, values)

    if table == "goals":
        return _exists_query(conn, table, {"name": row.get("name")})

    if table == "subscriptions":
        values = {key: row.get(key) for key in ("vendor_name", "amount_cents", "frequency") if key in table_cols}
        if "use_type" in table_cols:
            values["use_type"] = row.get("use_type")
        return _exists_query(conn, table, values)

    if table == "category_mappings":
        values = {"source_category": row.get("source_category")}
        if "source" in table_cols:
            values["source"] = row.get("source")
        return _exists_query(conn, table, values, nocase_columns={"source_category"}, coalesce_blank_columns={"source"})

    if table == "pl_section_map":
        return _exists_query(conn, table, {"category_id": row.get("category_id")})

    if table == "schedule_c_map":
        values = {"category_id": row.get("category_id")}
        if "tax_year" in table_cols:
            values["tax_year"] = row.get("tax_year")
        return _exists_query(conn, table, values)

    if table == "mileage_rates":
        return _exists_query(conn, table, {"tax_year": row.get("tax_year")})

    if table == "settings":
        return _exists_query(conn, table, {"key": row.get("key")})

    if table == "provider_routing":
        return _exists_query(conn, table, {"institution_name": row.get("institution_name")})

    if table == "tax_config":
        values = {"tax_year": row.get("tax_year")}
        if "config_key" in table_cols:
            values["config_key"] = row.get("config_key")
        return _exists_query(conn, table, values)

    if table == "user_strategy_preferences":
        return _exists_query(conn, table, {"domain": row.get("domain")})

    if table == "account_alert_rules":
        return _exists_query(conn, table, {"idempotency_key": row.get("idempotency_key")})

    return False


def _build_insert_row(
    table_config: dict[str, Any],
    row: dict[str, Any],
    *,
    table_columns_order: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    table = str(table_config["table"])
    table_cols = set(table_columns_order)
    insert_row = dict(row)

    if table_config.get("natural_pk") is None and "id" in table_cols and "id" not in insert_row:
        insert_row["id"] = uuid.uuid4().hex

    for key in list(insert_row):
        if key in _SPECIAL_EXPORT_KEYS:
            insert_row.pop(key, None)
            continue
        if key not in table_cols:
            _append_warning_once(
                warnings,
                f"Skipping unknown column {table}.{key} for target schema compatibility",
            )
            insert_row.pop(key, None)

    return insert_row


def _import_table(
    conn: sqlite3.Connection,
    table_config: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    cat_map: dict[str, str],
    mode: str,
    dry_run: bool,
    warnings: list[str],
) -> tuple[int, int, int, int]:
    table = str(table_config["table"])
    table_columns_order = _get_table_columns(conn, table)
    table_cols = set(table_columns_order)
    if not table_cols:
        _append_warning_once(warnings, f"Table {table} not found in target DB; skipping")
        return 0, 0, 0, 0

    if mode == "overwrite" and not dry_run:
        try:
            conn.execute(f"DELETE FROM {table}")
        except sqlite3.OperationalError:
            _append_warning_once(warnings, f"Table {table} not found in target DB; skipping")
            return 0, 0, 0, 0

    imported = 0
    skipped = 0
    accounts_resolved = 0
    accounts_unresolved = 0

    for raw_row in rows:
        try:
            prepared_row, row_accounts_resolved, row_accounts_unresolved = _prepare_import_row(
                conn,
                table_config,
                raw_row,
                cat_map=cat_map,
                warnings=warnings,
            )
        except sqlite3.OperationalError:
            _append_warning_once(warnings, f"Table {table} not found in target DB; skipping")
            return imported, skipped, accounts_resolved, accounts_unresolved

        if prepared_row is None:
            skipped += 1
            continue

        accounts_resolved += row_accounts_resolved
        accounts_unresolved += row_accounts_unresolved
        insert_row = _build_insert_row(table_config, prepared_row, table_columns_order=table_columns_order, warnings=warnings)

        if mode == "merge":
            try:
                if _row_conflicts(conn, table_config, insert_row, table_cols):
                    skipped += 1
                    continue
            except sqlite3.OperationalError:
                _append_warning_once(warnings, f"Table {table} not found in target DB; skipping")
                return imported, skipped, accounts_resolved, accounts_unresolved

        if dry_run:
            imported += 1
            continue

        try:
            _insert_generic_row(conn, table, insert_row, table_columns_order)
        except sqlite3.IntegrityError as exc:
            if table == "budgets":
                skipped += 1
                _append_warning_once(warnings, f"Skipped overlapping budget during import: {exc}")
                continue
            skipped += 1
            _append_warning_once(warnings, f"Skipped conflicting row for {table}: {exc}")
            continue
        except sqlite3.OperationalError:
            _append_warning_once(warnings, f"Table {table} not found in target DB; skipping")
            return imported, skipped, accounts_resolved, accounts_unresolved

        imported += 1

    return imported, skipped, accounts_resolved, accounts_unresolved


def _copy_file_if_allowed(
    *,
    tar: tarfile.TarFile,
    member_name: str,
    target_path: Path,
    mode: str,
    copied: list[str],
) -> None:
    if mode == "merge" and target_path.exists():
        return
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(_read_tar_member(tar, member_name))
    copied.append(member_name)


def _copy_config_files(
    bundle_path: Path,
    *,
    data_dir: Path,
    mode: str,
    rules_path: Path | None = None,
) -> list[str]:
    copied: list[str] = []
    resolved_data_dir = _normalize_data_dir(data_dir)
    resolved_rules_path = _normalize_rules_path(rules_path, data_dir=resolved_data_dir)

    with tarfile.open(bundle_path, "r:gz") as tar:
        names = set(tar.getnames())
        if "rules.yaml" in names:
            _copy_file_if_allowed(
                tar=tar,
                member_name="rules.yaml",
                target_path=resolved_rules_path,
                mode=mode,
                copied=copied,
            )
            if "rules.yaml" in copied:
                invalidate_rules_cache()

        if "agent_memory.md" in names:
            _copy_file_if_allowed(
                tar=tar,
                member_name="agent_memory.md",
                target_path=resolved_data_dir / "agent_memory.md",
                mode=mode,
                copied=copied,
            )

        sessions_target = resolved_data_dir / "sessions"
        session_members = sorted(
            name
            for name in names
            if name.startswith("sessions/") and len(Path(name).parts) > 1 and not name.endswith("/")
        )
        if mode == "overwrite":
            shutil.rmtree(sessions_target, ignore_errors=True)
        for member_name in session_members:
            relative_path = Path(member_name).relative_to("sessions")
            target_path = sessions_target / relative_path
            if mode == "merge" and target_path.exists():
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(_read_tar_member(tar, member_name))
            copied.append(member_name)

    return copied


def import_preferences(
    bundle_path: Path,
    conn: sqlite3.Connection,
    *,
    mode: str = "merge",
    create_missing_categories: bool = False,
    dry_run: bool = True,
    data_dir: Path | None = None,
    rules_path: Path | None = None,
) -> ImportResult:
    if mode not in {"merge", "overwrite"}:
        raise ValueError("mode must be 'merge' or 'overwrite'")

    resolved_data_dir = _normalize_data_dir(data_dir)
    resolved_rules_path = _normalize_rules_path(rules_path, data_dir=resolved_data_dir)
    validation = validate_bundle(bundle_path, conn)
    if not validation.valid:
        raise ValueError(f"Invalid bundle: {'; '.join(validation.errors)}")

    manifest = validation.manifest or {}
    warnings = list(validation.warnings)

    if mode == "overwrite" and not dry_run:
        try:
            from finance_cli.backup import create_backup
        except ImportError as exc:
            raise RuntimeError("B4 create_backup() not available — build B4 first or skip overwrite mode") from exc
        create_backup(
            conn,
            backup_type="pre_restore",
            data_dir=resolved_data_dir,
            rules_path=resolved_rules_path,
        )

    categories_resolved: dict[str, str] = {}
    categories_missing: list[str] = []
    categories_created: list[str] = []
    for raw_name in manifest.get("categories_referenced", []):
        if not isinstance(raw_name, str) or not raw_name:
            continue
        category_id = _resolve_category_id(conn, raw_name)
        if category_id:
            categories_resolved[raw_name] = category_id
            continue
        if create_missing_categories and not dry_run:
            try:
                created_id = _create_missing_category(conn, raw_name)
            except sqlite3.OperationalError:
                _append_warning_once(warnings, f"Failed creating missing category {raw_name}; categories table unavailable")
                categories_missing.append(raw_name)
            else:
                categories_resolved[raw_name] = created_id
                categories_created.append(raw_name)
            continue
        categories_missing.append(raw_name)

    tables_imported: dict[str, int] = {}
    tables_skipped: dict[str, int] = {}
    accounts_resolved = 0
    accounts_unresolved = 0
    files_copied: list[str] = []

    try:
        with tarfile.open(bundle_path, "r:gz") as tar:
            names = set(tar.getnames())
            for table_config in EXPORT_TABLES:
                table = str(table_config["table"])
                jsonl_name = f"{table}.jsonl"
                if jsonl_name not in names:
                    tables_imported[table] = 0
                    tables_skipped[table] = 0
                    continue
                rows = _parse_jsonl(_read_tar_member(tar, jsonl_name))
                imported, skipped, resolved_count, unresolved_count = _import_table(
                    conn,
                    table_config,
                    rows,
                    cat_map=categories_resolved,
                    mode=mode,
                    dry_run=dry_run,
                    warnings=warnings,
                )
                tables_imported[table] = imported
                tables_skipped[table] = skipped
                accounts_resolved += resolved_count
                accounts_unresolved += unresolved_count

            imported_flags = 0
            skipped_flags = 0
            if "account_business_flags.jsonl" in names:
                flag_rows = _parse_jsonl(_read_tar_member(tar, "account_business_flags.jsonl"))
                account_cols = set(_get_table_columns(conn, "accounts"))
                if account_cols and "is_business" in account_cols:
                    if mode == "overwrite" and not dry_run:
                        try:
                            conn.execute("UPDATE accounts SET is_business = 0 WHERE is_business = 1")
                        except sqlite3.OperationalError:
                            _append_warning_once(warnings, "accounts.is_business not available in target DB; skipping")
                    for flag_row in flag_rows:
                        try:
                            account_id = _resolve_account(
                                conn,
                                str(flag_row.get("institution_name") or ""),
                                flag_row.get("card_ending"),
                                flag_row.get("account_name"),
                            )
                        except sqlite3.OperationalError:
                            _append_warning_once(warnings, "accounts table not available in target DB; skipping flags")
                            skipped_flags = len(flag_rows)
                            break
                        if not account_id:
                            skipped_flags += 1
                            accounts_unresolved += 1
                            _append_warning_once(
                                warnings,
                                f"Account not found in target DB: {flag_row.get('institution_name')} "
                                f"ending {flag_row.get('card_ending')}",
                            )
                            continue
                        accounts_resolved += 1
                        if dry_run:
                            imported_flags += 1
                            continue
                        try:
                            conn.execute(
                                "UPDATE accounts SET is_business = ? WHERE id = ?",
                                (flag_row.get("is_business", 0), account_id),
                            )
                        except sqlite3.OperationalError:
                            _append_warning_once(warnings, "accounts.is_business not available in target DB; skipping")
                            skipped_flags += 1
                            continue
                        imported_flags += 1
                else:
                    skipped_flags = len(flag_rows)
                    _append_warning_once(warnings, "accounts.is_business not available in target DB; skipping")
            tables_imported["account_business_flags"] = imported_flags
            tables_skipped["account_business_flags"] = skipped_flags

        if not dry_run:
            files_copied = _copy_config_files(
                bundle_path,
                data_dir=resolved_data_dir,
                mode=mode,
                rules_path=resolved_rules_path,
            )
            conn.commit()
    except Exception:
        if not dry_run:
            conn.rollback()
        raise

    return ImportResult(
        dry_run=dry_run,
        mode=mode,
        tables_imported=tables_imported,
        tables_skipped=tables_skipped,
        categories_resolved=categories_resolved,
        categories_missing=categories_missing,
        categories_created=categories_created,
        accounts_resolved=accounts_resolved,
        accounts_unresolved=accounts_unresolved,
        files_copied=files_copied,
        warnings=warnings,
    )
