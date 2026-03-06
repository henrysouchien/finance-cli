"""Statement and CSV ingest commands."""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from ..ai_statement_parser import DEFAULT_MAX_TEXT_CHARS, DEFAULT_MAX_TOKENS, _default_model
from ..db import backup_database
from ..extractors import EXTRACTOR_BACKENDS, ExtractOptions, get_extractor
from ..importers import detect_csv_institution, import_normalized_rows, normalize_csv
from ..importers.pdf import import_extracted_statement
from ..ingest_validation import validate_extract_result
from ..user_rules import UserRules, load_rules

logger = logging.getLogger(__name__)

_DEFAULT_CONFIDENCE_WARN = 0.80
_DEFAULT_CONFIDENCE_BLOCK = 0.60


def register(subparsers, format_parent) -> None:
    ingest_parser = subparsers.add_parser("ingest", parents=[format_parent], help="Import statements and CSV exports")
    ingest_sub = ingest_parser.add_subparsers(dest="ingest_command", required=True)

    p_stmt = ingest_sub.add_parser(
        "statement",
        parents=[format_parent],
        help="Parse and import a PDF statement",
    )
    p_stmt.add_argument("--file", help="Path to a single PDF statement")
    p_stmt.add_argument("--dir", help="Directory of PDF statements to batch-process")
    p_stmt.add_argument("--backend", choices=list(EXTRACTOR_BACKENDS), help="Extractor backend")
    p_stmt.add_argument("--provider", help="AI provider: claude or openai (AI backend only)")
    p_stmt.add_argument("--model", help="Model name override (AI backend only)")
    p_stmt.add_argument("--max-tokens", type=int, help="Max output tokens for AI response (default: 16384)")
    p_stmt.add_argument("--institution", help="Institution hint (for non-AI backends)")
    p_stmt.add_argument("--card-ending", help="Card ending hint (for non-AI backends)")
    p_stmt.add_argument("--commit", action="store_true", help="Write to DB (default is dry-run)")
    p_stmt.add_argument("--replace", action="store_true", help="Replace previously imported data for same file")
    p_stmt.add_argument(
        "--allow-partial",
        action="store_true",
        help="Import unblocked rows even when some are confidence-blocked",
    )
    p_stmt.add_argument(
        "--require-reconciled",
        action="store_true",
        help="Fail when statement totals mismatch extracted totals",
    )
    p_stmt.add_argument("--account-id", help="Account ID to tag transactions with")
    p_stmt.set_defaults(func=handle_ingest_statement, command_name="ingest.statement")

    p_csv = ingest_sub.add_parser(
        "csv",
        parents=[format_parent],
        help="Import institution CSV export via normalizer",
    )
    p_csv.add_argument("--file", required=True, help="Path to CSV export file")
    p_csv.add_argument(
        "--institution",
        required=True,
        help="Institution name (apple_card, barclays, etc.)",
    )
    p_csv.add_argument("--commit", action="store_true", help="Write to DB (default is dry-run)")
    p_csv.set_defaults(func=handle_ingest_csv, command_name="ingest.csv")

    p_batch = ingest_sub.add_parser(
        "batch",
        parents=[format_parent],
        help="Process a folder of mixed PDF and CSV files",
    )
    p_batch.add_argument("--dir", required=True, help="Directory containing PDF/CSV files")
    p_batch.add_argument("--backend", choices=list(EXTRACTOR_BACKENDS), help="Extractor backend for PDFs")
    p_batch.add_argument("--provider", help="AI provider for PDF parsing: claude or openai")
    p_batch.add_argument("--model", help="Model name override for PDF parsing")
    p_batch.add_argument("--max-tokens", type=int, help="Max output tokens for AI response (default: 16384)")
    p_batch.add_argument("--institution", help="Institution hint (for non-AI backends)")
    p_batch.add_argument("--card-ending", help="Card ending hint (for non-AI backends)")
    p_batch.add_argument("--commit", action="store_true", help="Write to DB (default is dry-run)")
    p_batch.add_argument(
        "--allow-partial",
        action="store_true",
        help="For PDFs: import unblocked rows when some are confidence-blocked",
    )
    p_batch.set_defaults(func=handle_ingest_batch, command_name="ingest.batch")


def _coerce_positive_int(raw: Any, key: str, default: int) -> int:
    value = default if raw is None else raw
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ai_parser.{key}: expected integer > 0, got {value!r}") from exc
    if out <= 0:
        raise ValueError(f"Invalid ai_parser.{key}: expected integer > 0, got {value!r}")
    return out


def _coerce_unit_float(raw: Any, key: str, default: float) -> float:
    value = default if raw is None else raw
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ai_parser.{key}: expected number in [0, 1], got {value!r}") from exc
    if out < 0 or out > 1:
        raise ValueError(f"Invalid ai_parser.{key}: expected number in [0, 1], got {value!r}")
    return out


def _resolve_ai_runtime_config(args, rules: UserRules) -> tuple[str, str, int, int, float, float]:
    ai_parser = rules.ai_parser if isinstance(rules.ai_parser, dict) else {}

    provider_from_cli = args.provider is not None and str(args.provider).strip() != ""
    raw_provider = args.provider if provider_from_cli else ai_parser.get("provider")
    provider = str(raw_provider or "").strip().lower()
    if not provider:
        raise ValueError("AI provider is required; set --provider or ai_parser.provider in rules.yaml")

    cli_model = str(args.model or "").strip()
    if cli_model:
        model = cli_model
    elif provider_from_cli:
        model = _default_model(provider)
    else:
        config_model = str(ai_parser.get("model") or "").strip()
        model = config_model or _default_model(provider)

    max_text_chars = _coerce_positive_int(ai_parser.get("max_text_chars"), "max_text_chars", DEFAULT_MAX_TEXT_CHARS)
    cli_max_tokens = getattr(args, "max_tokens", None)
    if cli_max_tokens is not None:
        max_tokens = cli_max_tokens
    else:
        max_tokens = _coerce_positive_int(ai_parser.get("max_tokens"), "max_tokens", DEFAULT_MAX_TOKENS)

    confidence_warn = _coerce_unit_float(ai_parser.get("confidence_warn"), "confidence_warn", _DEFAULT_CONFIDENCE_WARN)
    confidence_block = _coerce_unit_float(ai_parser.get("confidence_block"), "confidence_block", _DEFAULT_CONFIDENCE_BLOCK)
    if confidence_warn < confidence_block:
        raise ValueError("Invalid ai_parser confidence thresholds: confidence_warn must be >= confidence_block")

    return provider, model, max_text_chars, max_tokens, confidence_warn, confidence_block


def _resolve_backend(args, rules: UserRules) -> str:
    cli_backend = str(getattr(args, "backend", "") or "").strip().lower()
    if cli_backend:
        backend = cli_backend
    else:
        extractors_cfg = rules.extractors if isinstance(rules.extractors, dict) else {}
        backend = str(extractors_cfg.get("default_backend") or "ai").strip().lower() or "ai"

    if backend not in EXTRACTOR_BACKENDS:
        supported = ", ".join(EXTRACTOR_BACKENDS)
        raise ValueError(f"Unsupported extractor backend '{backend}'. Supported: {supported}")
    return backend


def _build_extractor_config(backend: str, args, rules: UserRules) -> dict[str, Any]:
    extractors_cfg = rules.extractors if isinstance(rules.extractors, dict) else {}

    if backend == "ai":
        provider, model, max_text_chars, max_tokens, confidence_warn, confidence_block = _resolve_ai_runtime_config(
            args,
            rules,
        )
        return {
            "provider": provider,
            "model": model,
            "max_text_chars": max_text_chars,
            "max_tokens": max_tokens,
            "confidence_warn": confidence_warn,
            "confidence_block": confidence_block,
        }

    backend_cfg = extractors_cfg.get(backend) if isinstance(extractors_cfg.get(backend), dict) else {}
    return dict(backend_cfg)


def _validate_file_dir_args(file_arg: str | None, dir_arg: str | None) -> None:
    has_file = bool(str(file_arg or "").strip())
    has_dir = bool(str(dir_arg or "").strip())
    if has_file == has_dir:
        raise ValueError("Exactly one of --file or --dir must be provided")


def _assert_account_exists(conn: sqlite3.Connection, account_id: str | None) -> None:
    if not account_id:
        return
    row = conn.execute("SELECT id FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if not row:
        raise ValueError(f"Account '{account_id}' not found")


def _process_statement_file(
    conn: sqlite3.Connection,
    *,
    file_path: Path,
    backend: str,
    extractor_config: dict[str, Any],
    commit: bool,
    replace_existing_hash: bool,
    allow_partial: bool,
    require_reconciled: bool,
    institution_hint: str | None,
    card_ending_hint: str | None,
    account_id: str | None,
    auto_commit: bool = True,
) -> dict[str, Any]:
    extractor = get_extractor(backend, extractor_config)
    options = ExtractOptions(
        allow_partial=allow_partial,
        require_reconciled=require_reconciled,
        institution_hint=institution_hint,
        card_ending_hint=card_ending_hint,
    )
    output = extractor.extract(file_path, options)

    errors, warnings = validate_extract_result(output.result)
    if errors:
        raise ValueError(f"Extraction validation failed: {'; '.join(errors)}")
    output.result.warnings.extend(warnings)

    if require_reconciled and output.meta.reconcile_status != "matched":
        raise ValueError(
            f"reconciliation status is '{output.meta.reconcile_status}' and require_reconciled=True"
        )

    logger.info(
        "Parsed statement file=%s backend=%s provider=%s model=%s transaction_count=%s reconcile_status=%s",
        file_path,
        output.meta.backend,
        output.meta.provider,
        output.meta.model_version,
        len(output.result.transactions),
        output.meta.reconcile_status,
    )

    validation_json = (
        json.dumps(output.meta.validation_summary, ensure_ascii=True)
        if output.meta.validation_summary is not None
        else None
    )
    import_report = import_extracted_statement(
        conn,
        extracted=output.result,
        file_path=file_path,
        bank_parser=output.meta.bank_parser_label,
        dry_run=not commit,
        replace_existing_hash=replace_existing_hash,
        account_id=account_id,
        content_text=output.meta.content_text,
        ai_raw_output_json=output.meta.raw_api_response,
        ai_validation_json=validation_json,
        ai_model=output.meta.model_version,
        ai_prompt_version=output.meta.ai_prompt_version,
        ai_prompt_hash=output.meta.ai_prompt_hash,
        auto_commit=auto_commit,
    )

    inserted = int(import_report.get("inserted", 0))
    skipped_duplicates = int(import_report.get("skipped_duplicates", 0))
    transaction_count = len(output.result.transactions)

    data: dict[str, Any] = {
        "file": str(file_path),
        "backend": output.meta.backend,
        "provider": output.meta.provider,
        "model": output.meta.model_version,
        "validation": output.meta.validation_summary,
        "transaction_count": transaction_count,
        "reconciled": output.meta.reconcile_status == "matched",
        "reconcile_status": output.meta.reconcile_status,
        "inserted": inserted,
        "skipped_duplicates": skipped_duplicates,
        "dry_run": not commit,
        "input_tokens": int(output.meta.input_tokens),
        "output_tokens": int(output.meta.output_tokens),
        "elapsed_ms": int(output.meta.elapsed_ms),
        "warnings": list(import_report.get("warnings") or []),
    }

    if bool(import_report.get("already_imported", False)):
        data["already_imported"] = True

    return {
        "data": data,
        "summary": {"total_transactions": transaction_count, "total_amount": 0},
        "cli_report": (
            f"file={file_path.name} backend={output.meta.backend} provider={output.meta.provider} "
            f"model={output.meta.model_version} txns={transaction_count} "
            f"reconcile_status={output.meta.reconcile_status} inserted={inserted} dry_run={not commit} "
            f"tokens=in:{int(output.meta.input_tokens)}/out:{int(output.meta.output_tokens)} "
            f"elapsed={int(output.meta.elapsed_ms)}ms"
        ),
    }


def _process_csv_file(
    conn: sqlite3.Connection,
    *,
    file_path: Path,
    commit: bool,
    auto_commit: bool = True,
) -> dict[str, Any]:
    institution = detect_csv_institution(file_path)
    if institution is None:
        raise ValueError(
            f"Could not detect institution for {file_path.name}. "
            "Use ingest csv --institution for this file."
        )
    logger.info("Detected CSV institution file=%s institution=%s", file_path, institution)

    normalized = normalize_csv(file_path, institution)
    report = import_normalized_rows(
        conn,
        normalized.rows,
        normalized.source_name,
        dry_run=not commit,
        file_path=file_path,
        auto_commit=auto_commit,
    )

    data = {
        "file": str(file_path),
        "institution": institution,
        "source_name": normalized.source_name,
        "raw_row_count": normalized.raw_row_count,
        "skipped_row_count": normalized.skipped_row_count,
        "inserted": report.inserted,
        "skipped_duplicates": report.skipped_duplicates,
        "errors": report.errors,
        "dry_run": not commit,
        "warnings": normalized.warnings,
    }

    cli_lines = [
        (
            f"file={file_path.name} institution={institution} rows={normalized.raw_row_count} "
            f"inserted={report.inserted} skipped_duplicates={report.skipped_duplicates} "
            f"errors={report.errors} dry_run={not commit}"
        )
    ]
    if normalized.warnings:
        cli_lines.append(f"Warnings ({len(normalized.warnings)}):")
        for warning in normalized.warnings:
            cli_lines.append(f"  - {warning}")

    return {
        "data": data,
        "summary": {"total_transactions": report.inserted, "total_amount": 0},
        "cli_report": "\n".join(cli_lines),
    }


def handle_ingest_statement(args, conn: sqlite3.Connection) -> dict[str, Any]:
    _validate_file_dir_args(args.file, args.dir)
    _assert_account_exists(conn, args.account_id)

    backup_path: str | None = None
    if args.commit and args.replace:
        backup_path = str(backup_database(conn=conn))

    rules = load_rules()
    backend = _resolve_backend(args, rules)
    extractor_config = _build_extractor_config(backend, args, rules)

    if args.dir:
        folder = Path(args.dir)
        if not folder.exists() or not folder.is_dir():
            raise FileNotFoundError(f"Directory not found: {folder}")

        files = sorted(path for path in folder.glob("*.pdf") if path.is_file())
        if not files:
            raise ValueError(f"No PDF files found in {folder}")

        inserted = 0
        skipped_duplicates = 0
        failures = 0
        total_input_tokens = 0
        total_output_tokens = 0
        total_elapsed_ms = 0
        reports: list[dict[str, Any]] = []

        for file_path in files:
            savepoint_active = False
            if args.commit:
                conn.execute("SAVEPOINT file_ingest")
                savepoint_active = True
            try:
                report = _process_statement_file(
                    conn,
                    file_path=file_path,
                    backend=backend,
                    extractor_config=extractor_config,
                    commit=args.commit,
                    replace_existing_hash=args.replace,
                    allow_partial=args.allow_partial,
                    require_reconciled=args.require_reconciled,
                    institution_hint=args.institution,
                    card_ending_hint=args.card_ending,
                    account_id=args.account_id,
                    auto_commit=not args.commit,
                )
                if savepoint_active:
                    conn.execute("RELEASE SAVEPOINT file_ingest")
                    savepoint_active = False
                file_data = dict(report.get("data", {}))
                reports.append(file_data)
                inserted += int(file_data.get("inserted", 0))
                skipped_duplicates += int(file_data.get("skipped_duplicates", 0))
                total_input_tokens += int(file_data.get("input_tokens", 0))
                total_output_tokens += int(file_data.get("output_tokens", 0))
                total_elapsed_ms += int(file_data.get("elapsed_ms", 0))
            except Exception as exc:
                if savepoint_active:
                    conn.execute("ROLLBACK TO SAVEPOINT file_ingest")
                    conn.execute("RELEASE SAVEPOINT file_ingest")
                    logger.warning(
                        "Rolled back statement ingest savepoint file=%s error=%s",
                        file_path,
                        exc,
                    )
                failures += 1
                reports.append({"file": str(file_path), "error": str(exc)})

        data: dict[str, Any] = {
            "reports": reports,
            "inserted": inserted,
            "skipped_duplicates": skipped_duplicates,
            "errors": failures,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_elapsed_ms": total_elapsed_ms,
        }
        if backup_path:
            data["backup_path"] = backup_path

        return {
            "data": data,
            "summary": {"total_transactions": inserted, "total_amount": 0},
            "cli_report": (
                f"inserted={inserted} skipped_duplicates={skipped_duplicates} "
                f"errors={failures} files={len(files)} "
                f"tokens=in:{total_input_tokens}/out:{total_output_tokens} elapsed={total_elapsed_ms}ms"
            ),
        }

    file_path = Path(args.file)
    result = _process_statement_file(
        conn,
        file_path=file_path,
        backend=backend,
        extractor_config=extractor_config,
        commit=args.commit,
        replace_existing_hash=args.replace,
        allow_partial=args.allow_partial,
        require_reconciled=args.require_reconciled,
        institution_hint=args.institution,
        card_ending_hint=args.card_ending,
        account_id=args.account_id,
    )
    if backup_path:
        data = dict(result.get("data") or {})
        data["backup_path"] = backup_path
        result["data"] = data

    # Move file to processed/ on successful commit (skip for --replace and already-imported)
    if args.commit and not args.replace and file_path.exists():
        result_data = result.get("data") or {}
        already_imported = result_data.get("already_imported", False)
        if not already_imported and int(result_data.get("inserted", 0)) > 0:
            processed_dir = file_path.parent / "processed"
            processed_dir.mkdir(exist_ok=True)
            destination = processed_dir / file_path.name
            try:
                if destination.exists():
                    raise FileExistsError(f"destination already exists: {destination}")
                file_path.rename(destination)
            except OSError as exc:
                logger.warning(
                    "Failed to move processed file source=%s destination=%s error=%s",
                    file_path,
                    destination,
                    exc,
                )

    return result


def handle_ingest_batch(args, conn: sqlite3.Connection) -> dict[str, Any]:
    folder = Path(args.dir)
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Directory not found: {folder}")

    pdf_files = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".pdf"]
    csv_files = [path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".csv"]
    files = sorted(pdf_files + csv_files, key=lambda path: path.name)
    if not files:
        raise ValueError(f"No PDF or CSV files found in {folder}")

    logger.info(
        "Starting ingest batch dir=%s total_files=%s pdf_files=%s csv_files=%s dry_run=%s",
        folder,
        len(files),
        len(pdf_files),
        len(csv_files),
        not args.commit,
    )

    inserted = 0
    skipped_duplicates = 0
    failures = 0
    total_input_tokens = 0
    total_output_tokens = 0
    total_elapsed_ms = 0
    reports: list[dict[str, Any]] = []
    processed_files: list[Path] = []
    move_error_files: list[dict[str, str]] = []
    backend_config: tuple[str, dict[str, Any]] | None = None

    for file_path in files:
        savepoint_active = False
        if args.commit:
            conn.execute("SAVEPOINT file_ingest")
            savepoint_active = True
        try:
            logger.info("Processing ingest batch file=%s extension=%s", file_path, file_path.suffix.lower())
            if file_path.suffix.lower() == ".pdf":
                if backend_config is None:
                    rules = load_rules()
                    backend = _resolve_backend(args, rules)
                    config = _build_extractor_config(backend, args, rules)
                    backend_config = (backend, config)

                report = _process_statement_file(
                    conn,
                    file_path=file_path,
                    backend=backend_config[0],
                    extractor_config=backend_config[1],
                    commit=args.commit,
                    replace_existing_hash=False,
                    allow_partial=args.allow_partial,
                    require_reconciled=False,
                    institution_hint=args.institution,
                    card_ending_hint=args.card_ending,
                    account_id=None,
                    auto_commit=not args.commit,
                )
            elif file_path.suffix.lower() == ".csv":
                report = _process_csv_file(
                    conn,
                    file_path=file_path,
                    commit=args.commit,
                    auto_commit=not args.commit,
                )
            else:
                if savepoint_active:
                    conn.execute("RELEASE SAVEPOINT file_ingest")
                continue

            if savepoint_active:
                conn.execute("RELEASE SAVEPOINT file_ingest")
                savepoint_active = False
            file_data = dict(report.get("data", {}))
            reports.append(file_data)
            inserted += int(file_data.get("inserted", 0))
            skipped_duplicates += int(file_data.get("skipped_duplicates", 0))
            total_input_tokens += int(file_data.get("input_tokens", 0))
            total_output_tokens += int(file_data.get("output_tokens", 0))
            total_elapsed_ms += int(file_data.get("elapsed_ms", 0))
            processed_files.append(file_path)
        except Exception as exc:
            if savepoint_active:
                conn.execute("ROLLBACK TO SAVEPOINT file_ingest")
                conn.execute("RELEASE SAVEPOINT file_ingest")
                logger.warning("Rolled back ingest batch savepoint file=%s error=%s", file_path, exc)
            failures += 1
            logger.warning("Ingest batch file failed file=%s error=%s", file_path, exc)
            reports.append({"file": str(file_path), "error": str(exc)})

    if args.commit and processed_files:
        processed_dir = folder / "processed"
        processed_dir.mkdir(exist_ok=True)
        for fp in processed_files:
            destination = processed_dir / fp.name
            try:
                if destination.exists():
                    raise FileExistsError(f"destination already exists: {destination}")
                fp.rename(destination)
            except OSError as exc:
                logger.warning(
                    "Failed to move processed ingest file source=%s destination=%s error=%s",
                    fp,
                    destination,
                    exc,
                )
                move_error_files.append({"file": str(fp), "error": str(exc)})

    move_errors = len(move_error_files)

    cli_lines = [
        (
            f"inserted={inserted} skipped_duplicates={skipped_duplicates} "
            f"errors={failures} ingest_errors={failures} move_errors={move_errors} "
            f"files={len(files)} dry_run={not args.commit} "
            f"tokens=in:{total_input_tokens}/out:{total_output_tokens} elapsed={total_elapsed_ms}ms"
        )
    ]
    for report in reports:
        file_name = Path(str(report.get("file", ""))).name
        if report.get("error"):
            cli_lines.append(f"  {file_name}: ERROR: {report['error']}")
            continue
        cli_lines.append(
            (
                f"  {file_name}: inserted={int(report.get('inserted', 0))} "
                f"skipped_duplicates={int(report.get('skipped_duplicates', 0))} "
                f"errors={int(report.get('errors', 0))} "
                f"tokens=in:{int(report.get('input_tokens', 0))}/out:{int(report.get('output_tokens', 0))} "
                f"elapsed={int(report.get('elapsed_ms', 0))}ms"
            )
        )
        for warning in report.get("warnings") or []:
            cli_lines.append(f"    WARNING: {warning}")
    for move_error in move_error_files:
        move_name = Path(move_error["file"]).name
        cli_lines.append(f"  {move_name}: MOVE_ERROR: {move_error['error']}")

    return {
        "data": {
            "reports": reports,
            "inserted": inserted,
            "skipped_duplicates": skipped_duplicates,
            "errors": failures,
            "ingest_errors": failures,
            "move_errors": move_errors,
            "move_error_files": move_error_files,
            "files": len(files),
            "dry_run": not args.commit,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_elapsed_ms": total_elapsed_ms,
        },
        "summary": {"total_transactions": inserted, "total_amount": 0},
        "cli_report": "\n".join(cli_lines),
    }


def handle_ingest_csv(args, conn: sqlite3.Connection) -> dict[str, Any]:
    file_path = Path(args.file)
    if not file_path.exists():
        raise FileNotFoundError(f"CSV not found: {file_path}")

    normalized = normalize_csv(file_path, args.institution)
    report = import_normalized_rows(
        conn,
        normalized.rows,
        normalized.source_name,
        dry_run=not args.commit,
        file_path=file_path,
    )

    data = {
        "file": str(file_path),
        "institution": args.institution,
        "source_name": normalized.source_name,
        "raw_row_count": normalized.raw_row_count,
        "skipped_row_count": normalized.skipped_row_count,
        "inserted": report.inserted,
        "skipped_duplicates": report.skipped_duplicates,
        "errors": report.errors,
        "dry_run": not args.commit,
        "warnings": normalized.warnings,
    }

    cli_lines = [
        (
            f"file={file_path.name} institution={args.institution} rows={normalized.raw_row_count} "
            f"inserted={report.inserted} skipped_duplicates={report.skipped_duplicates} "
            f"errors={report.errors} dry_run={not args.commit}"
        )
    ]
    if normalized.warnings:
        cli_lines.append(f"Warnings ({len(normalized.warnings)}):")
        for warning in normalized.warnings:
            cli_lines.append(f"  - {warning}")

    return {
        "data": data,
        "summary": {"total_transactions": report.inserted, "total_amount": 0},
        "cli_report": "\n".join(cli_lines),
    }
