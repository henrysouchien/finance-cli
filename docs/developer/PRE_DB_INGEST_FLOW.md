# Pre-DB Ingest Flow

This document describes the ingestion pipeline from CLI entry through parsing, normalization, identity, and dedupe checks, up to the write boundary.

## Scope and Boundary

Pre-DB ingest includes:
- command routing and input validation
- file type routing (PDF vs CSV)
- AI parse + validation (PDF path)
- CSV normalization (plus institution detection in batch mode)
- canonical source normalization
- account identity derivation
- dedupe key/fingerprint preparation and duplicate checks

The write boundary is reached inside importer functions:
- `import_extracted_statement(...)` for PDF/AI
- `import_normalized_rows(...)` for CSV

With `dry_run=True` (default when `--commit` is not set), these paths stop before persistence.

## High-Level Flow

```text
CLI ingest command
  -> route by subcommand and file type
  -> parse/normalize into canonical transaction shape
  -> canonicalize institution/source identity
  -> derive effective account identity (override or deterministic hash path)
  -> build dedupe keys/fingerprints and run duplicate checks
  -> dry_run? if yes: return preview
             if no: begin DB writes (transactions + import batch records)
```

## Text Diagram

```text
+-----------------------+
|  finance_cli ingest   |
+-----------------------+
           |
           v
+------------------------------+
| Subcommand + input routing   |
| statement | csv | batch      |
+------------------------------+
      |               \
      |                \ (batch routes per file)
      v                 v
+----------------+   +---------------------------+
| PDF path       |   | CSV path                  |
| ai_parse_...   |   | normalize_csv(...)        |
| + validation   |   | (institution required or  |
| gates          |   | detected in batch mode)   |
+----------------+   +---------------------------+
      |                          |
      v                          v
+-----------------------------------------------+
| Shared importer pre-DB stage                  |
| - canonicalize institution/source identity    |
| - derive effective account identity           |
| - build dedupe keys/fingerprints              |
| - run duplicate checks                        |
+-----------------------------------------------+
                      |
                      v
              +------------------+
              | dry_run ?        |
              +------------------+
               |              |
               | yes          | no (`--commit`)
               v              v
     +----------------+   +----------------------+
     | preview/report |   | DB writes begin      |
     | no persistence |   | txns + import batch  |
     +----------------+   +----------------------+
```

## Command Paths

### 1) `ingest statement` (PDF, single file or directory)

1. Validate exactly one of `--file` or `--dir`.
2. Validate `--account-id` (if provided) exists before processing.
3. Resolve AI runtime config (provider/model/thresholds) from CLI plus config.
4. For each PDF:
   - run `ai_parse_statement(...)`
   - convert + validate via `ai_result_to_extract_result(...)`
   - apply `--allow-partial` and `--require-reconciled` gates
5. Hand off to `import_extracted_statement(...)` with `dry_run=not --commit`.

### 2) `ingest csv` (single CSV)

1. Require `--file` and `--institution`.
2. Validate institution support and normalize with `normalize_csv(...)`.
3. Produce canonical rows plus `source_name`.
4. Hand off to `import_normalized_rows(...)` with `dry_run=not --commit`.

### 3) `ingest batch` (mixed PDF + CSV directory)

1. Require `--dir`, enumerate `.pdf` and `.csv`.
2. Reuse one resolved AI runtime config for all PDF files.
3. Route each file:
   - PDFs use the statement pipeline (`_process_statement_file`)
   - CSVs use the CSV pipeline (`_process_csv_file`)
4. Aggregate per-file results; on commit, processed files are moved after successful processing.

## Shared Identity and Account Derivation

The shared importer layer canonicalizes institution names before account derivation.

Current behavior:
- institution aliases are normalized to a canonical base label (for example, BofA variants -> `Bank of America`)
- account IDs are deterministic from canonical source + card ending via `_account_id_for_source(...)`
- if `--account-id` is passed on statement ingest, that explicit ID is used instead of derived identity

This keeps CSV, PDF, and batch paths aligned on account identity and dedupe behavior.

## Dedupe Preparation (Before Writes)

CSV path:
- builds normalized fingerprints from canonical transaction fields
- uses duplicate ordinals so repeated same-value rows are stable

PDF path:
- computes file hash and per-row keys (`pdf:<sha256>:<row_index>`)
- supports replace behavior in commit mode when requested on statement ingest

In both paths, duplicate checks execute before writes and are the terminal step in dry-run mode.

## Flag Effects on Pre-DB Flow

- `--commit`: toggles `dry_run`; without it, pipeline stops before writes.
- `--allow-partial` (statement): allows import of valid rows when some rows are blocked.
- `--require-reconciled` (statement): blocks file before import if reconciliation fails.
- `--account-id` (statement): bypasses derived account mapping, but must reference an existing account.

## Key Files

- `finance_cli/commands/ingest.py`
- `finance_cli/ai_statement_parser.py`
- `finance_cli/importers/csv_normalizers.py`
- `finance_cli/importers/pdf.py`
- `finance_cli/importers/__init__.py`
- `finance_cli/institution_names.py`
- `finance_cli/tests/test_ingest_cmd.py`
