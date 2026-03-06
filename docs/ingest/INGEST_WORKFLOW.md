# Ingest Workflow

Operational playbook for importing transactions from Plaid, PDF statements, and institution CSV exports.

## Current Import Surface

| Path | Command | Best Use |
|---|---|---|
| Plaid sync | `finance_cli plaid sync` | Ongoing automatic updates for linked institutions |
| PDF ingest | `finance_cli ingest statement` | Any PDF statement via AI (default), Azure, or BSC backends |
| Normalized CSV ingest | `finance_cli ingest csv` | Deterministic import when a supported CSV export exists |
| Mixed folder ingest | `finance_cli ingest batch` | Monthly dump-and-go for mixed `.pdf` + `.csv` folders |

Notes:
- `ingest` commands are dry-run by default; add `--commit` to write.
- Legacy `txn import` still exists, but `ingest statement|csv|batch` is the primary workflow.

## Recommended Monthly Runbook

```bash
# 1) Drop statement PDFs + CSV exports into one folder
mkdir -p inbox
cp ~/Downloads/*.pdf ~/Downloads/*.csv inbox/

# 2) Dry-run (preview inserts/skips/errors)
finance_cli ingest batch --dir ./inbox/ --format cli

# 3) Commit write
finance_cli ingest batch --dir ./inbox/ --commit --format cli

# 4) Link accounts and deduplicate
finance_cli dedup backfill-aliases --commit
finance_cli dedup cross-format                  # review first
finance_cli dedup cross-format --commit
```

Commit behavior:
- successful files are moved to `inbox/processed/`
- failed files remain in `inbox/`
- per-file failures are isolated; other files continue
- check `data.errors` in output; batch can return success with file-level failures

## Command Details

### `ingest batch` (mixed folder)

```bash
finance_cli ingest batch --dir ./inbox/ --format cli
finance_cli ingest batch --dir ./inbox/ --commit --format cli
```

Behavior:
- reads top-level `*.pdf` and `*.csv` in the target directory
- routes PDFs through AI statement ingest
- auto-detects CSV institution and normalizes before import
- processes files in sorted filename order

Batch CSV auto-detection currently recognizes:
- `apple_card`
- `barclays`
- `chase_credit`
- `amex`
- `bofa_checking`

Flags:
- `--backend` (`ai`, `azure`, `bsc`) — default `ai`; configurable in `rules.yaml` under `extractors.default_backend`
- `--provider` (`openai` or `claude`) for AI backend PDF parsing
- `--model` model override for AI backend PDF parsing
- `--institution` institution name hint (required for BSC, optional for Azure)
- `--card-ending` card ending hint (recommended for non-AI backends)
- `--max-tokens` output token cap for AI parse
- `--allow-partial` allow PDF imports with blocked rows removed
- `--commit` write mode

Important limits:
- batch mode does not expose statement-only controls like `--replace`, `--require-reconciled`, or `--account-id`

### `ingest statement` (PDF)

```bash
# single file
finance_cli ingest statement --file statement.pdf --format cli

# directory of PDFs
finance_cli ingest statement --dir ./statements/2026-01/ --commit --format cli
```

Behavior:
- requires exactly one of `--file` or `--dir`
- validates output before import (5-gate validation for AI, universal validator for all backends)
- supports strict reconciliation and replace workflows
- requires credentials per backend:
  - AI: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`
  - Azure: `AZURE_DI_ENDPOINT` + `AZURE_DI_API_KEY`
  - BSC: `BSC_API_KEY`

Flags:
- `--backend` (`ai`, `azure`, `bsc`) — default `ai`
- `--provider` (`openai` or `claude`) — AI backend only
- `--model` — AI backend only
- `--institution` — institution name hint (required for BSC)
- `--card-ending` — card ending hint (recommended for non-AI backends)
- `--max-tokens`
- `--allow-partial`
- `--require-reconciled`
- `--replace`
- `--account-id` (must already exist)
- `--commit`

### `ingest csv` (single CSV file)

```bash
finance_cli ingest csv --file export.csv --institution apple_card --format cli
finance_cli ingest csv --file export.csv --institution apple_card --commit --format cli
```

Supported institutions:
- `amex`
- `american_express`
- `apple`
- `apple_card`
- `barclays`
- `bofa_checking`
- `chase_credit`

Notes:
- `--institution` is required for `ingest csv`
- for mixed folders, use `ingest batch` to auto-detect instead

## Daily Plaid Sync

```bash
finance_cli plaid sync --format cli
finance_cli plaid sync --force --format cli
finance_cli plaid sync --item <item_id> --format cli

finance_cli plaid balance-refresh --format cli
finance_cli plaid liabilities-sync --format cli
```

Use Plaid for ongoing linked accounts; use `ingest` commands for backfill, statement-only institutions, and recovery when linking fails.

## Choosing a Path

1. Mixed PDFs + CSVs in one folder: `ingest batch`.
2. Linked institution ongoing updates: `plaid sync`.
3. Single supported CSV export: `ingest csv`.
4. Single PDF statement or unsupported CSV format: `ingest statement`.
5. Need replace/reconciliation/account override controls: `ingest statement` (not batch).

## Backfill Patterns

### Bulk mixed backfill

```bash
mkdir -p inbox
cp ./statements/*.pdf ./exports/*.csv inbox/
finance_cli ingest batch --dir ./inbox/ --commit --format cli
```

### Bulk PDF-only backfill

```bash
finance_cli ingest statement --dir ./statements/ --commit --format cli
```

### Bulk CSV-only backfill

```bash
for f in ./exports/*.csv; do
  finance_cli ingest csv --file "$f" --institution apple_card --commit --format cli
done
```

## Post-Import: Aliases and Cross-Format Dedup

After importing CSV/PDF data alongside Plaid, run these steps to link accounts and remove duplicates:

```bash
# 1) Link hash-based CSV/PDF accounts to their Plaid counterparts
finance_cli dedup backfill-aliases              # dry-run
finance_cli dedup backfill-aliases --commit     # persist

# 2) Find cross-format duplicates (same transaction from different sources)
finance_cli dedup cross-format                  # dry-run
finance_cli dedup cross-format --commit         # deactivate duplicates
```

How it works:
- `backfill-aliases` scans hash-based accounts (from CSV/PDF) and matches them to Plaid accounts using canonicalized institution names and card endings. Aliases are stored in `account_aliases`.
- `cross-format` groups transactions by (account, date, amount) across aliased accounts, then deactivates the lower-priority source. Priority: `csv_import` > `plaid` > `pdf_import`.
- Both commands are idempotent and safe to re-run.

When to run:
- After any CSV/PDF import where the institution is also Plaid-linked (e.g., Barclays, Chase)
- After linking a new institution via Plaid that already has CSV/PDF history
- The monthly runbook should include these as steps 4-5 after `ingest batch`

## Dedup and Idempotency

- Plaid dedupe uses Plaid transaction identity.
- PDF ingest dedupes by file hash + backend and per-row PDF keys (backend-scoped).
- CSV ingest dedupes by normalized row fingerprint keys.
- Cross-format dedup resolves account aliases and matches across sources by (account, date, amount).
- Re-importing the same data is safe; duplicates are skipped.

## Verification

```bash
# Verify statement window
finance_cli txn list --from 2026-01-01 --to 2026-01-31 --format cli

# Inspect recent import batches
sqlite3 "${FINANCE_CLI_DB:-finance_cli/data/finance.db}" \
"SELECT source_type, bank_parser, file_path, imported_count, skipped_count, reconcile_status, created_at
   FROM import_batches
  ORDER BY created_at DESC
  LIMIT 20;"
```

## Adding Institutions

For new CSV adapters, follow `docs/overview/ADD_INSTITUTION_RUNBOOK.md`.
For PDF-only institutions, `ingest statement` works without a new regex parser. Multiple backends can be used to compare extraction quality on the same PDF.
