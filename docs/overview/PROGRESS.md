# Project Progress & Current State

Last updated: 2026-02-22 (post E2E Run 4)

## What This Project Is

Personal finance CLI tool (Mint replacement). SQLite storage, integer cents for all money, structured JSON output. Supports Plaid integration, PDF/CSV import, AI categorization, budgets, and subscriptions.

Key files: `docs/overview/PROJECT_GUIDE.md` (quick guide), `docs/overview/HOW_IT_WORKS.md` (authoritative architecture), `docs/planning/BUG_BACKLOG.md` (open bugs).

## Completed Work (Chronological)

### Phases 1–4: Core CLI (Pre-2026)
- Transaction CRUD, search (FTS5), CSV import, categorization with vendor memory
- Budget engine with forecasting, weekly summaries, daily rundown
- Subscription detection, liquidity calculator, monthly planning
- User rules system (keyword rules, split rules, category overrides, aliases)
- AI categorizer (Claude + OpenAI, batch processing, auto-remember)
- PDF statement importers: Chase (checking/credit), BofA (checking/credit), Schwab (checking), Barclays, Citi, Apple Card
- Income CSV import, Wave accounting export

### Phase 5: Plaid Integration (Feb 2026)
- Full Plaid client: link/sync/status/unlink + products backfill
- Balance refresh + liabilities sync lifecycle
- Institution linking for 7 live institutions
- Plan: `docs/completed/BALANCES_LIABILITIES_PLAN.md`

### Plaid API Cooldown (Feb 2026)
- Timestamp-based cooldown on sync/balance/liabilities API calls
- Per-item `last_sync_at`, `last_balance_refresh_at`, `last_liabilities_fetch_at` columns
- `--force` flag to bypass, env var overrides, error-status bypass
- Smoke-tested live: cooldown skips work, force bypasses work
- Plan: `docs/completed/COOLDOWN_PLAN.md`
- 97 tests at completion

### AI Statement Parser — Phase 0 of Export/Ingest (Feb 2026)
- AI-powered PDF statement parsing via Claude/OpenAI (parallel to existing regex parsers)
- 5-gate validation framework: schema → field → semantic → reconciliation → confidence
- `import_extracted_statement()` with replace-existing-hash reimport mode
- Migration 006: AI audit columns on `import_batches`
- Plan: `docs/completed/AI_PARSER_PLAN.md` (9 review rounds, 24 issues found and fixed)
- 156 tests at completion

**Smoke test results (gpt-4o):**

| Statement | Bank | Txns | Accuracy | Verified Against |
|-----------|------|------|----------|-----------------|
| Apple Card Jan 2026 | Apple | 27 | 100% | CSV export (exact match) |
| Barclays Jan-Feb 2026 | Barclays | 6 | 100% | CSV export (exact match) |
| Citi Jan-Feb 2026 | Citi | 2 | 100% | Spot-checked |
| Bloomingdale's Jan 2026 | Bloomingdale's | 2 | 100% | PDF summary (verified) |

Key findings from smoke tests:
- Sign convention works correctly (negative=expense, positive=payment/refund)
- Interest charges, fees, and adjustments extracted from separate PDF sections
- Schema was overhauled from `statement_total` to explicit totals (`new_balance`, `total_charges`, `total_payments`) to avoid sign ambiguity
- gpt-4o produces high-quality output with all confidence scores at 1.0
- Prompt was updated to explicitly request interest/fees/adjustments after initial test missed an interest charge

### CSV Normalization Adapters — Phase 4A of Export/Ingest (Feb 2026)
- Institution-specific CSV export normalizers: Apple Card, Barclays
- Apple Card: negate-all-amounts sign convention, payment detection via `Type == "Payment"`
- Barclays: robust header scanning, card ending extraction from account number, payment detection via description
- Refactored `import_csv()` into shared `_import_row_iter()` + new `import_normalized_rows()`
- Fixed dry-run side effects: account/category creation no longer occurs during dry-run
- Plan: `docs/completed/CSV_NORMALIZERS_PLAN.md` (3 review rounds, 4 issues found and fixed)
- 195 tests at completion

**Smoke test results:**

| Export | Institution | Rows | Skipped | Warnings | Key Checks |
|--------|------------|------|---------|----------|------------|
| Apple Card (14 months) | Apple | 349 | 0 | 0 | 14 payments positive, 13 interest negative, refunds not mislabeled as payments |
| Barclays (2 years) | Barclays | 326 | 0 | 0 | Card ending XXXX parsed, 27 payments detected, 3 non-payment credits correctly excluded |

### Ingest CLI Command — Phase 3a of Export/Ingest (Feb 2026)
- `ingest statement` command wiring AI parser into CLI workflow
- Dry-run by default, `--commit` to write, `--replace` for re-import
- Provider-first config cascade: CLI flags > rules.yaml > defaults (prevents cross-provider model mismatch)
- Config knob forwarding: `max_text_chars`, `confidence_warn`, `confidence_block` from rules.yaml
- Batch `--dir` support, `--allow-partial`, `--require-reconciled`, `--account-id`
- Enhanced confidence calibration in AI prompt + smarter all-null/zero handling
- Plan: `docs/completed/INGEST_CLI_PLAN.md` (5 review rounds, 14 issues found and fixed)
- 195 tests at completion

**Smoke test results (gpt-4o via `ingest statement` CLI):**

| Statement | Bank | Txns | Result |
|-----------|------|------|--------|
| Apple Card Jan 2026 | Apple | 27 | Dry-run success, matches Phase 0 smoke test |
| Barclays Jan-Feb 2026 | Barclays | 6 | Dry-run success, matches Phase 0 smoke test |

### CSV Audit Parity — Phase 1 of Export/Ingest (Feb 2026)
- `import_csv()` and `import_normalized_rows()` now write `import_batches` entries
- File-hash dedup: batch INSERT is idempotent (one `import_batches` row per unique file)
- Row-level `dedupe_key` dedup preserved unchanged (per-row `skipped_duplicates` counts)
- `import_normalized_rows()` accepts `file_path` as keyword-only param for batch audit
- Phase 2 (`--require-reconciled`) already implemented in `ingest` CLI command
- Plan: `docs/completed/CSV_AUDIT_PARITY_PLAN.md` (3 review rounds, 3 issues found and fixed)
- 201 tests at completion

### Ingest CSV CLI — Phase 3b of Export/Ingest (Feb 2026)
- `ingest csv --file X --institution apple_card [--commit]` subcommand
- Wires CSV normalizers into CLI: `normalize_csv()` → `import_normalized_rows()` with `file_path=` for batch audit
- Dry-run by default, `--commit` to write; `--file` and `--institution` both required
- Plan: `docs/completed/INGEST_CSV_PLAN.md`
- 208 tests at completion

**Smoke test results:**

| Export | Institution | Rows | Result |
|--------|------------|------|--------|
| Apple Card (14 months) | apple_card | 349 | Dry-run success, 0 skipped, 0 warnings |
| Barclays (2 years) | barclays | 326 | Dry-run success, 0 skipped, 0 warnings |

### Cross-Format Dedup (Feb 2026)
- `dedup cross-format` command: finds and resolves duplicate transactions across CSV, PDF, and Plaid imports
- Institution canonicalization map: normalizes AI free-form institution names to match CSV normalizer canonical names
- Statement-level `card_ending` in AI prompt schema + cascade: canonical > statement > account_label last-4 > per-txn
- PDF `import_extracted_statement()` auto-derives `account_id` when not provided, using same `_get_or_create_account()` as CSV
- Matching: group by `(account_id, date, amount_cents)` across sources, verify with normalized description (exact/substring)
- Source preference: `csv_import` > `plaid` > `pdf_import`; soft-delete loser (`is_active=0`), preserve `import_batches` metadata
- Dry-run by default, `--commit` to apply, optional `--account-id`/`--from`/`--to` scope filters
- Plan: `docs/completed/CROSS_FORMAT_DEDUP_PLAN.md` (5 review rounds, 8 blocking issues found and fixed)
- 243 tests at completion

**Smoke test results:**

| Scenario | Matches | Result |
|----------|---------|--------|
| Apple Card CSV (349 rows) + 5 synthetic PDF duplicates | 5 exact | CSV kept, PDF soft-deleted, idempotent rerun finds 0 |

### Ingest Batch — Dump-and-Go Workflow (Feb 2026)
- `ingest batch --dir ./inbox/ [--commit]` — single command processes mixed PDF + CSV folders
- Auto-detects CSV institution from file content (Apple Card via `"Amount (USD)"` header, Barclays via preamble)
- Routes: `.pdf` → AI parser, `.csv` → normalizer → import
- Lazy AI config resolution: CSV-only batches skip AI setup entirely
- Per-file error isolation: one failure doesn't stop the batch
- Auto-archive: successfully processed files move to `inbox/processed/` on commit
- Plan: `docs/completed/INGEST_BATCH_PLAN.md` (5 review rounds, 8 issues found and fixed)
- 265 tests at completion

### End-to-End Import (Feb 2026)
All real transaction files imported into the database via `ingest batch`:

| Source | Transactions |
|--------|-------------|
| Plaid (auto-sync) | 700 |
| CSV imports (Apple Card 349 + Barclays 326) | 675 |
| PDF imports (Apple Card 27 + Barclays 6 + Citi 2 + Bloomingdale's 2) | 36 |
| **Total** | **1,411** |

Cross-format dedup handled all overlaps automatically. 6 import batches logged with full audit trail.

### CSV Normalizer Backfill — Chase Credit, Amex, BofA Checking (Feb 2026)
- 3 new institution CSV normalizers added to `csv_normalizers.py` in a single autonomous session
- Chase Credit: pass-through sign convention, `Type == "Payment"` detection, card ending from filename regex
- Amex: inverted sign convention (negate all), `"PAYMENT - THANK YOU"` detection, Reference → Transaction ID, multiline field handling
- BofA Checking: header junk scanning (6-line preamble), pass-through signs, `Is Payment` always false (checking account)
- Auto-detection added for all 3 in `detect_csv_institution()`
- Registry: 4 new keys (`chase_credit`, `amex`, `american_express`, `bofa_checking`)
- Account name unification: aligned CSV/PDF/AI source names (`"BoA Checking"` not `"BofA Checking"`) + parity tests
- Plans: `docs/completed/chase_credit_normalizer_plan.md`, `docs/completed/amex_normalizer_plan.md`, `docs/completed/bofa_checking_normalizer_plan.md`, `docs/completed/account_unification_plan.md`
- 288 tests at completion

**Smoke test results:**

| Export | Institution | Rows | Skipped | Payments | Non-payment Credits |
|--------|------------|------|---------|----------|---------------------|
| Chase Credit (1 year) | chase_credit | 55 | 0 | 13 | 0 |
| Amex (1 year) | amex | 941 | 0 | 12 | 9 |
| BofA Checking (1 month) | bofa_checking | 34 | 1 (beginning balance) | 0 | 10 |

### Account Identity Unification — Alias-Based Cross-Format Linking (Feb 2026)
- Migration 008 added `account_aliases` table to link hash-import accounts to canonical Plaid accounts
- Shared institution canonicalization extracted to `finance_cli/institution_names.py` and reused by importers + AI parser
- Import path now registers/refreshes aliases during `_get_or_create_account()` using unique Plaid match rules
- Dedup engine updated to resolve aliases in both account filter expansion and grouping keys
- New CLI command: `dedup backfill-aliases` (dry-run default, `--commit` to persist)
- CSV normalizers now emit explicit `Account Type` metadata for better alias matching
- Documentation updates shipped: README + Project Guide include alias backfill workflow and `dedup --help` discovery
- Plan: `docs/completed/account_unification_plan.md`
- 305 tests at completion

### Code Quality Sweep (Feb 2026)
- Systematic codebase review identified 10 issues (CQ-001 through CQ-010), 9 fixed, 1 deferred
- CQ-001: Added logging to silent exception handlers in importers
- CQ-002: Added `st_mtime_ns`-based cache to `load_rules()` — eliminates per-transaction YAML re-parsing
- CQ-003: Expanded importer test coverage from 2 to 16 tests (vendor memory, income CSV, amount parsing, alias backfill)
- CQ-004: Unified duplicate `normalize_date` implementations into single permissive version in `models.py`
- CQ-005: `key_only` dedup matches now excluded from `--commit` by default; `--include-key-only` to opt in
- CQ-006: Archived 6 dead PDF regex extractors to `_legacy_pdf_extractors.py`
- CQ-007: Migration runner now commits DDL + version record atomically
- CQ-008: Replaced deprecated `datetime.utcnow()` with timezone-aware equivalent
- CQ-009: Deferred — DB path inside package dir, `TODO(CQ-009)` in `config.py` for pre-packaging fix
- CQ-010: Added warning log when Chase card ending can't be extracted from filename
- Plans: `docs/planning/CODE_QUALITY.md`, `docs/completed/CODE_QUALITY_PLAN.md`, `docs/completed/CQ005_PLAN.md`
- 351 tests at completion

### Institution Name Pre-Validation (Feb 2026)
- Inline validation in `ai_result_to_extract_result()`: canonical name lookup + fuzzy matching for AI-extracted institution names
- Prevents unknown institutions from reaching account creation
- Plan: `docs/completed/NAME_VALIDATION_PLAN.md` (6 review rounds)

### Multi-Backend Extractor Abstraction (Feb 2026)
- `StatementExtractor` Protocol with 3 implementations: `AIExtractor`, `AzureExtractor`, `BSCExtractor`
- Unified `ExtractResult` + `ExtractorMeta` interchange format via `ExtractorOutput`
- New CLI flags: `--backend` (ai/azure/bsc), `--institution`, `--card-ending`
- Backend-scoped dedupe: keys changed from `pdf:{hash}:{idx}` to `pdf:{backend}:{hash}:{idx}`
- Migration 010: rewrites legacy AI dedupe keys, unique index on `(file_hash_sha256, bank_parser)`
- Universal `validate_extract_result()` for all backends
- Lazy factory with optional Azure/BSC deps
- Plan: `docs/completed/EXTRACTOR_ABSTRACTION.md` (4 review rounds, 12 issues found and fixed)
- 354 tests at completion

### AI Schema Overhaul — Balance + Reconciliation Fix (Feb 2026)
- Replaced `statement_total` in AI prompt with 3 clear fields: `new_balance`, `total_charges`, `total_payments` — all always-positive, as-printed
- Fixes systematic sign flip causing 7/10 reconciliation failures
- `new_balance` now writes to `balance_snapshots` (source='manual') and updates `accounts.balance_current_cents`
- Charge+payment reconciliation replaces old statement_total comparison
- Statement period dates wired to `import_batches.statement_period`
- Currency wired to `accounts.iso_currency_code`
- Migration 011: new columns on `import_batches` (total_charges_cents, total_payments_cents, new_balance_cents, expected_transaction_count)
- Prompt version bumped v4 → v5
- Plans: `docs/completed/PDF_RECONCILIATION.md` (investigation), `docs/completed/PDF_RECONCILIATION_FIX.md` (v4 prompt fix), `docs/completed/AI_SCHEMA_OVERHAUL.md` (full overhaul, 4 review rounds)
- 373 tests at completion

### AI Prompt v6 — Remove Charge/Payment Reconciliation (Feb 2026)
- Removed `total_charges` and `total_payments` from AI prompt schema and validation gate
- Charge/payment reconciliation produced false mismatches in 3/10 test PDFs (interest in separate section, multi-account PDF)
- Transaction extraction was correct in all cases — only the reconciliation check failed
- Simplified: AI path now always returns `no_totals`; `new_balance` (the real value) still extracted and persisted
- Azure/BSC backends still compute their own reconciliation independently
- Prompt version bumped v5 → v6
- 374 tests at completion

### E2E Test Runs 1 & 2 (Feb 2026)
- Full pipeline validation: DB wipe → Plaid sync → ingest batch → alias backfill → cross-format dedup
- Run 1: Found 10 issues (BofA/Merrill name mismatch, Apple Card PDF wrong type, duplicate PDF imports, plaid unlink orphans, .CSV case sensitivity, provider hardcoding, token tracking, etc.)
- Run 2 (v6 prompt): Validated charge/payment removal, 0 false mismatches, content-hash dedup working
- All Run 1/2 issues resolved in subsequent commits
- Plans: `docs/completed/E2E_IMPORT_TEST.md`, `docs/completed/E2E_ISSUES.md`, `docs/completed/E2E_FIXES_PLAN.md`

### Pipeline Observability (Feb 2026)
- Structured logging + token/timing tracking across AI parser, categorizer, dedup, plaid sync
- `input_tokens`, `output_tokens`, `elapsed_ms` surfaced in CLI/JSON output for all AI operations
- `ExtractorMeta` enriched with token fields; batch summaries accumulate totals
- `DedupReport.elapsed_ms` + plaid sync per-item timing
- 442 tests at completion

### Ingestion Pipeline Hardening (Feb 2026)
- Per-file savepoint isolation in batch/dir ingest (`SAVEPOINT`/`ROLLBACK TO`)
- `auto_commit=False` parameter on importers to prevent inner commits destroying savepoints
- AI reconciliation with `statement_total` field (prompt v7) — 4/14 PDFs now `matched`
- Strict mode (`--require-reconciled`) uniformly rejects both `mismatch` and `no_totals` across all backends
- `ai_model` provenance fix: stores model version not parser label
- Batch move hardening: collision detection, per-file try/except, `move_errors` in output
- Plan: `docs/completed/INGESTION_FINDINGS_EXECUTION_PLAN_2026-02-21.md`
- 442 tests at completion

### E2E Test Run 3 (Feb 2026)
- Full pipeline validation of v7 prompt, savepoint isolation, and observability
- 23/23 files processed (Bloomingdale's institution name variant fixed inline)
- 2,735 active transactions, 166 dedup matches (including 22 key_only reviewed and applied)
- Savepoint isolation validated: Bloomingdale's failure rolled back cleanly
- statement_total reconciliation: 4 matched (YearEndSummary), 8 mismatch, 2 no_totals
- Content-hash dedup: 0 leaked duplicates (eStmt blocked correctly)
- All Run 1/2 issues confirmed resolved; data quality spot check clean
- Plan: `docs/completed/E2E_IMPORT_TEST.md`, `docs/completed/E2E3_OBSERVATIONS.md`

### Institution Management Hardening (Feb 2026)
- **OBS-002b**: Test that all `CANONICAL_NAMES` keys are pre-normalized (`key == normalize_key(key)`)
- **OBS-001**: Enhanced `plaid status` — per-item report with status, last_sync timestamp, error highlighting, re-link command hints; `active_count`/`error_count` in summary and data
- **OBS-008**: New `dedup detect-equivalences` subcommand — auto-detects candidate institution pairs via shared card_ending + overlapping `(date, amount_cents)` transactions; suggestion-only output, skips pairs already in `_INSTITUTION_EQUIVALENTS`
- Plan: `docs/completed/INSTITUTION_HARDENING_PLAN.md`
- 448 tests at completion

### OBS-003: Remove statement_total from AI Prompt (Feb 2026)
- Removed `statement_total` from AI prompt schema and instructions (v7 → v8)
- AI consistently confused "New Balance" with "statement_total" on credit cards (7/14 mismatch in E2E Run 3)
- Simplified AI reconciliation: always `no_totals` — transaction extraction was correct in all cases
- `--require-reconciled` still works for Azure/BSC backends; correctly rejects AI path
- Azure/BSC extractors, legacy PDF regex extractors, and DB schema unchanged
- Plan: `docs/completed/OBS003_STATEMENT_TOTAL_REMOVAL_PLAN.md` (3 Codex review rounds, 6 issues found and fixed)
- 444 tests at completion

## In Progress / Next Up

### Open Bugs (docs/planning/BUG_BACKLOG.md)
- **PLAID-005**: Schwab doesn't expose liabilities product via Plaid
- **PLAID-006**: Citi auth blocked by debit-card OTP requirement (mitigated by AI parser)
- **INTEGRATION-001**: No source for Schwab Bank deposit balances

### E2E Test Run 4 (Feb 2026)
- Validated prompt v8 (OBS-003 fix), institution hardening, and pipeline stability
- 23/23 files, 2,759 active txns, 166 dedup matches, 0 errors, 0 new issues
- All E2E Run 3 follow-ups confirmed resolved (OBS-001, 002b, 003, 008, 011)
- Plan: `docs/completed/E2E4_OBSERVATIONS.md`

## Architecture Quick Reference

```
finance_cli/
├── institution_names.py      # Shared institution canonicalization map/helpers
├── ai_statement_parser.py    # AI parse: PDF → LLM → JSON → validation → ExtractResult (v7 prompt)
├── extractors/               # Multi-backend extractor abstraction
│   ├── __init__.py           # StatementExtractor Protocol, ExtractorMeta/Output, shared helpers
│   ├── ai_extractor.py       # AI backend (wraps ai_statement_parser)
│   ├── azure_extractor.py    # Azure Document Intelligence backend
│   └── bsc_extractor.py      # BankStatementConverter API backend
├── dedup.py                  # Cross-format dedup engine (CSV vs PDF vs Plaid)
├── ingest_validation.py      # 5-gate validation + universal validate_extract_result()
├── ai_categorizer.py         # AI transaction categorization (Claude/OpenAI)
├── plaid_client.py           # Plaid API (sync/balances/liabilities + cooldown)
├── importers/
│   ├── __init__.py           # CSV import + import_normalized_rows + income CSV
│   ├── csv_normalizers.py    # Institution CSV adapters + auto-detection
│   └── pdf.py                # ExtractResult dataclass + import_extracted_statement() + legacy regex parsers
├── commands/
│   ├── ingest.py             # ingest statement|csv|batch (--backend ai/azure/bsc)
│   └── dedup_cmd.py          # dedup cross-format + alias backfill/review/audit/detect-equivalences
├── migrations/               # 001-011 SQL migrations
└── tests/                    # 448 tests collected
```

## Key Technical Decisions

- **Integer cents everywhere** — all money stored as `int` via `dollars_to_cents(Decimal(...))`
- **Multi-backend extractors** — AI, Azure, BSC all produce `ExtractResult` via `StatementExtractor` Protocol
- **Validation before import** — AI output must pass 5 gates; all backends run universal `validate_extract_result()`
- **Reconciliation is backend-specific** — AI path uses `statement_total` (v7); Azure/BSC compute their own statement totals
- **Balance snapshots from PDF** — `new_balance` writes to `balance_snapshots` (source='manual')
- **Backend-scoped dedupe** — same PDF can be imported by different backends without collision
- **Confidence blocking** — low-confidence rows are warnings; partial import requires explicit override
- **Cooldown timestamps** — per-item, per-operation; NULL = never fetched = always allow
- **Cross-format dedup** — post-import step, not at-import-time; soft-delete preserves audit trail
- **Institution canonicalization** — maps AI free-form names to CSV normalizer canonical names for account_id parity
- **LlamaParse** noted as future alternative for PDF text extraction

## Test Counts

| Milestone | Tests |
|-----------|-------|
| Pre-cooldown | 79 |
| Post-cooldown | 97 |
| Post-AI parser | 156 |
| Post-CSV normalizers | 195 |
| Post-ingest CLI | 195 |
| Post-CSV audit parity | 201 |
| Post-ingest CSV CLI | 208 |
| Post-cross-format dedup | 243 |
| Post-ingest batch | 265 |
| Post-CSV normalizer backfill | 288 |
| Post-account unification aliases | 305 |
| Post-code quality sweep | 351 |
| Post-name validation | 351 |
| Post-extractor abstraction | 354 |
| Post-AI schema overhaul (v5) | 373 |
| Post-v6 simplification | 374 |
| Post-E2E fixes + observability | 442 |
| Post-ingestion hardening (v7) | 442 |
| Post-OBS-011 fix (single-file move) | 446 |
| Post-institution hardening (OBS-002b/001/008) | 448 |
| Post-OBS-003 statement_total removal (v8) | 444 |
