# How The System Works (Step by Step)

Last updated: 2026-03-04

This is the authoritative plain-English walkthrough of `finance_cli` — from command entry to DB writes and reporting. It covers runtime behavior, data flows, and architecture.

Related docs:
- `docs/ingest/INGEST_WORKFLOW.md` — operational playbook for running imports
- `docs/developer/PRE_DB_INGEST_FLOW.md` — detailed pre-DB pipeline internals

## 0) What this system is

`finance_cli` is a local-first personal finance CLI (Mint replacement). Agent-first design — all commands return structured JSON. SQLite storage, integer cents for all money.

External systems:
- **Plaid API** — transactions, balances, liabilities for linked institutions
- **AWS Secrets Manager** — Plaid access token storage
- **LLM providers** (OpenAI / Anthropic) — AI statement parsing and AI categorization

| Layer | Main Files | Responsibility |
|---|---|---|
| CLI routing | `__main__.py`, `commands/*.py` | parse args, call handlers, emit envelope |
| Ingest (PDF/CSV/batch) | `commands/ingest.py`, `extractors/*`, `ai_statement_parser.py`, `ingest_validation.py`, `importers/*` | multi-backend parse/normalize/import with dedupe and audit |
| Plaid integration | `commands/plaid_cmd.py`, `plaid_client.py` | link, sync, balance refresh, liabilities sync, unlink |
| Stripe integration | `commands/stripe_cmd.py`, `stripe_client.py` | balance transaction sync, revenue reporting |
| Dedup + account unification | `dedup.py`, `commands/dedup_cmd.py`, `importers/__init__.py`, `institution_names.py` | cross-format duplicate resolution + account alias mapping |
| Categorization/rules | `categorizer.py`, `ai_categorizer.py`, `user_rules.py` | deterministic categorization + optional AI categorization |
| Debt analysis | `debt_calculator.py`, `spending_analysis.py`, `commands/debt_cmd.py` | dashboard, interest projection, paydown simulation, lump sum modeling |
| Business accounting | `commands/biz_cmd.py`, `forecasting.py` | P&L, cash flow, Schedule C, estimated tax, forecasting, runway |
| Planning/reporting | `budget_engine.py`, `liquidity.py`, `subscriptions.py`, `exporters.py` | downstream analytics/exports |
| Financial visibility | `commands/summary_cmd.py`, `commands/spending_cmd.py`, `commands/projection_cmd.py`, `commands/goal_cmd.py` | health dashboard, trend analysis, net worth projection, goal tracking |
| MCP server | `mcp_server.py` | 131 tools exposing CLI handlers to Claude Code via FastMCP |
| Telegram bot | `telegram_bot/` | Claude agent with MCP tools, chat persistence, request metrics |

## 1) Every command starts the same way

When you run `python3 -m finance_cli ...`, the same core flow always happens:

1. CLI args are parsed in `finance_cli/__main__.py` via `build_parser()`.
2. DB bootstrap runs via `initialize_database()` in `finance_cli/db.py`.
3. A DB connection is opened via `connect()` in `finance_cli/db.py`.
4. The command handler (stored in `args.func`) executes.
5. Output is wrapped in a success/error envelope via:
   - `success_envelope(...)` / `error_envelope(...)`
   - `print_envelope(...)`
   in `finance_cli/commands/common.py`.

```text
+------------------------+
| User command           |
| python3 -m finance_cli |
+-----------+------------+
            |
            v
+-------------------------------+
| __main__.py: main()           |
| - build_parser()              |
| - db.initialize_database()    |
| - db.connect()                |
+---------------+---------------+
                |
                v
+-------------------------------+
| args.func command handler     |
+---------------+---------------+
                |
                v
+-------------------------------+
| common.py envelope/output     |
| - success_envelope()          |
| - error_envelope()            |
| - print_envelope()            |
+---------------+---------------+
                |
                v
+-------------------------------+
| JSON/CLI response to user     |
+-------------------------------+
```

Envelope contract:
- **Success:** `{"status": "success", "command": "...", "version": "1.0.0", "data": {...}}` — optional `summary` and `cli_report`
- **Error:** `{"status": "error", "command": "...", "version": "1.0.0", "error": "..."}`

## 2) Write paths: how data gets into the DB

There are two primary ingestion families:
- File ingest (`ingest statement`, `ingest csv`, `ingest batch`)
- Plaid ingest (`plaid sync`, `plaid balance-refresh`, `plaid liabilities-sync`)

### 2A) `ingest statement` (PDF + multi-backend extractors)

Main flow:
1. `handle_ingest_statement(...)` in `finance_cli/commands/ingest.py`
2. `_process_statement_file(...)` in the same file
3. Backend resolution: `--backend` CLI flag > `rules.extractors.default_backend` > `"ai"`
4. `extractor.extract(pdf_path, options)` via `StatementExtractor` Protocol in `finance_cli/extractors/`
   - AI backend: `ai_parse_statement(...)` → `ai_result_to_extract_result(...)` in `ai_statement_parser.py`
   - Azure backend: Azure Document Intelligence SDK in `azure_extractor.py`
   - BSC backend: BankStatementConverter HTTP API in `bsc_extractor.py`
5. `validate_extract_result(...)` — universal validation for all backends
6. `import_extracted_statement(...)` in `finance_cli/importers/pdf.py`

Important behavior:
- Default is dry-run (no writes) unless `--commit`.
- Re-import guard uses PDF hash + backend in `import_batches` (same PDF can be imported by different backends).
- If no `--account-id` is provided, account is derived automatically from parsed source/card signals.
- Non-AI backends may need `--institution` and/or `--card-ending` hints.
- AI path writes `new_balance` to `balance_snapshots` and `accounts.balance_current_cents`.

### 2B) `ingest csv` (institution normalizer + shared importer)

Main flow:
1. `handle_ingest_csv(...)` in `finance_cli/commands/ingest.py`
2. `normalize_csv(...)` in `finance_cli/importers/csv_normalizers.py`
3. `import_normalized_rows(...)` in `finance_cli/importers/__init__.py`
4. `_import_row_iter(...)` in `finance_cli/importers/__init__.py`

Important behavior:
- Institution-specific adapters normalize source formats into a common row schema.
- Row-level dedupe uses stable `dedupe_key` generation in `_import_row_iter(...)`.
- Default dry-run behavior matches the PDF path.

### 2C) `ingest batch` (directory router)

Main flow:
1. `handle_ingest_batch(...)` in `finance_cli/commands/ingest.py`
2. For each file:
   - PDF: `_process_statement_file(...)`
   - CSV: `_process_csv_file(...)`
3. Commit mode can move successfully processed files into `inbox/processed/`.

```text
+--------------------------+
| User                     |
| ingest batch --commit    |
+------------+-------------+
             |
             v
+--------------------------+       +-------------------------+
| CLI (__main__.py)        | ----> | SQLite                  |
| parse args + init db     |       | initialize + connect    |
+------------+-------------+       +-------------------------+
             |
             v
+--------------------------+       +-------------------------+
| commands/ingest.py       | ----> | Filesystem              |
| handle_ingest_batch      |       | enumerate *.pdf/*.csv   |
+------------+-------------+       +-------------------------+
             |
             v
     +-------+----------------------------------------------+
     | loop files (sorted)                                  |
     |                                                      |
     |  PDF branch:                                         |
     |  ingest.py -> extractors/{ai,azure,bsc}_extractor   |
     |            -> importers/pdf.py                       |
     |            -> SQLite (transactions + import_batches  |
     |               + balance_snapshots)                   |
     |                                                      |
     |  CSV branch:                                         |
     |  ingest.py -> csv_normalizers.py                     |
     |            -> importers/__init__.py                  |
     |            -> SQLite (transactions + import_batches) |
     +-------+----------------------------------------------+
             |
             v
+--------------------------+       +-------------------------+
| commands/ingest.py       | ----> | Filesystem              |
| aggregate report         |       | move success -> processed/
+------------+-------------+       +-------------------------+
             |
             v
+--------------------------+
| CLI envelope to user     |
+--------------------------+
```

### 2D) Dedupe semantics by source

Each import path generates dedupe keys differently:

| Source | Dedupe Key | Guard |
|---|---|---|
| CSV import | `SHA256(source + account + date + description + amount + ordinal)` → `transactions.dedupe_key` | Row-level fingerprint; same CSV re-import is idempotent |
| PDF import | `pdf:<backend>:<file_sha256>:<row_index>` → `transactions.dedupe_key` | File hash + backend in `import_batches` prevents re-import by same backend |
| Plaid sync | `plaid:<transaction_id>` → `transactions.dedupe_key` | Plaid transaction identity; cursor-based sync handles updates |

### 2E) Plaid write paths

Main command handlers in `finance_cli/commands/plaid_cmd.py`:
- `handle_sync(...)` → `run_sync(...)` in `finance_cli/plaid_client.py`
- `handle_balance_refresh(...)` → `refresh_balances(...)`
- `handle_liabilities_sync(...)` → `fetch_liabilities(...)`

Link and token lifecycle:
1. `plaid link [--wait]` creates a hosted Link session
2. Optional browser polling for completion
3. Exchanges public_token for access token
4. Stores access token in AWS Secrets Manager
5. Upserts local `plaid_items` metadata

Safety controls:
- Duplicate institution guard with optional `--allow-duplicate`
- Token payload includes item identity checks to prevent mismatch
- Unlink protects against deleting shared token refs used by active items
- Update mode: `plaid link --update --item <item_id>` for additional product consent

Cooldown model (per-item timestamps in `plaid_items`):
- Sync: 300s default (`PLAID_SYNC_COOLDOWN`)
- Balance refresh: 600s default (`PLAID_BALANCE_COOLDOWN`)
- Liabilities: 3600s default (`PLAID_LIABILITIES_COOLDOWN`)
- `--force` bypasses cooldown checks

#### Sync sequence

```text
+---------------------------+
| User                      |
| plaid sync [--force]      |
+-------------+-------------+
              |
              v
+---------------------------+      +-------------------------+
| CLI + plaid_cmd.py        | ---> | SQLite                  |
| route to plaid_client     |      | load plaid_items        |
+-------------+-------------+      +-------------------------+
              |
              v
      +-------+----------------------------------------------+
      | for each plaid item                                  |
      |                                                      |
      |  [cooldown active and not --force]                   |
      |      -> mark skipped_cooldown                        |
      |                                                      |
      |  [otherwise]                                         |
      |      -> AWS Secrets Manager: resolve access token    |
      |      -> Plaid /transactions/sync (paginate)          |
      |      -> SQLite: upsert accounts                      |
      |      -> SQLite: upsert balance_snapshots (sync)      |
      |      -> SQLite: upsert txns (added/modified)         |
      |      -> SQLite: soft-deactivate removed txns         |
      |      -> SQLite: update cursor/status/cooldown        |
      |                                                      |
      |  [on error]                                          |
      |      -> SQLite: set plaid_items.status='error'       |
      +-------+----------------------------------------------+
              |
              v
+---------------------------+
| totals -> envelope output |
+---------------------------+
```

#### Link sequence

```text
+-----------------------------+
| User                        |
| plaid link --wait           |
+---------------+-------------+
                |
                v
+-----------------------------+      +-----------------------+
| plaid_client                | ---> | Plaid API             |
| create_hosted_link_session  |      | /link/token/create    |
+---------------+-------------+      +-----------+-----------+
                |                                |
                |  hosted_link_url + link_token  |
                +<-------------------------------+
                |
      (optional) v
          +--------------------+
          | Browser open URL   |
          +--------------------+
                |
                v
+-----------------------------+      +-----------------------+
| plaid_client                | ---> | Plaid API             |
| complete_link_session       |      | polling + exchange    |
+---------------+-------------+      +-----------+-----------+
                |                                |
                | access_token + item_id         |
                +<-------------------------------+
                |
                v
+-----------------------------+      +-----------------------+
| AWS Secrets Manager         | <--- | plaid_client          |
| store access token          |      | secret ref only in DB |
+-----------------------------+      +-----------+-----------+
                                                 |
                                                 v
                                       +-----------------------+
                                       | SQLite plaid_items    |
                                       | upsert metadata       |
                                       +-----------+-----------+
                                                   |
                                                   v
                                       +-----------------------+
                                       | envelope output       |
                                       +-----------------------+
```

## 3) Identity model: how accounts are unified

This is the core logic that keeps cross-format data aligned:

1. Institution names are canonicalized in `finance_cli/institution_names.py` (`canonicalize(...)`).
2. Hash-based account IDs are derived by `_account_id_for_source(...)` in `finance_cli/importers/__init__.py`.
3. During import, `_get_or_create_account(...)` attempts to find a unique Plaid counterpart (`_find_plaid_account(...)`).
4. If matched, alias is upserted into `account_aliases` (`migration 008`).
5. Historical alias repair is handled by `backfill_account_aliases(...)` (exposed via `dedup backfill-aliases`).

Why this matters:
- CSV/PDF imports use deterministic hash IDs.
- Plaid uses Plaid-linked account IDs.
- `account_aliases` bridges those so dedup and analysis can treat them as one logical account.

## 4) Cross-format dedup logic

Main code: `finance_cli/dedup.py`

Core functions:
- `find_cross_format_duplicates(...)`
- `apply_dedup(...)`

Logic in plain English:
1. Load alias map from `account_aliases`.
2. Expand account filters so both hash and canonical IDs are included.
3. Group active transactions by effective account + date + amount.
4. Keep only groups with multiple sources (`csv_import`, `plaid`, `pdf_import`).
5. Choose keeper by source priority and description matching.
6. In commit mode, soft-delete duplicates (`is_active=0`) while preserving audit trail.

```text
+--------------------------+
| transactions (active)    |
+------------+-------------+
             |
             v
+--------------------------+
| Resolve account aliases  |
| via account_aliases      |
+------------+-------------+
             |
             v
+--------------------------+
| Group by                 |
| effective_account/date/  |
| amount                   |
+------------+-------------+
             |
             v
+--------------------------+
| Keep cross-source groups |
+------------+-------------+
             |
             v
+--------------------------+
| Match by description +   |
| source priority          |
+------------+-------------+
             |
             v
+--------------------------+
| Dedup report             |
+------------+-------------+
             |
             v
+--------------------------+
| --commit => soft-delete  |
| duplicate losers         |
+--------------------------+
```

## 5) Categorization logic

There are two categorization engines:

Rule-based categorization:
- `match_transaction(...)` and `apply_match(...)` in `finance_cli/categorizer.py`
- Used by command paths in `finance_cli/commands/txn.py` and `finance_cli/commands/cat.py`

AI categorization:
- `categorize_uncategorized(...)` in `finance_cli/ai_categorizer.py`
- Triggered via `handle_auto_categorize(...)` in `finance_cli/commands/cat.py`
- Can learn from confirmations and update memory rules.

## 6) Read/report commands are straightforward DB queries

Most non-ingest commands read from SQLite and return envelope data:
- Transactions: `finance_cli/commands/txn.py`
- Daily/weekly: `finance_cli/commands/daily.py`, `finance_cli/commands/weekly.py`
- Liquidity/balance/liability: `finance_cli/commands/liquidity_cmd.py`, `finance_cli/commands/balance_cmd.py`, `finance_cli/commands/liability_cmd.py`
- Budget/plan/subscriptions: `finance_cli/commands/budget.py`, `finance_cli/commands/plan.py`, `finance_cli/commands/subs.py`
- Financial health: `finance_cli/commands/summary_cmd.py` — one-page dashboard (net worth, cash flow, risk metrics, obligations, data health)
- Spending analysis: `finance_cli/commands/spending_cmd.py` — category-by-month pivot with trend arrows
- Debt tools: `finance_cli/commands/debt_cmd.py` — dashboard, interest projection, paydown simulation (avalanche/snowball), lump sum modeling, spending cut impact analysis
- Business accounting: `finance_cli/commands/biz_cmd.py` — P&L, cash flow, Schedule C, estimated tax, forecasting, runway, seasonal patterns, budgets, health check
- Projections/goals: `finance_cli/commands/projection_cmd.py` (net worth projection), `finance_cli/commands/goal_cmd.py` (goal set/list/status with progress tracking)

The pattern is consistent:
1. Parse args
2. Query/transform
3. Return `{data, summary, cli_report}` envelope

Important: all balance queries exclude aliased accounts (`AND a.id NOT IN (SELECT hash_account_id FROM account_aliases)`) to prevent double-counting from cross-format imports.

## 7) Storage architecture

Migrations are authoritative for schema (`finance_cli/migrations/001–030_*.sql`).

| Domain | Tables |
|---|---|
| Ledger + accounts | `transactions`, `accounts`, `categories`, `projects` |
| Ingestion audit | `import_batches`, `ai_categorization_log` |
| Plaid lifecycle | `plaid_items` |
| Stripe integration | `stripe_connections` |
| Balances / liabilities | `balance_snapshots`, `liabilities` |
| Planning / rules | `vendor_memory`, `budgets`, `subscriptions`, `recurring_flows`, `monthly_plans` |
| Cross-account unification | `account_aliases` |
| Business accounting | `pl_section_map`, `schedule_c_map`, `provider_routing` |
| Debt / goals | `goals` |
| Category mappings | `category_mappings` |
| Search | `txn_fts` (FTS5) |

Principles:
- Integer cents for all money — no floats anywhere
- Idempotent imports — re-running the same data is safe
- Soft-deactivation over destructive deletes (for auditability)

### Who writes what

Each command flow has specific tables it touches and a key that prevents duplicate writes.

**CSV import** (`ingest csv`, `ingest batch` for CSVs):
- Writes: `transactions`, `accounts`, `categories`, `account_aliases`, `import_batches`
- Dedupe: row fingerprint → `transactions.dedupe_key`
- File guard: `import_batches.file_hash_sha256`

**PDF import** (`ingest statement`, `ingest batch` for PDFs):
- Writes: `transactions`, `accounts`, `import_batches`, `balance_snapshots` (when `new_balance` extracted)
- Dedupe: `pdf:<backend>:<file_sha256>:<row_index>` → `transactions.dedupe_key`
- File guard: `import_batches.(file_hash_sha256, bank_parser)` unique index

**Plaid link** (`plaid link`):
- Writes: `plaid_items` + AWS Secrets Manager (external)
- Key: `plaid_items.plaid_item_id`

**Plaid sync** (`plaid sync`):
- Writes: `transactions`, `accounts`, `balance_snapshots`, `plaid_items`, `categories`
- Dedupe: `plaid:<transaction_id>` → `transactions.plaid_txn_id`
- Cursor: `plaid_items.sync_cursor`

**Balance refresh** (`plaid balance-refresh`):
- Writes: `accounts`, `balance_snapshots`, `plaid_items`
- Key: unique `(account_id, snapshot_date, source)`

**Liabilities sync** (`plaid liabilities-sync`):
- Writes: `liabilities`, `accounts`, `balance_snapshots`, `plaid_items`
- Key: unique `(account_id, liability_type)`

## 8) Reliability and safety model

- Migrations execute before command handling (schema always up to date)
- Ingest defaults to dry-run; explicit `--commit` required for writes
- Batch and Plaid flows isolate errors at file/item scope
- Import paths are designed for idempotent reruns
- Auditability preserved through `import_batches`, AI validation logs, and soft-deactivation

## 9) MCP server

`finance_cli/mcp_server.py` exposes 131 tools via FastMCP, wrapping CLI handlers directly via Python imports. Registered globally in `~/.claude.json` as `finance-cli`.

Tool categories span: transactions, categories, accounts, balances, debt, budgets, subscriptions, business accounting, Stripe, Plaid, dedup, export, setup, projections, goals, and more.

Each tool calls the underlying handler function and returns `{data, summary}` dicts. Large responses use field stripping and cache files to stay within MCP size limits.

## 10) Known constraints

Tracked in `docs/planning/BUG_BACKLOG.md`:
- Plaid Schwab does not expose liabilities product (`PLAID-005`)
- No source path for Schwab Bank deposit balances (`INTEGRATION-001`)
- Citi card linking blocked by institution auth flow (`PLAID-006`)
- PDF account_type heuristic can misclassify (`IMPORT-001`)

Practical constraints:
- Cross-format dedup is a deliberate explicit step, not automatic
- Batch commit moves successful files to `processed/`, affecting rerun behavior unless files are restored
- Debt projection (`project_interest`) only models credit cards; loans are held constant
- Net worth projection uses 3-month average cash flow (complete calendar months, excludes current partial month)

## 11) Mental model (short version)

Think of the system in five layers:

1. **Interface layer:** command handlers under `finance_cli/commands/` + MCP server (`mcp_server.py`)
2. **Pipeline layer:** parsing/normalization/import logic (`extractors/*`, `ai_statement_parser.py`, `importers/*`, `plaid_client.py`, `stripe_client.py`)
3. **Analysis layer:** `debt_calculator.py`, `spending_analysis.py`, `forecasting.py`, `budget_engine.py`, `liquidity.py`, `subscriptions.py`
4. **Identity + dedup layer:** `institution_names.py`, importer account helpers, `dedup.py`
5. **Storage layer:** SQLite schema + 30 migrations (`finance_cli/db.py`, `finance_cli/migrations/`)

If you trace a bug in this order, root cause isolation is usually fast.
