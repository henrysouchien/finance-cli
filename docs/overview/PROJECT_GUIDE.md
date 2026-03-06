# finance_cli — Project Guide

Last updated: 2026-03-04

Personal finance CLI tool (Mint replacement). Agent-first design — all commands return structured JSON. SQLite storage, integer cents for all money.

If you want a plain-English runtime walkthrough, read `docs/overview/HOW_IT_WORKS.md`.

## Quick Start

```bash
cd .
python3 -m finance_cli <command> [subcommand] [--format json|cli]
python3 -m pytest
```

## CLI Surface

Top-level commands:
- `txn`, `cat`, `daily`, `weekly`, `budget`, `export`, `ingest`, `db`
- `dedup`, `subs`, `liquidity`, `balance`, `liability`, `plan`, `plaid`, `rules`, `migrate`
- `debt`, `account`, `biz`, `stripe`, `provider`, `setup`, `monthly`
- `summary`, `spending`, `projection`, `goal`

Key command groups:
- `db`: `backup`, `reset`
- `dedup`: `cross-format`, `backfill-aliases`, `review-key-only`, `audit-names`, `suggest-aliases`, `create-alias`, `detect-equivalences`
- `plaid`: `link`, `sync`, `balance-refresh`, `liabilities-sync`, `status`, `unlink`, `products-backfill`
- `debt`: `dashboard`, `interest`, `simulate` (with `--lump-sum`), `impact`
- `account`: `list`, `show`, `set-type`, `set-business`, `deactivate`, `activate`
- `biz`: `pl`, `cashflow`, `tax`, `estimated-tax`, `forecast`, `runway`, `seasonal`, `budget set/status`, `health-check`
- `stripe`: `link`, `sync`, `status`, `revenue`, `unlink`
- `goal`: `set`, `list`, `status`
- `subs`: `list`, `detect`, `recurring`, `add`, `cancel`, `total`, `audit`

## Project Status

**Fully implemented:**
- Transaction CRUD, search (FTS5), CSV import, categorization with vendor memory
- Budget engine with forecasting, weekly summaries, daily rundown
- Subscription detection + audit (essential vs discretionary), liquidity calculator, monthly planning
- Plaid client: link/sync/status/unlink + products backfill + balance refresh + liabilities sync
- Stripe integration: balance transaction sync, revenue reporting, payout dedup
- User rules system (keyword rules, split rules, category overrides, aliases)
- AI categorizer (Claude + OpenAI providers, batch processing, auto-remember)
- PDF statement ingest: multi-backend extractors (AI primary, Azure and BSC available)
- CSV normalizers with institution auto-detection (Apple Card, Barclays, Chase Credit, Amex, BofA Checking)
- 5-gate validation framework (schema/field/semantic/reconciliation/confidence)
- Cross-format dedup engine (CSV vs PDF vs Plaid) with account alias resolution
- Account unification: `account_aliases` table links hash-based CSV/PDF accounts to Plaid accounts
- Business accounting: P&L, cash flow, Schedule C, estimated tax, forecasting, runway, seasonal patterns
- Debt tools: dashboard, interest projection, paydown simulation (avalanche/snowball), lump sum modeling, spending cut impact
- Financial visibility: summary dashboard, spending trends, net worth projection, goal tracking with progress bars
- Account management: list/show/set-type/deactivate/activate with business flags
- Provider routing: institution-level provider switching (Plaid vs direct API)
- Monthly runner: orchestrates sync → dedup → categorize → detect → export
- MCP server: 130 tools exposing all CLI handlers to Claude Code
- Income CSV import, Wave accounting export
- **Dump-and-go batch import**: `ingest batch --dir ./inbox/ --commit`

## Architecture

```
finance_cli/
├── __main__.py              # CLI entry point + arg parser
├── config.py                # Settings, env vars, paths
├── db.py                    # SQLite connection, migration runner (WAL, FK enforcement)
├── models.py                # Pydantic v2 domain models
├── mcp_server.py            # FastMCP server exposing 130 tools for Claude Code
├── categorizer.py           # Vendor memory: exact → prefix → keyword rule → Plaid
├── ai_categorizer.py        # AI categorization (Claude/OpenAI, batch, auto-remember)
├── ai_statement_parser.py   # AI PDF parsing: PDF → LLM → JSON → validation → ExtractResult (v8 prompt)
├── debt_calculator.py       # Debt dashboard, interest projection, paydown simulation, lump sum modeling
├── spending_analysis.py     # Shared essential category logic, N-month category spending averages
├── forecasting.py           # Revenue streams, trend regression, burn rate, runway, seasonal patterns
├── extractors/
│   ├── __init__.py          # StatementExtractor Protocol, ExtractorMeta/Output, shared helpers
│   ├── ai_extractor.py      # AI backend (wraps ai_statement_parser)
│   ├── azure_extractor.py   # Azure Document Intelligence backend
│   └── bsc_extractor.py     # BankStatementConverter API backend
├── ingest_validation.py     # 5-gate validation + universal validate_extract_result()
├── dedup.py                 # Cross-format dedup engine (CSV vs PDF vs Plaid)
├── institution_names.py     # Shared institution name canonicalization (single source of truth)
├── user_rules.py            # Keyword rules, splits, overrides, aliases, extractor config from rules.yaml
├── budget_engine.py         # Budget calculation + forecasting
├── liquidity.py             # Liquidity calculator
├── subscriptions.py         # Subscription detection + audit (essential vs discretionary)
├── plaid_client.py          # Plaid API (sync/balances/liabilities + cooldown)
├── stripe_client.py         # Stripe API (balance transaction sync, payout dedup)
├── provider_routing.py      # Institution-level provider routing guards
├── exporters.py             # CSV export, monthly summary, Wave export
├── sheets_export.py         # Google Sheets net worth export
├── importers/
│   ├── __init__.py          # CSV import + import_normalized_rows + income CSV
│   ├── csv_normalizers.py   # Institution CSV adapters + auto-detection
│   └── pdf.py               # AI ingest write path + legacy regex parser compatibility
├── commands/
│   ├── txn.py               # txn list|show|search|categorize|edit|tag|review|add|coverage|import
│   ├── ingest.py            # ingest statement|csv|batch (AI PDF + CSV normalizer + dump-and-go)
│   ├── db_cmd.py            # db backup|reset
│   ├── dedup_cmd.py         # cross-format dedup + alias workflows + key_only review
│   ├── cat.py               # cat list|add|auto-categorize|normalize|memory
│   ├── budget.py            # budget set|list|status|forecast|suggest
│   ├── daily.py             # daily [--date] [--pending] [--view]
│   ├── weekly.py            # weekly [--week] [--compare] [--view]
│   ├── plan.py              # plan create|show|review
│   ├── subs.py              # subs list|detect|recurring|add|cancel|total|audit
│   ├── liquidity_cmd.py     # liquidity [--forecast N] [--view]
│   ├── balance_cmd.py       # balance show|net-worth|history [--view]
│   ├── liability_cmd.py     # liability show|upcoming|obligations
│   ├── export.py            # export csv|summary|wave
│   ├── plaid_cmd.py         # plaid link|sync|balance-refresh|liabilities-sync|status|unlink
│   ├── rules.py             # rules show|edit|validate|test
│   ├── debt_cmd.py          # debt dashboard|interest|simulate|impact
│   ├── account_cmd.py       # account list|show|set-type|set-business|deactivate|activate
│   ├── biz_cmd.py           # biz pl|cashflow|tax|estimated-tax|forecast|runway|seasonal|budget|health-check
│   ├── stripe_cmd.py        # stripe link|sync|status|revenue|unlink
│   ├── provider_cmd.py      # provider status|switch
│   ├── setup_cmd.py         # setup check|init|connect|status
│   ├── monthly_cmd.py       # monthly run [--sync] [--ai] [--dry-run] [--skip]
│   ├── summary_cmd.py       # summary — one-page financial health dashboard
│   ├── spending_cmd.py      # spending trends — category-by-month pivot with trend arrows
│   ├── projection_cmd.py    # projection — net worth projection over N months
│   └── goal_cmd.py          # goal set|list|status — financial goal tracking
├── data/
│   ├── finance.db           # SQLite database
│   └── rules.yaml           # User rules + AI parser config + essential_categories + revenue_streams
├── migrations/              # 001-028 SQL migrations
└── tests/                   # 1155 tests across 65+ modules
```

## Import Workflow

The primary way to get transactions into the system:

### Dump-and-Go (Recommended)

```bash
# Drop PDFs and CSVs into inbox/, run one command
cp ~/Downloads/*.pdf ~/Downloads/*.csv inbox/
finance_cli ingest batch --dir ./inbox/ --commit --format cli
# Files auto-move to inbox/processed/ on success
```

### Individual Commands

```bash
# CSV with explicit institution
finance_cli ingest csv --file export.csv --institution apple_card --commit

# PDF via AI parser (default backend)
finance_cli ingest statement --file statement.pdf --provider openai --model gpt-4o --commit

# PDF via Azure Document Intelligence
finance_cli ingest statement --file statement.pdf --backend azure --institution "Chase" --commit

# PDF via BankStatementConverter
finance_cli ingest statement --file statement.pdf --backend bsc --institution "Chase" --card-ending XXXX --commit

# Plaid sync (automated)
finance_cli plaid sync --format cli
```

### Post-Import Dedup

```bash
# Discover available dedup subcommands and flags
finance_cli dedup --help

# Find and resolve cross-format duplicates
finance_cli dedup cross-format --commit --format cli

# Backfill account aliases (dry-run, then commit)
finance_cli dedup backfill-aliases --format cli
finance_cli dedup backfill-aliases --commit --format cli
```

See `docs/ingest/INGEST_WORKFLOW.md` for the full operational playbook.

## Key Design Decisions

- **All money is integer cents** — no floats anywhere in storage or logic
- **JSON envelope output** — `{"status": "success", "command": "...", "data": {...}}`
- **Dry-run by default** — all ingest commands require explicit `--commit` to write
- **AI output is untrusted** — must pass 5 validation gates before touching DB
- **Cross-format dedup is post-import** — soft-delete preserves audit trail
- **Institution auto-detection** — CSV institution detected from file content (batch mode)
- **Categorization priority:** exact vendor_memory → prefix match → keyword rule → Plaid → uncategorized
- **PDF dedupe:** SHA-256 file hash in `import_batches`; same file can be imported by different backends but not twice by the same backend
- **CSV dedupe:** row-level `dedupe_key` (SHA256 of source, account, date, description, amount)
- **Config cascade:** CLI flags > rules.yaml > hardcoded defaults (prevents cross-provider model mismatch)

## Database Tables

`transactions`, `categories`, `vendor_memory`, `budgets`, `projects`, `subscriptions`, `recurring_flows`, `accounts`, `account_aliases`, `balance_snapshots`, `liabilities`, `monthly_plans`, `plaid_items`, `import_batches`, `ai_categorization_log`, `category_mappings`, `pl_section_map`, `schedule_c_map`, `provider_routing`, `stripe_connections`, `goals`, `schema_version`, `txn_fts` (FTS5)

## Environment

- **DB location:** `FINANCE_CLI_DB` env var, or default `finance_cli/data/finance.db`
- **Plaid:** `PLAID_CLIENT_ID`, `PLAID_SECRET`, `PLAID_ENV` in `.env`
- **Stripe:** `STRIPE_API_KEY` in `.env`
- **AI:** `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` in `.env` (for AI parser + AI categorizer)
- **Rules:** `finance_cli/data/rules.yaml` — keyword rules, aliases, split rules, AI parser config, essential_categories, revenue_streams

## Testing

```bash
python3 -m pytest -q                       # 1155 tests across 65+ modules
python3 -m pytest -k "test_ingest"         # ingest command tests
python3 -m pytest -k "test_csv_normal"     # CSV normalizer + detection tests
python3 -m pytest -k "test_dedup"          # cross-format dedup tests
python3 -m pytest -k "test_ai"             # AI categorizer tests
python3 -m pytest -k "test_pdf"            # PDF importer tests
python3 -m pytest -k "balance or liabil"   # balances/liabilities tests
python3 -m pytest -k "test_debt"           # debt calculator + command tests
python3 -m pytest -k "test_biz"            # business accounting tests
python3 -m pytest -k "test_summary"        # financial summary tests
python3 -m pytest -k "test_goal"           # goal tracking tests
```

Tests use `tmp_path` fixtures with fresh SQLite databases. External APIs (Plaid, OpenAI, Claude, Stripe) are monkeypatched — no network calls in tests.

## Common Tasks

1. **Monthly close:** `finance_cli monthly run --sync --ai --format cli`
2. **Import transactions:** `finance_cli ingest batch --dir ./inbox/ --commit`
3. **Financial health check:** `finance_cli summary --format cli`
4. **Show net worth:** `finance_cli balance net-worth --format cli`
5. **Debt strategy:** `finance_cli debt simulate --extra 500 --strategy compare --format cli`
6. **Spending trends:** `finance_cli spending trends --months 6 --format cli`
7. **Net worth projection:** `finance_cli projection --months 12 --format cli`
8. **Goal tracking:** `finance_cli goal status --format cli`
9. **Business P&L:** `finance_cli biz pl --format cli`
10. **Run AI categorization:** `finance_cli cat auto-categorize --ai --format cli`
11. **Check Plaid status:** `finance_cli plaid status`
12. **Find cross-format dupes:** `finance_cli dedup cross-format --format cli`
13. **Add a new CLI command:** follow pattern in `commands/summary_cmd.py`, register in `__main__.py`
14. **Add a new CSV normalizer:** see `docs/developer/ADD_INSTITUTION_RUNBOOK.md`

## MCP Server

130 tools registered globally as `finance-cli` in `~/.claude.json`. Wraps all CLI handlers via Python imports. See `finance_cli/mcp_server.py`.

## Agent Workflows

8 operational playbooks in `docs/AGENT_WORKFLOWS.md`: morning briefing, monthly close, debt strategy, budget review, tax prep, new account onboarding, spending investigation, business accounting & tax compliance.
