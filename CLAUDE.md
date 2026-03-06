NEVER run `git checkout -- <files>`, `git checkout .`, `git checkout HEAD`, `git restore .`, `git reset --hard`, `git clean -f`, or ANY command that discards uncommitted changes. NO EXCEPTIONS. Multiple sessions may be running in parallel. If Codex or any tool modifies unexpected files, TELL the user which files and ASK what to do — do NOT revert them.

# finance_cli

Personal finance CLI: imports bank statements (Plaid, CSV, PDF), categorizes transactions via rules + AI, tracks budgets/subscriptions/net worth. SQLite backend, all amounts in integer cents.

## Quick Start

```bash
python3 -m finance_cli db status          # DB overview: counts, date range, uncategorized
python3 -m finance_cli daily              # Today's transactions
python3 -m finance_cli txn list --limit 10 # Recent transactions
```

All commands accept `--format json` (structured envelope) or `--format cli` (human-readable, default).

## CLI Commands

### Transactions
- `txn list` — Filter/paginate transactions (`--from`, `--to`, `--category`, `--uncategorized`, `--unreviewed`, `--limit`)
- `txn show <id>` — Full transaction details
- `txn explain <id>` — How a transaction was categorized (source, rule, reasoning)
- `txn search --query` — Full-text search (FTS5, falls back to LIKE)
- `txn categorize <id> --category` — Categorize single/bulk; `--remember` saves vendor memory; `--ids id1,id2` for multi-ID bulk; `--bulk --query` for search-based bulk
- `txn edit <id>` — Edit amount/date/description/notes
- `txn tag <id> --project` — Tag with project
- `txn review <id>` / `--all-today` / `--before DATE` — Mark reviewed (single, today, or bulk by date)
- `txn add` — Manual transaction entry
- `txn coverage` — Date coverage per account with gap detection
- `txn import --file` — Import CSV or income CSV

### Categories
- `cat list` / `cat tree` — List or show hierarchy with counts
- `cat add <name>` — Add category
- `cat auto-categorize` — Run pipeline on uncategorized txns; `--ai` for AI pass
- `cat normalize` — Backfill source_category, seed mappings, remap non-canonical names
- `cat memory list/add/disable/confirm/delete/undo` — Vendor memory CRUD

### Ingest
- `ingest statement --file <pdf>` — PDF import (dry-run default, `--commit` to save)
- `ingest csv --file <csv>` — CSV import
- `ingest batch --dir <dir>` — Batch process directory of PDFs/CSVs

### Dedup
- `dedup cross-format` — Detect/deactivate cross-format duplicates
- `dedup review-key-only` — Review risky key-only matches
- `dedup backfill-aliases` / `create-alias` / `suggest-aliases` — Account alias management
- `dedup audit-names` / `detect-equivalences` — Institution naming audit

### Rules
- `rules show` — Dump parsed rules.yaml
- `rules edit` — Open in $EDITOR
- `rules validate` — Check rules against DB categories
- `rules test --description "STARBUCKS"` — Test matching
- `rules add-keyword --keyword "VENDOR" --category "Dining"` — Add keyword to rules (`--use-type`, `--priority`)
- `rules remove-keyword --keyword "VENDOR"` — Remove keyword from rules
- `rules list` — Structured keyword rules listing (via MCP: `rules_list`)
- `rules update-priority` — Change rule priority (via MCP: `rules_update_priority(rule_index, priority)`)
- `rules add-keyword --keyword "VENDOR" --category Dining` — Add keyword to rules (`--use-type`, `--priority`)
- `rules remove-keyword --keyword "VENDOR"` — Remove keyword from rules

### Financial Reports
- `balance show` / `net-worth` / `history` — Balances and net worth
- `liability show` / `upcoming` / `obligations` — Liabilities, due dates, consolidated fixed monthly costs
- `liquidity` — Liquidity snapshot
- `summary` — Financial health dashboard (net worth, cash flow, risk metrics, obligations, data health)
- `spending trends` — Category-by-month spending pivot with trend arrows (`--months N`, `--view`)
- `projection` — Net worth projection over N months (`--months 12`)
- `budget set/update/delete/list/status/forecast/suggest` — Budget management
- `debt dashboard/interest/simulate/impact` — Debt breakdown, projections, payoff simulation (`--lump-sum` for windfall modeling), spending cut impact
- `subs list/detect/recurring/add/cancel/total/audit` — Subscription tracking
- `goal set/list/status` — Financial goal tracking with progress bars (`--metric`, `--target`, `--direction`)
- `plan create/show/review` — Monthly financial planning
- `daily` / `weekly` — Spending summaries (`--view personal|business|all`)
- `monthly run` — Pipeline runner: dedup → categorize → detect → export (`--sync`, `--ai`, `--dry-run`, `--skip`)

### Business Accounting
- `biz pl` — P&L income statement (`--month`, `--quarter`, `--year`, `--compare`)
- `biz cashflow` — Cash flow statement for business accounts
- `biz tax` — Schedule C summary with full line flow and deduction percentages
- `biz estimated-tax` — Quarterly estimated tax at configurable effective rate
- `biz forecast` — Revenue projections by stream with trend (`--months N`, `--streams`)
- `biz runway` — Burn rate and cash runway estimate (`--months N`)
- `biz seasonal` — Month-of-year seasonal revenue averages with confidence levels
- `biz budget set` — Set section-level expense budget (`--section`, `--amount`, `--period`, `--from`)
- `biz budget status` — Budget vs actual spend per P&L section (`--month`)
- `biz mileage add/list/summary` — Mileage log with IRS standard rate deduction (`--date`, `--miles`, `--purpose`)
- `biz contractor add/list/link` — 1099 contractor tracking with payment linking
- `biz 1099-report` — Per-contractor payment totals with threshold flagging (`--year`)
- `account set-business <id> --business --backfill` — Flag account as business

### Integrations
- `plaid link/sync/balance-refresh/liabilities-sync/status/unlink` — Plaid
- `schwab sync/status` — Schwab brokerage
- `stripe link/sync/status/revenue/unlink` — Stripe (balance transaction sync: gross charges + fees + refunds)
- `provider status/switch` — Institution-level provider routing

### Notifications
- `notify test --channel telegram` — Test notification channel
- `notify budget-alerts` — Send budget alerts via configured channel (`--channel`, `--month`, `--view`)

### Setup & System
- `setup check/init/connect/status` — Environment readiness, category seeding, Plaid linking, health dashboard
- `db status/backup/reset` — Database maintenance
- `export csv/summary/wave` — Data export
- `migrate --source <dir>` — Legacy migration

## Categorization Pipeline

Priority order (first match wins):

1. **Payment keywords** → `Payments & Transfers`, `is_payment=1` (from `rules.yaml` `payment_keywords`)
2. **Payment exclusions** → Suppresses Plaid `is_payment` flag (e.g., "Check" pattern for rent checks)
3. **Vendor memory (exact)** → Normalized description lookup in `vendor_memory` table
4. **Vendor memory (prefix)** → Prefix fuzzy match, longest pattern wins
5. **Plaid is_payment** → Demoted below vendor memory; only fires if no vendor memory match
6. **Keyword rules** → `rules.yaml` `keyword_rules`, longest keyword wins
7. **Category mappings** → `source_category` lookup in `category_mappings` table
8. **Plaid PFC** → Map Plaid `personal_finance_category` to canonical category
9. **AI** (optional) → `cat auto-categorize --ai`, batch calls Claude/OpenAI

After matching: category overrides may force `use_type`, split rules may split into business/personal child transactions. All match paths set `is_payment` based on whether the resolved category is "Payments & Transfers".

## Database Schema (Key Tables)

All monetary values: integer cents. IDs: hex UUIDs (TEXT). Backend: SQLite.

| Table | Purpose |
|---|---|
| `transactions` | Core: amount_cents, category_id, category_source, source, is_payment, is_active, dedupe_key |
| `categories` | Hierarchy: name, parent_id, is_income, level (0=parent, 1=leaf) |
| `vendor_memory` | Pattern→category rules: description_pattern, category_id, confidence, is_confirmed |
| `accounts` | Bank accounts: institution_name, account_type, card_ending, inline balances |
| `account_aliases` | Links hash-based (CSV/PDF) accounts to canonical Plaid accounts |
| `budgets` | Per-category budgets with overlap-prevention triggers |
| `subscriptions` | Tracked subscriptions: vendor, amount, frequency, sub_type (fixed/metered) |
| `recurring_flows` | Income/expense recurring patterns |
| `plaid_items` | Plaid connections: access_token_ref, sync_cursor, status |
| `balance_snapshots` | Historical balance records |
| `liabilities` | Credit/student/mortgage details |
| `import_batches` | Import audit trail: file_hash prevents re-import |
| `category_mappings` | source_category → canonical category lookups |
| `ai_categorization_log` | AI categorization audit trail |
| `provider_routing` | Institution→provider routing overrides |
| `pl_section_map` | Category → P&L section (revenue, cogs, opex_*) |
| `schedule_c_map` | Category → Schedule C line items with deduction % |
| `stripe_connections` | Stripe API connections: api_key_ref, last_sync cursor |
| `goals` | Financial goals: metric, target_cents/pct, starting_cents/pct, direction, deadline |
| `biz_section_budgets` | Section-level expense budgets: pl_section, amount_cents, period, effective_from/to |
| `mileage_log` | Business mileage trips: trip_date, miles, destination, purpose, vehicle |
| `mileage_rates` | IRS standard mileage rates by tax year (cents per mile) |
| `contractors` | 1099-NEC contractors: name, tin_last4, entity_type |
| `contractor_payments` | Links transactions to contractors with paid_via_card flag |

FTS5 virtual table `txn_fts` mirrors transaction descriptions for full-text search.

## Category Hierarchy

| Parent Group | Leaf Categories |
|---|---|
| Food & Drink | Coffee, Dining, Groceries |
| Travel & Vacation | Transportation, Travel |
| Housing | Rent, Utilities, Home Improvement, Office Expense, Supplies |
| Financial | Bank Charges & Fees, Payments & Transfers, Taxes, Insurance, Depreciation, Taxes & Licenses |
| Lifestyle | Shopping, Entertainment, Personal Expense, Donations, Childcare |
| Professional | Software & Subscriptions, Professional Fees, Advertising, Contract Labor |
| Health | Health & Wellness |
| Income | Income: Salary, Income: Business, Income: Other, Cost of Goods Sold |
| Other | (standalone) |

## Rules System (`finance_cli/data/rules.yaml`)

Hot-reloaded (mtime-cached). Sections:

- **`keyword_rules`** — keyword list → category + optional use_type + priority. Longest keyword wins ties.
- **`payment_keywords`** — flat list, highest priority, tags `Payments & Transfers`
- **`payment_exclusions`** — patterns that suppress Plaid's `is_payment` flag (e.g., "Check" for rent checks)
- **`category_aliases`** — non-canonical name → canonical name (or null to drop)
- **`split_rules`** — match by category/keyword → split into business_pct/personal with child transactions
- **`category_overrides`** — force use_type on specific categories (unless user-set)
- **`income_sources`** — named configs for income CSV column mapping
- **`essential_categories`** — list of category names treated as essential by `subs audit` and `debt impact` (default: Utilities, Insurance, Health & Wellness, Rent, Housing, Childcare)
- **`ai_parser`** / **`ai_categorizer`** — provider/model/batch config

## Key File Locations

| Path | Description |
|---|---|
| `finance_cli/__main__.py` | CLI entry point, argument parser |
| `finance_cli/commands/` | One module per command group |
| `finance_cli/categorizer.py` | `match_transaction()`, `apply_match()` |
| `finance_cli/user_rules.py` | `load_rules()`, `CANONICAL_CATEGORIES` |
| `finance_cli/dedup.py` | Cross-format dedup logic |
| `finance_cli/importers/` | CSV normalization layer |
| `finance_cli/extractors/` | PDF extraction backends |
| `finance_cli/provider_routing.py` | Institution-level provider routing guards |
| `finance_cli/debt_calculator.py` | Debt dashboard, interest projection, paydown simulation |
| `finance_cli/spending_analysis.py` | Shared essential categories, N-month category spending averages |
| `finance_cli/stripe_client.py` | Stripe API wrapper: balance transaction sync, payout dedup |
| `finance_cli/forecasting.py` | Revenue streams, trend analysis, burn rate, runway, seasonal patterns |
| `finance_cli/commands/biz_cmd.py` | Business accounting: P&L, cash flow, Schedule C, estimated tax, forecast, runway, seasonal, budget |
| `finance_cli/commands/stripe_cmd.py` | Stripe commands: link, sync, status, revenue, unlink |
| `finance_cli/commands/summary_cmd.py` | Financial health summary dashboard |
| `finance_cli/commands/spending_cmd.py` | Spending trends (month-over-month pivot) |
| `finance_cli/commands/projection_cmd.py` | Net worth projection |
| `finance_cli/commands/goal_cmd.py` | Goal tracking: set, list, status with progress bars |
| `finance_cli/notify.py` | Notification dispatch: Telegram + iMessage channels |
| `finance_cli/mcp_server.py` | FastMCP server exposing 130 tools for Claude Code |
| `finance_cli/migrations/` | 29 numbered SQL migrations (001–029) |
| `finance_cli/data/rules.yaml` | User-editable categorization rules + essential_categories |
| `finance_cli/data/finance.db` | SQLite database (override: `$FINANCE_CLI_DB`) |
| `finance_cli/tests/` | ~70 pytest modules, 1155 tests |

## Testing

```bash
python3 -m pytest -q                           # all tests
python3 -m pytest finance_cli/tests/test_categorizer.py -q  # single module
```

Tests use in-memory SQLite — no external services required.

## Common Diagnostic Patterns

**Uncategorized transactions:**
```bash
python3 -m finance_cli txn list --uncategorized --limit 20
python3 -m finance_cli cat auto-categorize --dry-run   # preview what would match
```

**Misclassified transaction:**
```bash
python3 -m finance_cli txn explain <id>        # see why it matched
python3 -m finance_cli rules test --description "MERCHANT NAME"  # test rules
```

**Dedup issues:**
```bash
python3 -m finance_cli dedup cross-format --account-id <id>  # dry-run by default
python3 -m finance_cli dedup audit-names       # find naming gaps
```

**Missing imports:**
```bash
python3 -m finance_cli txn coverage            # date gaps per account
python3 -m finance_cli db status               # overall counts
```

**Financial health overview:**
```bash
python3 -m finance_cli summary --format cli     # one-page financial dashboard
python3 -m finance_cli spending trends --months 6 --format cli  # category trends
python3 -m finance_cli projection --months 12 --format cli  # net worth projection
python3 -m finance_cli goal status --format cli  # progress on financial goals
```

**Debt management:**
```bash
python3 -m finance_cli debt dashboard --format cli  # current debt breakdown
python3 -m finance_cli debt simulate --extra 500 --strategy compare --format cli  # payoff strategies
python3 -m finance_cli debt simulate --extra 500 --lump-sum 5000 --format cli  # windfall modeling
python3 -m finance_cli debt impact --cut-pct 50 --format cli  # spending cut impact on debt
```

## MCP Server

`finance_cli/mcp_server.py` exposes 130 tools via FastMCP for Claude Code integration. Registered globally:

```bash
# Already registered in ~/.claude.json as "finance-cli"
# To re-register:
claude mcp add --scope user finance-cli -- python3 -m finance_cli.mcp_server
# Then fix cwd in ~/.claude.json (move from args to cwd field)
```

Tool categories: status (3), accounts (6), reports (17), transactions (9), categorization (10), setup/import (6), pipeline (1), database (1), business (7), stripe (5), debt (4), forecasting (3), budget (9), mileage (3), contractors (4), subscriptions (7), dedup (7), plan (3), goals (3), rules (8), notify (2), export (4), provider (2), schwab (2), sheets (1), liability (3). Each tool wraps existing CLI handlers and returns `{data, summary}` dicts. Large outputs use `summary_only` param (default True) to cap response size for agent consumption.

## Documentation

See `docs/` for deep dives:
- `docs/overview/HOW_IT_WORKS.md` — Architecture with sequence diagrams
- `docs/overview/PROJECT_GUIDE.md` — Detailed project guide
- `docs/AGENT_WORKFLOWS.md` — AI agent operational playbooks (8 workflows)
- `docs/ingest/INGEST_WORKFLOW.md` — Monthly import runbook
- `docs/developer/ADD_INSTITUTION_RUNBOOK.md` — Adding new CSV normalizers
- `docs/developer/AI_STATEMENT_PARSE_SPEC.md` — AI PDF parser contract
- `docs/planning/TODO.md` — Forward-looking tasks
- `docs/planning/BUG_BACKLOG.md` — Known issues