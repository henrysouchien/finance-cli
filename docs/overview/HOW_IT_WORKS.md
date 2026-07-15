# How The System Works

Last audited: 2026-06-27

This is the plain-English runtime walkthrough. For the shortest architecture map, read `../architecture/CURRENT_STATE.md` first.

## 1. CLI And Engine

Every CLI command follows the same skeleton:

```text
python3 -m finance_cli <command>
    |
    v
finance_cli/__main__.py
    - load .env
    - build argparse parser
    - initialize_database()
    - connect()
    |
    v
commands/<group>.py handler
    |
    v
commands/common.py envelope
    - status
    - command
    - data
    - optional summary
    - optional cli_report
```

JSON is the default output. `--format cli` prints the handler's human report when one exists.

`initialize_database()` applies unapplied SQLite migrations from `finance_cli/migrations/`. The schema version is defined in `finance_cli/db.py`.

## 2. Storage Dispatch

`finance_cli/db.py:connect()` is the persistence choke point.

```text
connect(db_path, expected_user_id)
    |
    +--> local sqlite/sqlcipher connection
    |
    +--> remote StorageConnection when:
          - user_id can be resolved
          - FINANCE_CLI_STORAGE_CLIENT_ENABLED=true
          - STORAGE_SERVER_URL is set
          - users.storage_mode is remote
          - request is inside the right storage lease when required
```

Storage modes:

- `local` — open the user's local SQLite/SQLCipher file.
- `migrating` or `replaying` — return a maintenance error and route requests/webhooks into the cutover queue.
- `remote` — use the gRPC storage server.

Remote storage is a one-way operational state unless the cutover runbook rolls the user back. Do not toggle the client off for remote users without following `../runbooks/STORAGE_SERVER_CUTOVER.md`.

## 3. Web Requests

FastAPI routes live under `finance-web/server/routers/`. Most authenticated routes depend on `get_user_conn()`.

```text
browser request
    |
    v
finance-web/server/app.py middleware
    |
    v
auth/session lookup
    |
    v
dependencies.get_user_conn()
    - acquire storage lease
    - provision user paths if missing
    - connect(expected_user_id=user_id)
    |
    v
shared finance_cli logic
    |
    v
JSON response
```

PostgreSQL owns web control-plane state: users, sessions, billing, sync sessions, storage modes, leases, cutover state, support requests, revocation queues, and ops rollups. The per-user finance ledger remains SQLite/SQLCipher, either local to the web host or remote through the storage server.

## 4. Storage Server

The storage server is a separate gRPC service under `services/storage_server/`. It owns remote per-user files:

```text
/var/lib/storage_server/<product>/<user_id>/
    finance.db
    db-dek.enc
    rules.yaml
    backups/
    uploads/
    other sidecars
```

Main RPC families:

- Session lifecycle: `OpenSession`, `CloseSession`
- SQL: `Execute`, `ExecuteMany`, `ExecuteBatch`, `ExecuteScript`
- Files: `ListFiles`, `ReadFile`, `WriteFile`, `DeleteFile`
- Backups/sync: `BackupDatabase`, `ExportUserBackup`, `ExportSyncSnapshot`, `RestoreUserBackup`
- Admin/health: `ProvisionUser`, `DeleteUser`, `AdminHealth`, `FindPlaidItemOwner`

Security checks happen server-side on every request: JWT signature, product/user claims, issuer allowlist, optional synthetic-only gate, path containment, SQL denylist/classification, per-user session binding, and write-lock coordination. KMS wraps per-user DB DEKs and bundle DEKs.

Use `../runbooks/STORAGE_SERVER_ARCHITECTURE.md` for production topology, monitoring, deploy, DR, and incident handling.

## 5. Ingest

There are three main ingest families.

### PDF statements

```text
ingest statement
    -> commands/ingest.py
    -> extractor backend: AI, Azure, or BSC
    -> ingest_validation.py
    -> importers/pdf.py
    -> transactions, balances, import_batches
```

Default behavior is dry-run. Use `--commit` to write.

### CSV files

```text
ingest csv
    -> commands/ingest.py
    -> importers/csv_normalizers.py
    -> importers/__init__.py
    -> transactions, accounts, import_batches
```

Institution adapters normalize source-specific CSVs into a shared row contract. Add new adapters with `../developer/ADD_INSTITUTION_RUNBOOK.md`.

### Providers

```text
plaid sync / balance-refresh / liabilities-sync
    -> commands/plaid_cmd.py
    -> plaid_client.py
    -> provider vault token ref
    -> accounts, transactions, balances, liabilities

stripe sync
    -> commands/stripe_cmd.py
    -> Stripe API
    -> revenue/billing/business reporting rows
```

Provider calls record usage/cost where applicable and obey cooldowns unless `--force` is provided.

## 6. Identity And Dedup

Account identity matters because the same bank account can appear through Plaid, CSV, and PDF.

```text
source file/provider row
    -> canonical institution name
    -> source account id
    -> account_aliases maps hash-based import IDs to canonical accounts
    -> dedupe_key prevents same-source repeats
    -> dedup cross-format handles CSV/PDF/Plaid overlap
```

Important files:

- `finance_cli/institution_names.py`
- `finance_cli/importers/__init__.py`
- `finance_cli/dedup.py`
- `finance_cli/commands/dedup_cmd.py`

## 7. Categorization

Categorization is deterministic first and AI-assisted only when requested.

Current priority shape:

```text
payment keywords
payment exclusions
vendor memory exact match
vendor memory prefix match
Plaid payment flag
keyword rules
source category mappings
Plaid personal_finance_category
optional AI categorization
```

The rules file is `finance_cli/data/rules.yaml` for the local default and per-user `rules.yaml` for web users. `user_rules.py` loads rules; `categorizer.py` applies them.

## 8. MCP, Gateway, And Chat

The MCP tool library is `finance_cli/mcp_server.py`. Tool metadata lives in `finance_cli/tool_registry.py`; gateway/web exposure is derived in `finance_cli/gateway/tools.py`.

```text
chat message
    -> web / Telegram / dev CLI
    -> /api/chat/init creates channel-bound session
    -> gateway streams model response
    -> tool request classified by metadata
    -> read-only tool auto-runs
    -> write tool prompts for approval
    -> BotStore persists messages, request metrics, tool calls
```

Identity-bound `GATEWAY_USER_KEYS` replaced shared gateway API keys. If old envs are set, startup validation fails.

## 9. Billing, Cost, And AI Egress

Billing and cost controls are shared across web, chat, Telegram, and AI categorization.

Key files:

- `finance_cli/billing.py`
- `finance_cli/cost_tracking.py`
- `finance_cli/ai_egress.py`
- `finance-web/server/routers/billing_router.py`
- `finance-web/server/routers/cost_router.py`

Concepts:

- Tiers and active engagements decide feature access.
- Plan config decides default model, cap behavior, and monthly allowance.
- BYOK routes cost attribution differently.
- Credit packs can cover usage beyond allowance.
- Cost rows use USD microdollars (`usd6`) to avoid float drift.

## 10. Observability

Primary observability paths:

- CLI/runtime errors: `finance_cli/error_capture.py`
- Frontend logs: `finance_cli/frontend_logs.py` and `finance-web/server/routers/logs_router.py`
- Performance samples: `finance_cli/perf.py`
- Operation log: `finance_cli/operation_log.py`
- Cost ledger: `finance_cli/cost_tracking.py`
- Storage-server access/audit/health logs: configured in `services/storage_server/src/storage_server/config.py`
- CloudWatch filters/dashboards: `infra/storage_server/` and `infra/finance_web/`

Redaction is shared across Python and frontend TypeScript through paired corpuses/tests.

## 11. Where To Change Things

| If you need to change... | Start here |
|---|---|
| A CLI command | `finance_cli/commands/<group>.py`, then tests in `finance_cli/tests/` |
| Shared finance behavior | Core module in `finance_cli/`, not route-specific code |
| Web API behavior | `finance-web/server/routers/` plus shared engine call sites |
| Web UI behavior | `finance-web/frontend/src/pages/` and `components/` |
| MCP/chat tool behavior | `mcp_server.py`, `tool_registry.py`, `gateway/tools.py` |
| Storage routing | `finance_cli/db.py`, `storage_client/`, `storage_lease.py` |
| Storage server RPCs | `services/storage_server/src/storage_server/` |
| CSV normalizers | `finance_cli/importers/csv_normalizers.py` and `../developer/ADD_INSTITUTION_RUNBOOK.md` |
| Secrets or env docs | `docs/operations/SECRET_ROTATION.md` and `.env.example` files |
