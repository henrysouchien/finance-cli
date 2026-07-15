# Project Guide

Last audited: 2026-06-27

This repo has four active runtime areas:

- `finance_cli/` — shared Python engine, CLI, MCP tools, gateway helpers, Telegram modules, sync/storage dispatch, migrations, tests.
- `finance-web/` — FastAPI backend, React/Vite frontend, auth, billing, web chat, Plaid/Telegram webhooks, per-user data.
- `services/storage_server/` — dedicated gRPC proxy for remote per-user SQLite/SQLCipher storage.
- `packages/cashnerd-mcp/` — npm package that installs the local MCP binary and skills.

The CLI/engine remains the source of truth for finance behavior. The web app and agents should call into it instead of duplicating ledger logic.

## Quick Start

CLI / engine:

```bash
cd .
python3 -m pip install -e '.[dev]'
python3 -m finance_cli --help
python3 -m pytest -q
```

Web stack:

```bash
python3 -m pip install -e './finance-web[dev]'
cd finance-web/frontend
npm install
```

Then use `finance-web/README.md` for `.env`, gateway, backend/frontend, and Docker Compose commands.

Storage server:

```bash
cd services/storage_server
python3 -m pip install -e '.[test]'
python3 -m pytest -q
```

## Command Surface

Top-level groups from `python3 -m finance_cli --help`:

```text
txn account cat daily weekly budget export ingest db dedup subs liquidity
balance liability loan plan plaid stripe schwab provider rules setup monthly
notify debt biz summary spending projection goal interventions reminders error issue
ops migrate
```

Command families:

- Ledger/data quality: `txn`, `account`, `cat`, `rules`, `dedup`, `db`
- Planning/reporting: `daily`, `weekly`, `summary`, `spending`, `projection`, `liquidity`, `balance`, `liability`, `debt`, `budget`, `goal`, `plan`, `biz`
- Ingest/integrations: `ingest`, `plaid`, `stripe`, `schwab`, `provider`, `export`, `monthly`
- Coaching/ops: `interventions`, `reminders`, `notify`, `setup`, `error`, `issue`, `ops`, `migrate`

JSON is the default output. Use `--format cli` for reports intended for people.

## Common Workflows

Health check:

```bash
python3 -m finance_cli setup check --format cli
python3 -m finance_cli db status --format cli
python3 -m finance_cli summary --format cli
```

Provider refresh:

```bash
python3 -m finance_cli plaid status --format cli
python3 -m finance_cli plaid sync --format cli
python3 -m finance_cli plaid balance-refresh --format cli
python3 -m finance_cli plaid liabilities-sync --format cli
python3 -m finance_cli stripe status --format cli
python3 -m finance_cli schwab status --format cli
```

Local file ingest:

```bash
python3 -m finance_cli ingest batch --dir ./inbox --commit --format cli
python3 -m finance_cli dedup cross-format --commit --format cli
python3 -m finance_cli dedup backfill-aliases --commit --format cli
```

Monthly cycle:

```bash
python3 -m finance_cli monthly run --sync --ai --format cli
python3 -m finance_cli notify budget-alerts --channel telegram --format cli
python3 -m finance_cli interventions list --format cli
```

Backup/export:

```bash
python3 -m finance_cli db backup --format cli
python3 -m finance_cli db export-preferences --format cli
python3 -m finance_cli export sheets --new --format cli
```

## Repo Map

```text
finance_cli/
  __main__.py              CLI entrypoint and command dispatch
  commands/                One module per command group
  migrations/              SQLite/SQLCipher migrations, current schema version 79
  mcp_server.py            FastMCP tool library
  tool_registry.py         MCP metadata/classification registry
  gateway/                 Chat gateway service, tools, code execution
  storage_client/          gRPC client for remote storage users
  sync/                    local/web sync auth, middleware, subscribers
  telegram_bot/            local Telegram bot helpers and BotStore
  tests/                   CLI/engine pytest suite

finance-web/
  server/app.py            FastAPI app factory
  server/routers/          API route modules
  server/migrations/       PostgreSQL migrations
  frontend/src/            React app

services/storage_server/
  src/storage_server/      gRPC proxy implementation
  proto/                   protobuf contract
  scripts/                 build, install, smoke, proto helpers
  tests/                   unit/integration/load tests

docs/
  architecture/            Current architecture and legacy rationale
  overview/                Start-here guides
  operations/              Ops runbooks
  runbooks/                Storage/Stripe runbooks
  developer/               Implementation guides
  planning/, completed/    Point-in-time planning/archive docs
```

## Runtime Notes

- `.env` at the repo root is auto-loaded by the CLI unless `FINANCE_CLI_DISABLE_DOTENV=1`.
- `FINANCE_CLI_DB` overrides the local DB path.
- Money is integer cents throughout the engine.
- File ingest is dry-run by default; use `--commit` to write.
- Provider sync cooldowns can be bypassed with `--force`.
- `GATEWAY_USER_KEYS` is required for gateway identity; the old shared gateway key envs are rejected.
- Web per-user routes should use `dependencies.get_user_conn()` so storage leases and remote dispatch are honored.
- Remote-storage users must not be forced back to local reads by toggling `FINANCE_CLI_STORAGE_CLIENT_ENABLED=false` without rollback.

## Verification

Last audit results from 2026-06-27:

```bash
.venv/bin/python -m pytest --collect-only -q finance_cli/tests
# 4245 tests collected

.venv/bin/python -m pytest --collect-only -q finance-web/server/tests
# 740 tests collected

cd finance-web/frontend
npm test -- --reporter=dot --passWithNoTests
# 545 tests passed in 48 files

cd services/storage_server
../../.venv/bin/python -m pytest --collect-only -q
# 157 tests collected
```

Prefer collect/test commands over copying these counts into new docs.

## Related Docs

- `../architecture/CURRENT_STATE.md` — current architecture.
- `HOW_IT_WORKS.md` — runtime walkthrough.
- `../../finance_cli/README.md` — core package guide.
- `../../finance-web/README.md` — web setup and verification.
- `../../services/storage_server/README.md` — storage proxy package guide.
- `../../scripts/README.md` — operator/package script guide.
- `../runbooks/STORAGE_SERVER_ARCHITECTURE.md` — storage-server on-call guide.
- `../operations/SECRET_ROTATION.md` — secret/key rotation.
- `../developer/ADD_INSTITUTION_RUNBOOK.md` — CSV normalizer workflow.
