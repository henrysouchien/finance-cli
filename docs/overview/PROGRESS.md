# Current State Snapshot

Last audited: 2026-06-27

This file is a short current-state snapshot. It replaced an older chronological progress log because the log had become stale. For architecture truth, use `../architecture/CURRENT_STATE.md`. For work planning, use `../planning/TODO.md` and verify against code before acting.

## Current Runtime

- Core engine/CLI: `finance_cli/`
- Web app: `finance-web/`
- Remote storage proxy: `services/storage_server/`
- Local MCP npm package: `packages/cashnerd-mcp/`
- Infrastructure/runbooks: `infra/`, `scripts/`, `docs/runbooks/`, `docs/operations/`

## Verified Counts

Collected on 2026-06-27:

| Area | Count |
|---|---:|
| CLI pytest collection | 4,245 tests |
| Web backend pytest collection | 740 tests |
| Frontend Vitest run | 545 tests / 48 files |
| Storage-server pytest collection | 157 tests |
| MCP tools in `tool_registry` | 290 |
| CLI SQL migration files | 75 files, schema version 79 |
| Web PostgreSQL migrations | 24 files |

Use the verification commands in `../../README.md` before refreshing these numbers.

## Current Capabilities

- Ledger, categorization, budgets, goals, subscriptions, debt tools, reminders, interventions, business reporting.
- Plaid transactions/balances/liabilities, Stripe billing/revenue, Schwab integration, CSV/PDF ingest.
- AI statement parsing, AI categorization, gateway chat, Telegram chat, MCP tools, coaching skills.
- Web auth, per-user data, billing tiers/credits/cost meter, support intake, account deletion, sync sessions.
- Remote storage-server data plane with JWT auth, SQL safety, KMS envelope encryption, session pooling, cutover/DR runbooks.

## Active Operational Priorities

Treat `../planning/TODO.md` as the current work tracker, not this file. At the time of this docs audit, the main areas called out by current docs are:

- Paid pilot launch signoff, privacy/legal publication, and provider/MFA gates.
- Storage-server architecture validation after representative active production use, plus additional-user migration gates.
- AWS local credential posture and MFA/SSO cleanup.
- AI cost/cap telemetry calibration after an attributed rollup window exists.
- Founder 2025 tax-prep data/document readiness.
- CFP/advisor substrate human review and broader-claims legal/compliance gates.
- Manual cleanup of the historical tokenless Plaid orphan tracked as `PLAID-011`.

## Non-Current History

The old chronological implementation history is intentionally not reproduced here. Use git history and `../completed/` when historical context is needed, and avoid treating old phase labels as current operational state.
