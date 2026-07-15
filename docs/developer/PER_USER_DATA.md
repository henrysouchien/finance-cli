# Per-User Data Structure

This document describes the multi-tenant data isolation model used in production.
Authentication lives in a shared PostgreSQL database; all financial data lives in
per-user SQLite databases and files on disk.

## Architecture Overview

```
PostgreSQL (shared)              File System (per-user)
┌──────────────────┐             {data_root}/
│ users             │             ├── 42/
│ user_sessions     │             │   ├── finance.db       ← SQLite (all financial data)
│ (api_key_enc col) │             │   ├── rules.yaml       ← categorization rules
└──────────────────┘             │   └── uploads/          ← file uploads
                                 └── 107/
                                     ├── finance.db
                                     ├── rules.yaml
                                     └── uploads/
```

User IDs are PostgreSQL `BIGSERIAL` integers (e.g. `42`, `107`), used directly as
directory names.

## PostgreSQL Auth DB

Managed by `app_platform` (`PostgresUserStore`, `PostgresSessionStore`).
Connected via `DATABASE_URL` env var.

| Table | Migration | Key Columns |
|-------|-----------|-------------|
| `users` | 001 | `id`, `email`, `auth_provider`, `google_user_id`, `tier`, timestamps |
| `user_sessions` | 001 | `session_id`, `user_id` (FK), `expires_at` |
| `anthropic_api_key_enc` | 003 | Column on `users` — Fernet-encrypted per-user API key |

Indexes on `google_user_id`, `email`, `session_id`, `user_id`, `expires_at`.

## Per-User Directory Layout

Each user directory at `{data_root}/{user_id}/` contains:

| File | Purpose | Created By |
|------|---------|------------|
| `finance.db` | SQLite database — transactions, accounts, categories, budgets, subscriptions, goals, vendor memory, and all other financial data. WAL mode enabled. | `provision_user()` via `initialize_database()` |
| `rules.yaml` | Keyword rules, payment keywords, split rules, category overrides, income sources. Hot-reloaded by the categorization pipeline. | Copied from `rules_template.yaml` on first provision |
| `uploads/` | User-uploaded CSV/PDF files for statement import. | Created on demand by the gateway |

The SQLite database runs all numbered migrations from `finance_cli/migrations/` on
init. Schema version is tracked in a `schema_version` table so migrations never re-run.

## Provisioning Flow

Three entry points trigger provisioning, all calling the same function:

1. **OAuth login** — `FinanceAuthService.on_user_created()` in `finance-web/server/auth.py`
2. **Web API request** — `get_user_paths()` dependency in `finance-web/server/dependencies.py`
3. **Gateway chat request** — `chat()` handler in `finance_cli/gateway/server.py`

All three call `provision_user()` from `finance_cli/user_provisioning.py`:

```python
provision_user(
    data_root=settings.data_root,
    user_id=user_id,
    template_rules_path=settings.template_rules_path,
)
```

**What it does:**
1. Validates `user_id` against path traversal (rejects `/`, `\\`, `..` segments)
2. Creates `{data_root}/{user_id}/` directory (`exist_ok=True`)
3. Runs `initialize_database()` on `finance.db` (applies pending migrations)
4. Copies `rules_template.yaml` → `rules.yaml` (only if `rules.yaml` doesn't exist)

**Idempotency:** Safe to call on every request. Directory creation, DB migrations,
and template copy are all no-ops if already done.

## Path Injection & Security

The web server and gateway both resolve user paths server-side and inject them into
tool calls. The model never supplies its own paths.

**Web (FastAPI):** `get_user_paths()` in `finance-web/server/dependencies.py` extracts
the user from the session cookie, provisions if needed, and returns `db_path` and
`rules_path` as a dependency.

**Gateway:** `UserScopedDispatcher` in `finance_cli/gateway/server.py` intercepts every
tool call and:
1. **Strips** any model-supplied arguments starting with `_user_` (prevents override)
2. **Injects** server-derived paths into the clean input:

```python
user_paths = {
    "_user_db_path": str(db_path),
    "_user_rules_path": str(rules_path),
    "_user_uploads_dir": str(uploads_dir),
    "_request_id": request_id,
    "_session_id": session_id,
}
```

This ensures complete isolation — a user's tools can only access their own database
and files, regardless of what the model requests.

**Path traversal protection** in `user_dir()`:
- Rejects user IDs containing `/`, `\\`, `.`, or `..`
- Resolves paths and verifies the result's parent is exactly `data_root`

## Environment Variables

| Variable | Default | Service | Purpose |
|----------|---------|---------|---------|
| `DATABASE_URL` | (required) | finance-web | PostgreSQL connection string |
| `FINANCE_WEB_DATA_ROOT` | `finance-web/data/users` | finance-web | Per-user data root |
| `FINANCE_GATEWAY_DATA_ROOT` | `finance-web/data/users` | gateway | Per-user data root |
| `FINANCE_WEB_RULES_TEMPLATE` | `finance_cli/data/rules_template.yaml` | finance-web | Template for new users |
| `FINANCE_GATEWAY_RULES_TEMPLATE` | `finance_cli/data/rules_template.yaml` | gateway | Template for new users |

In production, both services point `*_DATA_ROOT` at the same directory so that web API
requests and gateway chat sessions access the same user databases. The systemd units
(`infra/systemd/finance-web.service`, `finance-gateway.service`) load these from a
shared `.env` file.

**Deploy note:** The `scripts/deploy_web.sh` bundle excludes `finance-web/data/` entirely.
User data directories persist across deploys and are never overwritten.

## Key Files

| File | Role |
|------|------|
| `finance_cli/user_provisioning.py` | `user_dir()`, `provision_user()` — core provisioning logic |
| `finance-web/server/auth.py` | `FinanceAuthService.on_user_created()` — OAuth trigger |
| `finance-web/server/dependencies.py` | `get_user_paths()` — FastAPI path injection |
| `finance-web/server/config.py` | `Settings.from_env()` — web env var resolution |
| `finance_cli/gateway/server.py` | `UserScopedDispatcher` — gateway path injection + stripping |
| `finance_cli/gateway/config.py` | `load_settings()` — gateway env var resolution |
| `finance-web/server/migrations/001_users_sessions.sql` | PostgreSQL auth schema |
| `finance-web/server/migrations/003_user_api_keys.sql` | Encrypted API key column |
| `finance_cli/data/rules_template.yaml` | Template copied to new users |
| `scripts/deploy_web.sh` | Deploy script (excludes user data from bundles) |
| `infra/systemd/finance-web.service` | Web backend service unit |
| `infra/systemd/finance-gateway.service` | Gateway service unit |
