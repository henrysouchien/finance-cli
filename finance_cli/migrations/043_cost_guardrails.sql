CREATE TABLE IF NOT EXISTS cost_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'openai', 'plaid')),
    operation TEXT NOT NULL,
    cost_usd6 INTEGER NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_creation_tokens INTEGER,
    cache_read_tokens INTEGER,
    model TEXT,
    request_id TEXT,
    is_estimated INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_cost_provider_created ON cost_ledger(provider, created_at);
CREATE INDEX IF NOT EXISTS idx_cost_operation_created ON cost_ledger(operation, created_at);
CREATE INDEX IF NOT EXISTS idx_cost_created ON cost_ledger(created_at);

CREATE TABLE IF NOT EXISTS cost_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'openai', 'plaid', 'all')),
    period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    limit_usd6 INTEGER NOT NULL,
    action TEXT NOT NULL DEFAULT 'warn' CHECK (action IN ('warn', 'block')),
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(provider, period)
);

CREATE TABLE IF NOT EXISTS cost_alert_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL CHECK (provider IN ('claude', 'openai', 'plaid', 'all')),
    period TEXT NOT NULL CHECK (period IN ('daily', 'monthly')),
    threshold TEXT NOT NULL,
    period_bucket TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(provider, period, threshold, period_bucket)
);

INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('claude', 'daily', 5000000, 'warn');
INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('claude', 'monthly', 50000000, 'warn');
INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('openai', 'monthly', 20000000, 'warn');
INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('plaid', 'monthly', 10000000, 'warn');
INSERT OR IGNORE INTO cost_limits (provider, period, limit_usd6, action)
VALUES ('all', 'monthly', 100000000, 'warn');
