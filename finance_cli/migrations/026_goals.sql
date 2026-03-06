-- Goal tracking table
CREATE TABLE IF NOT EXISTS goals (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    metric TEXT NOT NULL CHECK(metric IN ('net_worth', 'liquid_cash', 'total_debt', 'investments', 'savings_rate')),
    target_cents INTEGER,
    target_pct REAL,
    starting_cents INTEGER,
    starting_pct REAL,
    direction TEXT NOT NULL DEFAULT 'up' CHECK(direction IN ('up', 'down')),
    deadline TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    CHECK(target_cents IS NOT NULL OR target_pct IS NOT NULL)
);
