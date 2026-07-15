CREATE TABLE IF NOT EXISTS intervention_mutes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_id TEXT NOT NULL UNIQUE,
    muted_at   TEXT NOT NULL DEFAULT (datetime('now')),
    reason     TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
