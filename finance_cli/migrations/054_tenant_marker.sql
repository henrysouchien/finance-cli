-- Tenant marker: second line of defense for per-user SQLite isolation.
-- Row is stamped by provision_user() after migrations run -- migrations
-- don't know which user_id owns the DB.

CREATE TABLE IF NOT EXISTS tenant_marker (
    singleton  INTEGER PRIMARY KEY CHECK (singleton = 1),
    user_id    TEXT NOT NULL,
    stamped_at TEXT NOT NULL DEFAULT (datetime('now'))
);
