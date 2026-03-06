CREATE TABLE IF NOT EXISTS provider_routing (
    institution_name TEXT PRIMARY KEY,
    provider         TEXT NOT NULL,
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
