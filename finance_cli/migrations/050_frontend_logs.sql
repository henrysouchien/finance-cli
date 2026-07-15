CREATE TABLE IF NOT EXISTS frontend_logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    level      TEXT NOT NULL CHECK (level IN ('warn', 'error')),
    namespace  TEXT,
    message    TEXT NOT NULL,
    page       TEXT,
    metadata   TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_frontend_logs_level_created ON frontend_logs(level, created_at);
CREATE INDEX IF NOT EXISTS idx_frontend_logs_namespace_created ON frontend_logs(namespace, created_at);
