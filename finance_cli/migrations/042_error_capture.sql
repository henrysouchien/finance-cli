CREATE TABLE IF NOT EXISTS errors (
    id                TEXT PRIMARY KEY,
    fingerprint       TEXT NOT NULL UNIQUE,
    severity          TEXT NOT NULL CHECK (severity IN ('critical', 'error', 'warning')),
    source            TEXT NOT NULL CHECK (source IN ('web', 'gateway', 'telegram', 'mcp', 'import', 'cron', 'frontend', 'cli', 'startup', 'agent')),
    endpoint          TEXT,
    error_type        TEXT NOT NULL,
    message           TEXT NOT NULL,
    traceback         TEXT,
    context           TEXT CHECK (context IS NULL OR json_valid(context)),
    user_id           TEXT,
    request_id        TEXT,
    environment       TEXT DEFAULT 'production' CHECK (environment IN ('production', 'development', 'test')),
    release_sha       TEXT,
    status            TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'wontfix')),
    resolved_at       TEXT,
    resolution        TEXT,
    occurrence_count  INTEGER NOT NULL DEFAULT 1,
    first_seen        TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen         TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_errors_status ON errors(status);
CREATE INDEX IF NOT EXISTS idx_errors_severity ON errors(severity);
CREATE INDEX IF NOT EXISTS idx_errors_source ON errors(source);
CREATE INDEX IF NOT EXISTS idx_errors_last_seen ON errors(last_seen);
CREATE INDEX IF NOT EXISTS idx_errors_user_id ON errors(user_id);
CREATE INDEX IF NOT EXISTS idx_errors_request_id ON errors(request_id);

CREATE TABLE IF NOT EXISTS error_occurrences (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    error_id    TEXT NOT NULL REFERENCES errors(id),
    request_id  TEXT,
    user_id     TEXT,
    context     TEXT CHECK (context IS NULL OR json_valid(context)),
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_error_occ_error ON error_occurrences(error_id, created_at);

CREATE TABLE IF NOT EXISTS error_alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint     TEXT NOT NULL,
    alert_reason    TEXT NOT NULL,
    window_key      TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(window_key)
);
CREATE INDEX IF NOT EXISTS idx_error_alerts_fp ON error_alerts(fingerprint, created_at);

CREATE TABLE IF NOT EXISTS issue_reports (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    description TEXT,
    severity    TEXT NOT NULL CHECK (severity IN ('bug', 'warning', 'suggestion')),
    status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'investigating', 'resolved', 'wontfix')),
    resolved_at TEXT,
    resolution  TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
