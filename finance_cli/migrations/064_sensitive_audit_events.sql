CREATE TABLE IF NOT EXISTS sensitive_audit_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    user_id         TEXT,
    actor_type      TEXT NOT NULL CHECK (actor_type IN ('user', 'system', 'admin', 'agent')),
    actor_id_hash   TEXT,
    event_type      TEXT NOT NULL,
    target_type     TEXT,
    target_id_hash  TEXT,
    surface         TEXT NOT NULL CHECK (surface IN ('web', 'mcp', 'cli', 'sync', 'telegram', 'cron')),
    outcome         TEXT NOT NULL DEFAULT 'succeeded' CHECK (outcome IN ('started', 'succeeded', 'failed', 'denied')),
    request_id      TEXT,
    session_id_hash TEXT,
    ip_hash         TEXT,
    user_agent_hash TEXT,
    details         TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details)),
    prev_hash       TEXT,
    row_hash        TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_sensitive_audit_user_ts
    ON sensitive_audit_events(user_id, ts);

CREATE INDEX IF NOT EXISTS idx_sensitive_audit_event_ts
    ON sensitive_audit_events(event_type, ts);

CREATE INDEX IF NOT EXISTS idx_sensitive_audit_target
    ON sensitive_audit_events(target_type, target_id_hash);

CREATE TRIGGER IF NOT EXISTS sensitive_audit_events_no_update
BEFORE UPDATE ON sensitive_audit_events
BEGIN
    SELECT RAISE(ABORT, 'sensitive_audit_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS sensitive_audit_events_no_delete
BEFORE DELETE ON sensitive_audit_events
BEGIN
    SELECT RAISE(ABORT, 'sensitive_audit_events is append-only');
END;
