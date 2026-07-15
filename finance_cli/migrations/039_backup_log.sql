CREATE TABLE IF NOT EXISTS backup_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_type     TEXT NOT NULL CHECK (backup_type IN ('local', 'offhost', 'pre_migration', 'pre_restore', 'restore')),
    status          TEXT NOT NULL CHECK (status IN ('started', 'completed', 'failed', 'verified')),
    bundle_path     TEXT,
    bundle_sha256   TEXT,
    bundle_size     INTEGER,
    db_sha256       TEXT,
    migration_ver   INTEGER,
    duration_ms     INTEGER,
    error_message   TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_backup_log_type ON backup_log(backup_type);
CREATE INDEX idx_backup_log_created ON backup_log(created_at);
CREATE INDEX idx_backup_log_status ON backup_log(status);
