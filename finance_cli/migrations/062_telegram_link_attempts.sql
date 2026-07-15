CREATE TABLE IF NOT EXISTS telegram_link_attempts (
    chat_id         TEXT PRIMARY KEY,
    failed_count   INTEGER NOT NULL DEFAULT 0,
    first_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
    locked_until   TEXT
);

CREATE INDEX IF NOT EXISTS idx_tg_link_attempts_locked_until
    ON telegram_link_attempts(locked_until);
