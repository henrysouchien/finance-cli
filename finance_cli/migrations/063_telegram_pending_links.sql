CREATE TABLE IF NOT EXISTS telegram_pending_links (
    id                 INTEGER PRIMARY KEY CHECK (id = 1),
    chat_id            TEXT NOT NULL,
    telegram_user_id   TEXT,
    telegram_username  TEXT,
    telegram_first_name TEXT,
    telegram_last_name TEXT,
    requested_at       TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tg_pending_links_expires_at
    ON telegram_pending_links(expires_at);
