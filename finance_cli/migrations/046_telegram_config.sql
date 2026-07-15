CREATE TABLE IF NOT EXISTS telegram_config (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    bot_token_ref   TEXT NOT NULL,
    bot_username    TEXT,
    bot_first_name  TEXT,
    bot_id          INTEGER,
    chat_id         TEXT,
    connected_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);
