ALTER TABLE telegram_config ADD COLUMN webhook_secret TEXT;
ALTER TABLE telegram_config ADD COLUMN webhook_url TEXT;

ALTER TABLE telegram_config ADD COLUMN telegram_user_id TEXT;
ALTER TABLE telegram_config ADD COLUMN link_code TEXT;
ALTER TABLE telegram_config ADD COLUMN link_code_expires_at TEXT;

ALTER TABLE telegram_config ADD COLUMN model_override TEXT;
ALTER TABLE telegram_config ADD COLUMN active_skill TEXT;
ALTER TABLE telegram_config ADD COLUMN current_session_id TEXT;
ALTER TABLE telegram_config ADD COLUMN last_message_time REAL;
ALTER TABLE telegram_config ADD COLUMN onboarding_flags TEXT;
ALTER TABLE telegram_config ADD COLUMN processing_since TEXT;
ALTER TABLE telegram_config ADD COLUMN processing_id TEXT;
ALTER TABLE telegram_config ADD COLUMN cancel_requested INTEGER DEFAULT 0;

CREATE TABLE IF NOT EXISTS telegram_pending_approvals (
    nonce                      TEXT PRIMARY KEY,
    tool_call_id               TEXT NOT NULL,
    tool_name                  TEXT NOT NULL,
    message_id                 INTEGER,
    chat_id                    TEXT NOT NULL,
    gateway_session_token      TEXT NOT NULL,
    gateway_session_id         TEXT NOT NULL,
    gateway_session_expires_at INTEGER NOT NULL,
    created_at                 TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at                 TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tg_approvals_expires
    ON telegram_pending_approvals(expires_at);

CREATE TABLE IF NOT EXISTS telegram_processed_updates (
    update_id  INTEGER PRIMARY KEY,
    claimed_at TEXT NOT NULL DEFAULT (datetime('now'))
);
