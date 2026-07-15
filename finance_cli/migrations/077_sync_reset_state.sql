CREATE TABLE IF NOT EXISTS sync_reset_state (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    reset_epoch TEXT NOT NULL,
    reset_reason TEXT,
    reset_at TEXT NOT NULL,
    origin_session_id TEXT
);

INSERT OR IGNORE INTO sync_reset_state (
    id,
    reset_epoch,
    reset_reason,
    reset_at,
    origin_session_id
)
VALUES (
    0,
    lower(hex(randomblob(16))),
    'initial',
    datetime('now'),
    ''
);
