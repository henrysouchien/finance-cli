CREATE TABLE IF NOT EXISTS notification_channels (
    channel    TEXT PRIMARY KEY CHECK (channel IN ('telegram', 'imessage')),
    config     TEXT NOT NULL CHECK (json_valid(config)),
    label      TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TRIGGER IF NOT EXISTS trg_notification_channels_updated
    AFTER UPDATE ON notification_channels
    FOR EACH ROW
BEGIN
    UPDATE notification_channels SET updated_at = datetime('now') WHERE channel = NEW.channel;
END;
