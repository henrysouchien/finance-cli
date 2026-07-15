ALTER TABLE bot_chat_messages ADD COLUMN compacted_at TEXT;
CREATE INDEX IF NOT EXISTS idx_bot_chat_messages_compacted ON bot_chat_messages(compacted_at);
