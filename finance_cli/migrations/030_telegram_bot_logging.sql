CREATE TABLE IF NOT EXISTS bot_chat_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    request_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bot_chat_messages_created ON bot_chat_messages(created_at);

CREATE TABLE IF NOT EXISTS bot_requests (
    request_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cache_creation_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    estimated_cost REAL DEFAULT 0.0,
    tool_call_count INTEGER DEFAULT 0,
    latency_ms INTEGER DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bot_requests_created ON bot_requests(created_at);

CREATE TABLE IF NOT EXISTS bot_tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES bot_requests(request_id),
    tool_name TEXT NOT NULL,
    server TEXT,
    duration_ms INTEGER NOT NULL,
    is_error INTEGER NOT NULL DEFAULT 0,
    result_bytes INTEGER DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bot_tool_calls_request ON bot_tool_calls(request_id);
