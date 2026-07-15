CREATE TABLE IF NOT EXISTS _operation_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    op_type             TEXT NOT NULL CHECK (length(op_type) > 0),
    surface             TEXT NOT NULL CHECK (length(surface) > 0),
    tool_name           TEXT,
    status              TEXT NOT NULL CHECK (status IN ('success', 'error')),
    started_at          TEXT NOT NULL,
    finished_at         TEXT NOT NULL,
    duration_ms         INTEGER NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    start_changelog_id  INTEGER NOT NULL DEFAULT 0 CHECK (start_changelog_id >= 0),
    end_changelog_id    INTEGER NOT NULL DEFAULT 0 CHECK (end_changelog_id >= 0),
    request_json        TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(request_json)),
    result_json         TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(result_json)),
    error_json          TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
    idempotency_key     TEXT,
    created_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_operation_log_surface_started
    ON _operation_log(surface, started_at);

CREATE INDEX IF NOT EXISTS idx_operation_log_tool_started
    ON _operation_log(tool_name, started_at);

CREATE INDEX IF NOT EXISTS idx_operation_log_status_started
    ON _operation_log(status, started_at);
