ALTER TABLE ai_categorization_log ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ai_categorization_log ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ai_categorization_log ADD COLUMN elapsed_ms INTEGER NOT NULL DEFAULT 0;
