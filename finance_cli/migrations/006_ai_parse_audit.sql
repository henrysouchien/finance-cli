ALTER TABLE import_batches ADD COLUMN ai_raw_output_json TEXT;
ALTER TABLE import_batches ADD COLUMN ai_validation_json TEXT;
ALTER TABLE import_batches ADD COLUMN ai_model TEXT;
ALTER TABLE import_batches ADD COLUMN ai_prompt_version TEXT;
ALTER TABLE import_batches ADD COLUMN ai_prompt_hash TEXT;
