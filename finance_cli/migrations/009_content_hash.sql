ALTER TABLE import_batches ADD COLUMN content_hash_sha256 TEXT;
CREATE INDEX IF NOT EXISTS idx_import_batches_content_hash ON import_batches(content_hash_sha256);
