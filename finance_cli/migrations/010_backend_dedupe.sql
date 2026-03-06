-- Backfill NULL parser values for safety before creating backend-scoped uniqueness.
UPDATE import_batches
   SET bank_parser = 'ai:unknown'
 WHERE bank_parser IS NULL;

-- Rewrite legacy AI dedupe keys: pdf:{hash}:{idx} -> pdf:ai:{hash}:{idx}.
-- Only rewrite when {hash} is known to have been imported by an AI parser.
UPDATE transactions AS t
   SET dedupe_key = 'pdf:ai:' || substr(t.dedupe_key, 5)
 WHERE t.dedupe_key LIKE 'pdf:%'
   AND t.dedupe_key NOT LIKE 'pdf:ai:%'
   AND t.dedupe_key NOT LIKE 'pdf:azure:%'
   AND t.dedupe_key NOT LIKE 'pdf:bsc:%'
   AND instr(substr(t.dedupe_key, 5), ':') > 0
   AND substr(t.dedupe_key, 5, instr(substr(t.dedupe_key, 5), ':') - 1) IN (
       SELECT file_hash_sha256
         FROM import_batches
        WHERE bank_parser LIKE 'ai:%'
          AND file_hash_sha256 IS NOT NULL
   )
   AND NOT EXISTS (
       SELECT 1
         FROM transactions AS t2
        WHERE t2.dedupe_key = 'pdf:ai:' || substr(t.dedupe_key, 5)
   );

DROP INDEX IF EXISTS idx_import_batches_hash;

CREATE UNIQUE INDEX IF NOT EXISTS idx_import_batches_hash_backend
    ON import_batches(file_hash_sha256, bank_parser)
    WHERE file_hash_sha256 IS NOT NULL AND bank_parser IS NOT NULL;
