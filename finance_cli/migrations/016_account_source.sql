ALTER TABLE accounts ADD COLUMN source TEXT;

-- Backfill: Plaid accounts (definitive signal: plaid_account_id is non-null)
UPDATE accounts SET source = 'plaid' WHERE plaid_account_id IS NOT NULL;

-- Backfill: infer from linked transactions for non-Plaid accounts
UPDATE accounts SET source = 'pdf_import'
 WHERE source IS NULL
   AND id IN (SELECT DISTINCT account_id FROM transactions WHERE source = 'pdf_import');

UPDATE accounts SET source = 'csv_import'
 WHERE source IS NULL
   AND id IN (SELECT DISTINCT account_id FROM transactions WHERE source = 'csv_import');

-- Fallback: anything remaining is manual or unknown
UPDATE accounts SET source = 'manual' WHERE source IS NULL;
