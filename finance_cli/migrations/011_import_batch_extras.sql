ALTER TABLE import_batches ADD COLUMN total_charges_cents INTEGER;
ALTER TABLE import_batches ADD COLUMN total_payments_cents INTEGER;
ALTER TABLE import_batches ADD COLUMN new_balance_cents INTEGER;
ALTER TABLE import_batches ADD COLUMN expected_transaction_count INTEGER;
