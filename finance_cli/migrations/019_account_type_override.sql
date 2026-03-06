ALTER TABLE accounts ADD COLUMN account_type_override TEXT
    CHECK (
        account_type_override IS NULL
        OR account_type_override IN ('checking', 'savings', 'credit_card', 'investment', 'loan')
    );

UPDATE accounts
   SET account_type_override = 'investment',
       account_type = 'investment'
 WHERE account_name = 'CMA-Edge'
   AND institution_name = 'Merrill'
   AND plaid_account_id IS NOT NULL
   AND source = 'plaid';
