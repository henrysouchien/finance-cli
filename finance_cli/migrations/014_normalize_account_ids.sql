UPDATE transactions
   SET account_id = (
           SELECT aa.canonical_id
             FROM account_aliases aa
            WHERE aa.hash_account_id = transactions.account_id
       ),
       updated_at = datetime('now')
 WHERE account_id IN (SELECT hash_account_id FROM account_aliases);

UPDATE subscriptions
   SET account_id = (
           SELECT aa.canonical_id
             FROM account_aliases aa
            WHERE aa.hash_account_id = subscriptions.account_id
       )
 WHERE account_id IN (SELECT hash_account_id FROM account_aliases);

DELETE FROM subscriptions
 WHERE rowid IN (
     SELECT rowid
       FROM (
           SELECT rowid,
                  ROW_NUMBER() OVER (
                      PARTITION BY vendor_name, frequency, account_id
                      ORDER BY is_auto_detected ASC, rowid ASC
                  ) AS rn
             FROM subscriptions
       ) ranked
      WHERE rn > 1
 );
