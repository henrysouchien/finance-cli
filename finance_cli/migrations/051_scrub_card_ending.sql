INSERT OR IGNORE INTO accounts (id, institution_name, account_name, account_type, card_ending, source, is_active)
SELECT 'a471c6e88014d681131e001e', a.institution_name, a.institution_name, a.account_type, NULL, a.source, 1
  FROM accounts a
 WHERE a.id = 'ebd916058bbc04f803c40585'
;

INSERT OR IGNORE INTO accounts (id, institution_name, account_name, account_type, card_ending, source, is_active)
SELECT '6c9e429d704de9dc9989a181', a.institution_name, a.institution_name, a.account_type, NULL, a.source, 1
  FROM accounts a
 WHERE a.id = 'd06f71950151e3dc1fac141e'
;

INSERT OR IGNORE INTO account_aliases (hash_account_id, canonical_id)
SELECT 'a471c6e88014d681131e001e',
       COALESCE(aa.canonical_id, a.id)
  FROM accounts a
  LEFT JOIN account_aliases aa ON aa.hash_account_id = a.id
 WHERE a.id = 'ebd916058bbc04f803c40585'
;

INSERT OR IGNORE INTO account_aliases (hash_account_id, canonical_id)
SELECT '6c9e429d704de9dc9989a181',
       COALESCE(aa.canonical_id, a.id)
  FROM accounts a
  LEFT JOIN account_aliases aa ON aa.hash_account_id = a.id
 WHERE a.id = 'd06f71950151e3dc1fac141e'
;

UPDATE accounts SET card_ending = NULL WHERE id = 'ebd916058bbc04f803c40585';
UPDATE accounts SET card_ending = NULL WHERE id = 'd06f71950151e3dc1fac141e';
