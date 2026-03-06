-- Normalize MM/DD/YYYY dates to YYYY-MM-DD (ISO 8601) in transactions table.
UPDATE transactions
   SET date = SUBSTR(date, 7, 4) || '-' || SUBSTR(date, 1, 2) || '-' || SUBSTR(date, 4, 2)
 WHERE date LIKE '__/__/____';
