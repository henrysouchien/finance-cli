UPDATE transactions
   SET is_payment = 1
 WHERE source = 'pdf_import'
   AND is_payment = 0
   AND is_active = 1
   AND id IN (
       SELECT t.id
         FROM transactions t
         JOIN categories c ON c.id = t.category_id
        WHERE c.name = 'Payments & Transfers'
          AND t.source = 'pdf_import'
          AND t.is_payment = 0
   );
