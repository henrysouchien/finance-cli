ALTER TABLE budgets ADD COLUMN use_type TEXT NOT NULL DEFAULT 'Personal'
    CHECK (use_type IN ('Personal', 'Business'));

UPDATE budgets SET use_type = 'Personal' WHERE use_type IS NULL;

DROP TRIGGER IF EXISTS budgets_no_overlap_insert;
DROP TRIGGER IF EXISTS budgets_no_overlap_update;

CREATE TRIGGER budgets_no_overlap_insert
BEFORE INSERT ON budgets
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM budgets b
    WHERE b.category_id = NEW.category_id
      AND b.period = NEW.period
      AND b.use_type = NEW.use_type
      AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
      AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
)
BEGIN
    SELECT RAISE(ABORT, 'budget range overlap');
END;

CREATE TRIGGER budgets_no_overlap_update
BEFORE UPDATE ON budgets
FOR EACH ROW
WHEN EXISTS (
    SELECT 1
    FROM budgets b
    WHERE b.id <> OLD.id
      AND b.category_id = NEW.category_id
      AND b.period = NEW.period
      AND b.use_type = NEW.use_type
      AND date(b.effective_from) <= date(COALESCE(NEW.effective_to, '9999-12-31'))
      AND date(COALESCE(b.effective_to, '9999-12-31')) >= date(NEW.effective_from)
)
BEGIN
    SELECT RAISE(ABORT, 'budget range overlap');
END;
