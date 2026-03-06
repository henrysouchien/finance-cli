ALTER TABLE accounts ADD COLUMN is_business INTEGER NOT NULL DEFAULT 0;

-- Add canonical business-accounting categories when their parents exist.
INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Advertising', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'professional'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'advertising'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Contract Labor', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'professional'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'contract labor'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Office Expense', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'housing'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'office expense'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Supplies', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'housing'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'supplies'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Depreciation', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'financial'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'depreciation'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Taxes & Licenses', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'financial'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'taxes & licenses'
       );

INSERT INTO categories (id, name, parent_id, level, is_income, is_system, sort_order)
SELECT lower(hex(randomblob(16))), 'Cost of Goods Sold', p.id, 1, 0, 1, 0
  FROM (
        SELECT id
          FROM categories
         WHERE lower(trim(name)) = 'income'
         ORDER BY rowid ASC
         LIMIT 1
       ) AS p
 WHERE NOT EXISTS (
           SELECT 1
             FROM categories c
            WHERE lower(trim(c.name)) = 'cost of goods sold'
       );

-- Reconcile existing rows for these categories to canonical hierarchy/system flags.
UPDATE categories
   SET parent_id = (
           SELECT id
             FROM categories p
            WHERE lower(trim(p.name)) = 'professional'
            ORDER BY rowid ASC
            LIMIT 1
       ),
       level = 1,
       is_income = 0,
       is_system = 1,
       sort_order = 0
 WHERE lower(trim(name)) IN ('advertising', 'contract labor')
   AND EXISTS (
           SELECT 1
             FROM categories p
            WHERE lower(trim(p.name)) = 'professional'
       );

UPDATE categories
   SET parent_id = (
           SELECT id
             FROM categories p
            WHERE lower(trim(p.name)) = 'housing'
            ORDER BY rowid ASC
            LIMIT 1
       ),
       level = 1,
       is_income = 0,
       is_system = 1,
       sort_order = 0
 WHERE lower(trim(name)) IN ('office expense', 'supplies')
   AND EXISTS (
           SELECT 1
             FROM categories p
            WHERE lower(trim(p.name)) = 'housing'
       );

UPDATE categories
   SET parent_id = (
           SELECT id
             FROM categories p
            WHERE lower(trim(p.name)) = 'financial'
            ORDER BY rowid ASC
            LIMIT 1
       ),
       level = 1,
       is_income = 0,
       is_system = 1,
       sort_order = 0
 WHERE lower(trim(name)) IN ('depreciation', 'taxes & licenses')
   AND EXISTS (
           SELECT 1
             FROM categories p
            WHERE lower(trim(p.name)) = 'financial'
       );

UPDATE categories
   SET parent_id = (
           SELECT id
             FROM categories p
            WHERE lower(trim(p.name)) = 'income'
            ORDER BY rowid ASC
            LIMIT 1
       ),
       level = 1,
       is_income = 0,
       is_system = 1,
       sort_order = 0
 WHERE lower(trim(name)) = 'cost of goods sold'
   AND EXISTS (
           SELECT 1
             FROM categories p
            WHERE lower(trim(p.name)) = 'income'
       );

CREATE TABLE IF NOT EXISTS pl_section_map (
    id            TEXT PRIMARY KEY,
    category_id   TEXT NOT NULL REFERENCES categories(id),
    pl_section    TEXT NOT NULL CHECK (pl_section IN (
        'revenue', 'cogs', 'opex_marketing', 'opex_technology',
        'opex_professional', 'opex_facilities', 'opex_people', 'opex_other'
    )),
    display_order INTEGER NOT NULL DEFAULT 0,
    UNIQUE(category_id)
);

CREATE TABLE IF NOT EXISTS schedule_c_map (
    id              TEXT PRIMARY KEY,
    category_id     TEXT NOT NULL REFERENCES categories(id),
    schedule_c_line TEXT NOT NULL,
    line_number     TEXT NOT NULL,
    deduction_pct   REAL NOT NULL DEFAULT 1.0,
    tax_year        INTEGER NOT NULL DEFAULT 2025,
    notes           TEXT,
    UNIQUE(category_id, tax_year)
);

WITH pl_seed(name, pl_section, display_order) AS (
    VALUES
        ('Income: Business', 'revenue', 10),
        ('Cost of Goods Sold', 'cogs', 20),
        ('Advertising', 'opex_marketing', 30),
        ('Software & Subscriptions', 'opex_technology', 40),
        ('Professional Fees', 'opex_professional', 50),
        ('Contract Labor', 'opex_people', 60),
        ('Rent', 'opex_facilities', 70),
        ('Utilities', 'opex_facilities', 71),
        ('Insurance', 'opex_facilities', 72),
        ('Office Expense', 'opex_facilities', 73),
        ('Bank Charges & Fees', 'opex_other', 80),
        ('Supplies', 'opex_other', 81),
        ('Taxes & Licenses', 'opex_other', 82),
        ('Depreciation', 'opex_other', 83),
        ('Transportation', 'opex_other', 84),
        ('Travel', 'opex_other', 85),
        ('Dining', 'opex_other', 86)
)
INSERT INTO pl_section_map (id, category_id, pl_section, display_order)
SELECT lower(hex(randomblob(16))), c.id, s.pl_section, s.display_order
  FROM pl_seed s
  JOIN categories c
    ON lower(trim(c.name)) = lower(trim(s.name))
ON CONFLICT(category_id) DO UPDATE
    SET pl_section = excluded.pl_section,
        display_order = excluded.display_order;

WITH schedule_seed(name, schedule_c_line, line_number, deduction_pct, tax_year, notes) AS (
    VALUES
        ('Advertising', 'Advertising', '8', 1.0, 2025, NULL),
        ('Transportation', 'Car and truck expenses', '9', 1.0, 2025, 'Requires mileage substantiation; commuting is nondeductible.'),
        ('Bank Charges & Fees', 'Commissions and fees', '10', 1.0, 2025, 'Includes platform and payment-processing commissions.'),
        ('Contract Labor', 'Contract labor', '11', 1.0, 2025, '1099 contractors.'),
        ('Depreciation', 'Depreciation and section 179', '13', 1.0, 2025, 'Equipment and computers.'),
        ('Insurance', 'Insurance (other than health)', '15', 1.0, 2025, NULL),
        ('Professional Fees', 'Legal and professional services', '17', 1.0, 2025, NULL),
        ('Office Expense', 'Office expense', '18', 1.0, 2025, NULL),
        ('Rent', 'Rent or lease (other business property)', '20b', 1.0, 2025, NULL),
        ('Supplies', 'Supplies', '22', 1.0, 2025, NULL),
        ('Taxes & Licenses', 'Taxes and licenses', '23', 1.0, 2025, NULL),
        ('Travel', 'Travel', '24a', 1.0, 2025, NULL),
        ('Dining', 'Deductible meals', '24b', 0.5, 2025, 'Business meals only.'),
        ('Utilities', 'Utilities', '25', 1.0, 2025, NULL),
        ('Software & Subscriptions', 'Other expenses', '27a', 1.0, 2025, 'SaaS tools and subscriptions.'),
        ('Cost of Goods Sold', 'COGS (Part III)', '42', 1.0, 2025, 'Platform fees and payment processing.')
)
INSERT INTO schedule_c_map (id, category_id, schedule_c_line, line_number, deduction_pct, tax_year, notes)
SELECT lower(hex(randomblob(16))), c.id, s.schedule_c_line, s.line_number, s.deduction_pct, s.tax_year, s.notes
  FROM schedule_seed s
  JOIN categories c
    ON lower(trim(c.name)) = lower(trim(s.name))
ON CONFLICT(category_id, tax_year) DO UPDATE
    SET schedule_c_line = excluded.schedule_c_line,
        line_number = excluded.line_number,
        deduction_pct = excluded.deduction_pct,
        notes = excluded.notes;
