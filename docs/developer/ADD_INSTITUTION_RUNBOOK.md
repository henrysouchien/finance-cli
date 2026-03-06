# Runbook: Adding a New Institution CSV Adapter

## When to Use

You have a CSV (or Excel) export from a new financial institution and want to import it into the finance CLI. This runbook covers the full process from sample analysis through tested normalizer.

## Prerequisites

- A sample export file from the institution (CSV preferred, Excel acceptable)
- Access to the `finance_cli` repo

## Step 1: Get and Inspect the Sample Export

Place the sample file in the repo root (it won't be committed — personal financial data).

Inspect the first 10-20 lines. Note:
- **File encoding**: UTF-8? BOM? (most exports are UTF-8 with BOM)
- **Header junk**: Are there non-CSV lines before the actual header row? (e.g., Barclays has bank name, account number, balance, blank line before the CSV header)
- **Column names**: What are the header fields called?
- **Date format**: MM/DD/YYYY? YYYY-MM-DD? Other?
- **Amount format**: Dollar signs? Commas? Parentheses for negatives?

## Step 2: Determine Account Types

Many institutions have **multiple account types** with different CSV formats (e.g., checking vs credit card). Check whether the institution requires separate normalizers.

| Pattern | When to use | Example |
|---------|------------|---------|
| **Single normalizer** | Institution has one CSV format | Amex (credit card only), Apple Card |
| **Split normalizers** | Checking and credit card CSVs have different columns/conventions | Chase (`chase_checking`, `chase_credit`), BofA (`bofa_checking`, `bofa_credit`) |

If split: get a sample export for **each** account type and run Steps 3-7 for each. Use `<institution>_checking` / `<institution>_credit` as normalizer keys — this matches the convention used by the PDF parsers.

## Step 3: Analyze the Format (5 Questions per Account Type)

Answer these 5 questions by looking at the sample data (repeat for each account type if split):

| Question | What to look for | Example |
|----------|-----------------|---------|
| **1. Header junk?** | Non-CSV lines before the header row | Barclays: 4 lines (bank, account, balance, blank) |
| **2. Column mapping?** | Which columns map to Date, Description, Amount | Apple Card: `Transaction Date`, `Description`, `Amount (USD)` |
| **3. Sign convention?** | Are expenses positive or negative? | Apple Card: expenses positive (inverted), Barclays: expenses negative (correct) |
| **4. Payment detection?** | How to identify payment/credit rows | Apple Card: `Type == "Payment"`, Barclays: description contains "Payment Received" |
| **5. Card ending / account ID?** | Is there a card number or account identifier? | Barclays: account number in header junk, Apple Card: hardcoded "Apple" |

## Step 4: Write the Normalizer

Create a `_normalize_<institution>()` function in `finance_cli/importers/csv_normalizers.py`. Follow the existing pattern (one function per account type if split):

```python
def _normalize_<institution>(file_path: Path) -> NormalizeResult:
    # 1. Open file (utf-8-sig encoding)
    # 2. Skip/scan past header junk if needed
    # 3. Read with csv.DictReader
    # 4. For each row:
    #    - Map columns to canonical names (Date, Description, Amount)
    #    - Fix sign convention (negate if needed)
    #    - Detect payments
    #    - Set Card Ending, Source
    # 5. Return NormalizeResult
```

Key rules:
- **Canonical sign convention**: negative = expense, positive = income/payment
- **Dates as raw strings**: pass through as-is (the importer's `normalize_date()` handles conversion)
- **Amount via Decimal**: use `_parse_amount()` and `_format_amount()` helpers
- **Account Type**: emit `"Account Type"` in every row (`"credit_card"`, `"checking"`, `"savings"`). This is used by the alias system to match hash accounts to Plaid accounts.
- **Skip rows missing date/description/amount**: increment `skipped_row_count`, add warning

## Step 5: Register in the Map

Add to `_NORMALIZER_MAP` in `csv_normalizers.py`:

```python
_NORMALIZER_MAP: dict[str, Callable[[Path], NormalizeResult]] = {
    # ... existing ...
    "<institution_key>": _normalize_<institution>,
}
```

Use lowercase, underscored keys. Add aliases if useful (e.g., `"apple"` and `"apple_card"` both map to the same function).

For split institutions, register each account type separately:

```python
    "chase_checking": _normalize_chase_checking,
    "chase_credit": _normalize_chase_credit,
```

## Step 6: Write Tests (~5-7 tests per normalizer)

Add to `finance_cli/tests/test_csv_normalizers.py`:

- Sign handling for each transaction type (purchases, payments, interest, refunds)
- Payment detection (true positives and true negatives)
- Header junk handling (if applicable)
- Card ending extraction (if applicable)
- Category pass-through or omission

## Step 7: Smoke Test Against Real Data

```python
from finance_cli.importers.csv_normalizers import normalize_csv

result = normalize_csv("<path_to_real_export>", "<institution_key>")
print(f"Rows: {len(result.rows)}, Skipped: {result.skipped_row_count}, Warnings: {len(result.warnings)}")

# Check sign convention
for r in result.rows[:5]:
    print(f"  {r['Date']} | {r['Description'][:40]:40s} | {r['Amount']:>10s} | Payment={r['Is Payment']}")

# Check payments
payments = [r for r in result.rows if r["Is Payment"] == "true"]
print(f"\nPayments: {len(payments)}")

# Check for non-payment credits (should NOT be flagged as payments)
credits = [r for r in result.rows if r["Is Payment"] == "false" and float(r["Amount"]) > 0]
print(f"Non-payment credits: {len(credits)}")
```

## Step 8: Run Full Test Suite

```bash
python3 -m pytest finance_cli/tests/ -v
```

Verify no regressions.

## Step 9: Verify Account Alias (if Plaid-linked)

If the institution is already linked via Plaid, the import should auto-create an alias in `account_aliases` linking the hash-based CSV account to the Plaid account. Verify:

```bash
# Check alias was created
python3 -m finance_cli dedup backfill-aliases

# If the institution has overlapping Plaid transactions, run dedup
python3 -m finance_cli dedup cross-format
```

The alias system uses `institution_names.py` for canonicalization. If the new institution's CSV `Source` name differs from the Plaid `institution_name`, add both variants to `CANONICAL_NAMES` in `finance_cli/institution_names.py` so they resolve to the same canonical name.

Example: Plaid stores "Barclays - Cards", CSV normalizer emits "Barclays" — both map to canonical "Barclays".

---

## Codex Review Template

After writing the normalizer, send to Codex for review with this prompt:

```
Review the new <INSTITUTION> CSV normalizer in finance_cli/importers/csv_normalizers.py
and its tests in finance_cli/tests/test_csv_normalizers.py.

Review against:
- The real sample export file: "<FILENAME>"
- Existing normalizer patterns (_normalize_apple_card, _normalize_barclays)
- finance_cli/importers/__init__.py (canonical column names expected by import_csv)

Focus on:
1. Sign convention correctness for all transaction types
2. Payment detection — no false positives (refunds, statement credits)
3. Header junk handling robustness
4. Card ending / account ID extraction
5. Edge cases in the real data

Flag issues as blocking or non-blocking. If everything looks good, say "PASS".
```

---

## Codex Implementation Template

To have Codex implement a new normalizer from scratch, use this prompt:

```
Implement a CSV normalizer for <INSTITUTION> exports in the finance_cli project.

Sample export file: <FILENAME> (in repo root)

Format analysis:
- Header junk: <describe any non-CSV lines before the header>
- Column names: <list the CSV column headers>
- Sign convention: <positive = expense or income?>
- Payment detection: <how to identify payments>
- Card ending: <where to find it, or hardcoded value>
- Date format: <MM/DD/YYYY, etc.>

Follow the existing patterns in:
- finance_cli/importers/csv_normalizers.py (normalizer functions + registry)
- finance_cli/tests/test_csv_normalizers.py (test structure)

Canonical format (negative = expense, positive = income):
- Required: Date, Description, Amount, Account Type
- Optional: Card Ending, Source, Use Type, Category, Transaction ID, Is Payment

Implementation:
1. Add _normalize_<institution>() function in csv_normalizers.py
2. Add to _NORMALIZER_MAP registry
3. Add ~5-7 tests covering sign handling, payment detection, and edge cases

Do NOT modify any existing normalizer functions or tests.
```

Fill in the format analysis from Step 3 of the runbook before sending to Codex.

For institutions with multiple account types (e.g., Chase checking + credit), send one Codex task per account type with the appropriate sample file and normalizer key (e.g., `chase_checking`, `chase_credit`).
