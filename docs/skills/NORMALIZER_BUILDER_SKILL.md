---
name: normalizer_builder
tool_packs:
  - normalizer
---

# Normalizer Builder Skill

Use this playbook when the agent enters dev mode to build or update a CSV normalizer for an unsupported institution.

## Goal

Produce a working normalizer module, validate it against the user's sample CSV, activate it only after the staged version passes tests, and then offer to run the import.

## Workflow

1. Sample
   - If a `<context>` block is provided below with `upload_path`, use that path directly with `statement_normalizer_sample_csv`. Never ask the user for a file path when `upload_path` is available.
   - If no context is provided, ask the user for the file path.
   - Read the first 20 lines of the CSV.
2. Analyze
   - Identify header row and any preamble/header junk before it.
   - Identify the columns that map to `Date`, `Description`, and `Amount`.
   - Identify the date format.
   - Identify the amount sign convention.
   - Identify payment indicators.
   - Identify the account type.
   - Identify where card ending or account ID comes from, if any.
3. Confirm
   - Present the analysis to the user and ask them to confirm or correct it before generating code.
4. Register institution
   - If the institution is not already known, call `normalizer_register_institution` before staging code.
5. Generate
   - Write the normalizer module with `PRIMARY_KEY`, `ALIASES`, `SOURCE_NAME`, `detect()`, and `normalize()`.
6. Stage
   - Call `statement_normalizer_stage` to write the module to staging.
7. Test
   - Call `statement_normalizer_test` against the sample CSV.
   - Review validation results, row counts, warnings, amount signs, payment flags, and sample normalized rows.
8. Fix
   - If the test fails or the output is wrong, call `normalizer_update` and re-run `statement_normalizer_test`.
9. Activate
   - Once tests pass, call `statement_normalizer_activate`.
10. Import
   - Offer to run the import with the new normalizer.

## Analysis Checklist

Answer these before writing code:

1. What unique header pattern should `detect(lines)` look for?
2. Which raw columns map to canonical fields?
3. Are expenses exported as positive or negative amounts?
4. How are payments identified?
5. What account type should every normalized row emit?
6. Is there header junk to skip before `csv.DictReader`?
7. Is there card ending, account number, or filename metadata worth extracting?

## Normalized Row Contract

Required fields on every normalized row:

- `Date`
- `Description`
- `Amount`
- `Account Type`

Optional fields:

- `Card Ending`
- `Category`
- `Is Payment`
- `Source`
- `Transaction ID`
- `Use Type`

Allowed `Account Type` values:

- `checking`
- `savings`
- `credit_card`
- `investment`
- `loan`

Use a stable `SOURCE_NAME` and set row `Source` to that same value when emitting it.

## User-Generated Module Interface

```python
PRIMARY_KEY = "wells_fargo"
ALIASES = ["wf"]
SOURCE_NAME = "Wells Fargo"

def detect(lines: list[str]) -> bool:
    """Return True when the raw file lines match this institution."""

def normalize(lines: list[str], file_name: str) -> dict:
    """Return:
    {
        "source_name": SOURCE_NAME,
        "rows": [...],
        "warnings": [...],
        "raw_row_count": int,
        "skipped_row_count": int,
    }
    """
```

Rules:

- `detect(lines: list[str]) -> bool` receives raw file lines and should check for unique header patterns.
- `normalize(lines: list[str], file_name: str) -> dict` receives raw file lines plus the basename of the original file.
- Do not call `open()` or use `Path` for file I/O. The harness already read the file.
- Keep imports limited to the allowed stdlib modules provided by the sandbox.

## Common Patterns From Existing Normalizers

- Use `io.StringIO("".join(lines[header_idx:]))` plus `csv.DictReader` after finding the real header row.
- Skip preamble lines before the header row instead of assuming the CSV starts at line 1.
- Flip amount signs when the export uses positive numbers for expenses.
- Detect payments from a `Type` column when available, otherwise from description phrases such as `payment`, `payment received`, or similar institution-specific markers.
- Extract card ending from header junk or filename when available; leave it blank when it does not exist.
- Use `_row_value(...)` to read columns safely and `_parse_amount(...)` / `_format_amount(...)` to normalize amounts.

## Error Handling

- Skip bad rows and add warnings such as `row 7: missing required fields` or `row 12: invalid amount 'abc'`.
- Do not crash because of one malformed row.
- Fail the test only when the file shape is incompatible or the output contract is invalid.
- If `statement_normalizer_test` shows validation errors, revise the code and re-test before activation.

## Activation Rules

- Never activate an untested staged normalizer.
- Never skip the stage -> test -> fix -> activate order.
- Built-in normalizers under `finance_cli/importers/normalizers/` are developer-owned; MCP tools are for user-generated modules in the user normalizer directory.
