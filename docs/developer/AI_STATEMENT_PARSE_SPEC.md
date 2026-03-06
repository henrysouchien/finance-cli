# AI Statement Parse Spec

Last updated: 2026-02-20

## Purpose

Define exactly how we instruct AI models to parse statement exports and how outputs must be validated before import.

This spec is for low-frequency, operator-supervised ingestion.

## Model Role

The model is an extractor, not an authority.

Rules:
- Extract only what is present in the source.
- Never fabricate missing values.
- Use `null` for uncertain fields.
- Preserve transaction descriptions close to statement text.

## Required Output JSON

```json
{
  "statement": {
    "institution": "string|null",
    "account_label": "string|null",
    "card_ending": "string|null",
    "statement_period_start": "YYYY-MM-DD|null",
    "statement_period_end": "YYYY-MM-DD|null",
    "new_balance": "number|null",
    "currency": "USD|null"
  },
  "transactions": [
    {
      "date": "YYYY-MM-DD",
      "description": "string",
      "amount": -12.34,
      "card_ending": "string|null",
      "transaction_id": "string|null",
      "confidence": 0.0,
      "evidence": "string|null"
    }
  ],
  "extraction_meta": {
    "model": "string",
    "prompt_version": "string",
    "notes": "string|null",
    "expected_transaction_count": "number|null"
  }
}
```

## Prompting Contract

System instructions should explicitly require:
1. Strict JSON only, no markdown, no code fences, no commentary.
2. Output must conform to schema keys exactly (do not add keys).
3. `amount` sign convention:
   - negative = expense/outflow
   - positive = payment/refund/inflow
4. `new_balance` is the ending balance shown on statement, extracted exactly as printed and always positive. Set to null if not explicitly shown.
5. If debit/credit columns exist:
   - derive one signed `amount`.
6. Include ALL line items: purchases, payments, refunds, interest charges, fees, and adjustments — even if they appear in separate sections.
7. All numeric fields must be JSON numbers, never strings.
8. Confidence calibration bands:
   - `0.90–1.00`: date, description, and amount are explicit and unambiguous
   - `0.70–0.89`: minor ambiguity but likely correct
   - `0.40–0.69`: notable ambiguity (layout noise, uncertain token mapping)
   - `0.00–0.39`: weak evidence; include only if the row is still extractable
   - Do not assign 0.0 to all rows by default.

## Validation Gates

## Gate 1: Schema

- JSON parse succeeds.
- Required top-level keys exist.
- `transactions` is an array.
- Required transaction fields exist.

## Gate 2: Field correctness

- `date` parses as ISO date.
- `amount` parses as decimal and converts to integer cents.
- `description` is non-empty after trim.

## Gate 3: Semantic checks

- Transaction dates should be within statement period when period exists (or within allowed drift window).
- Extremely large magnitudes are flagged for review.
- Exact duplicate rows are flagged before DB import.

## Gate 4: Reconciliation

Reconciliation is backend-specific:
- **AI backend:** The AI prompt does not request `total_charges` or `total_payments` (removed in v6 due to false mismatches with interest charges and multi-account PDFs). AI path always returns `no_totals`.
- **Azure/BSC backends:** May compute their own `statement_total_cents` for reconciliation. When present, compare against extracted transaction sum.
- Status values: `matched`, `mismatch`, `no_totals`

## Gate 5: Confidence policy

Suggested defaults:
- `min_confidence_warn = 0.80`
- `min_confidence_block = 0.60`

Behavior:
- Rows below warn threshold are reported.
- Rows below block threshold require manual review or explicit override.
- Commit policy default: block partial imports when any rows are below block threshold.
- Partial import is allowed only with explicit override (for operator-supervised edge cases).

## Canonical Conversion

After passing validation, convert to canonical CSV columns:
- `Date`
- `Description`
- `Amount`
- `Card Ending`
- `Account Type` — `credit_card`, `checking`, or `savings` (used by alias system to match hash accounts to Plaid accounts)
- `Source`
- `Use Type`
- `Category`
- `Transaction ID`
- `Is Payment`

Mapping notes:
- `Source` should be set by route/institution profile, not by model.
- `Account Type` is set by the institution canonicalization layer (`institution_names.py`), not by the model. For the PDF path, the existing heuristic (`credit_card` if card_ending else `checking`) is used as fallback — see `IMPORT-001` in `BUG_BACKLOG.md`.
- `Is Payment` derived from sign + keyword heuristics; allow override.
- Institution names are canonicalized via `institution_names.py` before account creation. This ensures CSV/PDF "Barclays" matches Plaid "Barclays - Cards" for alias linking.

## Audit Requirements

Store per ingest run:
- Raw input file hash
- Raw AI output JSON
- Validation report (errors/warnings, reconcile status)
- Model metadata (`model`, `prompt_version`)
- Final normalized artifact hash (if written)

## Failure Handling

If validation fails:
1. Do not import.
2. Return actionable error report with row indices.
3. Allow re-parse with adjusted prompt/model.

If reconciliation mismatches and strict mode is on:
1. Do not import.
2. Require manual review/override.

If the same source file was already imported and a corrected parse is needed:
1. Use explicit replacement mode for the file hash.
2. Replacement must be intentional because it supersedes prior imported rows for that file.

## Operator Workflow

1. Run AI parse on raw statement.
2. Review validation report.
3. If clean, commit import.
4. If issues, fix prompt or manually correct normalized rows, then re-validate and import.
