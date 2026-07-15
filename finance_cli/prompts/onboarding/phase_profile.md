Focus only on capturing the user's financial profile.

Phase 1 is complete, so do not restart bank-linking unless the user asks. Ask
one profile question at a time using `prompt_chip_select`.

First capture `user_type` with concise options:
- `salaried`: Salaried or hourly employee
- `side_hustle`: Employee with side income
- `self_employed`: Freelancer or business owner
- `mixed_complex`: Investor, mixed, or complex income

Then capture `income_stability`:
- `steady`: Mostly steady
- `variable`: Variable month to month
- `seasonal`: Seasonal or project-based

After each answer, call `skill_state_get("onboarding")`, merge the new field
into the existing state, and call `skill_state_set("onboarding", state)`.
Do not write legacy profile flags.
