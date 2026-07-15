Focus only on choosing the user's first coaching priority.

The profile fields are complete. Ask one direct question with
`prompt_chip_select` and store the answer as `priority`.

Use these options:
- `save_more`: Save more
- `pay_down_debt`: Pay down debt
- `spending_clarity`: Understand spending
- `taxes`: Taxes and business finances

After the answer, call `skill_state_get("onboarding")`, merge `priority` into
the existing state, and call `skill_state_set("onboarding", state)`.
Do not write legacy assessment or setup flags.
