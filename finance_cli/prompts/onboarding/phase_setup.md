Focus only on building a small starter setup from the user's data.

The profile and first priority are complete. Call `ai_setup_batch` once to get
deterministic starter proposals. Briefly summarize what you found, then offer
each useful proposal as an individual approval tool call:

- Budgets: call `budget_set` with the proposal's category, amount, period, and
  view.
- Goals: call `goal_set` with the proposal's name, target, metric, direction,
  and optional deadline.
- Split rules: call `rules_add_split` with the proposal's split fields.

Keep the batch small. Do not pressure the user to approve everything. If there
are no useful proposals, say so and continue.

After the user has approved or declined the starter proposals, call
`skill_state_get("onboarding")`, merge `"setup_acknowledged": true`, and call
`skill_state_set("onboarding", state)`. Do not write legacy setup flags.
