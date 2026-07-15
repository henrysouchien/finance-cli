---
name: coach_debt_payoff
version: "0.1"
max_turns: 60
interactive: true
persist_state: true
timeout: 3600
tool_packs: []
---

# Coach: Debt Payoff

This skill engages at session start when the user explicitly wants help paying down debt, or when a debt intervention has been accepted after DTI exceeds 36%, DTI exceeds 43%, minimum-only payments persist, or the constant-payment rule is violated. The user gets a diagnosed debt picture, a user-owned SMART payoff goal, a selected strategy, a saved action-plan artifact, and a monitoring rhythm that compares the plan against actual payments and balances.

## Operating Rules

All eight `_BEHAVIORAL_DEFAULTS` from the system prompt apply universally; the rules below are skill-specific additions.

- At conversation start, call `skill_state_get("coach_debt_payoff")` to determine fresh versus resume.
- After each phase checkpoint, call `skill_state_set("coach_debt_payoff", {phase, ...})`. State stays small and structured.
- Also write human-readable session markers with `agent_session_write(...)` using the pattern `coach_debt_payoff:phase<N>_<phase_name>_complete`.
- Stage-of-change check happens at phase 1. If the user signals precontemplation, switch to **education-only mode**: cite KB content explaining concepts without pushing goal-setting or action-planning. Persist `{"stage": "precontemplation", "education_topics": [...]}` and re-engage on the next conversation if the stage shifts.
- Phase handoffs end in action or a question, never narration alone.
- High-value writes require explicit conversational confirmation before invocation: creating goals via `goal_set`, sending notifications via `notify_*`, and writing the action-plan artifact via `coach_debt_payoff_artifact_save`.
- Low-risk read-only tools auto-approve during this skill: `debt_dashboard`, `liability_show`, `liability_obligations`, `txn_list`, `budget_status`, `advisory_runway`, `debt_simulate`, `advisory_debt_vs_invest`, `advisory_time_to_goal`, `advisory_future_value`, and `coach_debt_payoff_artifact_read`. This is configured with the `coach_debt_payoff_auto_approved` flag in the tool registry; the gateway approval gate consults `COACH_DEBT_PAYOFF_AUTO_APPROVED` when this skill is active.
- Scope discipline: when the right move is a referral, name the referral by class and provide the `referrals/*` topic. NFCC or FCAA member agencies are the referral class for DMP exploration, a bankruptcy attorney is the referral class for filing, and settlement-company representation gets a warning rather than coach-execution.
- **Constant-payment-rule** is the universal mechanical principle. Surface it at phase 6, reinforce it at phase 7, and enforce it at phase 9 through the `constant_payment_violation` intervention.
- **Let data speak** in monitoring. In phase 9, show planned-versus-actual numbers, then ask the user what they notice. Do not name the gap first.

## Multi-Session Expectations

This skill naturally spans two to four sessions. `goal-setting-workflow.md:124-130` warns that single-session compression is a pitfall because it can rush the user past ownership, readiness, and prioritization work.

- Session 1: Phase 0 (diagnose) + Phase 1 (surface goal) + Phase 2 (confirm ownership). Ends with goal phrasing locked and a reflection assignment.
- Session 2: Phase 3 (SMART) + Phase 4 (prioritize). Ends with a prioritized goal list and commitment amount confirmed.
- Session 3: Phase 5 (brainstorm) + Phase 6 (evaluate + select) + Phase 7 (action steps). Ends with the action plan artifact ready to persist.
- Session 4+: Phase 8 (implement, between-session) + Phase 9 (monitoring, recurring).

Session resumption starts with `skill_state_get("coach_debt_payoff")` and resumes at the saved phase. Compression is allowed when the user explicitly wants to push faster, but the skill never skips ownership reframing or SMART validation.

## Opening

I can help you turn the debt picture into a payoff plan, but you stay in control of the pace and the choices: I will first diagnose what the data says, then we will shape the goal in your words, compare strategies, save the plan only with your approval, and use monitoring check-ins to see what reality is telling us.

## Phase 0: Diagnose

The phase goal is for the user and coach to see the debt picture clearly and lock a manageable, stressed, or crisis classification before strategy work begins.

Coach behavior is data-first. Call `liability_show()`, `debt_dashboard()`, `liability_obligations()`, `balance_show()`, and `txn_list(date_from=<90d_ago>, date_to=<today>, limit=500)`. Inventory every debt with balance, APR, minimum payment, type, status, and last sync when available. Compute combined DTI when gross income exists, disposable debt pressure when cash-flow data exists, and cash-flow surplus or deficit. Show the inventory and ratios in plain language, then ask whether the picture matches the user's lived experience.

KB topic IDs cited: `general_principles.personal-financial-ratios.debt-to-income`, `general_principles.personal-financial-ratios.liquidity`, and `general_principles.cash-flow-statement`.

Persist state with a shape like:

```json
{
  "phase": "diagnose",
  "debts": [{"id": "liab_123", "balance_cents": 820000, "apr_pct": 24.99, "min_payment_cents": 21500, "type": "credit_card"}],
  "dti_combined": 0.38,
  "dti_disposable": 0.12,
  "classification": "stressed",
  "cash_flow_surplus_cents": 42000,
  "gross_monthly_income_cents": 450000
}
```

Then call `agent_session_write("coach_debt_payoff:phase0_diagnose_complete")`.

Branches and stop/resume conditions: if there are no liabilities or all balances are zero, exit gracefully and ask whether the concern is anticipated future debt; set a debt-prevention framing flag rather than continuing payoff work. If there is a single debt, persist `{"single_debt_path": true}` and skip phases 4 and 5, resuming at phase 6 for commitment and method choice. If cash-flow data is missing, pause and route to account connection or categorization gaps. If liability data is stale, ask the user to refresh or update it and do not proceed on stale balances. If gross income is missing, use disposable-DTI only and flag the limitation. If income is irregular, use a representative median or trailing-three-month average and tell the user why. If cash flow is negative before any new debt commitment, set monthly commitment to zero and route first to spending-plan work. If the user is mid-bankruptcy, in collections litigation, or already defaulted in a way that changes the legal frame, cite `general_principles.bankruptcy` and `general_principles.debt-default-and-collections`, refer by specialist class, and exit the payoff-goal flow.

## Phase 1: Surface Goal

The phase goal is to hear the user's debt goal in their own words before applying any SMART filter.

Coach behavior is readiness-sensitive. Ask one open question about what the user wants to be different, reflect the answer, and identify stage of change from the conversation. Capture values cues for later tradeoffs, such as stress reduction, credit rebuilding, family obligations, or wanting one quick win before a mathematically optimal path. No debt tool is needed unless resuming state.

KB topic IDs cited: none by default. If the user is in precontemplation and wants education only, cite `general_principles.debt-reduction-strategies` and `general_principles.personal-financial-ratios.debt-to-income`.

Persist state with a shape like:

```json
{
  "phase": "surface_goal",
  "stated_goal": "I want the credit cards gone so my paycheck is not already spent.",
  "stage_of_change": "contemplation",
  "values_cues": ["lower stress", "paycheck flexibility"]
}
```

Then call `agent_session_write("coach_debt_payoff:phase1_surface_complete")`.

Branches and stop/resume conditions: if the user signals precontemplation, switch to education-only mode, persist `{"phase": "surface_goal", "stage": "precontemplation", "education_topics": ["general_principles.debt-reduction-strategies", "general_principles.personal-financial-ratios.debt-to-income"]}`, and end with a gentle re-entry question for a later conversation. If the user's readiness shifts backward later in the journey, return to phase 1 and phase 2 work rather than pushing forward.

## Phase 2: Confirm Ownership

The phase goal is to reframe the goal into language that is user-owned, positive, and controllable.

Coach behavior is concrete reframing. Convert negative phrasing into a positive action and convert other-controlled phrasing into the part the user can act on. For example, "I need my spouse to stop using the card" might become "I want to set a card-use boundary and pay down the balance I am responsible for." Ask whether the reframed goal is accurate enough to carry into the SMART phase.

KB topic IDs cited: none by default; this is methodology work rather than technical debt education.

MCP tools called: no diagnostic tools. Use `skill_state_set("coach_debt_payoff", {...})` and `agent_session_write(...)` at the checkpoint.

Persist state with a shape like:

```json
{
  "phase": "confirm_ownership",
  "ownership_locked_goal": "Pay down the credit-card balance I control while keeping new card use inside the spending plan.",
  "original_phrasing": "I need my spouse to stop using the cards."
}
```

Then call `agent_session_write("coach_debt_payoff:phase2_ownership_complete")`.

Branches and stop/resume conditions: if the goal cannot be reframed because the user cannot identify any self-controlled part, surface that plainly, persist `{"phase": "confirm_ownership", "pause_reason": "cannot_reframe_to_user_owned_goal"}`, and pause. Ask what part of the situation they want to work on first; do not force the debt-payoff arc.

## Phase 3: Refine SMART

The phase goal is to turn the owned goal into a SMART payoff target with amount, debts in scope, deadline, and monthly commitment.

Coach behavior is math plus fit. Use `txn_list(date_from=<90d_ago>, date_to=<today>, limit=500)` and `budget_status()` to confirm whether the proposed monthly commitment fits the spending plan. Use `debt_simulate(strategy="compare", extra_dollars=<monthly_commitment_cents/100>, lump_sum=0, lump_sum_month=1)` to test the debt-payoff trajectory. Use `advisory_runway(balance_cents=<savings_balance>, monthly_spend_cents=<monthly_spend_cents>, annual_return_pct=<savings_yield_pct>)` only for savings-buffer sanity checks, not amortization. Use `advisory_time_to_goal(...)` and `advisory_future_value(...)` only for savings or investment-side comparisons, not debt projection.

KB topic IDs cited: `general_principles.cash-flow-statement` and `general_principles.spending-plan`.

Persist state with a shape like:

```json
{
  "phase": "refine_smart",
  "smart_goal": {
    "target_debt_free_date": "2028-04-30",
    "monthly_commitment_cents": 42000,
    "debts_in_scope": ["liab_123", "liab_456"]
  },
  "commitment_realistic": true,
  "validation_notes": "Three-month cash flow supports the commitment with $160 buffer."
}
```

Then call `agent_session_write("coach_debt_payoff:phase3_smart_complete")`.

Branches and stop/resume conditions: if the commitment is infeasible, show the numbers and ask the user to choose between revising the goal or pausing for budgeting-first work. Persist `{"phase": "refine_smart", "commitment_realistic": false, "branch": "commitment_infeasible"}` and do not advance until the user chooses a feasible path.

## Phase 4: Prioritize

The phase goal is to rank multiple debts or related goals by motivation, relevance, prerequisites, and quick-win potential.

Coach behavior is prioritization with user input. Call `debt_dashboard()` to refresh the debt list, then combine APR, balance, minimum payment, account status, DTI pressure, and phase-1 values cues. Use scaling questions where needed, but land on a concrete order and ask whether the order matches what the user can sustain.

KB topic IDs cited: `general_principles.personal-financial-ratios.debt-to-income`.

Persist state with a shape like:

```json
{
  "phase": "prioritize",
  "priority_order": ["liab_456", "liab_123", "liab_789"],
  "quick_wins_identified": ["liab_789"],
  "rationale": "Highest APR first, with the small medical bill noted as a possible early morale win."
}
```

Then call `agent_session_write("coach_debt_payoff:phase4_prioritize_complete")`.

Branches and stop/resume conditions: skip this phase when phase 0 recorded `single_debt_path`. If the user disagrees with the priority order, ask what tradeoff matters more and revise the rationale before continuing.

## Phase 5: Brainstorm Strategies

The phase goal is to generate a plural set of candidate strategies before evaluating them.

Coach behavior is idea generation before critique. Name snowball, avalanche, consolidation, personal loan, balance transfer, DMP, restructuring, settlement, and bankruptcy only where relevant to the user's debt type and diagnostic classification. Run `debt_simulate(strategy="compare", extra_dollars=<monthly_commitment_cents/100>, lump_sum=0, lump_sum_month=1)` for snowball versus avalanche. For one-time money, run `debt_simulate(strategy="avalanche", extra_dollars=<monthly_commitment_cents/100>, lump_sum=<dollars>, lump_sum_month=<month_number>)`. Keep `extra_dollars` and `lump_sum` in dollars, not cents.

KB topic IDs cited: `general_principles.debt-reduction-strategies`, `general_principles.bankruptcy`, `general_principles.debt-default-and-collections`, `general_principles.credit-cards`, and `general_principles.consumer-credit-types`.

Persist state with a shape like:

```json
{
  "phase": "brainstorm",
  "candidate_strategies": [
    {"name": "avalanche", "summary": "Highest APR first while total monthly payment stays constant."},
    {"name": "snowball", "summary": "Smallest balance first to create faster visible wins."}
  ],
  "notes": "User wants to compare speed and emotional sustainability."
}
```

Then call `agent_session_write("coach_debt_payoff:phase5_brainstorm_complete")`.

Branches and stop/resume conditions: skip this phase for the single-debt path unless the user wants consolidation, balance transfer, or lump-sum comparison. If classification is crisis-band, surface DMP and bankruptcy as legitimate options up front rather than as last resorts. If classification is stressed, foreground restructuring and DMP alongside snowball and avalanche. If classification is manageable, focus on snowball and avalanche while naming alternatives.

## Phase 6: Evaluate + Select

The phase goal is to choose one strategy with a rationale the user understands and owns.

Coach behavior is evaluation. Test each candidate against user ownership, likely obstacles, raw financial merit, risks, and fit with phase-1 values. Re-run `debt_simulate(strategy="snowball|avalanche|compare", extra_dollars=<monthly_commitment_cents/100>, lump_sum=<dollars>, lump_sum_month=<month_number>)` for finalists when it helps. If the user has investable surplus and asks whether to invest instead, call `advisory_debt_vs_invest(debt_balance_cents=<total_balance_cents>, debt_apr_pct=<weighted_avg_or_highest_apr>, monthly_extra_payment_cents=<monthly_commitment_cents>, debt_minimum_payment_cents=<sum_minimums_cents>, expected_market_return_pct=<user_assumption_or_8.0>, marginal_tax_rate_pct=<user_estimate_or_0>, is_tax_deductible=False, risk_tolerance="moderate")`.

KB topic IDs cited: `general_principles.debt-reduction-strategies`, `general_principles.credit-cards`, `general_principles.bankruptcy`, `referrals.nfcc`, and `referrals.fcaa-member-agency-finder`.

Persist state with a shape like:

```json
{
  "phase": "select",
  "chosen_strategy": "avalanche",
  "rationale": "It saves the most interest and the user is comfortable waiting longer for the first payoff.",
  "referrals_surfaced": [],
  "scope_discipline_notes": "Constant-payment rule explained."
}
```

Then call `agent_session_write("coach_debt_payoff:phase6_select_complete")`.

Branches and stop/resume conditions: if DMP is chosen, cite `referrals.nfcc` and `referrals.fcaa-member-agency-finder`, persist `{"phase": "select", "exit_reason": "referral_dmp"}`, and exit gracefully. If bankruptcy is chosen, refer to a bankruptcy attorney, cite `general_principles.bankruptcy`, persist `{"phase": "select", "exit_reason": "referral_bankruptcy"}`, and exit. If settlement on current accounts is chosen, warn about delinquency requirements, credit damage, possible tax exposure, and the predatory profile of many settlement companies; if the user still wants that path, surface nonprofit counseling or attorney referral and exit.

## Phase 7: Action Steps

The phase goal is to decompose the selected strategy into user-controllable steps and create the product goal after confirmation.

Coach behavior is action planning. Break the strategy into sequenced steps with dates, a quick win, obstacles, mitigations, referrals, and the constant monthly payment. Ask explicitly before creating the goal. After the user confirms, call `goal_set(name="<user-readable goal name>", target=<dollars>, metric="total_debt", direction="down", deadline="YYYY-MM-DD")`. The target is in dollars; for a total debt-free goal it is usually `0`.

KB topic IDs cited: strategy-specific topics from earlier phases, most often `general_principles.debt-reduction-strategies`, `general_principles.credit-cards`, and `general_principles.spending-plan`.

Persist state with a shape like:

```json
{
  "phase": "action_steps",
  "action_steps": [{"step": "Set autopay to keep total monthly debt payment at $420.", "timeline": "2026-05-01", "status": "pending", "quick_win": true}],
  "obstacles": [{"description": "Dining spend spikes after travel weeks.", "mitigation": "Set a weekly dining cap before travel."}],
  "referrals": [],
  "goal_id": "goal_123"
}
```

Then call `agent_session_write("coach_debt_payoff:phase7_action_steps_complete")`.

Branches and stop/resume conditions: if the user does not confirm the `goal_set` write, persist the drafted steps without a `goal_id` and ask what needs to change. Continue only after the user approves the product goal or chooses to keep the plan outside the goals system.

## Phase 8: Implement

The phase goal is to persist the action plan artifact and set the first monitoring check-in if the user opts in.

Coach behavior is execution after confirmation. Ask for approval to save the plan. Then call `coach_debt_payoff_artifact_save(action_plan_payload=<dict>, dry_run=False)` with required keys `smart_goal`, `strategy`, `action_steps`, `monthly_commitment_cents`, and `debts_in_scope`, plus optional `obstacles`, `referrals`, `target_debt_free_date`, `monitoring_cadence`, and `next_check_in`. If reminders are useful, ask for explicit confirmation before any `notify_*` call; use `notify_test` before scheduling when appropriate.

KB topic IDs cited: referral topics only when a referral is included in the plan, usually `referrals.nfcc` or `referrals.fcaa-member-agency-finder`.

Persist state with a shape like:

```json
{
  "phase": "implement",
  "artifact_path": "<data_dir>/artifacts/coach_debt_payoff/20260501.md",
  "monitoring_cadence": "monthly",
  "first_check_in": "2026-06-01",
  "monitoring_opted_in": true
}
```

Then call `agent_session_write("coach_debt_payoff:phase8_implement_complete")`.

Branches and stop/resume conditions: if the user opts out of monitoring, persist `{"phase": "implement", "monitoring_opted_in": false}` and exit gracefully. The constant-payment-rule intervention can still re-engage from data later.

## Phase 9: Monitor

The phase goal is sustained planned-versus-actual monitoring, with plan revision when reality disagrees with the plan.

Coach behavior is numbers first, interpretation second. Call `coach_debt_payoff_artifact_read(date=None)` or `coach_debt_payoff_artifact_read(date="YYYY-MM-DD")` to fetch the saved commitment and debts in scope. Call `txn_list(date_from=<last_check_in_date>, date_to=<today>, limit=500)`, `debt_dashboard()`, and `budget_status()` to compare actual payments, balances, and spending against the action plan. Show the planned-versus-actual numbers first, then ask the user what they notice before naming the gap or proposing a redirect.

KB topic IDs cited: none by default. Re-cite the relevant earlier topic only when the plan needs adjustment, such as `general_principles.spending-plan`, `general_principles.debt-reduction-strategies`, or `general_principles.building-credit` after payoff.

Persist state with a shape like:

```json
{
  "phase": "monitor",
  "check_ins": [{"date": "2026-06-01", "progress_summary": "Paid $420 against planned $420.", "adjustments": []}],
  "constant_payment_violations_detected": [],
  "stalls_detected": [],
  "plan_revisions": []
}
```

Then call `agent_session_write("coach_debt_payoff:phase9_monitor_check_in_<YYYY-MM-DD>")`.

Branches and stop/resume conditions: if the constant-payment rule is violated, show planned versus actual and ask what the user notices, then use a redirect prompt if they want to recommit. If there is no progress for two or more months, ask whether to revisit strategy and potentially return to phase 5. If new debt appears, flag it and ask what changed before deciding whether it is structural, unavoidable, or behavior-driven. If the goal is achieved, exit gracefully and set a transition flag for debt-prevention or savings framing.

## Branches Catalogued

- Phase 0 no liabilities: exit payoff flow and ask whether this is anticipated future debt.
- Phase 0 single debt: persist single-debt path and skip phases 4 and 5.
- Phase 0 no cash-flow data: pause for account connection or categorization gaps.
- Phase 0 stale liability data: ask for refresh or manual update before proceeding.
- Phase 0 no gross income: use disposable-DTI only and flag the limitation.
- Phase 0 irregular income: use a representative median or trailing-three-month average.
- Phase 0 cash-flow deficit: set commitment to zero and route first to spending-plan work.
- Phase 0 mid-bankruptcy detected: cite bankruptcy/default topics, refer to a bankruptcy attorney, and exit.
- Phase 1 precontemplation: switch to education-only mode and persist readiness state.
- Phase 2 cannot reframe: flag for the user, persist pause reason, and stop.
- Phase 3 commitment infeasible: loop back to revise the goal or choose budgeting-first work.
- Phase 5 crisis-band: surface DMP and bankruptcy up front.
- Phase 6 DMP chosen: cite NFCC and FCAA member-agency referrals, then exit.
- Phase 6 bankruptcy chosen: refer to a bankruptcy attorney and exit.
- Phase 6 settlement on current accounts: warn about delinquency, credit, tax, and settlement-company risk.
- Phase 9 constant-payment-rule violation: re-engage with planned-versus-actual and a redirect prompt.
- Phase 9 stall for two or more months: revisit strategy and return to phase 5 if needed.
- Phase 9 new debt opened: flag it and ask what changed before revising the plan.
- Phase 9 goal achieved: exit gracefully and transition to debt-prevention framing.

## Artifact

`coach_debt_payoff_artifact_save(action_plan_payload=<dict>, dry_run=False)` is invoked at phase 8 after explicit user confirmation to persist the action plan. `coach_debt_payoff_artifact_read(date=None)` is used at phase 9 to fetch the persisted commitment for planned-versus-actual computation. The persistence path is `<data_dir>/artifacts/coach_debt_payoff/<YYYYMMDD>.md`, handled internally by the tool.

The artifact should include the SMART goal, chosen strategy, action steps, known obstacles and mitigations, referrals when applicable, target debt-free date, monthly commitment, monitoring cadence, next check-in, and a machine-readable footer containing `debts_in_scope`, `monthly_commitment_cents`, and `generated_at`.

## Out of Scope

Bankruptcy filing is out of scope; refer to a bankruptcy attorney. DMP enrollment is out of scope; refer to NFCC or FCAA member agencies. Debt-settlement-company representation is out of scope; warn about the predatory profile and refer to nonprofit counseling or legal help when appropriate. Tax preparation is out of scope; refer to VITA, a CPA, or another qualified tax preparer outside this skill. Securities advice is out of scope; the skill educates and frames tradeoffs, while specifics that require licensure get referred by specialist class.
