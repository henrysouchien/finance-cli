---
topic_id: general_principles.goals-and-constraints-inventory
cfp_domains: [general_principles, psychology]
cfp_steps: [understand, identify, monitor]
depth: foundational
scope: full
refresh_cadence: static
jurisdiction: us_federal
sources:
  - "CFP Board: Guide to the 7-Step Financial Planning Process, verified 2026-06-22"
  - "CFP Board: Practice Standards Reference Guide, verified 2026-06-22"
  - "CFPB: Financial Well-Being tool and scale guide, verified 2026-06-22"
related_topics:
  - professional_conduct.financial-planning-scope
  - general_principles.financial-planning-snapshot
  - general_principles.spending-plan
  - psychology.financial-well-being-and-user-context
---

# Goals and Constraints Inventory

A goals and constraints inventory turns a broad financial concern into planning
inputs. It captures what the user wants, why it matters, when it matters, what
resources can be used, and what limits cannot be ignored.

The inventory is not a promise that every goal can be funded at once. Its job is
to make priorities and tradeoffs visible before a plan recommends the next
workflow.

## Goal Fields

For each goal, capture:

- name and plain-language description;
- owner: user, household, child, business, or other;
- time horizon: immediate, short, medium, long, retirement, or unknown;
- priority: high, medium, low, required, or exploratory;
- target amount or non-dollar success measure;
- current progress and existing linked goal, if any;
- funding source or monthly capacity, if known;
- deadline flexibility;
- reason the goal matters to the user.

The "why" matters because two goals with identical math can have different
priority. Paying off a credit card before a baby is born is different from
paying off the same balance because the user dislikes debt.

## Constraint Fields

Constraints are facts that limit the plan:

- cash-flow surplus or deficit;
- emergency reserve gap;
- required debt payments and high-interest balances;
- income volatility or job risk;
- dependents, caregiving duties, housing commitments, or business obligations;
- tax-year facts that could affect contribution or withholding choices;
- insurance gaps or known out-of-pocket exposure;
- legal or estate-document needs;
- account access, transfer limits, payroll timing, and provider constraints;
- user preferences such as "do not increase debt" or "keep $X in checking."

Do not treat constraints as objections to overcome. They are part of the plan's
design.

## Prioritization

Prioritize goals by combining:

1. **Required obligations.** Current bills, minimum debt payments, insurance
   premiums, taxes, and other must-pay items come first.
2. **Risk protection.** Thin liquidity, uninsured risks, unstable income, or
   legal deadlines can outrank higher-return opportunities.
3. **Time horizon.** Short-horizon goals need stable cash. Long-horizon goals
   can tolerate different planning conversations.
4. **User values.** The plan should reflect what the user is actually willing to
   do, not only what optimizes a spreadsheet.
5. **Professional boundaries.** Goals requiring securities, tax, legal,
   insurance, or plan-administration judgment route to the right handoff.

When goals conflict, surface the conflict directly. Do not silently allocate the
same surplus dollar to debt payoff, savings, retirement, and investing.

## Practical Application

For `coach_financial_plan_intake`, the inventory feeds the planning sequence.
Example outputs:

- "Debt payoff comes before taxable investing because the high-interest balance
  is consuming the same monthly surplus."
- "The house down payment is short-horizon, so this is not the cash to expose to
  market risk."
- "The next workflow should be risk/insurance readiness because unknown
  disability and health out-of-pocket exposure could change every other
  recommendation."

The inventory should preserve uncertainty. A goal with missing deadline, target
amount, or priority can still be saved, but it should be marked `data_needed`.

## Common Pitfalls

- Treating all goals as equal because they are all user-stated.
- Ignoring required obligations while optimizing discretionary goals.
- Assigning the same surplus to multiple goals.
- Letting a long-term aspiration override a near-term liquidity risk.
- Failing to record the user's values and constraints in their own words.

## Sources

- CFP Board: Guide to the 7-Step Financial Planning Process
- CFP Board: Practice Standards Reference Guide
- CFPB: Financial Well-Being tool and scale guide
