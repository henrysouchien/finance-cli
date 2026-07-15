---
topic_id: general_principles.financial-planning-snapshot
cfp_domains: [general_principles, professional_conduct, psychology]
cfp_steps: [understand, identify, analyze, develop, present, monitor]
depth: intermediate
scope: framing_only
specialist_resources: [fiduciary, attorney, cpa, insurance_agent]
refresh_cadence: industry_practice_shift
jurisdiction: us_federal
sources:
  - "CFP Board: Guide to the 7-Step Financial Planning Process, verified 2026-06-22"
  - "CFP Board: Practice Standards Reference Guide, verified 2026-06-22"
  - "CFPB: Financial Well-Being tool and scale guide, verified 2026-06-22"
related_topics:
  - professional_conduct.financial-planning-scope
  - general_principles.goals-and-constraints-inventory
  - general_principles.balance-sheet-net-worth-statement
  - psychology.financial-well-being-and-user-context
  - general_principles.cash-flow-statement
  - general_principles.spending-plan
  - general_principles.personal-financial-ratios.liquidity
  - general_principles.personal-financial-ratios.debt-to-income
  - general_principles.debt-vs-investing-decision-frame
  - investment.investment-readiness
  - retirement.retirement-accounts
  - tax.tax-basics
  - estate.estate-planning
---

# Financial Planning Snapshot

A Financial Planning Snapshot is a cross-domain intake artifact. It gathers the
facts CashNerd already knows, the goals the user stated, the constraints that
limit the plan, and the next planning workflows that should run.

The snapshot is not a comprehensive financial plan. It is a durable triage layer
for the advisor agent.

## Core Sections

**Scope.** What question started the snapshot, what data sources are in use, and
which domains are included.

**Household context.** Employment, income stability, dependents, housing,
business obligations, and other facts that change planning priorities.

**Goals and constraints.** User-stated goals, deadlines, priorities, values,
monthly capacity, required obligations, and known restrictions.

**Balance sheet.** Liquid cash, short-horizon savings, investment/retirement
assets, debts, manual balances, stale values, and net worth trend if available.

**Cash flow.** Income, essential expenses, surplus or deficit, timing risks, and
recurring obligations.

**Domain readiness.** Debt, emergency fund, tax, retirement, investment, risk,
estate, and professional-handoff status.

**Planning sequence.** Recommended next advisor-agent skill or human handoff,
with a plain rationale.

**Data gaps.** Missing facts that would change the answer.

**Monitoring.** Events or review dates that should refresh the snapshot.

## Readiness Statuses

Use consistent statuses across domains:

**ready.** Enough facts exist for a scoped CashNerd planning workflow.

**active_plan.** A sibling skill or artifact already owns the next step.

**data_needed.** Missing facts block a defensible plan.

**fix_first.** A prerequisite risk or obligation should be addressed before the
requested goal.

**refer.** A specialist or regulated partner should own the next decision.

**not_applicable.** The domain does not appear relevant from current facts.

## Sequencing Rules

The snapshot should not try to solve every domain at once. It should choose the
next useful workflow.

Common sequencing patterns:

- high-interest debt or missed payments before taxable investing;
- starter emergency reserve before aggressive discretionary goals;
- insurance/risk intake before reducing cash reserves for long-horizon goals;
- tax readiness when withholding, self-employment, or contribution facts are
  missing;
- estate document readiness after major life events, dependents, or asset
  changes;
- advisor handoff when the user wants securities, tax, legal, or insurance
  implementation.

When multiple goals compete for the same surplus, show the conflict and preserve
the user's preference. Do not allocate the same dollar twice.

## Practical Application

For `coach_financial_plan_intake`, the snapshot should be saved even when it is
incomplete. A `data_needed` snapshot is valuable because it tells the user
exactly what must be gathered before the next planning decision.

The output should include:

- facts used;
- facts missing;
- assumptions;
- next workflow;
- handoff triggers;
- user-confirmed priorities.

This artifact becomes the read source for later advisor-agent skills so each
skill does not restart intake from zero.

## Boundaries

The snapshot may recommend a process step, a CashNerd skill, a checklist, or a
professional handoff. It must not:

- recommend securities, funds, portfolios, trades, or allocations;
- decide tax filing positions or credit eligibility;
- draft or interpret legal documents;
- recommend insurance product purchase, cancellation, replacement, or coverage
  amount;
- imply a comprehensive human CFP engagement occurred.

## Common Pitfalls

- Producing a polished narrative that hides missing data.
- Ranking goals without recording the user's values.
- Treating the snapshot as a one-time onboarding form instead of a living
  planning artifact.
- Letting professional handoff needs disappear because they are inconvenient.
- Overfitting the next action to whichever domain the user mentioned first.

## Sources

- CFP Board: Guide to the 7-Step Financial Planning Process
- CFP Board: Practice Standards Reference Guide
- CFPB: Financial Well-Being tool and scale guide

## Effective-Date Notice

This topic is a process artifact. Refresh when the skill state model, artifact
schema, professional-handoff policy, or advisor-agent release mode changes.
