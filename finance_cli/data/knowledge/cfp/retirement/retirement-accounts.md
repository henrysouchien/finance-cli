---
topic_id: retirement.retirement-accounts
cfp_domains: [retirement, tax]
cfp_steps: [understand, analyze, develop]
depth: foundational
scope: vocabulary
specialist_resources: [fiduciary, cpa]
refresh_cadence: annual_regulatory
jurisdiction: us_federal
sources:
  - "AFCPE: AFCPE Money Management Essentials, Module 9 topic1606 - defined-benefit vs defined-contribution plans, IRA and Roth IRA vocabulary, catch-up contributions, RMDs, and retirement-plan type comparison"
  - "AFCPE: AFCPE Money Management Essentials, Module 4 topic1591 - employer benefits, retirement-plan matching contributions, vesting, payroll deduction mechanics, and plan-loan vocabulary"
  - "IRS: Notice 2025-67 - 2026 retirement plan and IRA cost-of-living adjusted limitations, verified 2026-06-22"
  - "IRS: Retirement topics - IRA contribution limits, verified 2026-06-22"
  - "IRS: Retirement topics - 401(k) and profit-sharing plan contribution limits, verified 2026-06-22"
related_advisory_tools:
  - advisory_contribution_priority
  - advisory_roth_vs_traditional
  - advisory_future_value
  - advisory_time_to_goal
related_topics:
  - general_principles.financial-planning-snapshot
  - general_principles.employee-benefits
  - tax.employee-plan-tax-treatment
  - investment.investment-readiness
  - general_principles.debt-vs-investing-decision-frame
---

# Retirement Accounts

Retirement accounts are tax-advantaged containers for long-horizon saving. This topic gives the coach shared vocabulary for account types, contribution mechanics, annual room, and plan-provider routing. It does not choose securities, funds, allocations, rollovers, or Roth conversion execution for a user.

Use this topic inside `coach_retirement_contribution_readiness` when the user is trying to understand where the next contribution dollar could reasonably go. Use deterministic helpers for annual limits and contribution room; do not quote current-year dollar figures from this file or from a skill playbook.

## Key Concepts

**Defined-benefit plans promise a benefit.** Pensions calculate a retirement income stream from plan formulas such as service years and compensation history. The user usually needs the plan administrator's statement to know the actual benefit.

**Defined-contribution plans define contributions, not outcomes.** A 401(k), 403(b), 457(b), TSP, SIMPLE plan, SEP, or solo 401(k) accumulates employee and sometimes employer contributions plus investment returns. The balance depends on contribution rate, fees, investment performance, time, and withdrawals.

**IRAs are individually owned.** Traditional and Roth IRAs are personal retirement accounts. The annual IRA limit is combined across traditional and Roth IRA contributions for the same person. Roth eligibility can phase out based on filing status and modified AGI, so the coach needs tax-year, filing-status, income, compensation, age, and existing-contribution facts before discussing available room.

**Traditional and Roth describe tax treatment.** Traditional contributions may reduce current taxable income and are generally taxed when withdrawn. Roth contributions use after-tax dollars and qualified withdrawals can be tax-free. The useful coaching frame is current marginal tax rate versus expected retirement marginal tax rate, not "Roth is always better" or "deductible is always better."

**HSA is a health account with retirement relevance.** A Health Savings Account is available only when the user is HSA-eligible under HDHP rules and has no disqualifying coverage. It is not a retirement account, but unused HSA balances can become a long-horizon health-cost reserve. Include HSA in contribution sequencing only after the user confirms HDHP/HSA eligibility and coverage type.

**Contribution room is tax-year specific.** Annual limits, catch-up amounts, phaseout ranges, and plan-specific limits change. The skill should pass an explicit user-confirmed `tax_year` to helpers such as `advisory_contribution_priority` or future contribution-room helpers and surface the helper's source metadata.

**Plan documents can be stricter than IRS ceilings.** An employer plan may set eligibility, payroll timing, matching, vesting, loan, hardship, and investment-menu rules that are narrower than general IRS limits. The coach can explain categories and questions to ask; the plan provider or HR confirms the exact rule.

## Contribution Readiness Use

For v0.1 contribution readiness, use this sequence as vocabulary, not as an unverified recommendation:

1. Capture any employer match the user can afford.
2. Do not increase retirement contributions beyond match without checking high-interest debt and emergency-fund posture.
3. Include HSA only when eligibility is confirmed.
4. Evaluate IRA room and Roth/traditional sensitivity using explicit tax-year and income facts.
5. Evaluate additional workplace-plan room only after the user understands cash-flow and plan-document constraints.

The MCP `advisory_contribution_priority` helper owns the deterministic sequencing output. The coach owns the conversation around data quality, user preference, confirmation, and boundaries.

## Hand-Off Discipline

Route to a specialist or plan administrator when the user needs:

- rollover execution, backdoor Roth execution, Roth conversion execution, or multi-year tax projection;
- self-employed plan selection such as SEP versus solo 401(k);
- securities, fund, asset-allocation, or fee-analysis recommendations beyond general education;
- employer plan-document interpretation, vesting disputes, hardship withdrawals, plan loans, QDROs, or beneficiary legal advice;
- current-year tax filing treatment for a contribution already made or needing correction.

## Common Pitfalls

- Leaving employer match unclaimed while funding lower-priority accounts.
- Treating annual contribution limits as static.
- Counting all IRA contributions separately instead of combined across traditional and Roth IRAs.
- Assuming Roth eligibility without checking modified AGI and filing status.
- Treating HSA contributions as available without confirming HDHP/HSA eligibility.
- Ignoring plan-specific payroll, match, vesting, and contribution-window rules.
- Using retirement-account withdrawals to solve cash-flow problems before exploring safer alternatives.

## Sources

- AFCPE: AFCPE Money Management Essentials, Module 9 topic1606 - retirement account categories, traditional/Roth IRA vocabulary, contribution/distribution/RMD/catch-up framing, and account-type comparison
- AFCPE: AFCPE Money Management Essentials, Module 4 topic1591 - employer retirement-plan benefits, match, vesting, payroll deductions, plan loans, and hardship-withdrawal vocabulary
- IRS: Notice 2025-67 - 2026 retirement plan and IRA cost-of-living adjusted limitations
- IRS: Retirement topics - IRA contribution limits
- IRS: Retirement topics - 401(k) and profit-sharing plan contribution limits

## Effective-Date Notice

This topic is evergreen vocabulary with annual-regulatory source checks. Contribution limits, catch-up amounts, Roth IRA phaseout ranges, traditional IRA deduction phaseout ranges, highly compensated employee thresholds, and related tax-year figures must come from deterministic helpers backed by current IRS data, not from this article or a skill playbook.
