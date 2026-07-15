---
topic_id: risk_insurance.risk-inventory-and-handoff
cfp_domains: [risk_insurance, professional_conduct]
cfp_steps: [analyze, present, implement]
depth: intermediate
scope: framing_only
specialist_resources: [insurance_agent, fiduciary, attorney]
refresh_cadence: industry_practice_shift
jurisdiction: us_federal
sources:
  - "External: CFP Board Principal Knowledge Topics and financial planning process materials - risk management and handoff scaffold, verified 2026-06-22"
  - "External: NAIC Consumer resources and state insurance department directory - consumer education, complaint routing, and insurance professional lookup context, verified 2026-06-22"
---

# Risk Inventory And Handoff

A risk inventory is the planning artifact that says what the household knows,
what it does not know, which gaps can block adjacent plans, and which questions
belong with a licensed or credentialed professional.

This topic gives the handoff discipline for `coach_risk_insurance_readiness`.
It is not an insurance recommendation engine.

## Inventory Sections

The readiness artifact should organize facts into these sections:

- household and dependents;
- employment and benefits;
- cash reserves and essential expenses;
- health cost-sharing and medical-cost exposure;
- disability-income protection;
- life-insurance and beneficiary review;
- homeowners, renters, auto, umbrella, flood, earthquake, and other property or
  liability context;
- long-term-care, business, professional-liability, or specialty exposures when
  volunteered;
- open claims, denials, cancellations, nonrenewals, complaint issues, or legal
  questions;
- professional relationships and documents to gather.

Each section should support a status such as `known`, `unknown`,
`needs_review`, `not_applicable`, or `refer`.

## Handoff Triggers

Prepare a professional handoff when:

- the user asks what policy, rider, deductible, limit, insurer, or product to
  buy;
- coverage amount, replacement, cancellation, surrender, lapse, or bundling is
  the decision;
- a claim, denial, complaint, nonrenewal, rescission, fraud accusation, or
  liability issue is active;
- policy wording, legal fault, beneficiary legal effect, trust ownership, estate
  tax, divorce, business ownership, or state law controls the answer;
- Medicare, Medigap, long-term-care, annuity, or professional-liability details
  are material;
- the user's financial plan depends on an unverified policy fact.

The handoff should name the professional type and the question to bring. It
should not sneak in the answer being handed off.

## Handoff Packet Contents

A good handoff packet includes:

- the user's stated concern;
- known policies or benefits by category;
- missing documents or values;
- planning decision currently blocked;
- questions for the professional;
- relevant CashNerd artifacts, such as financial plan intake, homebuying,
  estate, retirement, investment readiness, debt payoff, or emergency fund
  plans;
- caution that CashNerd has not selected or recommended a policy.

Keep the packet metadata-level. Do not store full policy contracts, claim
letters, medical records, Social Security numbers, account credentials, or
attorney communications.

## Professional Types

Use specific routing language:

- **insurance agent or broker:** policy options, quotes, limits, riders,
  replacement, underwriting, and product-specific questions;
- **benefits team or plan administrator:** employer coverage, open enrollment,
  group disability, group life, payroll deductions, and portability;
- **state insurance department:** complaints, agent licensing, insurer lookup,
  and state consumer assistance;
- **fiduciary planner or RIA:** insurance interactions with investment,
  retirement, cash-flow, or estate planning when regulated advice is needed;
- **attorney:** legal interpretation, beneficiary disputes, trust ownership,
  estate, divorce, liability, claim litigation, or state-law questions;
- **CPA or tax professional:** tax treatment of premiums, benefits, policy loans,
  withdrawals, or business deductions.

The current specialist enum does not include every operational label, so
artifact prose can name benefits teams and state insurance departments even when
frontmatter uses the closest durable specialist resources.

## Cross-Skill Boundaries

This topic should help other skills pause or proceed:

- `coach_financial_plan_intake` may summarize insurance gaps in the global
  planning sequence.
- `coach_homebuying_readiness` may require property, flood, or reserve review.
- `coach_investment_readiness` may defer account funding if reserves cannot
  absorb known health or property risk.
- `coach_retirement_contribution_readiness` may ask the user to verify benefits
  before changing payroll deductions.
- `coach_estate_document_readiness` may trigger beneficiary and attorney review.

Sibling skills should read the risk artifact as context, not as permission to
recommend products.

## Common Pitfalls

- Writing a handoff that hides the regulated recommendation inside "questions."
- Storing sensitive policy, claim, or medical-document contents in an artifact.
- Treating a state complaint process as legal advice.
- Letting a stale risk inventory silently approve a new plan.
- Collapsing "insurance professional," "fiduciary," "attorney," and "tax
  professional" into one generic referral.

## Sources

- External: CFP Board Principal Knowledge Topics and financial planning process
  materials - risk management and handoff scaffold.
- External: NAIC Consumer resources and state insurance department directory -
  consumer education, complaint routing, and insurance professional lookup
  context.

## Effective-Date Notice

This topic is evergreen handoff process. Professional roles, state complaint
routes, policy-market practices, referral rules, and regulatory requirements can
change; review source links and jurisdiction before operationalizing any
handoff.
