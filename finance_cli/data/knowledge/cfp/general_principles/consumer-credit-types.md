---
topic_id: general_principles.consumer-credit-types
cfp_domains: [general_principles]
cfp_steps: [understand, analyze]
depth: foundational
scope: full
refresh_cadence: static
jurisdiction: us_federal
sources:
  - "AFCPE: AFCPE Money Management Essentials, Module 5 topic1593 — installment (closed-ended) credit definition, revolving (open-ended) credit definition, service credit definition, secured vs unsecured definitions, amortization with Exhibit 5-4 sample schedule ($10,000 at 5% / 4 years), principal definition, acceleration clause definition"
related_topics:
  - general_principles.credit-cards
  - general_principles.building-credit
  - general_principles.debt-default-and-collections
  - general_principles.bankruptcy
  - general_principles.debt-reduction-strategies
---

# Consumer Credit Types

Consumer credit comes in three structural categories — installment, revolving, and service — that behave differently from each other in how they're scored, how they're collected on, and how their interest cost evolves over time. A second cross-cutting distinction (secured vs. unsecured) determines what the lender can claim if the borrower stops paying. Before any product-level conversation, the categorical map clarifies which debt is which and which mechanics apply.

This topic is the foundational credit-categorization layer that downstream credit topics reference. Credit cards, building credit, debt-default-and-collections, debt-reduction-strategies, and short-term financing all derive their treatment from these categories.

## The Three Types of Consumer Credit

### Installment Credit (Closed-Ended)

A fixed amount borrowed upfront, repaid in scheduled installments over a set period. Each payment includes both **principal** (reducing the balance) and **interest** (the cost of borrowing). Once the balance reaches zero, the account closes; to borrow again, the client applies for a new loan.

Common examples:

- **Mortgages** — secured by the home, 15- or 30-year terms typical.
- **Auto loans** — secured by the vehicle, 3- to 7-year terms typical.
- **Student loans** — typically unsecured, 10- to 25-year terms depending on plan.
- **Personal loans** — typically unsecured, 2- to 7-year terms.
- **Credit-builder loans** — typically 6- to 24-month terms; the borrowed amount sits in a savings account until payoff.

What makes installment credit predictable is the fixed payment schedule. Budgeting can treat the payment as a fixed expense.

### Revolving Credit (Open-Ended)

A maximum credit limit is established; the client can borrow and repay repeatedly within that limit. Payments are required each month (at minimum, a percentage of the outstanding balance plus accrued interest), but the client chooses how much to pay above the minimum and how much to carry. The account stays open as long as it remains in good standing.

Common examples:

- **Credit cards** — the dominant revolving product.
- **Home equity lines of credit (HELOCs)** — revolving credit secured by home equity.
- **Personal lines of credit** — unsecured revolving lines from a bank or credit union.
- **Overdraft lines of credit** — small revolving lines linked to a checking account.

Revolving credit carries a scoring mechanic that installment does not: **utilization**, the ratio of current balance to credit limit. A $3,000 balance on a $10,000 credit card is 30% utilization; the same $3,000 balance on an auto loan has no analogous ratio that the credit score cares about.

### Service Credit

An agreement to pay for a service already delivered — utility accounts, cell phone plans, gym memberships, some medical billing arrangements. Service is rendered continuously and paid in arrears, typically monthly.

Service credit has an asymmetric reporting pattern: it usually does NOT report to the credit bureaus during normal operation, but it DOES show up when something goes wrong (missed payment sent to collections, closed account with unpaid final balance). The utility account doesn't build credit when paid on time, but it can damage credit when paid late.

Some newer alternative-data services (Experian Boost and similar) report on-time service payments to the bureaus for clients who opt in, partly closing this asymmetry.

## Secured vs. Unsecured Credit

Cross-cutting the type distinction: whether the credit is backed by collateral.

**Secured credit** is backed by an asset the lender can claim if the borrower defaults. The asset is **collateral**; the lender holds a legal interest in it for the life of the loan. On default, the lender can seize and sell the asset to recover what's owed.

Examples: mortgages (home), auto loans (vehicle), secured credit cards (cardholder deposit), home equity loans and HELOCs (home equity), margin loans (investment securities).

**Unsecured credit** is backed only by the borrower's promise to pay and their creditworthiness. On default, the lender's recovery is collection activity, credit-report damage, and lawsuits — but no specific asset is automatically forfeit without a court judgment.

Examples: most credit cards, personal loans, most student loans, medical debts, most collections.

**Implications:**

- **Interest rates are lower on secured credit.** The collateral reduces the lender's risk, and the rate reflects that. A mortgage at 6% and a personal loan at 12% on the same borrower reflect the collateral difference, not (primarily) borrower quality.
- **Default consequences differ materially.** Missing mortgage payments eventually leads to foreclosure (the client loses the home). Missing credit-card payments leads to charge-off, collections, possibly lawsuit and wage garnishment, but no automatic asset loss.
- **Amortization behavior is the same** for secured and unsecured installment products. Collateral does not change how the math works; it changes what happens when the math breaks.

Edge case: an "underwater" loan (loan balance higher than collateral value) is still secured, but repossession does not fully satisfy the debt. The lender can pursue the **deficiency** as unsecured debt. This matters when a client asks "can I just give them the car back?" — usually yes, but the deficiency follows them.

## Amortization

**Amortization** is the process of paying off an installment loan through regular payments that each cover both interest and principal, with the balance reaching zero at the end of the term.

The payment stays constant; the composition shifts. Early in the loan, the balance is high, so most of the payment goes to interest and only a small portion chips at principal. As the balance falls, interest due each month falls with it, so more of the same payment goes to principal. By the end, almost the entire payment is principal.

### Worked example (AFCPE Exhibit 5-4)

A $10,000 loan at 5% for 4 years has a monthly payment of approximately $230.29.

- **Payment 1** — interest $41.67, principal $188.62, balance $9,811.38.
- **Payment 2** — slightly less interest (balance is lower), principal $189.41.
- **Payment 47** — interest $1.51, principal $228.78.
- **Payment 48** — zeros the balance.

What this reveals:

- **Early payments barely touch principal.** The "I've been paying this mortgage for three years and the balance is barely lower" feel is structurally correct — not a sign of a bad loan.
- **Extra principal payments early save disproportionate interest.** Paying an additional $100 against principal in month 3 means that $100 stops generating interest for the remaining 45 months. The same $100 in month 46 has almost no effect — it was about to be paid off anyway.
- **Longer terms mean more total interest** even at the same rate. A 30-year mortgage at 6% pays roughly 2× the original loan in cumulative interest; a 15-year mortgage at the same rate pays closer to 0.5×. Same rate, same house, very different total cost.

For accelerated-payoff strategies (extra principal, biweekly schedules, lump-sum applications), the value comes primarily from shortening the tail — stopping interest earlier on principal that would otherwise have continued accruing.

## The Acceleration Clause

A detail buried in most installment loan agreements: the **acceleration clause** lets the lender demand the entire remaining balance immediately if the borrower triggers certain conditions — typically a missed payment, but also events like selling collateral, filing bankruptcy, or breaking other specific terms.

In practice, lenders rarely accelerate on a single missed payment; the usual path is collection activity, late fees, credit-report damage, and only then acceleration if the situation persists. But the clause exists. A client who falls far enough behind can find themselves facing a demand for the full balance rather than a payment plan.

Counseling implication: "I just missed one payment, I'll catch up next month" is usually fine. "I've missed three payments and I'm getting acceleration language in the letters" is a different situation that needs prompt intervention — communication with the lender, a hardship-modification request, or escalation to debt-default counseling.

## Practical Application

For an AI coach, type-categorization is a useful background layer. When the user discusses a debt, inferring type (from context, transaction data, or linked-account metadata) lets the coach produce more appropriate responses:

- **Utilization advice** when the debt is revolving.
- **Acceleration-risk warnings** when the debt is installment and payments are slipping.
- **Collateral-at-risk warnings** when the debt is secured and default is plausible.
- **Asymmetric-reporting reminders** when the debt is service credit and the client is asking about credit-building impact.

When running long-term plans (debt-free projections, refinancing scenarios), the amortization math is a direct input. The coach can compute exact interest savings from a proposed extra-principal plan rather than relying on rules of thumb. Precision is achievable and high-value here.

Coaching uses where type-mapping shows up:

- **A client explaining stress.** "I'm behind on my car payment and my credit card" involves two different credit types with two different default behaviors. The categorical distinction clarifies which debt is most urgent.
- **Choosing a repayment strategy.** Snowball and avalanche treat all debt uniformly; the real world doesn't. Secured debt where the asset is at risk usually needs to be prioritized above unsecured debt even if the rate is lower.
- **Building credit.** An installment account plus a revolving account produces a more complete scoring picture than either alone — credit-mix rewards both types.
- **Why paying off a mortgage slowly "feels wrong."** Amortization explains it. The feel is correct.

## Common Pitfalls

- **Treating all debt as interchangeable in a consolidation decision.** Rolling an auto loan into a home equity loan changes unsecured-with-respect-to-the-house debt into secured-with-the-house debt. Interest savings may be real; risk profile changes materially.
- **Forgetting service-credit asymmetry.** A client who has "always paid my phone bill on time" should not expect that to appear on the credit report. A client who let the phone bill go to collections will find it there.
- **Paying down a 30-year mortgage as if it were a 4-year auto loan.** Extra-principal math is real but the dollars-per-month feel is muted early on. Clients sometimes get discouraged by slow balance movement and stop the acceleration plan; understanding amortization prevents that.
- **Underestimating risk in secured credit during a downturn.** The asset can lose value while the loan balance does not. An underwater loan turns the "just give it back" option into a residual-deficiency problem.
- **Assuming all revolving credit behaves like a credit card.** HELOCs often have a draw period followed by a repayment period with different terms. Personal lines of credit may have variable rates that reset. Read the terms once, at origination.
- **Ignoring acceleration-clause language.** Most clients don't read it; most lenders don't invoke it; but when circumstances align, the clause turns a manageable situation into a full-balance demand.

## Sources

- AFCPE: AFCPE Money Management Essentials, Module 5 topic1593 — installment (closed-ended) credit definition, revolving (open-ended) credit definition, service credit definition, secured vs unsecured definitions, amortization with Exhibit 5-4 sample schedule ($10,000 at 5% / 4 years), principal definition, acceleration clause definition
