---
topic_id: general_principles.banking-basics
cfp_domains: [general_principles]
cfp_steps: [understand, analyze, develop, implement]
depth: foundational
scope: full
specialist_resources: []
referrals: []
refresh_cadence: event_driven_regulatory
jurisdiction: us_federal
legal_basis:
  - us_federal:fdic_insurance
  - us_federal:ncua_insurance
  - us_federal:reg_e
  - us_federal:reg_d
  - us_federal:ncua_pal
related_topics:
  - general_principles.short-term-financing
  - general_principles.government-financial-assistance-programs
  - general_principles.personal-financial-ratios.liquidity
  - general_principles.credit-cards
  - general_principles.building-credit
  - general_principles.cash-flow-statement
  - general_principles.spending-plan
sources:
  - "AFCPE: AFCPE Money Management Essentials, Module 4 topic1589 — institution definitions (bank, credit union), FDIC/NCUA $250,000 deposit insurance, account-type overview (checking, savings, money market deposit account, certificate of deposit), ATM and debit card mechanics, electronic funds transfers and electronic bill pay, savings strategies (emergency fund first, separate accounts per goal, revolving savings), alternatives for unbanked / underbanked clients"
  - "Federal law: Federal Deposit Insurance Act (12 U.S.C. § 1811 et seq.) — FDIC deposit-insurance regime; $250,000 standard maximum deposit insurance amount per depositor, per insured institution, per ownership category"
  - "Federal law: Federal Credit Union Act insurance provisions (12 U.S.C. § 1751 et seq.) — NCUA share-insurance regime via the National Credit Union Share Insurance Fund (NCUSIF); parallel coverage to FDIC"
  - "Federal law: Regulation E — CFPB implementing regulation for the Electronic Fund Transfer Act; consumer rights and liability limits on debit, ACH, P2P, and ATM transactions; error-resolution procedures"
  - "Federal law: Regulation D — Federal Reserve reserve-requirements regulation; historically imposed a six-per-month withdrawal limit on savings deposits and MMAs (federal limit removed April 2020; many institutions retain it as policy)"
  - "Federal law: NCUA Payday Alternative Loan rule (12 CFR 701.21) — federal credit unions may offer small-dollar short-term loans at an interest rate up to 28% (1,000 basis points above the Federal Credit Union loan ceiling) plus a separate cap on application fees"
  - "External: Bank On account standards (Cities for Financial Empowerment Fund) — national certification identifying low-fee, no-overdraft entry-level accounts"
---

# Banking Basics

Account infrastructure is the substrate every household financial number sits on top of. Pick the wrong institution, the wrong account type, or the wrong payment rail, and the cost shows up as fees, missed grace periods, late-payment damage, and predictable cash-flow timing breaks — none of which look like a "money problem" in isolation. This topic covers what a coach needs to surface at intake, during deficit triage, when goals are being structured, and when a client is paying the underbanked tax without realizing it. The companion ratios in `general_principles.personal-financial-ratios.liquidity` cite the deposit-product subset of liquid assets defined here.

## Banks vs. Credit Unions

Functionally, the two institution types do the same things: take deposits, lend, issue cards, route electronic payments. The structural difference is ownership.

- **Bank** — for-profit, shareholder-owned. Branches range from local community banks to global institutions.
- **Credit union** — nonprofit, member-owned cooperative. Membership often gated by geography, employer, or affinity. Surplus that would otherwise flow to shareholders returns to members in the form of better rates and lower fees.

For day-to-day coaching, treat them interchangeably. Where the choice matters: credit unions tend to win on fee schedule and deposit rates, and frequently approve clients (past overdrafts, thin file) that a national bank will decline.

## Deposit Insurance

Both institution types carry a federal deposit guarantee, which is the actual answer to "what if the bank fails."

- **FDIC** insures deposits at member banks under the Federal Deposit Insurance Act.
- **NCUA** insures deposits at federally insured credit unions through the National Credit Union Share Insurance Fund (NCUSIF), under the Federal Credit Union Act.

Coverage is **$250,000 per depositor, per insured institution, per ownership category**. The three dimensions multiply: joint, individual, and trust accounts at the same institution each get their own $250K. Households with cash above the cap can structure ownership or split across institutions to keep every bucket under the line. For most clients the cap is academic; flag it when a client mentions home-sale proceeds, an inheritance, or a business exit landing in cash.

Coverage is **deposits only**. Investment products sold through a bank — mutual funds, annuities, brokered securities — are not insured by FDIC or NCUA even when the bank channel made the sale. The deposit set covered is checking, savings, MMDAs, and CDs; everything else sits outside the guarantee.

## Account Types

Four account types cover almost every household.

- **Checking.** Transaction account. Designed for inflow (paycheck deposit, transfers in) and outflow (debit purchases, bill pay, ATM withdrawals). Interest negligible. The operational hub.
- **Savings.** Holding account for cash that should not be one-tap accessible. Historically capped at six monthly withdrawals under Regulation D; the Federal Reserve removed that federal limit in April 2020, but many banks and credit unions still enforce a similar cap as institutional policy. Modest tiered interest.
- **Money market deposit account (MMA / MMDA).** Savings-checking hybrid sitting inside the deposit guarantee. Typically pays better than a vanilla savings account in exchange for a higher minimum balance to earn the rate or waive fees. Limited check-writing access on some products.
- **Certificate of deposit (CD).** Time deposit. Principal locked for a stated term (3 months to 5 years is the common range) at a fixed rate, with an early-withdrawal penalty enforced as a flat or interest-based fee. Right tool for cash with a known future use date and no need for liquidity in between.

**MMDA ≠ MMMF.** The deposit-side money market account (MMA/MMDA) sits inside FDIC/NCUA coverage. A money market *mutual fund* (MMMF) is an investment product, sold through a brokerage, with no deposit guarantee. Confusing the two is common. The broader liquid-asset taxonomy — which includes MMMFs — lives in `general_principles.personal-financial-ratios.liquidity`; this topic only covers the deposit subset.

### Cards Attached to Accounts

- **ATM card.** Cash access plus basic ATM transactions; no point-of-sale capability. Out-of-network fees stack: a typical $3 surcharge plus a $3 issuer fee, twice weekly, drains $312 a year for nothing.
- **Debit card.** Point-of-sale capable; pulls funds from the linked checking account in real time. Fraud handling is governed by Regulation E, which sets statutory liability caps that escalate based on how quickly the cardholder reports the loss.

Coaching distinction worth being precise about: a debit transaction *moves the client's cash out of the account immediately*; a credit transaction *creates an obligation the client pays later*. The downstream consequence is that fraud on a debit card leaves the checking account empty during the dispute window, while fraud on a credit card is a line on a statement the client hasn't paid yet — and Fair Credit Billing Act protections on credit are practically stronger than Regulation E's protections on debit. See `general_principles.credit-cards`.

## Electronic Money Management

Almost every recurring money movement in a modern household runs over **electronic funds transfer (EFT)** rails, governed by Regulation E. The mechanics:

- **Direct deposit.** Paycheck or benefits payment posted to the recipient's account on the pay date. Removes both the check-cashing cost and the mail-delay window that breaks timing for clients living paycheck to paycheck.
- **Autopay and bank bill pay.** Either the biller pulls on the due date (autopay) or the bank pushes on a scheduled day (bank bill pay). Both convert the chronic-late-payment problem into a one-time setup task — and payment history is the largest component of credit scores (see `general_principles.building-credit`).
- **ACH transfers.** The settlement rails behind most direct deposits, bill payments, and bank-to-bank moves. Typical settlement: 1–3 business days. Free or low-fee.
- **Peer-to-peer apps** (Venmo, Cash App, Zelle, etc.). Consumer wrappers on top of ACH or card rails. Useful for splitting costs and paying individuals. Critical caveat: balances held inside the app are generally **not** FDIC-insured unless the provider explicitly sweeps them to an insured partner bank. Clients who let app balances accumulate as a de facto checking account are unwittingly running outside the deposit guarantee.

Most public assistance programs default to electronic delivery: SNAP runs over EBT debit cards; TANF and unemployment are state-administered and use either EBT-style state debit cards or direct deposit depending on the state; SSI/SSDI federal benefits require electronic payment, with Direct Express (a Treasury-issued prepaid debit card) available as the no-bank-account alternative. Clients without a bank account aren't blocked from receiving benefits, but routing the money in and back out tends to push them onto fee-bearing prepaid products or check cashers. See `general_principles.government-financial-assistance-programs`.

A coach's role is rarely to set any of this up directly. It is to name the option. Clients with recurring late fees, paper-check delays, or overdraft cycles can usually retire the whole problem class by moving paycheck to direct deposit and fixed bills to autopay.

## Savings Strategies

A savings account is a container; strategy is what determines which container holds which dollar for which purpose. Three approaches show up in coaching.

### Emergency Fund First

Liquid cash reserved for income disruption, unplanned medical events, or single emergencies is the foundational savings layer. It funds before discretionary goals and, in most cases, before accelerated debt paydown — because a shock during a debt push almost always returns as new credit-card debt when no reserve exists, undoing the paydown progress. Recommended runway is **3–6 months of expenses** per AFCPE guidance; the liquidity-ratio math lives in `general_principles.personal-financial-ratios.liquidity`, and where the contribution sits in monthly inflows and outflows lives in `general_principles.cash-flow-statement`.

### Separate Accounts per Goal

For medium-horizon goals — a car replacement, a down payment, a wedding, a planned family event — give each goal its own savings sub-account. A balance labeled "Car Fund" behaves differently from the same dollars sitting in an undifferentiated "Savings" balance: the labeling itself produces discipline that willpower alone won't. Most banks and credit unions open additional sub-accounts in a few minutes with no fee.

The behavioral payoff is concrete. A household with one savings balance hits a car breakdown by drawing from the same pool that was supposed to be the down payment. A household with named sub-accounts feels the cross-purpose move and pushes back on it.

### Revolving Savings (Periodic Expense Smoothing)

Some recurring expenses don't land monthly: annual insurance premiums, quarterly self-employment tax payments, holiday spending, back-to-school costs, annual subscription renewals. The fix is a dedicated sub-account funded monthly at the smoothed rate.

Mechanics:

1. Enumerate every known periodic expense with its typical amount and month.
2. Sum to an annual total; divide by 12 for the monthly contribution.
3. Set an automatic transfer of that amount into the dedicated sub-account.
4. Pay the bill from the sub-account when it arrives.

Households that fund revolving consistently stop having "bad months" caused by predictable-but-irregular bills. Integration with the broader monthly plan is covered in `general_principles.spending-plan`.

## Alternatives When Traditional Banking Isn't Accessible

A meaningful slice of clients can't use a standard bank or credit union right now. Reported account mishandling or involuntary closures (a ChexSystems mark stays on the record for five years), immigration-status concerns, or a previous bad experience can each produce the gap. The underbanked tax compounds quickly: check-cashing fees, money-order fees, prepaid-card monthly fees, payday lenders standing in for any short-term credit need. Surfacing entry-level alternatives is usually the highest-leverage move a coach can make for this client subset.

Options to walk through, in roughly increasing willingness on the institution's side to take a complicated client:

- **Community-development credit unions.** Mission-driven institutions chartered to serve underserved populations; often approve clients other institutions decline. Local-specific.
- **Second-chance checking accounts.** Some banks offer these specifically for clients with negative ChexSystems history. Usually fee-bearing but a path back to a standard account after a clean period.
- **Bank On-certified accounts.** A national certification administered by the Cities for Financial Empowerment Fund identifying low-fee, no-overdraft, low-minimum entry-level accounts. Safe default recommendation when an institution-specific account is needed.
- **Payday Alternative Loans (PALs) at federal credit unions.** NCUA's PAL rule (12 CFR 701.21) caps the interest rate at 28% (1,000 basis points above the federal credit union loan ceiling) and imposes a separate cap on application fees. Storefront payday loans typically run 300%+ APR equivalents. Not every federal credit union offers PALs — worth checking locally. The broader lower-cost-borrowing taxonomy lives in `general_principles.short-term-financing`.

General rule: before a client signs up for a check casher, prepaid card, or payday loan, walk through the bank-account alternatives first. The annual cost difference is large enough to matter to almost any household budget.

## Practical Application

Three coaching moments where banking comes up directly:

1. **Intake.** Inventory accounts, institutions, and structure. Direct input for `general_principles.cash-flow-statement` and the net-worth picture. A client carrying three checking accounts across two banks usually has unnecessary complexity, redundant fees, and reconciliation drag.
2. **Deficit triage.** Late fees, overdrafts, ATM surcharges, and maintenance fees are all bank-mechanics problems with bank-mechanics fixes — direct deposit, autopay, in-network ATM use, account-tier upgrade. Concrete early wins here build momentum for the harder conversations about discretionary spending and income.
3. **Goal structure.** Goals without dedicated containers tend to drift; goals with named sub-accounts tend to land. The setup is five minutes per sub-account; the durability is years.

For an AI coach, banking data is unusually rich. Categorizers tag fees as their own line; late payments surface as discrete events; ATM-withdrawal patterns aggregate cleanly. The coach can present a specific number — "you paid $X in ATM fees over the past six months, and an in-network card switch eliminates roughly $Y of that" — without the client having to dig through statements. Named sub-accounts are similarly observable: the coach can flag the gap between "client has a saving-for-house goal" and "client has no account named for it."

## Common Pitfalls

- **Emergency fund kept in checking.** The whole point of the envelope is to take the cash out of one-tap reach. A checking-account balance fails that test.
- **Debit card used for large or disputed purchases.** Reg E on debit gives weaker practical protection than FCBA on credit. For online purchases, unfamiliar merchants, or high-dollar transactions, a credit card the client pays off monthly carries less risk. See `general_principles.credit-cards`.
- **P2P app balances assumed to be deposit-insured.** Funds inside Venmo, Cash App, Zelle, or similar apps are generally outside FDIC coverage unless the app sweeps to an insured partner. Clients using these as informal checking accounts are carrying unguaranteed-balance risk they don't realize.
- **CD broken in the first months of the term.** The early-withdrawal penalty can exceed the interest the CD has accrued, producing a net loss. CDs are not emergency-fund vehicles.
- **All savings in one labeled "Savings" balance.** Without separate sub-accounts, the car fund, the vacation fund, and the emergency fund are the same dollars defending themselves against every demand simultaneously. The first demand to arrive wins.
- **Bank fees treated as fixed life cost.** ATM surcharges, overdraft fees, monthly maintenance fees, and paper-statement fees are nearly all avoidable through account choice or usage change. A household paying $40/month in routine fees is losing $480/year for nothing of value.

## Sources

- AFCPE: AFCPE Money Management Essentials, Module 4 topic1589 — institution definitions (bank, credit union), FDIC/NCUA $250,000 deposit insurance, account-type overview (checking, savings, money market deposit account, certificate of deposit), ATM and debit card mechanics, electronic funds transfers and electronic bill pay, savings strategies (emergency fund first, separate accounts per goal, revolving savings), alternatives for unbanked / underbanked clients
- Federal law: Federal Deposit Insurance Act (12 U.S.C. § 1811 et seq.) — FDIC deposit-insurance regime; $250,000 standard maximum deposit insurance amount per depositor, per insured institution, per ownership category
- Federal law: Federal Credit Union Act insurance provisions (12 U.S.C. § 1751 et seq.) — NCUA share-insurance regime via the National Credit Union Share Insurance Fund (NCUSIF); parallel coverage to FDIC
- Federal law: Regulation E — CFPB implementing regulation for the Electronic Fund Transfer Act; consumer rights and liability limits on debit, ACH, P2P, and ATM transactions; error-resolution procedures
- Federal law: Regulation D — Federal Reserve reserve-requirements regulation; historically imposed a six-per-month withdrawal limit on savings deposits and MMAs (federal limit removed April 2020; many institutions retain it as policy)
- Federal law: NCUA Payday Alternative Loan rule (12 CFR 701.21) — federal credit unions may offer small-dollar short-term loans at an interest rate up to 28% (1,000 basis points above the Federal Credit Union loan ceiling) plus a separate cap on application fees
- External: Bank On account standards (Cities for Financial Empowerment Fund) — national certification identifying low-fee, no-overdraft entry-level accounts
