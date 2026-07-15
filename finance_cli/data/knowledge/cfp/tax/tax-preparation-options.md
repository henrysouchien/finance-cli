---
topic_id: tax.tax-preparation-options
cfp_domains: [tax]
cfp_steps: [understand, analyze, implement]
depth: foundational
scope: framing_only
specialist_resources: [cpa, attorney]
referrals:
  - referrals.vita-tce
  - referrals.irs-free-file
  - referrals.miltax
  - referrals.low-income-taxpayer-clinics
  - referrals.211
refresh_cadence: annual_regulatory
jurisdiction: us_federal
legal_basis:
  - us_federal:vita_tce
  - us_federal:irs_free_file
  - us_federal:litc
  - us_federal:ptin
related_interventions:
  - T-1
  - T-5
related_topics:
  - tax.tax-basics
sources:
  - "AFCPE: AFCPE Money Management Essentials, Module 4 topic1590 — VITA, TCE, United Way tax-preparation routing, Refund Anticipation Loan warning"
  - "IRS: IRS free tax return preparation programs at irs.gov/individuals/irs-free-tax-return-preparation-programs — VITA/TCE locator surface and current program framing"
  - "IRS: IRS Free File at irs.gov/filing/irs-free-file-do-your-taxes-for-free and apps.irs.gov/app/freeFile — official Free File access point"
  - "IRS: Choosing a tax professional at irs.gov/tax-professionals/choosing-a-tax-professional — PTIN, credential, and ghost-preparer warnings"
  - "IRS: Low Income Taxpayer Clinic map at irs.gov/advocate/low-income-taxpayer-clinics/low-income-taxpayer-clinic-map — LITC referral surface"
  - "Military OneSource: MilTax military tax services at militaryonesource.mil/financial-legal/taxes/miltax-military-tax-services/ — military-specific free tax resource"
---

# Tax Preparation Options

Tax preparation is a referral surface for CashNerd, not a return-preparation surface. The coach helps the user choose the right path, gather documents, avoid predatory tax-season products, and route to the correct preparer or public resource.

This topic is **`scope: framing_only`**. It supports route selection and appointment preparation. It does not authorize the coach to prepare a return, determine program eligibility, or advise on a disputed tax position.

## Route Selection

Common paths:

- **VITA/TCE.** IRS-sponsored free basic return preparation by IRS-certified volunteers for qualifying taxpayers. Use for simple working-family returns, older taxpayers, disability or limited-English-access needs, and low-to-moderate income situations. Verify current-year limits and site scope before routing.
- **IRS Free File.** Official IRS access point for partner software offers. Use when the user is comfortable self-filing and appears to fit current-year Free File rules. Route through IRS.gov, not through a commercial provider's general "free" landing page.
- **MilTax.** Military OneSource tax services for eligible military households. Use when the user is active-duty, a qualifying dependent or spouse, or otherwise within the current MilTax eligibility window.
- **Commercial software.** Reasonable for moderate complexity when the user is comfortable entering their own information and can tolerate upsell checks.
- **Retail preparer.** Useful when the user wants in-person help for a straightforward return, with explicit care around fees and refund products.
- **Credentialed preparer: CPA, enrolled agent, or tax attorney.** Route here for self-employment with meaningful complexity, rentals, multi-state work, partnership or S-corp K-1s, prior-year issues, IRS notices, audits, back taxes, or legal uncertainty.
- **Low Income Taxpayer Clinic.** Route here for qualifying users with an IRS dispute, audit, collection issue, or language-barrier tax-rights issue. LITCs are not ordinary return-preparation services.

## Document Readiness

A tax-readiness plan should gather:

- Identity and household basics: Social Security or ITIN documents for filer, spouse, and dependents; filing-status assumptions; prior-year return.
- Income forms: W-2, 1099-NEC, 1099-K, 1099-INT, 1099-DIV, 1099-B, unemployment, pension, Social Security, and marketplace health forms when applicable.
- Deduction and credit support: childcare provider information, education forms, retirement contribution records, charitable receipts, mortgage interest and property tax records, medical records when itemization may be plausible.
- Business records: income summary, Schedule C expense categories, mileage log, home-office facts, contractor payments, and tax configuration used by CashNerd business tools.
- Direct deposit details for refund routing.
- IRS or state notices, prior-year unfiled-return details, or payment-plan information if present.

## Products and Practices to Avoid

Surface these patterns explicitly:

- **Refund Anticipation Loan.** A short-term loan against an expected refund. The fee often looks small but annualizes poorly.
- **Refund Anticipation Check or refund transfer.** Lets a user pay preparation fees from the refund, often with an additional transfer fee.
- **Ghost preparer.** A paid preparer who refuses to sign the return or provide a PTIN. Paid preparers are required to sign returns they prepare.
- **Search-result "free" traps.** Commercial free tiers can upsell. Official IRS Free File should start at IRS.gov.
- **IRS phishing.** Threatening calls, texts, or emails demanding immediate payment or credentials should be treated as scams.

## CashNerd Product Fit

This topic pairs with the existing business/tax tool surface:

- `biz_tax_setup` configures Schedule C assumptions.
- `biz_tax`, `biz_tax_detail`, and `biz_tax_package` summarize business tax records.
- `biz_estimated_tax` models quarterly estimates.
- `biz_mileage_summary` and `biz_mileage_list` expose mileage evidence.
- `biz_1099_report`, `biz_contractor_list`, and `contractor_january_prep_flags_list` support contractor readiness.
- `setup_home_office_tracking` captures simplified home-office tracking assumptions when the user confirms a dedicated workspace.

## Boundaries

The coach must not:

- File the return or fill forms as the preparer.
- Declare that a specific tax position is correct.
- Determine whether VITA/TCE, Free File, MilTax, LITC, or a credit applies to the user without official confirmation.
- Suggest a refund product as a primary solution to cash-flow stress.
- Treat out-of-scope VITA cases as "probably fine"; route complex cases to a credentialed preparer.

## Sources

- AFCPE: AFCPE Money Management Essentials, Module 4 topic1590 — VITA, TCE, United Way tax-preparation routing, Refund Anticipation Loan warning
- IRS: IRS free tax return preparation programs at irs.gov/individuals/irs-free-tax-return-preparation-programs
- IRS: IRS Free File at irs.gov/filing/irs-free-file-do-your-taxes-for-free and apps.irs.gov/app/freeFile
- IRS: Choosing a tax professional at irs.gov/tax-professionals/choosing-a-tax-professional
- IRS: Low Income Taxpayer Clinic map at irs.gov/advocate/low-income-taxpayer-clinics/low-income-taxpayer-clinic-map
- Military OneSource: MilTax military tax services at militaryonesource.mil/financial-legal/taxes/miltax-military-tax-services/

## Effective-Date Notice

Program thresholds, site availability, Free File offers, MilTax eligibility windows, and LITC clinic listings change by filing season. Verify the official source during each filing-season workflow.
