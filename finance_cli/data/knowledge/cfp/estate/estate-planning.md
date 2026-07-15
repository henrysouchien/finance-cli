---
topic_id: estate.estate-planning
cfp_domains: [estate]
cfp_steps: [understand, analyze, develop, implement, monitor]
depth: foundational
scope: framing_only
specialist_resources: [attorney, cpa]
refresh_cadence: event_driven_regulatory
jurisdiction: us_state:*
sources:
  - "AFCPE: AFCPE Money Management Essentials, Module 9 topic1607 - estate-planning scope, will/probate/trust/beneficiary/gift vocabulary, standard will elements, life-event review triggers, and professional handoff"
  - "Industry convention: beneficiary-designation audit and estate-document review practices used in financial counseling and estate-attorney preparation"
related_topics:
  - general_principles.financial-planning-snapshot
  - estate.end-of-life-planning
---

# Estate Planning

Estate planning is the document and decision system for asset transfer after death. It answers who receives assets, who handles estate administration, who may care for minor children, and which assets bypass the probate estate through beneficiary designations or ownership structure.

For CashNerd, this topic is a readiness and referral frame. The coach may help a user identify whether documents exist, whether beneficiary designations need review, which questions to bring to an attorney, and which life events should trigger a refresh. The coach does not draft, interpret, validate, summarize, or store legal-document contents.

## Key Concepts

**Will.** A will is the foundational estate document for many users. It can name an executor, direct probate-estate distributions, and nominate a guardian for minor children. State execution requirements vary, so document validity and wording route to an estate attorney.

**Probate.** Probate is the court-supervised process for validating a will, settling debts, retitling assets, and distributing the probate estate. The process, cost, timing, privacy, and small-estate alternatives vary by state.

**Beneficiary designations.** Retirement accounts, life insurance, annuities, payable-on-death accounts, transfer-on-death accounts, and some jointly owned assets can pass outside the will. Those designations can override a will's general distribution language. A beneficiary review is often a high-value checklist task because old designations can survive major life changes.

**Executor, guardian, trustee, and beneficiary are different roles.** An executor administers the estate. A guardian cares for minor children if legally appointed. A trustee manages trust assets. A beneficiary receives assets. The coach can explain the vocabulary and help the user list questions, but choosing or legally naming people for these roles is attorney-guided decision work.

**Trusts are legal structures, not generic accounts.** Trusts may help with probate avoidance, privacy, staged distributions, disabled beneficiaries, real estate, business interests, or other complex facts. Whether a trust is appropriate and what kind to use is legal advice and routes to an estate attorney.

**Gifts and estate tax planning are specialist territory.** Lifetime gifts, estate-tax exposure, gift-tax reporting, basis consequences, Medicaid planning, and charitable or multi-generation strategies require attorney and often CPA review. The coach may flag the topic and prepare questions, not execute a strategy.

## Document Readiness Use

Use this topic in `coach_estate_document_readiness` to structure a checklist around metadata only:

- whether the user has a will;
- when it was last reviewed;
- whether the user has moved states since signing;
- whether a major life event has occurred since signing;
- whether executor, guardian, trustee, or beneficiary names may need review;
- whether beneficiary designations exist and have been checked recently;
- whether original documents are stored somewhere accessible to the right people;
- whether an attorney appointment is needed.

Allowed status values should stay administrative: `present`, `missing`, `unknown`, `stale`, `needs_attorney_review`, or `not_applicable`. Do not ask the user to paste will clauses, trust language, signatures, account credentials, or attorney communications.

## Life-Event Review Triggers

Prompt for review when the user reports:

- marriage, divorce, separation, or remarriage;
- birth, adoption, or guardianship responsibility for a minor child;
- death, incapacity, estrangement, or relocation of a named executor, guardian, trustee, proxy, or beneficiary;
- move to a different state;
- major home purchase or sale;
- business creation, sale, or wind-down;
- major inheritance, asset change, or debt change;
- diagnosis, caregiving role, or other health event;
- a long time since last review, especially when the user cannot remember the date.

The output should be "review this with an estate attorney" when legal effect, state law, document wording, or beneficiary suitability matters.

## Hand-Off Discipline

The coach may:

- explain document categories and vocabulary;
- help the user inventory what exists;
- help the user identify missing facts and life events;
- generate attorney-meeting questions;
- encourage beneficiary-designation review across user-identified accounts;
- prepare a checklist the user controls.

The coach must route out for:

- drafting, editing, reviewing, or interpreting a will, codicil, trust, deed, beneficiary form, probate filing, or court document;
- deciding who should be named as beneficiary, guardian, executor, trustee, or agent;
- choosing a trust structure;
- determining whether an existing document is valid in a state;
- probate strategy, estate tax, gift tax, generation-skipping transfer tax, Medicaid planning, asset protection, or business-succession legal design;
- advice about whether to disinherit someone, name a minor directly, or route assets through a trust.

## Common Pitfalls

- Assuming estate planning is only for older or wealthy people.
- Treating a will as if it controls accounts with beneficiary designations.
- Letting old beneficiary forms survive divorce, death, remarriage, or estrangement.
- Naming minor children directly on accounts without attorney review.
- Moving states and never reviewing documents.
- Keeping original documents somewhere survivors or agents cannot access.
- Treating a trust as automatically better than a will.
- Treating estate planning as complete forever instead of a periodic review discipline.

## Sources

- AFCPE: AFCPE Money Management Essentials, Module 9 topic1607 - estate-planning scope, will/probate/trust/beneficiary/gift vocabulary, standard will elements, life-event review triggers, and professional handoff
- Industry convention: beneficiary-designation audit and estate-document review practices used in financial counseling and estate-attorney preparation

## Effective-Date Notice

This topic is evergreen readiness framing with state-law sensitivity. State requirements for wills, probate, trusts, guardianship nomination, beneficiary defaults, and transfer-on-death mechanisms vary and can change through legislation or court decisions. The national checklist should remain metadata-only; jurisdiction-specific document validity and legal effect require official current sources and attorney review.
