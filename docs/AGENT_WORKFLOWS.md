# Agent Workflows

Documented workflows for how an AI agent uses the finance CLI to help users manage their finances. Each workflow describes the steps, decisions, and CLI commands involved.

## Table of Contents

1. [Financial Gap Analysis & Action Planning](#financial-gap-analysis--action-planning) — Full financial picture, gap identification, and phased action plan
2. [Monthly Financial Review](#monthly-financial-review) — Monthly check-in workflow
3. [Debt Payoff Planning](#debt-payoff-planning) — Analyze and optimize debt repayment
4. [Goal Setting & Tracking](#goal-setting--tracking) — Set and monitor financial goals
5. [Business Accounting & Tax Compliance](#business-accounting--tax-compliance) — P&L, Schedule C, estimated taxes, 1099s
6. [Category Taxonomy Design](#category-taxonomy-design) — Build personalized category hierarchy
7. [Subscription Audit](#subscription-audit) — Deep-dive subscription verification, cleanup, and cost analysis
8. [Expense Budget Setting](#expense-budget-setting) — Set and tighten discretionary budgets tied to action plan targets
9. [Budget Monitoring & Alerts](#budget-monitoring--alerts) — Proactive run-rate monitoring to enforce budget discipline
10. [Category Data Quality Cleanup](#category-data-quality-cleanup) — Fix misclassifications
11. [Post-Import QA](#post-import-qa) — Verify newly imported transactions

---

## Financial Gap Analysis & Action Planning

**Goal:** Build a complete picture of income vs expenses, identify the monthly gap, understand what's driving it, and create a phased action plan to close it. This is the "first session" workflow — the diagnostic that informs all subsequent financial decisions.

**When to run:** First engagement with a new user, or when the user wants a full financial reset / "where do I stand?" analysis. Also useful after major life changes (job loss, new income stream, big expense shift).

**Prerequisites:**
- At least 3 months of transaction history (more is better for averages)
- Plaid accounts linked and syncing
- Debt/liability data available (`plaid liabilities-sync`)
- Categories reasonably clean (run [Category Data Quality Cleanup](#category-data-quality-cleanup) first if needed)

**Output:** Two documents in `docs/planning/`:
- `FINANCIAL_GAP_ANALYSIS.md` — Baseline diagnostic (snapshot of current state)
- `FINANCIAL_ACTION_PLAN.md` — Living action plan with phases, checklists, and monthly scorecard

### Phase 1: Gather the Numbers

Pull data from multiple angles to build the full picture. Run these in parallel:

#### 1a. Income Sources

```bash
# Business revenue
python3 -m finance_cli biz forecast --months 6 --format cli

# Search for specific income types
python3 -m finance_cli txn list --category "Income: Business" --from 2025-09-01 --limit 50
python3 -m finance_cli txn list --category "Income: Salary" --from 2025-09-01 --limit 20
python3 -m finance_cli txn search --query "YOUTUBE PARTNER"
python3 -m finance_cli txn search --query "GUMROAD"
python3 -m finance_cli txn search --query "DIVIDEND"
```

**Build an income table:** List each income stream with its average monthly amount. Include:
- Regular employment (salary/W-2)
- Business revenue (Stripe, platform payouts)
- Side income (YouTube, freelance, royalties)
- Passive income (dividends, interest)

#### 1b. Expense Run Rate

```bash
python3 -m finance_cli spending trends --months 3 --view all --format cli
python3 -m finance_cli spending trends --months 3 --view personal --format cli
python3 -m finance_cli spending trends --months 3 --view business --format cli
```

**Key numbers to extract:**
- Total monthly expenses (3-month average, excluding payments/transfers)
- Personal vs business split
- Top spending categories ranked by monthly average
- Trend direction per category (growing, stable, shrinking)

#### 1c. Debt Picture

```bash
python3 -m finance_cli debt dashboard --sort apr --format cli
python3 -m finance_cli debt interest --months 12 --format cli
```

**Key numbers:**
- Total debt balance
- Monthly interest cost
- Monthly minimum payments
- Number of active cards/loans
- Highest-APR accounts (avalanche targets)
- Smallest-balance accounts (snowball / quick wins)

#### 1d. Subscription Burn

```bash
python3 -m finance_cli subs total --format cli
python3 -m finance_cli subs list --format cli
```

**Key numbers:**
- Total monthly subscription cost
- Business vs personal split
- Largest subscriptions

#### 1e. Net Worth & Liquidity

```bash
python3 -m finance_cli summary --format cli
python3 -m finance_cli balance net-worth --include-investments --format cli
python3 -m finance_cli liquidity --format cli
```

### Phase 2: Analyze the Gap

With all numbers gathered, calculate:

1. **Monthly gap** = Total Income - Total Expenses
   - If negative: this is how much is going onto credit cards each month
   - If positive: this is available for debt payoff / savings

2. **Interest coverage** = Monthly Minimums vs Monthly Interest
   - If minimums < interest: **debt grows even with perfect payments** — this is a critical finding
   - The minimum "extra" payment needed = interest - minimums (just to stop the bleeding)

3. **Cash flow waterfall:**
   ```
   Income ($X)
   → Minimum debt payments ($Y)
   → Remaining for expenses ($X - $Y)
   → Actual expenses ($Z)
   → Monthly deficit ($Z - ($X - $Y))
   ```

4. **Composition analysis** — what's driving the gap?
   - Is it an income problem (low earnings)?
   - Is it an expense problem (high spend)?
   - Is it a debt problem (interest eating cash flow)?
   - Usually it's a combination — quantify each lever

### Phase 3: Present to User

Walk through the analysis conversationally. Key framing:

1. **Start with the gap** — "You're earning $X/mo and spending $Y/mo. The $Z/mo difference is going onto credit cards."

2. **Show the debt trap** — If minimums don't cover interest, explain clearly: "Your minimum payments ($X) don't cover monthly interest ($Y). Even with perfect payments, the debt grows by $Z/mo."

3. **Break down where money goes** — Show the top expense categories. Users often don't realize how much goes to dining, subscriptions, etc.

4. **Identify the levers** — Frame as three paths:
   - **Cut expenses** — realistic range, what categories have room
   - **Increase income** — which streams can grow, fastest path
   - **Reduce debt cost** — balance transfers, APR negotiation, avalanche strategy

5. **Ask the user which levers resonate** — don't prescribe, collaborate

### Phase 4: Create the Gap Analysis Document

Write `docs/planning/FINANCIAL_GAP_ANALYSIS.md` with sections:

1. Key Metrics Snapshot (table format)
2. Income Sources (with monthly amounts)
3. Expense Breakdown (personal + business, top categories)
4. Debt Overview (sorted by APR, with interest/minimums)
5. The Gap (cash flow waterfall)
6. Levers to Close the Gap (expense, income, debt cost)
7. Data Gaps & Known Issues (reference BUG_BACKLOG.md)
8. Next Steps

**Important:** Use real numbers from the data pulls, not estimates. This document is the baseline everything else references.

### Phase 5: Create the Action Plan

Write `docs/planning/FINANCIAL_ACTION_PLAN.md` with phased approach:

**Phase 1: Stop the Bleeding (Week 1-2)**
- Subscription audit — target specific dollar savings (link to [Subscription Audit](#subscription-audit) workflow)
- Clear micro-balances — pay off cards under $X-X to simplify
- Set expense budgets for top discretionary categories

**Phase 2: Increase Income (Weeks 2-8)**
- Income targets by stream with month 3 / month 6 goals
- Fastest path to new income (consulting, freelance, scaling existing streams)

**Phase 3: Attack Debt (Month 2+)**
- Debt payoff simulations at different extra payment levels
- Avalanche order with specific card payoff sequence
- Balance transfer research checklist
- APR negotiation checklist

**Phase 4: Structural Changes (Month 3-6)**
- Rent optimization
- Business expense audit
- Long-term cost structure improvements

**Include a Monthly Scorecard** — blank table with baseline values that the user fills in each month during [Monthly Financial Review](#monthly-financial-review).

**Include a Milestones section** — specific, time-bound targets (e.g., "Month 1: gap reduced to under $X").

### Phase 6: Execute Phase 1

Immediately start the first action items while the analysis is fresh:

1. **Run [Subscription Audit](#subscription-audit)** — detailed walkthrough of every subscription
2. **Set expense budgets:**
   ```bash
   python3 -m finance_cli budget set --category Dining --amount 400
   python3 -m finance_cli budget set --category Shopping --amount 150
   python3 -m finance_cli budget set --category Entertainment --amount 100
   ```
3. **Identify micro-balance cards** from the debt dashboard and pay them off
4. **Set financial goals:**
   ```bash
   python3 -m finance_cli goal set --name "Close Monthly Gap" --target 0 --metric monthly_gap --direction down
   ```

### Agent Decisions — Key Patterns

- **Income vs expense emphasis**: If income is very low relative to expenses, lead with income growth. Expense cuts alone rarely close a large gap — you can't cut your way to prosperity if rent is 38% of expenses.
- **Debt trap detection**: Always check if minimums cover interest. If not, flag this prominently — it changes the entire framing from "pay off debt" to "stop the bleeding first."
- **Quick wins vs structural changes**: Present both. Quick wins (cancel stale subs, pay micro-balances) build momentum. Structural changes (income growth, rent reduction) close the gap.
- **Don't overwhelm**: Present data in digestible pieces. Walk through one section at a time. Let the user react and ask questions before moving on.
- **Document everything**: The gap analysis and action plan documents become the reference for all future sessions. Update them monthly.

### Lessons from Real Sessions

From the March 2026 gap analysis:
- **The debt trap was the critical finding**: $X/mo interest > $X/mo minimums = debt grows $X/mo even at perfect payments. This reframed the entire conversation from "how to pay off debt" to "how to stop losing $X/mo before anything else."
- **Subscription audit found different savings than expected**: Stale subs were a small win ($X/mo cancelled), but the real insight was that $X/mo goes to AI dev tooling (Claude + OpenAI) — a legitimate business expense, not waste.
- **Income was identified as the primary lever**: With $X/mo rent (non-negotiable short-term), expense cuts max out around $X/mo. Closing a $X gap requires $X+/mo in new income.
- **Data quality issues surfaced during analysis**: Misclassified plan fees, wrong subscription amounts, and missing subscriptions all came out during the audit — fixing these improved the accuracy of all future reports.

---

## Monthly Financial Review

**Goal:** Give the user a comprehensive monthly check-in on their financial health, catch issues early, and keep data clean.

**When to run:** First week of each month, or when the user asks "how am I doing?"

**Prerequisites:**
- Plaid accounts linked and syncing
- Categories and budgets configured
- At least 2-3 months of transaction history

### Step 1: Sync & Pipeline

Run the monthly pipeline to ensure data is current:
```bash
python3 -m finance_cli monthly run --sync --month YYYY-MM
```
This runs: Plaid sync → dedup → auto-categorize → subscription detect → export.

If `--ai` is needed for remaining uncategorized transactions:
```bash
python3 -m finance_cli cat auto-categorize --ai
```

### Step 2: Financial Health Dashboard

Pull the one-page summary:
```bash
python3 -m finance_cli summary --format cli
```

**Key metrics to review with the user:**
- **Net worth trend** — is it growing or shrinking month over month?
- **Savings rate** — negative means spending exceeds income. Flag if below 0%.
- **Emergency fund** — less than 3 months is a warning; less than 1 month is critical.
- **Debt-to-income** — above 3x warrants immediate attention.
- **Data health** — unreviewed and uncategorized counts should trend toward zero.

### Step 3: Spending Trends

Compare spending by category across recent months:
```bash
python3 -m finance_cli spending trends --months 3 --format cli
```

**Agent decisions:**
- Flag categories with ↑ trend arrows — these are growing faster than their historical average.
- If a category jumped >50% month-over-month, investigate: `txn list --category "Category Name" --from YYYY-MM-01 --to YYYY-MM-30`
- Compare against budgets: `budget status --format cli`

### Step 4: Budget Review

Check budget performance:
```bash
python3 -m finance_cli budget status --format cli
```

**Agent decisions:**
- Categories consistently over budget → suggest budget increase or spending reduction
- Categories consistently under budget → suggest budget decrease (free up budget capacity)
- New high-spend categories without budgets → suggest adding budgets

### Step 5: Fixed Obligations Check

Review recurring costs:
```bash
python3 -m finance_cli liability obligations --format cli
```

**Agent decisions:**
- Compare total obligations to income — if obligations exceed 70% of income, flag as high risk
- Look for new subscriptions via `subs detect` — any unexpected additions?
- Check for subscriptions that could be cancelled via `subs audit`

### Step 6: Forward Look

Project where things are heading:
```bash
python3 -m finance_cli projection --months 6 --format cli
python3 -m finance_cli goal status --format cli
```

**Agent decisions:**
- If net worth projection is declining, summarize the key drivers (high expenses vs low income vs debt growth)
- If goals are "not on track", suggest concrete actions (increase savings, reduce spending in specific categories, apply lump sum to debt)
- If debt payoff timeline is long, run `debt simulate --extra N --strategy compare` with achievable extra payment amounts

### Step 7: Create Monthly Plan

Lock in targets for the coming month:
```bash
python3 -m finance_cli plan create --month YYYY-MM
python3 -m finance_cli plan show --month YYYY-MM --format cli
```

### Presentation to User

Summarize findings in this order:
1. **Headlines** — net worth change, savings rate, emergency fund status
2. **Wins** — categories under budget, debt reduction progress, goal milestones
3. **Concerns** — overspending categories, rising trends, data quality issues
4. **Recommendations** — 2-3 specific, actionable items for the next month

---

## Debt Payoff Planning

**Goal:** Help the user understand their debt situation and develop an optimal payoff strategy.

**When to run:** When the user asks about debt, wants to pay off credit cards, or has a windfall (tax refund, bonus) to allocate.

**Prerequisites:**
- Liability data from Plaid (`plaid liabilities-sync`)
- Credit card balances current (`plaid balance-refresh`)

### Step 1: Current Debt Picture

```bash
python3 -m finance_cli debt dashboard --format cli
```

**Review with user:**
- Total debt balance and weighted average APR
- Which cards have highest APR (avalanche targets)
- Which cards have lowest balance (snowball targets)
- Cards with unknown APR — these need manual lookup
- Credit utilization per card (>30% impacts credit score)

### Step 2: Minimum Payment Baseline

```bash
python3 -m finance_cli debt interest --months 24 --format cli
```

Shows what happens with minimum payments only — total interest paid, balance trajectory. This is the "do nothing extra" scenario.

### Step 3: Find Extra Payment Capacity

```bash
python3 -m finance_cli debt impact --cut-pct 50 --months 3 --format cli
```

Shows which discretionary categories could be cut and the debt payoff impact. Helps the user find realistic extra payment amounts.

**Agent decisions:**
- Identify the user's actual discretionary spending (the tool classifies automatically using `essential_categories` from rules.yaml)
- Suggest a realistic extra payment amount (not the maximum — the user needs to sustain it)
- If no discretionary spending to cut, suggest income-side strategies

### Step 4: Compare Strategies

```bash
python3 -m finance_cli debt simulate --extra 500 --strategy compare --format cli
```

Shows avalanche vs snowball vs minimum-only:
- **Avalanche** — mathematically optimal, saves most interest
- **Snowball** — pays off smallest balances first, psychological wins
- For most users, avalanche is better unless they need motivational wins

### Step 5: Model Windfalls (if applicable)

If the user has a lump sum (tax refund, bonus, gift):
```bash
python3 -m finance_cli debt simulate --extra 500 --lump-sum 5000 --strategy compare --format cli
```

Shows the impact of a one-time payment on top of regular extra payments.

**Agent decisions:**
- Compare "lump sum to debt" vs "lump sum to emergency fund" if emergency fund is below 3 months
- If multiple windfall amounts are possible, run scenarios at different levels

### Step 6: Set Goals

Once a strategy is chosen:
```bash
python3 -m finance_cli goal set --name "Debt Free" --target 0 --metric total_debt --direction down
```

Track progress monthly via `goal status`.

### Presentation to User

1. **Current state** — total debt, monthly interest cost, time to payoff at minimums
2. **Opportunity** — how much extra payment is realistic from their budget
3. **Strategy comparison** — avalanche vs snowball with months and interest saved
4. **Recommendation** — specific strategy, monthly amount, and projected payoff date
5. **Next check-in** — set expectation for monthly review

---

## Goal Setting & Tracking

**Goal:** Help the user define and track financial milestones.

**When to run:** During initial setup, at life changes, or when the user expresses financial aspirations.

### Supported Goals

| Goal Type | Metric | Direction | Example |
|-----------|--------|-----------|---------|
| Emergency fund | `liquid_cash` | up | $X in liquid savings |
| Debt freedom | `total_debt` | down | $X total debt |
| Net worth milestone | `net_worth` | up | $X net worth |
| Investment target | `investments` | up | $X in investments |
| Savings rate | `savings_rate` | up | 20% savings rate |

### Step 1: Assess Current Position

```bash
python3 -m finance_cli summary --format cli
```

Review current values for each metric to establish baselines.

### Step 2: Set Goals

Work with the user to define realistic, ordered goals:

```bash
# Priority 1: Emergency fund (3-6 months expenses)
python3 -m finance_cli goal set --name "Emergency Fund" --target 25000 --metric liquid_cash

# Priority 2: Debt elimination
python3 -m finance_cli goal set --name "Debt Free" --target 0 --metric total_debt --direction down

# Priority 3: Net worth milestone
python3 -m finance_cli goal set --name "Net Worth 100K" --target 100000 --metric net_worth
```

**Agent decisions:**
- For emergency fund targets: calculate from actual monthly expenses (`summary` shows this)
- For debt goals: always use direction=down with target=0
- For savings rate: 20% is a common benchmark; adjust based on user's debt situation
- Suggest prioritization: emergency fund → high-interest debt → investments → other goals

### Step 3: Monitor Progress

```bash
python3 -m finance_cli goal status --format cli
```

Shows progress bars and time-to-target estimates for each goal.

**Agent decisions:**
- If a goal shows "Not on track" — the monthly trend is flat or adverse. Investigate why and suggest corrective actions.
- If progress is ahead of schedule — celebrate the win and discuss whether to accelerate or maintain pace.
- If a goal is completed (100%) — congratulate and suggest the next goal in priority order.

### Step 4: Course Corrections

When goals are off track:
1. Run `spending trends` to find where money is going
2. Run `budget status` to see where overspending occurs
3. Run `debt impact` to quantify the effect of spending changes
4. Update budgets or suggest specific category cuts

### Integration with Monthly Review

Goal status should be part of every [Monthly Financial Review](#monthly-financial-review) (Step 6). Track progress over time and adjust targets as circumstances change.

---

## Business Accounting & Tax Compliance

**Goal:** Help the user manage their business finances — track income/expenses, generate tax reports, file estimated taxes, track contractors and mileage deductions.

**When to run:** Quarterly for estimated tax payments, annually for tax prep, monthly for P&L review. Also when setting up a new business account or onboarding Stripe.

**Prerequisites:**
- At least one account flagged as business (`account set-business <id> --business --backfill`)
- Business transactions categorized with `use_type = 'Business'`
- Split rules configured for shared expenses (rent, utilities, internet) in `rules.yaml`
- P&L section mappings seeded (happens automatically via `setup init`)

### Initial Setup (One-Time)

#### 1. Flag Business Accounts

Identify which accounts are business-use and flag them:
```bash
python3 -m finance_cli account list --format cli
python3 -m finance_cli account set-business <account_id> --business --backfill
```
The `--backfill` flag retroactively sets `use_type = 'Business'` on all transactions for that account.

#### 2. Configure Split Rules

For expenses shared between business and personal (home office rent, internet, electric), ensure `rules.yaml` has split rules:
```yaml
split_rules:
  - match:
      category: Rent
    business_pct: 25
    business_category: Rent
    personal_category: Rent
    note: "25% business use of home office"

  - match:
      keywords: [VERIZON]
    business_pct: 80
    business_category: Utilities
    personal_category: Utilities
    note: "80% business use of internet"
```

After editing rules, re-run categorization to generate split child transactions:
```bash
python3 -m finance_cli cat auto-categorize
```

#### 3. Connect Stripe (if applicable)

```bash
python3 -m finance_cli stripe link
python3 -m finance_cli stripe sync
```

This syncs gross charges, fees, and refunds as business transactions. Revenue appears in `Income: Business` with `source = 'stripe'`.

#### 4. Configure Tax Settings

Set up home office and tax parameters for the year:
```bash
python3 -m finance_cli biz tax-setup --year 2026 --method simplified --sqft 200
```

Options:
- `--method simplified` — IRS simplified method ($X/sqft, max 300 sqft = $X deduction)
- `--method actual` — Actual expense method (requires tracking all home expenses)
- `--sqft` — Square footage of dedicated office space
- `--filing-status single|mfj` — For QBI deduction threshold
- `--salary` — W-2 salary for total income context in estimated tax

### Monthly Business Review

#### Step 1: P&L Statement

```bash
python3 -m finance_cli biz pl --month 2026-03 --format cli
```

Shows income statement broken into sections: Revenue, COGS, Gross Profit, Operating Expenses (by section: Marketing, Technology, Professional, Facilities, People, Other), and Net Income.

**Agent decisions:**
- Compare to prior month: `biz pl --month 2026-02 --format cli`
- If expenses in a section seem high, drill in: `txn list --view business --category "Software & Subscriptions" --from 2026-03-01 --to 2026-03-31`
- Check P&L section mappings: `biz pl` will show "(unmapped)" for categories not assigned to a P&L section

#### Step 2: Cash Flow

```bash
python3 -m finance_cli biz cashflow --month 2026-03 --format cli
```

Shows operating, investing, and financing cash flows from business accounts only.

#### Step 3: Business Budget Check

```bash
python3 -m finance_cli biz budget status --month 2026-03 --format cli
```

Compares actual spend against section-level budgets. Set budgets with:
```bash
python3 -m finance_cli biz budget set --section opex_technology --amount 500 --period monthly --from 2026-01
```

#### Step 4: Revenue Trends

```bash
python3 -m finance_cli biz forecast --months 6 --format cli
python3 -m finance_cli biz seasonal --format cli
```

Forecast shows revenue trend by stream (Stripe, Kartra, Gumroad, Amazon KDP, etc.) with linear regression projections. Seasonal shows month-of-year patterns with confidence levels.

#### Step 5: Runway

```bash
python3 -m finance_cli biz runway --months 6 --format cli
```

Shows burn rate and how many months of operating cash remain at current spend levels.

### Quarterly Estimated Tax

Run before each IRS estimated tax deadline (Apr 15, Jun 15, Sep 15, Jan 15):

```bash
python3 -m finance_cli biz estimated-tax --quarter 2026-Q1 --include-se --format cli
```

**What it calculates:**
- YTD net business income (from Schedule C categories)
- Self-employment tax (Social Security + Medicare on 92.35% of net income)
- Federal income tax estimate (using bracket tables)
- Total estimated tax due for the quarter
- Safe harbor amount (prior year's total tax / 4)

**Agent decisions:**
- Compare estimated tax to safe harbor — if safe harbor is lower, the user can pay that instead
- If income is lumpy (e.g., big Q4), suggest adjusting quarterly amounts rather than equal installments
- Flag if the user hasn't set `--salary` and has W-2 income — total income affects bracket

### Annual Tax Prep

#### Step 1: Schedule C Summary

```bash
python3 -m finance_cli biz tax --year 2025 --format cli
```

Shows the full Schedule C with:
- Line 1: Gross receipts
- Line 2-6: Returns, COGS, gross profit
- Line 7-27: Expenses by Schedule C line item (mapped via `schedule_c_map` table)
- Line 28-31: Net profit/loss
- Home office deduction (simplified or actual method)
- QBI (Qualified Business Income) deduction estimate

**Agent decisions:**
- Verify all business categories map to Schedule C lines — unmapped categories show as "(unmapped)" and won't appear on the return
- Check deduction percentages in `schedule_c_map` — some categories like "Meals" are only 50% deductible
- Flag large or unusual expense categories for user review

#### Step 2: Full Tax Package Export

```bash
python3 -m finance_cli biz tax-package --year 2025 --format cli
```

Exports a comprehensive tax package including:
- Schedule C summary
- All business transactions grouped by category
- Revenue by source
- Home office calculation
- Mileage deduction summary

#### Step 3: 1099 Contractor Report

```bash
python3 -m finance_cli biz 1099-report --year 2025 --format cli
```

Shows per-contractor payment totals. Flags contractors paid $X (1099-NEC threshold).

**Prerequisite:** Contractors must be registered and payments linked:
```bash
python3 -m finance_cli biz contractor add --name "Jane Smith" --tin-last4 1234 --type individual
python3 -m finance_cli biz contractor link --contractor-id <id> --transaction-id <txn_id>
```

#### Step 4: Mileage Deduction

```bash
python3 -m finance_cli biz mileage summary --year 2025 --format cli
```

Shows total business miles and deduction at the IRS standard rate. Trips are logged throughout the year:
```bash
python3 -m finance_cli biz mileage add --date 2026-03-04 --miles 25.5 --purpose "Client meeting" --destination "Midtown"
```

### Key Tables & Mappings

| Table | Purpose |
|-------|---------|
| `pl_section_map` | Maps categories → P&L sections (revenue, cogs, opex_*) |
| `schedule_c_map` | Maps categories → Schedule C line items with deduction % |
| `biz_section_budgets` | Section-level expense budgets |
| `mileage_log` | Business trip records: date, miles, purpose, destination |
| `mileage_rates` | IRS standard mileage rates by year |
| `contractors` | 1099-NEC contractors: name, TIN last 4, entity type |
| `contractor_payments` | Links transactions to contractors |
| `stripe_connections` | Stripe API connections for revenue sync |

### Split Rule Mechanics

When a split rule matches a transaction, the system:
1. Creates two child transactions (business and personal portions)
2. Sets the parent transaction to `is_active = 0`
3. Children inherit the parent's date, description, and account
4. Each child gets the appropriate `use_type`, `category`, and proportional `amount_cents`

This means `biz pl` only sees the business portion of split expenses, and personal reports only see the personal portion.

### Agent Decisions — Common Patterns

- **New vendor shows up in business expenses but isn't mapped** → Add keyword rule to `rules.yaml` with `use_type: Business`, then check if the category maps to a P&L section
- **User asks "is this deductible?"** → Check `schedule_c_map` for the category. If mapped with `deduction_pct < 100`, explain the partial deduction (e.g., meals at 50%)
- **Revenue seems low** → Check `stripe sync` status, verify business income transactions aren't miscategorized, run `biz forecast` to see the trend
- **Estimated tax seems too high** → Check if personal expenses are leaking into business categories (audit `txn list --view business`), verify split rule percentages are correct
- **XXXX threshold approaching** → Proactively flag when a contractor is nearing $X to ensure the user has their TIN on file

---

## Category Taxonomy Design

**Goal:** Help the user build a personalized hierarchical category taxonomy from their existing flat category set.

**When to run:** After the user has imported enough transactions to have meaningful data (100+ transactions across multiple categories). Usually after initial data quality cleanup.

**Prerequisites:**
- Categories table has data (`cat list` returns categories with transaction counts)
- Category data quality is clean (no major misclassification issues)
- `parent_id` and `level` columns exist on the categories table

### Phase 1: Audit Current State

Understand what the user's data looks like before proposing any structure.

1. **Pull category distribution** — query active transaction counts by category to understand where spending volume lives:
   ```sql
   SELECT c.name, c.is_income, COUNT(t.id) AS txn_count,
          SUM(t.amount_cents) AS total_cents
     FROM categories c
     LEFT JOIN transactions t ON t.category_id = c.id AND t.is_active = 1
    GROUP BY c.id ORDER BY txn_count DESC;
   ```

2. **Review provider category signals** — the `source_category` field preserves what Plaid/bank originally called each transaction. These already have a natural hierarchy (e.g., Plaid PFC: `FOOD_AND_DRINK` → `FOOD_AND_DRINK_RESTAURANT`). Analyze which provider groups map to which local categories:
   ```sql
   SELECT c.name AS local_category,
          t.source_category,
          COUNT(*) AS cnt
     FROM transactions t
     JOIN categories c ON c.id = t.category_id
    WHERE t.is_active = 1 AND t.source_category IS NOT NULL
    GROUP BY c.name, t.source_category
    ORDER BY c.name, cnt DESC;
   ```

3. **Identify natural groupings** — look for categories that share a provider primary group. For example, if Coffee, Dining, and Groceries all come from `FOOD_AND_DRINK_*` source categories, they naturally belong under a "Food & Drink" parent.

4. **Check for user-created categories** — `is_system = 0` categories were created by the user or by import processes. These may need to be folded into the hierarchy or kept as custom leaves.

### Phase 2: Research & Draft Hierarchy

Use provider taxonomy as the backbone, but design for the user's actual spending patterns.

1. **Map Plaid PFC primary groups to parent categories.** The `PFC_PRIMARY_MAP` in `categorizer.py` already defines the mapping from Plaid's primary groups to local categories. Use this as the starting skeleton:
   - `FOOD_AND_DRINK` → Food & Drink (children: Coffee, Dining, Groceries)
   - `TRANSPORTATION` → Transportation (single child or split: Rideshare, Gas, Transit, Parking)
   - `RENT_AND_UTILITIES` → Housing (children: Rent, Utilities)
   - etc.

2. **Decide depth** — 2 levels is the sweet spot for personal finance. More than 2 levels adds complexity without proportional insight. Parent categories are for rollup reporting; leaf categories are for transaction-level detail.

3. **Handle edge cases:**
   - **Income categories** — keep separate tree (Income → Salary, Business, Other)
   - **Payments & Transfers** — exclude from spending rollups (flagged via `is_payment`). Note: Plaid marks ALL `TRANSFER_OUT` as payments, including checks. The `payment_exclusions` list in `rules.yaml` suppresses `is_payment` for patterns like "Check " so they surface as uncategorized instead of being hidden.
   - **Personal Expense** — this is a catch-all. The agent should help the user decide: keep as a leaf under a parent, or break it up further based on their data
   - **User-created categories** — ask the user where these fit, or suggest placements based on transaction descriptions

4. **Present the draft to the user** — show a tree view with transaction counts at each node so they can see how their spending rolls up:
   ```
   Food & Drink (817)
   ├── Dining (492)
   ├── Groceries (293)
   └── Coffee (52)

   Transportation (465)
   └── Transportation (465)

   Housing (39)
   ├── Rent (10)
   └── Utilities (29)
   ```

### Phase 3: User Review & Iteration

This is interactive — the agent proposes, the user adjusts.

1. **Walk through each parent group** — explain what's in it, why, and what the rollup means for their spending reports.

2. **Ask about ambiguous categories:**
   - "Personal Expense has 426 transactions — do you want to keep it as-is, or should we break it into subcategories?"
   - "Software & Subscriptions could live under 'Professional' or stay standalone — what makes sense for how you think about this spending?"

3. **Ask about granularity preferences:**
   - "Do you want Transportation broken into Rideshare / Gas / Transit, or is one bucket enough?"
   - "Should we split Entertainment into subcategories (Music, Streaming, Events)?"

4. **Iterate** — update the tree based on feedback, re-show with counts.

### Phase 4: Apply the Hierarchy

Once the user approves the tree:

1. **Create parent categories:**
   ```
   cat add "Food & Drink"
   cat add "Housing"
   ```

2. **Assign parents to existing leaf categories:**
   ```sql
   UPDATE categories SET parent_id = (SELECT id FROM categories WHERE name = 'Food & Drink'), level = 1
    WHERE name IN ('Coffee', 'Dining', 'Groceries');
   ```
   (Or via future `cat set-parent` command if implemented.)

3. **Verify the tree:**
   ```
   cat list  -- should show parent/child relationships
   ```

4. **Update `CANONICAL_CATEGORIES`** in `user_rules.py` if new parent categories are added as system categories.

5. **Test rollup queries** — confirm spending reports aggregate correctly to parent level.

### Phase 4b: Post-Hierarchy Data Quality Audit

The rollup view often reveals misclassifications that weren't obvious in the flat list. After applying the hierarchy, audit each parent group's composition.

1. **Check catch-all categories** — Personal Expense, Shopping, and Other tend to accumulate misclassified transactions. Query the provider signal (`source_category`) for each to find items that belong elsewhere:
   ```sql
   SELECT t.source_category, COUNT(*) AS cnt,
          printf('$%.0f', ABS(SUM(t.amount_cents))/100.0) AS total
     FROM transactions t JOIN categories c ON c.id = t.category_id
    WHERE c.name = 'Personal Expense' AND t.is_active = 1
    GROUP BY t.source_category ORDER BY cnt DESC;
   ```

2. **Look for keyword overrides** — transactions where a keyword rule placed them in a catch-all but the provider knew the right category. Common patterns:
   - "Fees & Adjustments" provider label overridden by broad keywords (e.g., "ANNUAL FEE" in Personal Expense instead of Bank Charges & Fees)
   - Installment plan fees (e.g., "PLAN FEE - CURSOR") matched by vendor keywords instead of fee keywords
   - AI-categorized transactions from before payment detection was added

3. **Fix keyword placement** — move fee/charge keywords to Bank Charges & Fees, remove overbroad keywords from catch-all blocks. Then reset affected transactions and re-run:
   ```
   cat auto-categorize
   ```

4. **Verify parent group sizes make sense** — if a parent group has unexpectedly high or low transaction counts relative to its children, investigate. The rollup should feel intuitively right.

### Phase 5: Enable Hierarchy-Aware Reporting

Built into the CLI — reporting commands automatically roll up to parent groups.

**Commands updated:**

1. **`weekly`** — groups spending by parent category with indented children:
   ```
   Food & Drink: -303.33
     Groceries: -149.17
     Dining: -104.58
     Coffee: -49.58
   Professional: -632.10
     Professional Fees: -362.54
     Software & Subscriptions: -269.56
   Transportation: -6.00
   ```
   Standalone categories (no children) show without indentation. JSON output includes `group_name` on every row for programmatic access.

2. **`daily`** — each transaction includes `group_name` alongside `category_name`, enabling `Group > Category` display (e.g., "Professional > Software & Subscriptions").

3. **`budget suggest`** — cut suggestions roll up to parent groups so recommendations are actionable at the right level (e.g., "cut Food & Drink by $X" instead of three separate line items).

4. **`budget status`** and **`budget forecast`** — group by parent category with rollup totals and indented children, same pattern as `weekly`. Budgets are still set per-leaf, but the display shows parent-level aggregation:
   ```
   Food & Drink: spent=-1309.94 budget=775.00 remaining=-534.94
     Dining: spent=-918.65 budget=300.00 remaining=-618.65
     Groceries: spent=-284.46 budget=400.00 remaining=115.54
     Coffee: spent=-106.83 budget=75.00 remaining=-31.83
   ```
   Setting a budget on a parent category is blocked — `set_budget()` enforces leaf-only budgets.

5. **`export monthly-summary`** — CSV includes `group_name` column for pivot table analysis.

**Implementation pattern:**
All rollup queries use the same JOIN:
```sql
LEFT JOIN categories p ON p.id = c.parent_id
COALESCE(p.name, c.name) AS group_name
```
This means standalone categories (like Transportation with no parent) use their own name as the group.

### Design Principles

- **2 levels max** — parent (rollup) and leaf (transaction-level). Keep it simple.
- **Provider-informed, user-decided** — use Plaid PFC / bank labels as the starting skeleton, but the user has final say on groupings.
- **Leaf categories don't change** — the existing 22 canonical categories remain as leaves. We're adding parents above them, not renaming or merging.
- **Payments excluded** — `is_payment = 1` transactions are excluded from spending rollups regardless of hierarchy.
- **Reversible** — setting `parent_id = NULL` on a category removes it from the tree with no data loss.

### Key Data Sources

| Source | What it tells us | How to query |
|--------|-----------------|--------------|
| `categories` table | Current flat category set | `cat list` |
| `transactions.source_category` | Provider's original label (Plaid PFC detailed, bank label) | SQL group by |
| `PFC_PRIMARY_MAP` in `categorizer.py` | Plaid primary → local category mapping | Code read |
| `category_mappings` table | Source category → canonical category rules | SQL query |
| `rules.yaml` `category_aliases` | Legacy/bank label → canonical name aliases | File read |

---

## Subscription Audit

**Goal:** Produce a clean, accurate subscription tracker — detect recurring charges, verify amounts/categories/use_type, identify stale entries, surface cancellation opportunities, and quantify total subscription burn.

**When to run:** Monthly as part of Phase 1 "Stop the Bleeding", or when the user suspects subscription creep.

**Prerequisites:**
- At least 3 months of transaction history (for pattern detection)
- Categories and keyword rules configured
- `subs detect` has been run at least once

### Step 1: Run Detection & Pull Current List

```bash
python3 -m finance_cli subs detect
python3 -m finance_cli subs list --format cli
```

**What to look for:**
- Total monthly burn — is it in line with expectations?
- Any vendors the user doesn't recognize
- Subscriptions marked as wrong `use_type` (Business vs Personal)

### Step 2: Verify Each Subscription Against Transaction Data

For each subscription in the list, pull the actual transaction history to verify:

```bash
python3 -m finance_cli txn search --query "VENDOR_NAME" --limit 20
```

**Common issues the auto-detector gets wrong:**

1. **Inflated amounts from plan fees** — Amex "Pay It Plan It" creates `PLAN FEE - <vendor>` transactions at $X-X each. The detector sees these as the subscription charge instead of (or in addition to) the real charge, inflating the detected amount and distorting frequency.
   - Fix: Ensure plan fee keyword rule has higher priority/longer match than vendor keywords (see `rules.yaml` "PLAN FEE -" at priority 10)

2. **Variable-amount subscriptions missed** — Usage-based services (Anthropic API, OpenAI usage charges) have different amounts each month. The detector expects consistent amounts and misses them entirely.
   - Fix: Manually add with `subs add` at the average monthly amount

3. **Multiple charges per vendor per month** — Services with multiple seats/plans (e.g., OpenAI with 2 seats + usage) appear as inconsistent patterns rather than a single subscription.
   - Fix: Add each seat/plan as a separate subscription entry

4. **Stale subscriptions not auto-expired** — Cancelled services remain "active" in the tracker indefinitely. No mechanism to flag subs with no charges in 60+ days.
   - Fix: For each subscription, verify there are recent charges. Cancel stale entries with `subs cancel <id>`

5. **Apple.com/Bill bundles multiple services** — Apple bills all subscriptions (iCloud, Apple TV+, apps) under one merchant name at different amounts on different dates. The detector picks up one pattern and misses the others.
   - Fix: Look at all distinct recurring amounts. Cancel the auto-detected entry and re-add as a combined total

### Step 3: Walk Through With User

Go subscription by subscription and ask:
- **Is this still active?** — If no charges in 60+ days, suggest cancelling from tracker
- **Is the amount correct?** — Compare to actual recent charges
- **Is the use_type right?** — Business tools on personal cards may be detected as Personal
- **Is this essential?** — Flag discretionary subs for potential cuts

**Agent decisions per subscription:**
- If the user says "already cancelled" → `subs cancel <id>`
- If amount is wrong → cancel and re-add with correct amount
- If use_type is wrong → cancel and re-add (no update command currently)
- If the user doesn't recognize it → search transactions to identify the vendor

### Step 4: Identify Missing Subscriptions

The auto-detector misses subscriptions that don't match its pattern criteria. Manually search for known subscription vendors:

```bash
# Search for common SaaS/subscription patterns
python3 -m finance_cli txn search --query "ANTHROPIC"
python3 -m finance_cli txn search --query "OPENAI"
python3 -m finance_cli txn search --query "CLAUDE.AI"
python3 -m finance_cli txn search --query "CURSOR"
```

Also check the user's `Software & Subscriptions` category for recurring vendors not in the tracker:
```bash
python3 -m finance_cli txn list --category "Software & Subscriptions" --from 2025-12-01 --limit 50
```

Add missing subscriptions:
```bash
python3 -m finance_cli subs add --vendor "Claude.ai (Max)" --amount 217.75 --frequency monthly --category "Software & Subscriptions" --use-type Business
```

### Step 5: Fix Underlying Categorization Issues

Subscription audit often reveals categorization problems:

1. **Transactions in wrong category** — e.g., NordVPN in "Casinos and Gambling", plan fees in "Software & Subscriptions"
   - Fix: `txn categorize <id> --category "correct category" --remember`
   - For bulk: loop through IDs via CLI (MCP bulk categorize not yet available — see FEATURE-002)

2. **Missing keyword rules** — new vendors not matched by any rule
   - Fix: Add to `rules.yaml` under the appropriate category block
   - Then: `cat auto-categorize` to apply retroactively

3. **Keyword rule priority conflicts** — e.g., "PLAN FEE - CLAUDE.AI" matching "CLAUDE.AI" keyword (9 chars) instead of "PLAN FEE -" (10 chars) because longest-keyword-wins
   - Fix: Ensure the correct keyword is longer, or bump its priority in `rules.yaml`

### Step 6: Produce Final Summary

```bash
python3 -m finance_cli subs list --format cli
python3 -m finance_cli subs total --format cli
```

Present to the user organized by use_type:
- **Business subscriptions** — total, largest items, any that could be downgraded
- **Personal subscriptions** — total, essential vs discretionary
- **Potential savings** — specific subs to cancel/downgrade and the monthly impact

### Step 7: Connect to Action Plan

If the user has a financial action plan or debt payoff strategy:
- Quantify subscription savings vs baseline
- Run `subs audit` to classify essential vs discretionary
- Run `debt impact` to model how subscription cuts accelerate debt payoff

```bash
python3 -m finance_cli subs audit --format cli
python3 -m finance_cli debt impact --cut-pct 50 --format cli
```

### Known Detector Limitations (SUB-001)

The auto-detector has documented limitations (see BUG_BACKLOG.md SUB-001):
1. Misses variable-amount recurring charges (usage-based billing)
2. Confused by multiple charges per vendor per month (multi-seat plans)
3. No staleness detection (cancelled subs stay "active")
4. Plan fees ($X-X Amex installment fees) inflate detected amounts

Until these are fixed, **manual verification against transaction data is essential** for every subscription audit.

### Lessons from Real Audits

From the March 2026 audit (baseline $X/mo → cleaned $X/mo):
- **7 stale/cancelled subs removed** (ClassPass, Super.Fans, ToBeMagnetic, Caveday, Sui Yoga, Moonflash/NordVPN, Cursor) — none had recent charges
- **4 missing subs added** (Claude.ai Max, Claude.ai Pro, OpenAI Seat 1, OpenAI Seat 2) — all missed by detector due to variable amounts or being new
- **Apple.com/Bill corrected** — was tracked as 1 Business sub at $X biweekly; actually 3 Personal recurring charges totaling $X/mo
- **Cursor amount was wrong** — showed $X/mo but real charges were $X/mo (plan fees inflated the amount)
- **18 PLAN FEE transactions recategorized** — were matching vendor keywords instead of the fee keyword rule
- **8 Moonflash/NordVPN transactions recategorized** — were in Shopping/Entertainment/"Casinos and Gambling", moved to Software & Subscriptions with vendor memory

---

## Expense Budget Setting

**Goal:** Set realistic but disciplined spending budgets for discretionary categories, tied to the financial action plan's savings targets. Budgets create accountability for the expense-cutting lever of the gap analysis.

**When to run:** After the [Financial Gap Analysis](#financial-gap-analysis--action-planning) identifies a monthly gap, and after the [Subscription Audit](#subscription-audit) has cleaned up recurring costs. Also useful during monthly reviews when budgets need tightening or loosening.

**Prerequisites:**
- Gap analysis complete with savings targets defined
- Subscription audit complete (so subscription costs are accurate)
- At least 2-3 months of spending data for reliable averages
- Categories are clean (run [Category Data Quality Cleanup](#category-data-quality-cleanup) first if needed — budget tracking is meaningless if transactions are miscategorized)

### Step 1: Pull Spending Averages

Get the 3-month average for each category to understand the baseline:

```bash
python3 -m finance_cli spending trends --months 3 --view personal --format cli
python3 -m finance_cli spending trends --months 3 --view business --format cli
```

**Key output:** Per-category monthly average and trend direction (growing/shrinking). Focus on the top discretionary categories — these have the most room to cut.

### Step 2: Check Existing Budgets

```bash
python3 -m finance_cli budget list --format cli
```

**Watch for:**
- **Duplicate entries** — `budget set` creates new rows, not upserts. Same category can have multiple budget rows (see FEATURE-003 in BUG_BACKLOG.md)
- **Missing use_type visibility** — Personal and Business budgets for the same category look identical in the list (see UX-001 in BUG_BACKLOG.md). Query the DB directly if unclear:
  ```bash
  python3 -c "
  import sqlite3
  conn = sqlite3.connect('finance_cli/data/finance.db')
  conn.row_factory = sqlite3.Row
  for r in conn.execute('''
      SELECT b.id, c.name, b.amount_cents, b.use_type, b.effective_from, b.effective_to
      FROM budgets b JOIN categories c ON c.id = b.category_id
      ORDER BY c.name, b.use_type
  '''):
      print(f'{r[\"id\"][:12]}  {r[\"name\"]:25s}  \${r[\"amount_cents\"]/100:>8.2f}  {r[\"use_type\"]:10s}  to={r[\"effective_to\"]}')
  "
  ```
- **Expired entries** — budgets with `effective_to` in the past. These are dead rows that should be cleaned up

### Step 3: Clean Up Stale Budgets

Remove expired or duplicate budget entries. Since there's no `budget delete` CLI command yet (FEATURE-003), use direct SQL:

```python
conn.execute("DELETE FROM budgets WHERE id = ?", (budget_id,))
```

Only delete entries that are clearly stale (expired `effective_to`) or true duplicates (same category + use_type + period).

### Step 4: Build the Budget Table

Create a comparison table for the user:

| Category | 3-Mo Avg | Current Budget | Proposed Target | Monthly Savings |
|---|---|---|---|---|
| Dining | $X | $X | $X | $X |
| Shopping | $X | $X | $X | $X |
| ... | ... | ... | ... | ... |

**How to set targets:**

1. **Discretionary categories** (Dining, Shopping, Entertainment, Coffee, Travel) — these are the primary levers. Set targets aggressively below the average, but not to zero. The user needs to sustain these budgets month over month.

2. **Essential categories** (Rent, Utilities, Insurance, Groceries) — set at or slightly above actual spend. These aren't cut targets, they're tracking guardrails.

3. **Business categories** — set separately using `--view business`. Business budgets are about cost control, not gap-closing (business expenses should generate ROI).

**Agent decisions:**
- Total savings from discretionary budget cuts should align with the action plan target (e.g., ~$X/mo)
- If the user pushes back on a target, negotiate — a $X Dining budget is better than no budget even if $X was the plan
- Flag categories where the average is already below budget — these don't need tightening

### Step 5: Set the Budgets

For new budgets:
```bash
python3 -m finance_cli budget set --category Dining --amount 400
python3 -m finance_cli budget set --category Shopping --amount 150
python3 -m finance_cli budget set --category Entertainment --amount 100
python3 -m finance_cli budget set --category Coffee --amount 40
python3 -m finance_cli budget set --category Travel --amount 200
```

For updating existing budgets (no update command yet — use direct SQL until FEATURE-003 is implemented):
```python
conn.execute("UPDATE budgets SET amount_cents = ? WHERE id = ?", (new_cents, budget_id))
```

**Important:** `budget set` creates new rows. If budgets already exist for a category, update the existing row directly rather than creating duplicates.

### Step 6: Verify and Present

```bash
python3 -m finance_cli budget status --format cli
```

Shows current month spend vs budget for each category. Present to the user with:
- Which categories are already over budget (if mid-month)
- Which have headroom
- Total monthly savings if all budgets are hit

### Step 7: Connect to Action Plan

Update the action plan document with:
- Budget targets set (mark as complete)
- Expected monthly savings from budget discipline
- How the savings feed into debt payoff (e.g., "$X/mo in budget savings → applied to highest-APR card")

### Monthly Budget Review

Part of every [Monthly Financial Review](#monthly-financial-review):

```bash
python3 -m finance_cli budget status --format cli
python3 -m finance_cli budget forecast --format cli
```

**Agent decisions during review:**
- If a category is consistently over budget for 2+ months → either the budget is unrealistic (raise it) or the user needs more aggressive behavior change (discuss strategies)
- If a category is consistently under budget → consider tightening further or celebrating the win
- If new spending categories emerge that aren't budgeted → add them
- Adjust targets quarterly based on actual performance

### Lessons from Real Sessions

From the March 2026 budget setting:
- **Existing budgets had no delete/update path** — forced direct DB queries to clean up and tighten. This is the most common operational gap (FEATURE-003).
- **Use_type split was invisible** — Professional Fees appeared as a duplicate ($X + $X) but was actually Personal + Business. The agent needs to query the DB directly to see this until UX-001 is fixed.
- **Budget targets came from the action plan** — not arbitrary. Each target was calculated from the gap analysis: "we need $X/mo in discretionary savings, here's how it splits across categories." This linkage is critical — budgets without a reason behind them don't stick.
- **Averages can be misleading in partial months** — March data was only a few days, dragging down 3-month averages. Use Jan + Feb (full months) for the baseline when setting initial budgets.

---

## Budget Monitoring & Alerts

**Goal:** Proactively surface budget overages and at-risk categories so the user gets early warnings before the month ends, rather than discovering overspending after the fact. This is the enforcement mechanism that makes budgets stick.

**When to run:** Continuously throughout the month — integrated into weekly summaries, monthly pipeline, and available on-demand via CLI/MCP. Most valuable mid-month (day 10-20) when projections stabilize.

**Prerequisites:**
- Budgets set for key categories (see [Expense Budget Setting](#expense-budget-setting))
- Transactions flowing regularly (Plaid sync or manual imports)
- At least a few days of data in the current month (day 1-2 projections are noisy)

### How It Works

The `budget alerts` command wraps the existing `budget forecast` run-rate projection with alert classification:

1. Calculates **daily run rate** = total spend / days elapsed
2. Projects **end-of-month spend** = daily run rate × days in month
3. Classifies each budgeted category by severity:
   - **OVER** — actual spend already exceeds budget (immediate attention)
   - **AT RISK** — projected to exceed budget at current pace (course-correct now)
   - **WARNING** — projected to exceed 80% of budget (watch closely)
   - **OK** — on track (no action needed)

### Step 1: Run Budget Alerts

**Standalone check:**
```bash
python3 -m finance_cli budget alerts --view personal --format cli
```

**Via MCP (for agent use):**
```python
budget_alerts(view="personal")
```

**Example output:**
```
Budget Alerts — March 2026 (day 15 of 31, 16 days remaining)

OVER BUDGET:
  Dining [Personal]: $X spent / $X budget (105%) — $X over

AT RISK (projected to exceed):
  Shopping [Personal]: $X spent / $X budget — on pace for $X ($X over)

WARNING (>80% projected):
  Coffee [Personal]: $X spent / $X budget — on pace for $X ($X over)

12 categories on track
```

### Step 2: Interpret the Alerts

**Agent decisions by severity:**

- **OVER**: The budget is already blown. Discuss with the user:
  - Was this a one-time spike or a pattern?
  - Should the budget be raised (unrealistic target)?
  - Or should spending be frozen for the rest of the month?

- **AT RISK**: There's still time to course-correct. Show the user:
  - Current daily run rate vs what they can spend per remaining day
  - `remaining_daily_budget` field tells them exactly: "$X/day remaining for Shopping"
  - Specific transactions driving the overage if a few big purchases skewed it

- **WARNING**: Flag but don't alarm. Mention in passing during check-ins.

- **OK**: Don't mention — no news is good news.

### Step 3: Early-Month Noise

**Important caveat:** In the first 5-7 days of the month, projections are unreliable. A single grocery run on day 2 extrapolated to 31 days looks like a massive overage.

**Agent decisions:**
- Days 1-5: Mention that projections are early and may be noisy. Don't recommend drastic action.
- Days 6-15: Projections stabilize. Flag at-risk categories with moderate confidence.
- Days 16+: Projections are reliable. OVER and AT RISK categories need real attention.

The `low_confidence` flag in the alert data indicates when `days_elapsed < 3`.

### Integration Points

Budget alerts are already integrated into three places:

1. **`weekly` command** — alerts appended to current-week output (CLI and JSON). Skipped for historical `--week` views since projections use `date.today()`.

2. **`monthly run` pipeline** — budget check runs as a health check step, reporting counts: `✓ Budget check: 1 over budget, 2 at risk`

3. **Standalone `budget alerts`** — CLI and MCP tool for on-demand checking.

### Step 4: Connect to Action Plan

Budget alerts tie directly to the [Financial Gap Analysis](#financial-gap-analysis--action-planning) savings targets:

- If discretionary budgets (Dining, Shopping, Entertainment, Coffee, Travel) are consistently AT RISK or OVER, the $X/mo savings target won't be met
- Use `budget status` for the full picture (all categories with remaining amounts)
- Use `debt impact` to show the user what budget overages mean for debt payoff timeline

### Monitoring Cadence

| Timing | Action |
|--------|--------|
| Weekly | Check `budget alerts` during weekly review — built into `weekly` output |
| Mid-month (day 15) | Standalone `budget alerts` check — projections are now reliable |
| End of month | `budget status` for final actuals vs budget |
| Monthly pipeline | `monthly run` includes budget check automatically |
| Agent check-in | Proactively call `budget_alerts()` MCP tool at start of finance conversations |

### Agent Check-In Protocol

When the user starts a conversation related to finances, spending, budgets, or the action plan, the agent should **proactively** run `budget_alerts(view="personal")` before diving into the requested task. This surfaces budget issues early without the user having to ask.

**When to trigger:**
- User asks about finances, spending, budget status, or the action plan
- User asks to review transactions, do a weekly/monthly check-in, or plan purchases
- Start of any session where finance CLI work is expected
- NOT needed for pure development/code tasks on the finance_cli codebase itself

**How to present:**
- If **no alerts** (all OK): Don't mention it — no news is good news
- If **warnings only** (>80% projected): Brief one-liner: "Heads up — Dining is trending at 92% of budget this month."
- If **at risk** (projected to exceed): Call it out with the daily remaining: "Shopping is on pace to exceed budget by $X. You have $X/day remaining to stay on track."
- If **over budget**: Lead with it: "Dining is already $X over the $X budget with 16 days left in the month."

**Tone:** Informational, not judgmental. The goal is awareness, not guilt. Present the data and let the user decide how to respond.

**Timing within conversation:** Surface alerts early (first or second message), then move on to whatever the user actually asked about. Don't belabor the point — one mention per conversation unless the user wants to dig in.

### Lessons from Implementation

- **Run-rate projection already existed** in `monthly_budget_forecast()` — the alerts feature is a thin classification layer on top, not new math
- **Early-month noise is real** — day 5 of March showed 6 categories "at risk" that were mostly just normal spending extrapolated from a tiny sample
- **The `remaining_daily_budget` field is the most actionable number** — it tells the user exactly how much they can spend per day to stay on budget (e.g., "$X/day for Dining")
- **Codex review caught 10 issues** across 4 rounds before implementation — edge cases like zero budgets, historical week views, and return contract mismatches were all fixed pre-implementation

---

## Category Data Quality Cleanup

**Goal:** Fix systematic misclassifications by leveraging provider category signals (`source_category`) and tuning keyword rules.

**When to run:** After initial data import, or when the user notices spending reports look wrong.

**Reference implementation:** See `docs/planning/CATEGORY_DATA_CLEANUP_PLAN.md` for a completed example of this workflow.

### Steps

1. **Dry run to assess scope:**
   ```
   cat auto-categorize --dry-run
   ```
   Shows how many transactions would change and by which source (vendor_memory, keyword_rule, category_mapping, AI).

2. **Audit provider signals** — query `source_category` for transactions in catch-all categories (Personal Expense, Shopping) to see if the provider already knows the right category:
   ```sql
   SELECT t.source_category, COUNT(*) FROM transactions t
     JOIN categories c ON c.id = t.category_id
    WHERE c.name = 'Personal Expense' AND t.is_active = 1
      AND t.source_category IS NOT NULL
    GROUP BY t.source_category ORDER BY COUNT(*) DESC;
   ```

3. **Identify overbroad keywords** — keywords like `TST` (Toast POS prefix) or `TIN` (matches "CUPERTINO") that override correct provider categories. Remove or narrow them in `rules.yaml`.

4. **Move misplaced keywords** — keywords in the wrong category block (e.g., LYFT in Personal Expense instead of Transportation).

5. **Add missing keywords** — vendors that consistently land in the wrong category.

6. **Apply and verify:**
   ```
   cat auto-categorize
   cat auto-categorize --ai  # for remaining NULLs
   ```

7. **Spot check by vendor** — query transactions for specific vendors to confirm correct placement.

### Key principle
Provider categories (`source_category`) are a strong signal — Plaid and banks usually know what a transaction is. Keyword rules should only override when the provider is wrong, not as a catch-all.

---

## Post-Import QA

**Goal:** Ensure newly imported transactions are correctly categorized and no data quality issues were introduced.

**When to run:** After every CSV import, PDF import, or Plaid sync.

### Steps

1. **Run cross-format dedup** — catch duplicates across import paths (CSV vs PDF vs Plaid), including ±1 day date offsets:
   ```
   dedup cross-format --format cli
   ```
   Review the matches. Fuzzy-date matches (different dates, same amount/merchant) require description confirmation and are safe to commit. Key-only matches (same date/amount, no description match) need manual review:
   ```
   dedup cross-format --commit
   ```

2. **Check for uncategorized transactions:**
   ```sql
   SELECT COUNT(*) FROM transactions
    WHERE is_active = 1 AND is_payment = 0 AND category_id IS NULL;
   ```

3. **Auto-categorize:**
   ```
   cat auto-categorize
   ```
   Applies keyword rules, vendor memory, and category mappings. For remaining NULLs:
   ```
   cat auto-categorize --ai
   ```

4. **Review AI-categorized transactions** — AI categorization has lower confidence. Spot check a sample:
   ```sql
   SELECT t.description, c.name, t.category_confidence
     FROM transactions t JOIN categories c ON c.id = t.category_id
    WHERE t.category_source = 'ai' AND t.is_active = 1
    ORDER BY t.date DESC LIMIT 20;
   ```

5. **Fix misclassifications** — use `txn set-category` for individual corrections. This automatically creates vendor_memory entries so the same vendor is handled correctly on future imports.

6. **Check payment detection** — verify payment transactions are correctly flagged:
   ```sql
   SELECT t.description, t.is_payment, c.name
     FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
    WHERE t.is_active = 1 AND t.date >= date('now', '-7 days')
      AND (t.description LIKE '%PAYMENT%' OR t.description LIKE '%TRANSFER%' OR t.description LIKE '%Check %')
    ORDER BY t.date DESC;
   ```
   Note: Check transactions (e.g., "Check 1143") are excluded from `is_payment` via `payment_exclusions` in `rules.yaml`. They show up as uncategorized rather than hidden in Payments & Transfers. Categorize them appropriately (e.g., Rent) and use `--remember` to seed vendor memory for future matches.

### Agent decisions
- If AI confidence is below 0.7, flag the transaction for user review.
- If the same vendor appears uncategorized repeatedly, propose a keyword rule or vendor_memory entry.
- If a payment was missed by keyword detection, consider adding a new pattern to `payment_keywords` in `rules.yaml`.
