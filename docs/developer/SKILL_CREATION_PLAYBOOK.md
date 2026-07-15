# Runbook: Creating a New Coaching Skill (Vertical Slice)

## When to Use

You're adding a new multi-phase coaching skill to CashNerd — a guided journey the agent enters at session start (debt payoff, emergency fund building, retirement on-ramp, tax-prep walkthrough, etc.). This runbook captures the full pipeline that produced the first slice (`coach_debt_payoff`, shipped 2026-04-29) so the next slice doesn't re-discover the gotchas.

**Calibration honesty:** this playbook is n=1. Sections marked **[CONFIRM AFTER SLICE 2]** codify decisions made once that may turn out to be slice-specific accidents. Treat the second slice as a deliberate test of this playbook, not a blind follow.

## Prerequisites

- The domain has at least Stage-1 wiki coverage (raw → wiki distillation complete). Skill markdown leans on KB articles for content, and `topic_id` references in the playbook need to resolve.
- `_BEHAVIORAL_DEFAULTS` is shipped (it is, as of 2026-04-28). The skill markdown references it; do not duplicate the 8 MI/AFCPE rules.
- You can run Codex review via `mcp__codex__codex` with the project's MCP defaults (no model/reasoning override — inherit from `~/.codex/config.toml`).

**KB Stage-2 readiness checklist** (verify before starting Phase 0):
- All relevant M-module wiki articles for the domain are shipped (Stage 1)
- Topic files for each entry-signal data domain exist with proper `topic_id: {primary_domain}.*` (NOT `cfp.{primary_domain}.*`) and required frontmatter (`scope`, `specialist_resources`, `legal_basis`, `refresh_cadence`)
- Referral edges listed where domain expertise crosses (e.g., debt → bankruptcy-attorney, credit-counselor)
- Source parity: every authoritative claim cites an AFCPE/CFP source the agent can quote

## Pipeline overview

```
Distillation  →  Journey sketch  →  Catalog + schema audit  →  Slice plan  →  Codex review (plan)
                                                                                       ↓
                       Live drive  ←  Tests  ←  Implementation  ←  Codex review (impl prompt)
```

Each arrow is a STOP point. Don't skip review rounds — they have caught real bugs every time (topic_id convention, missing registry layers, read-only tool conflicts).

---

## Phase 0: Pick the journey + identify entry signals

**Goal:** decide what the skill actually does, who it's for, and how the agent knows to enter it.

Read the relevant wiki articles (the M-modules for the domain). For debt payoff this was M3 (debt). Identify:

- **The journey arc.** What are the natural phases a coach walks a client through? Pick the AFCPE framework that fits the domain (e.g., debt-payoff lifecycle, emergency-fund ladder, retirement on-ramp). Stages-of-change is a *cross-cutting check* — gate Phase 1 on it, but don't structure the arc around it. Debt payoff landed on 10 phases (P0 Diagnose → P9 Monitor) **[CONFIRM AFTER SLICE 2 — phase count is n=1]**.
- **Entry signals.** What in the user's data triggers entry? For debt payoff: `dti_threshold_36`, `dti_threshold_43`, `minimum_only_payments`. These are also the skill-coupled intervention patterns (see Phase 5).
- **Outcome + completion criteria.** What artifact does the skill produce that proves the journey ran? Debt payoff produces a structured action-plan markdown with a YAML footer the intervention engine reads back.

**Output:** rough sketch in your head + 1-2 paragraphs in the slice plan's "Journey" section. Don't over-formalize yet.

## Phase 0.5: Audit existing catalog + map data dependencies

**Goal:** before designing new patterns, understand what's already in the intervention catalog and what data you actually have. This is the phase that catches "we built a pattern that can't fire because the data doesn't exist" and "we duplicated a catalog pattern with no extra coaching value."

This phase produces three audit tables that feed directly into the slice plan. Don't skip — slice 1 only avoided wasted impl work because we ran these.

### 0.5a — Catalog overlap audit

Walk **every catalog domain touched by the journey's entry signals, maintenance signals, artifact reads, or referral boundaries** — not just the obvious one. Slice 2 (emergency fund, retirement, etc.) will likely span cash-flow + income + tax + investment + goal surfaces simultaneously. Cross-reference `docs/COACHING_PLAYBOOK.md` against the actual `finance_cli/interventions/*.py` modules — **the doc and the code drift**.

For each existing pattern in scope, capture:

| Field | What it means |
|---|---|
| Doc entry | Does the pattern have a `COACHING_PLAYBOOK.md` entry? |
| Registered evaluator | Is there a `@register_pattern` evaluator in `finance_cli/interventions/*.py`? (Doc-only entries are common — only some catalog IDs have shipped code.) |
| Move / Priority / CFPProcessStep | Pull from `finance_cli/interventions/registry.py` enums for each registered evaluator |
| Cooldown + backing tool | Per the playbook entry; flag any tool-not-yet-shipped status |
| Anti-pattern note | The "Anti-pattern" line in the playbook entry; respect it when designing your skill response |

Then decide disposition:

| Disposition | When | Action |
|---|---|---|
| **Reuse catalog as-is** | Doc entry + registered evaluator both exist, granularity is right, response is one-shot | No code change — reference it from skill markdown's "Entry Signals" section |
| **Register skill-coupled variant** | Same data trigger but the skill's response is multi-phase coaching, not one-shot | Catalog version stays; skill-coupled gets a semantic snake_case ID. Document coexistence rationale in slice plan ("compatible coexistence — catalog is one-shot, skill is journey entry surface"). |
| **Implement missing evaluator** | Doc entry exists but no registered evaluator. Decision: ship the catalog evaluator first, or skip the doc entry entirely | If shipping: scope it as catalog work, not skill work. Don't smuggle catalog backfill into a skill PR. |
| **Skip** | Pattern is downstream of a different journey | No work |

Slice 1 reference: D-1..D-6 existed in the catalog doc; only D-1 has a registered evaluator (`finance_cli/interventions/debt.py`). D-4 overlapped conceptually with the skill's needs, so we added `minimum_only_payments` as a skill-coupled evaluator and left D-4's catalog entry unchanged. We did **not** backfill registered evaluators for D-2, D-3, D-5, D-6 — that's separate catalog work.

### 0.5b — Schema dependency check

For every pattern (catalog reuse or new skill-coupled) AND every phase that reads user data, map the data points needed to **actual** SQL tables / columns / artifact fields by reading the relevant evaluator code. Don't guess — the doc and the schema drift.

| Pattern / Phase | Data needed | Source (cite tables + key columns) | Status |
|---|---|---|---|
| `dti_threshold_36` | monthly income + total monthly debt obligations | `transactions` joined to `categories.is_income=1` for income; `liabilities` + `manual_loans` for obligations | OK |
| `constant_payment_violation` | current comparison-month debt-payment run rate + scoped debt-cleared status + agreed commitment | artifact YAML payload (`monthly_commitment_cents`, `debts_in_scope`); `liabilities` + `accounts`; `manual_loans`; `balance_snapshots`; `transactions.is_payment=1` (scoped to debt accounts); `loan_payments` | **GAP — bounded workaround** |

Any row marked **GAP** must resolve to one of three explicit outcomes in the slice plan — a GAP row should not stay in-scope without a decision:

| Outcome | When | What goes in the plan |
|---|---|---|
| **Migrate now** | Schema gap is small and the workaround is fragile | Add a migration to the slice plan. Bumps scope; needs explicit user approval. |
| **Bounded workaround + tests** | The workaround is honest and testable, with known precision limits | Document the precision limit (e.g., "monthly aggregation, not transaction-level") + ship deterministic tests covering the workaround. |
| **Defer** | Pattern is nice-to-have, not core to the journey | Drop the pattern from this slice; file the schema gap as a follow-up. |

Don't fake gaps — the workaround is honest engineering; pretending the data exists is not. Slice 2 should not silently inherit slice 1's workarounds.

Slice 1 reference: no `liability_payment_history` table exists, so `constant_payment_violation` chose **bounded workaround + tests** — reads the artifact's stored `monthly_commitment_cents` + `debts_in_scope`, then aggregates `transactions.is_payment=1` (scoped to the debt accounts) plus `loan_payments` for current-month-to-date run rate. Documented precision limits: current-month-to-date aggregation only; card payments are observed only when `transactions.is_payment=1` is on the debt account; no clean checking-transfer-to-liability linkage. Migration to a dedicated payment-history table is filed as a deferred follow-up in `PLAN_SKILL_COACH_DEBT_PAYOFF.md`.

### 0.5c — Findings against real data (pre-plan sanity check only)

**Scope:** this is a pre-plan data sanity check, not a test of the trigger code. Deterministic fixture tests for "does the pattern fire" land in Phase 5; the post-implementation live drive is Phase 6. Don't conflate.

Pick a real dev-database user (your own works fine). For each proposed pattern, trace through actual data to confirm the trigger condition is even *observable* before you commit to writing the evaluator:

- **True-positive observable** — find a real user where the condition is true. Use `txn list`, `liability show`, `balance show`, raw SQL — no Python yet.
- **True-negative observable** — find a real user where the condition is false.
- **Edge cases** — zero-debt user, user with only a HELOC, user with stale data — would the pattern handle these gracefully or would the trigger logic explode?

If you cannot find a true-positive user in any dev DB, mark the pattern **UNOBSERVED** and pick an explicit outcome in the slice plan:

| Outcome | When |
|---|---|
| **Revise the trigger** | The threshold or condition is wrong for the population |
| **Synthetic fixture with rationale** | The condition is real but rare in your dev population — document why a fixture stands in for live data |
| **Defer** | Pattern probably isn't worth shipping yet; drop it from the slice |

Time-box: ~30 minutes. Goal: catch "looks good in theory, never triggers in practice" before writing trigger code.

Slice 1 reference: Henry's dev data ($10,400 across 3 cards, $600/month commitment) provided observable true-positives for every entry-signal pattern. The post-implementation live drive (Phase 6) re-exercised them end-to-end including artifact save — but that's a separate verification, not part of 0.5.

**Output:** the three audit tables (0.5a / 0.5b / 0.5c) + UNOBSERVED outcomes (if any) go into the slice plan's "Audit" section. Codex will review them in Phase 2 and is likely to ask hard questions about any GAP and UNOBSERVED rows.

## Phase 1: Journey design template + slice plan

**Goal:** flesh the sketch into a reviewable plan.

Use **`docs/planning/JOURNEY_DESIGN_TEMPLATE.md`** as the meta-pattern. It has 6 sections:

| Section | What goes here |
|---|---|
| Entry Signals | Which intervention patterns activate the skill |
| Outcome + Completion | The persisted artifact + YAML contract |
| Phase Arc | P0..PN with goals, exits, and AFCPE framework mapping (stages-of-change is a cross-cutting check, not the arc itself) |
| Cross-Cutting Methodology | What's covered by `_BEHAVIORAL_DEFAULTS` (don't duplicate); what's slice-specific |
| Interventions Wired | Skill-coupled patterns + reused catalog patterns |
| Inventory Checklist | All files touched (skill MD, registry, MCP tools, interventions, playbook, tests) |

Then write the slice plan as **`docs/planning/PLAN_SKILL_<NAME>.md`**. The debt-payoff plan went through 3 Codex review rounds + a cleanup pass before implementation **[CONFIRM AFTER SLICE 2 — round count should converge if this playbook works]**. Aim for the same standard.

**Slice plan must include:**

- **Audit tables from Phase 0.5** — catalog overlap dispositions, schema dependency table with GAP rows called out, real-data findings
- Inventory of every file to add or modify (Codex catches missing layers here)
- Concrete artifact contract (frontmatter fields, YAML footer keys, units — cents not dollars)
- List of skill-coupled intervention patterns with semantic snake_case IDs (not `X-N`)
- Test plan: what's deterministic now, what's deferred to LLM harness
- Deferred follow-ups (each GAP row from 0.5b lands here with explicit rationale)

## Phase 2: Codex review the plan + factual-claim verification sweep

Plan review has TWO gates. Both must clear before commit.

### 2a — Codex review (logical / architectural consistency)

```
mcp__codex__codex with the slice plan as input
sandbox: workspace-write (read-only review — no writes expected)
approval-policy: never
cwd: <repo root>
```

Iterate until **PASS**. Common findings on n=1 / n=2:

- `topic_id` convention errors — KB files use `{primary_domain}.*`, NOT `cfp.{primary_domain}.*`. The CFP domain is the first segment per `CONTRACT.md`, not a namespace. Catches the playbook + plan both.
- Missing registry layers in the inventory (4-layer auto-approval is easy to miss one).
- Generic-vs-concrete artifact tool decision unclear — push for concrete.
- MCP tool signatures wrong (arg names, types, return shape) — Codex will catch these against actual `mcp_server.py`.
- Intervention model errors (claiming "continuous-after-event" cadence when registry only supports `cooldown`).
- Cross-skill state coupling that contradicts itself or doesn't compose with the existing artifact-read pattern.

`coach_emergency_fund` (n=2) needed 7 Codex rounds (R1 FAIL → R7 PASS, ~17M tokens) — most rounds caught contract-level issues. Don't be surprised by 4–7 rounds for a non-trivial skill.

### 2b — Factual-claim verification sweep (Claude reads the actual files)

**Codex PASS does NOT certify factual claims** — it verifies logical and architectural consistency, but narrow factual claims slip through, especially in late-iteration plans where attention is on contract correctness. The `coach_emergency_fund` plan reached Codex R7 PASS while still claiming `general_principles.short-term-financing` was shipped batch 1 (it wasn't) and that `banking-basics` covered HYSA + MMMF (the wiki has neither). Both errors were caught in post-PASS review by reading the actual files.

After Codex PASS, run an explicit verification pass before committing:

| Claim type | How to verify |
|---|---|
| **"Topic X is shipped batch N"** | `ls finance_cli/data/knowledge/cfp/{primary_domain}/` — does the file exist? |
| **"Wiki source X covers Y"** | `head -200 docs/afcpe/wiki/concepts/X.md` — does the body actually say Y? Beware marketing names (HYSA), investment-vs-banking distinctions (MMMF), implied-but-unstated content. |
| **"MCP tool X has signature Y"** | `grep -n "def X" finance_cli/mcp_server.py` then read the param list. Don't trust Codex's restatement of args. |
| **"Existing skill Y does Z"** | Read `docs/skills/Y_SKILL.md` and the relevant `finance_cli/interventions/Y.py` — confirm the precedent claim is accurate. |
| **"Intervention pattern Y persists data Z"** | Read the actual evaluator function — engine state model is `cooldown`-only, no `last-evaluation-state` persistence. |
| **"`refresh_cadence: <value>` is valid"** | `grep "^| \`<value>\`" docs/knowledge/CONTRACT.md` — must be in the CONTRACT enum table. |
| **"`specialist_resources: [...]` values are valid"** | Same as above — check CONTRACT enum. |
| **"`legal_basis` IDs exist in registry"** | `grep "us_federal:<id>" docs/knowledge/REGISTRY_LEGAL_BASIS.md`. If not present, the plan must add them to PR-A scope explicitly. |

If the sweep finds errors → fix in a labeled "factual-correction pass" (e.g., R8 in `coach_emergency_fund`'s revision history). Re-run Codex if the fix is non-trivial; commit directly if the fix is purely factual (file-existence, wiki-scope, registry-membership).

After both 2a and 2b clear: commit the plan **before** implementation. Implementation diffs are reviewed against the committed plan.

## Phase 3: Codex review the implementation prompt

This is the step that's easy to skip and shouldn't be.

Write the prompt you'll send to Codex for implementation. Include:

- Pointer to the committed plan
- Explicit list of files to write/modify
- Reasoning effort (use the MCP defaults — do not override)
- Critical reminder: **"Use apply_patch to write files. Do not narrate writes — execute them."** (see Gotcha #2)

Send *the prompt itself* to a fresh Codex session for review before sending it for implementation. The PR-B review caught 3 P1 bugs in the prompt for `coach_debt_payoff`:

1. Missing `tool_registry.py` (`ToolMetadata` field add)
2. Missing `gateway/server.py` (runtime gate consultation)
3. `coach_debt_payoff_auto_approved=True` on a read-only tool conflicts with validation

All three would have shipped broken code. The review round took ~5 minutes. Always do it.

## Phase 4: Implementation (4-layer wiring + skill assets)

Below is the canonical layering, with `coach_debt_payoff` as the reference example.

### 4.1 Skill markdown

**File:** `docs/skills/<NAME>_SKILL.md`

Frontmatter:
```yaml
---
name: <skill_name>
version: "0.1"
max_turns: 60          # [CONFIRM AFTER SLICE 2 — n=1 ceiling]
interactive: true
persist_state: true
timeout: 3600          # [CONFIRM AFTER SLICE 2 — n=1 ceiling]
tool_packs: []         # [CONFIRM AFTER SLICE 2 — slice 1 didn't need any]
---
```

Sections: Operating Rules, Multi-Session Expectations, Opening, Phase 0..N, Branches Catalogued, Artifact, Out of Scope. Reference `_BEHAVIORAL_DEFAULTS` — do not redefine the 8 rules.

### 4.2 Skill registration

**File:** `finance_cli/skills.py`

```python
SKILL_FILES: dict[str, str] = {
    "normalizer_builder": "NORMALIZER_BUILDER_SKILL.md",
    "onboarding": "ONBOARDING_SKILL.md",
    "<skill_name>": "<NAME>_SKILL.md",
}
```

`SKILL_FILES` order doesn't matter at runtime — the Telegram bot sorts for display. But `test_telegram_bot.py` has a hardcoded sorted "available skills" list assertion that breaks every time a skill is registered; update it in the same commit.

### 4.3 Per-skill auto-approval flag — 4 layers

The skill needs to persist state + artifacts without prompting on every routine write. This is the layer most likely to be partially-implemented. **All four layers are required.** The per-skill boolean field pattern is **[CONFIRM AFTER SLICE 2 — may not scale past ~5 skills before this becomes a registry of skills rather than a flag explosion]**.

| Layer | File | What to add |
|---|---|---|
| Registry field | `finance_cli/tool_registry.py` | New `<skill>_auto_approved: bool = False` field on `ToolMetadata`; mirror the 2 `__post_init__` + `validate_registry` checks from `onboarding_auto_approved`. **Don't set this flag on read-only tools** — they already bypass approval (`needs_approval` short-circuits in `gateway/tools.py`). The validator currently rejects auto-flags without `approval_required=True`, which catches the common mistake. |
| Decorator metadata | `finance_cli/mcp_server.py` | Add `<skill>_auto_approved` to `_REGISTRY_KWARGS` allowlist; set `<skill>_auto_approved=True` on the **low-risk persistence writes only**: `skill_state_set`, `skill_state_clear`, `agent_session_write`, and the skill's own artifact-save tool. **Don't blanket-flag** high-value writes (`goal_set`, `notify_*`, `txn_*`) — those keep their normal approval gates so the user explicitly confirms each one. |
| Gateway derivation | `finance_cli/gateway/tools.py` | Entry in `_DERIVATIONS` table → exports `<SKILL>_AUTO_APPROVED` set. Add the skill name to `_NON_ACTIVATABLE_SKILLS` so it can only be entered at session start **[CONFIRM AFTER SLICE 2 — session-start-only is right for journeys, may be wrong for short-form tools]**. |
| Runtime gate | `finance_cli/gateway/server.py` | Import the new derivation set; add `<skill> = (skill == "<skill_name>")` in 3 locations; extend `merged_needs_approval` and `onboarding_needs_approval` to consult it. |

### 4.4 Concrete artifact tools

**Decision:** concrete tools per skill, not a generic `write_artifact`.

**Why:** skill markdown is a playbook with no Python execution context. The MCP tool *is* the contract — its signature defines the artifact shape, validation lives in the tool body, and the intervention engine reads through `_parse_<skill>_artifact`. A generic tool would push schema enforcement into prose, which Codex correctly flagged as fragile.

**Pattern (in `mcp_server.py`):**

- `_<SKILL>_ARTIFACT_REQUIRED_KEYS` — set of YAML footer keys the intervention engine depends on
- `_<SKILL>_ARTIFACT_TEMPLATE` — markdown skeleton for rendering
- `_<skill>_artifact_dir(user_id)` — returns the per-user artifact directory path
- `_render_<skill>_artifact(payload)` — payload dict → markdown + YAML footer
- `_parse_<skill>_artifact(text)` — markdown → payload dict (used by interventions)
- `<skill>_artifact_save` — `approval_required` + `<skill>_auto_approved=True`
- `<skill>_artifact_read` — `read_only=True` (do NOT set the auto_approved flag — read-only tools already bypass approval, and the validator rejects auto-flags without `approval_required=True`)

**Defer the generic `write_artifact` tool until at least slice 2 needs it.** [CONFIRM AFTER SLICE 2]

### 4.5 Interventions wired

**File:** `finance_cli/interventions/<skill_name>.py`

Skill-coupled patterns use **semantic snake_case IDs** (`dti_threshold_36`, `constant_payment_violation`), not the catalog's `X-N` convention. They're entry surfaces for multi-phase journeys, not one-shot recommendations.

For each pattern:
- `@register_pattern` decorator
- `Move` enum (DIAGNOSE / WARN / COACH / PRESCRIBE / COMPARE / PATTERN_CATCH)
- `Priority` enum
- `CFPDomain` + `CFPProcessStep` axes
- Trigger function reading from the user's data
- Copy + action recommendation

**Choosing CFP axes:** `CFPDomain` enum is in `finance_cli/interventions/registry.py` (8 values: General Principles, Education, Risk Management, Investment, Tax, Retirement, Estate, Financial Planning Process). `CFPProcessStep` is the 7-step lifecycle (`understand` / `identify` / `analyze` / `develop` / `present` / `implement` / `monitor`). Pick the *primary* domain the pattern serves (debt payoff lives mostly in General Principles + Risk Management) and the *current* process step the agent is in when the pattern fires. Use the shipped debt-payoff CFP table in `docs/planning/PLAN_SKILL_COACH_DEBT_PAYOFF.md` (in `docs/completed/` post-ship) as the reference example.

**Reading the persisted artifact:** lazy-import from `mcp_server` *inside* the trigger function to avoid registry import cycle:

```python
def _evaluate(...):
    from finance_cli.mcp_server import _<skill>_artifact_dir, _parse_<skill>_artifact
    ...
```

**Update `catalog_drift.py`** so the regex matches both ID styles:

```python
_CATALOG_PATTERN_RE = re.compile(r"^#### ([A-Z]-\d+):", re.MULTILINE)
_SKILL_COUPLED_PATTERN_RE = re.compile(r"^##### ([a-z][a-z0-9_]*)\s*$", re.MULTILINE)

def extract_pattern_ids(markdown_text: str) -> set[str]:
    return set(_CATALOG_PATTERN_RE.findall(markdown_text)) | set(
        _SKILL_COUPLED_PATTERN_RE.findall(markdown_text)
    )
```

**Update `docs/COACHING_PLAYBOOK.md`** with a "Skill-coupled triggers — `<skill_name>` (semantic IDs)" subsection at the end of the relevant DOMAIN section. Each entry: Move, Tier, Cooldown, CFP domains/steps, Trigger, Data, Calculation, Copy, Action, Anti-pattern.

### 4.6 Behavioral defaults check

`_BEHAVIORAL_DEFAULTS` (in `finance_cli/gateway/prompt.py`) ships once and applies to every skill. If the new slice exposes a methodology gap (e.g., a new motivational-interviewing pattern not in the 8 rules), that's a separate parallel track — not part of this slice.

## Phase 5: Tests

**Deterministic now (always ship):**

| File | What it covers |
|---|---|
| `finance_cli/tests/test_intervention_patterns_<skill>.py` | Each pattern fires/doesn't fire with synthetic inputs |
| `finance_cli/tests/test_mcp_<skill>_artifact.py` | Save/read round-trip; YAML footer keys present |
| `finance_cli/tests/test_gateway_<skill>_auto_approval.py` | Auto-approval flags consulted in the gateway gate |
| `finance_cli/tests/test_skills.py` (extend) | Skill registered, dev CLI lists it, alphabetical order |

**Deferred (LLM harness track):** scripted-agent tests that need an LLM-driven harness which doesn't exist yet. Inventory them explicitly in the slice plan as a follow-up — don't try to ship them now, but don't reduce them to "defer" either:

- **Phase progression** — agent advances P0→PN in the right order, no skipping
- **Branch coverage** — every "Branches Catalogued" entry hit at least once across runs
- **Lifecycle (multi-session)** — phase resume across sessions, monitoring re-entry, artifact persists
- **MI question discipline** — open questions, reflective listening, restate-before-propose, no premature advice
- **Precontemplation education-only path** — agent doesn't push action when user is pre-action
- **Branch loops** — agent doesn't get stuck cycling between branches
- **Artifact write confirmation** — agent confirms with user before persisting (the auto-approval flag bypasses the gateway, not the conversational confirmation)
- **Monitoring re-entry** — `constant_payment_violation`-style intervention fires the next month and pulls the agent back in
- **End-to-end smoke** — clean run from invitation to monitoring without intervention

## Phase 6: Live drive via dev CLI

`mcp__finance-cli__*` tools route through the **prod** server's tool registry. To exercise local changes, restart the local services and use the dev CLI:

```bash
# Restart local services (services-mcp, not manual nohup)
service_restart finance_gateway
service_restart finance_web_backend

# Drive the skill end-to-end (per-skill named session keeps each test isolated)
python3 -m agent_gateway_cli --config-namespace cashnerd chat --session <skill_name> --skill <skill_name> --new "<opening message>"

# Continue the conversation in a separate invocation — history persists for this --session
python3 -m agent_gateway_cli --config-namespace cashnerd chat --session <skill_name> --skill <skill_name> "<follow-up>"
```

**Use `--session <skill_name>`** to keep each skill's transcript isolated under `~/.cache/cashnerd/sessions/`. `--new` resets that session's history while preserving `created_at`. Without `--session`, both runs share the `default` session.

Verify the artifact persisted at `finance-web/data/users/<user_id>/artifacts/<skill_name>/<YYYYMMDD>.md` and that `skill_state_set` + `agent_session_write` were called. The YAML footer should have every `_<SKILL>_ARTIFACT_REQUIRED_KEYS` entry — that's the contract the next month's intervention reads.

## Phase 7: SHIPPED writeup

Append a SHIPPED summary to the top of the slice plan:

- Per-PR commit table (PR-A plan, PR-B impl, PR-C interventions if split)
- Architectural decisions made (concrete-vs-generic, semantic IDs, lazy import)
- Deferred follow-ups with explicit rationale (LLM harness, schema gaps, etc.)
- Move plan to `docs/completed/` once writeup is committed

Update the appropriate memory files so future sessions know the slice shipped.

---

## Gotchas catalog

### #1 — `topic_id` convention
KB files use `{primary_domain}.*` not `cfp.{primary_domain}.*`. The CFP domain is the first segment per `CONTRACT.md`, **not** a namespace prefix. Audit both the slice plan and the playbook entries before sending to Codex.

### #2 — Codex narration without writing
Codex sometimes produces agent messages saying "I'll edit X next" but issues zero `apply_patch` commands. Mitigation: include the explicit reminder **"Use apply_patch to write files. Do not narrate writes — execute them."** in every implementation prompt. Do **not** override the reasoning level to compensate — inherit from `~/.codex/config.toml` per CLAUDE.md.

### #3 — Concurrent Codex processes flap files
If you launch a follow-up Codex run while the previous one is still writing, files can flap between two states (R-2 wrote 18918 bytes, follow-up wrote 18727, etc.). Always `ps aux | grep codex` and confirm no in-flight processes against the repo before kicking off the next round.

### #4 — Catalog drift regex too narrow
The default regex only matches `^#### ([A-Z]-\d+):`. Skill-coupled patterns use `##### <snake_case>` headings, which the regex must also match (see 4.5 above). Without the update, the drift test will not see new skill patterns.

### #5 — Read-only tool conflicts with auto-approval flag
Don't set `<skill>_auto_approved=True` on read-only tools — they already bypass approval via `gateway/tools.py:needs_approval`. The current validator catches the common mistake because auto-flags require `approval_required=True`, but the underlying rule is simply "leave the flag off for read-only tools."

### #6 — Registry import cycle
If an interventions module imports from `mcp_server` at module top-level, you get a registry init cycle on startup. Fix: lazy-import inside the trigger function.

### #7 — Stale skill list assertion
`test_telegram_bot.py` has a hardcoded alphabetical "available skills" list that breaks every time a new skill is registered. Update it in the same commit as the `SKILL_FILES` change.

### #8 — `mcp__finance-cli__*` routes through prod
Local changes to MCP tools won't appear in `mcp__finance-cli__*` calls until the local `finance_gateway` + `finance_web_backend` are restarted. For local testing, prefer the dev CLI.

### #9 — Dev CLI session isolation
The dev CLI persists per-`--session` history at `~/.cache/cashnerd/sessions/<name>.json` (since 2026-04-29). Use a distinct `--session <skill_name>` for each skill so transcripts don't cross-contaminate. Without `--session`, runs share the `default` session. `--new` truncates the named session before sending.

### #10 — Codex PASS doesn't certify factual claims
Codex review verifies logical and architectural consistency, not narrow factual claims. The `coach_emergency_fund` plan reached Codex R7 PASS while still claiming a non-existent KB topic was "shipped batch 1" and that a wiki article covered account types it doesn't actually cover. Both errors slipped past 7 review rounds because Codex was focused on contract correctness; factual file-existence + wiki-content scope claims fall through. **Always run the Phase 2b verification sweep before committing a Codex-PASS plan.** See Phase 2 for the verification checklist.

### #11 — Lexicographic filename sort returns the WRONG "latest" when same-day revisions exist
`sorted(artifact_dir.glob("*.md"))[-1]` is a trap whenever artifact filenames use the `YYYYMMDD.md` / `YYYYMMDD-rN.md` convention. ASCII `-` (0x2D) sorts BEFORE `.` (0x2E), so `20260607-r2.md` lexicographically compares less than `20260607.md` and the **base file** gets picked as "latest" instead of the highest revision. The savings-goal PR-C diff caught this in 5 call sites across debt-payoff + e-fund + savings-goal MCP read tools and intervention-side latest-artifact helpers. Fix: extract a `_latest_artifact_path(artifact_dir)` helper that parses filenames via `^(\d{8})(?:-r(\d+))?\.md$` and sorts by `(date_stem, revision_int)`. Audit every `sorted(...glob("*.md"))[-1]` call site in a new skill's PR.

### #12 — Deterministic tests can't catch LLM-interpretation bugs in load-bearing schemas
Deterministic fixture-based tests verify "given a known-shape payload, does the tool behave correctly?" — they assume the agent will pass the right shape. They do NOT catch the case where the **LLM at runtime constructs a coherent-but-wrong payload** that satisfies prose discipline in the markdown but violates a downstream invariant. The savings-goal live drive on 2026-05-22 surfaced exactly this: agent saved a `target_phase=starter_only` artifact with milestones scaled to the full-target trajectory and missing `original_full_*` fields. Both errors would silently break the Phase 9 accepted-unlock write flow. **Lesson: when the contract is load-bearing for an automated downstream flow, enforce it at the tool boundary, not just in markdown prose.** The artifact tool is where you can REJECT bad LLM interpretation with a clear ValueError naming the failing field — the LLM can self-correct from that signal. See savings-goal patch `51eab09` for the validator pattern (positive-int strict coercion with bool/Decimal/fractional-float guards, target_phase enum, starter_only requires unlock_blocker + all four original_full_* with canonical YYYY-MM-DD round-trip, full requires both as None, milestone threshold_cents ≤ target_balance_cents). The deterministic tests for the validator are EASY to write — exhaustive `assert raises(ValueError, "expected_field")` per gate.

---

## n=3 caveats — partial closeout after slice 3 implementation + live drive (`coach_savings_goal`)

Slice 3 fully implemented + live-driven by 2026-05-22 with follow-up patch `51eab09` after the live drive surfaced an LLM-interpretation bug. Updates below close out where n=3 confirms n=1/n=2 patterns vs. adds new lessons.

1. **Boundary validation as a coordinate of "shipped".** The savings-goal slice ran the full deterministic test surface AND Codex review chain (4 rounds: R1→R4 PASS-WITH-EDITS→PASS on PR-C alone) AND was live-driven via dev CLI. The LLM-interpretation bug surfaced ONLY in the live drive — deterministic tests test what the test author thought the agent would send. **Add live drive to the per-slice ship gate** for any skill whose artifact contract is load-bearing for a Phase 9-style automated downstream flow. Codify in Phase 6: live-drive surfaces interpretation bugs the test suite can't.
2. **Artifact tool as the contract boundary (n=3 codification).** All three shipped skills (debt-payoff, e-fund, savings-goal) save artifacts that downstream interventions / evaluators read back. The savings-goal patch is the first to add strict validation to `_normalize_<skill>_payload` beyond required-keys + generated_at-fill. Future slices should consider whether ANY artifact field is load-bearing for an automated flow; if so, add validation at the tool boundary even if the deterministic tests pass. Pattern in `_normalize_savings_goal_payload`: enum-check target_phase, positive-int strict coercion with type guards (`_coerce_positive_int` helper), cross-field invariants (starter vs full schema requirements), canonical-form persistence (write coerced int back to payload).
3. **Cross-skill scope creep can be the right call.** The savings-goal PR-C added a reciprocal disjointness gate to e-fund's `cash_flow_surplus_no_savings` evaluator + extracted `_latest_artifact_path` and updated 5 call sites (3 MCP read tools + 2 intervention modules) for consistency. Both were technically outside "savings-goal slice scope" but the alternative — fixing only savings-goal — would have left the same bug across the codebase OR forced a separate follow-up PR. Codex explicitly endorsed the cross-skill scope creep at PR-C R4. **Rule of thumb: if a fix in your slice exposes the same bug elsewhere in adjacent shipped code, fix it there in the same PR rather than filing a separate one.** Cite the parallel files in the commit message so reviewers can verify symmetry.
4. **Codex review round count holds.** Slice 1: 3 rounds + cleanup. Slice 2: 7 rounds plan + 1 factual-correction. Slice 3: 9 rounds total (R1→R9 on the plan) + 4 rounds on PR-C + 4 rounds on the follow-up patch. The trajectory is "round count tracks how load-bearing the contract is" — surface-level scope = 3 rounds, cross-skill handoffs + multi-layer contracts = 7-9 rounds. Plan for the higher end on any slice that touches an existing skill's evaluator or shares an artifact-read pathway across skills.

(Earlier n=1/n=2 caveats in the section below confirmed: 10-phase arc, ~4 ± 1 patterns per skill, concrete artifact tools, semantic snake_case pattern IDs, lazy mcp_server import, per-skill boolean auto-approval fields, frontmatter ceilings, session-start-only entry, factual-claim verification gap.)

---

## n=1 caveats — partial closeout after slice 2 plan (`coach_emergency_fund`)

Slice 2's plan reached Codex PASS at R7 + R8 factual-correction commit `7fa9b63` on 2026-04-30. Implementation has not yet started (gated on consolidated batch 2 distillation, which itself waits on the other two Phase 1 plans). Updates below reflect what the *plan* exercise revealed; implementation-stage caveats are still open.

1. **Generic vs concrete artifact tools.** Slice 2 also chose concrete (`coach_emergency_fund_artifact_save` / `_read`), with the explicit decision to revisit when slice 3 produces an artifact. Two data points pointing at concrete suggests the right inflection is "generalize at slice 3+, not at slice 2."
2. **Phase arc length.** Slice 2 also lands at 10 phases (P0 Diagnose → P9 Monitor) following the same `goal-setting-workflow + diagnostic Phase 0` structure. This is starting to look like a typical floor for goal-pursuit journeys, not coincidence. Skills with different framework backbones (e.g., risk-management-process, retirement-counseling-process) likely vary.
3. **Skill-coupled pattern shape.** Slice 2 also has 4 patterns. Pattern split: 2 entry signals (`liquidity_below_3_months`, `cash_flow_surplus_no_savings`), 1 maintenance check (`emergency_fund_drawdown_no_replenishment`), 1 contextual pivot (`income_shock_detected`). Total count matches slice 1; entry-vs-maintenance ratio differs (slice 1 was 3:1, slice 2 is 2:2). **Tentative pattern: 4 ± 1 patterns per skill is plausible default.**
4. **Codex review round count.** Slice 1: 3 rounds + cleanup. Slice 2: **7 rounds (R1 FAIL → R7 PASS, ~17M tokens)** + 1 post-PASS factual-correction (R8). Slice 2's higher round count traces to: (a) deeper architectural choices the plan made (cross-skill handoff design, intervention persistence model) that surfaced contract-level errors round-by-round, (b) Codex itself more aggressively reading source files and catching inconsistencies, (c) the new factual-claim verification gap (Gotcha #10) which surfaces additional rounds. Playbook capturing more lessons doesn't necessarily reduce rounds — it can shift where errors are caught (earlier rounds vs late). **Plan for 4–7 rounds on a non-trivial skill; budget accordingly.**
5. **Per-skill boolean auto-approval fields.** Confirmed at n=2: still unwieldy but tractable (`coach_emergency_fund_auto_approved` mirrors `coach_debt_payoff_auto_approved` cleanly). Watch for n=4+ — at that point, registry-of-skills is probably right.
6. **Frontmatter ceilings.** Slice 2 also fits under `max_turns: 60` / `timeout: 3600` / `tool_packs: []`. Two data points; raise ceilings only when a real skill demands it.
7. **Session-start-only entry.** Slice 2 also session-start-only. Confirmed for journey-shaped skills.
8. **NEW post-slice-2: Factual-claim verification gap.** Codex PASS does not certify file-existence or wiki-content scope claims. See Phase 2b + Gotcha #10. Slice 2's post-PASS R8 caught two factual errors (non-existent KB topic claimed shipped, wiki-scope claim wrong). Future skill plans need the explicit verification sweep.
9. **NEW post-slice-2: Cross-skill handoff design.** Slice 2's Phase 4 surfaces debt facts and asks the user (no automated debt-heavy threshold heuristic). Cross-skill state observable = the other skill's `_artifact_read()` MCP tool. This pattern likely scales to other handoffs (savings-goal ↔ debt-payoff, spending-plan ↔ debt-payoff). If slice 3 needs the same handoff shape, lift to a "cross-skill recommendation" recipe in the playbook.

Not a caveat: **semantic snake_case pattern IDs** for skill-coupled patterns. This is deliberate (entry surfaces vs one-shot recommendations) and has code support already (catalog-drift regex extension). Don't second-guess this one.

---

## Reference: files touched for `coach_debt_payoff` (canonical example)

```
docs/planning/JOURNEY_DESIGN_TEMPLATE.md         (new, reusable across slices)
docs/planning/PLAN_SKILL_COACH_DEBT_PAYOFF.md    (slice plan, →completed/ post-ship)
docs/skills/COACH_DEBT_PAYOFF_SKILL.md           (skill markdown)
docs/COACHING_PLAYBOOK.md                        (+58 lines, skill-coupled section)
finance_cli/skills.py                            (+1 line, registry entry)
finance_cli/tool_registry.py                     (+7 lines, ToolMetadata field + validation)
finance_cli/mcp_server.py                        (+277 lines, helpers + 2 artifact tools)
finance_cli/gateway/tools.py                     (+5 lines, derivation + non-activatable)
finance_cli/gateway/server.py                    (+14 lines, runtime gate)
finance_cli/interventions/coach_debt_payoff.py   (new, 959 lines, 4 patterns)
finance_cli/interventions/catalog_drift.py       (regex extended for snake_case IDs)
finance_cli/tests/test_intervention_patterns_coach_debt_payoff.py  (new, 9 tests)
finance_cli/tests/test_mcp_coach_debt_payoff_artifact.py           (new, 5 tests)
finance_cli/tests/test_gateway_coach_debt_payoff_auto_approval.py  (new, 2 tests)
finance_cli/tests/test_skills.py                                   (+3 tests)
finance_cli/tests/test_telegram_bot.py                             (alphabetical fix)
```

When wiring slice 2, walk this list top to bottom — anything you don't touch is something to confirm is intentionally skipped, not forgotten.
