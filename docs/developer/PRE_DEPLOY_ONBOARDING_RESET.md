# Pre-Deploy Onboarding Reset Runbook

Use this before deploying the onboarding PR that removes legacy wizard progress writes.

The new shell derives progress from the current contract:

- `connect`: account and transaction coverage from the user's finance DB
- `profile`: `skill_state.json` has `user_type` and `income_stability`
- `focus`: `skill_state.json` has `priority`
- `setup`: `skill_state.json` has `setup_acknowledged: true`

The retired progress fields are `profile_complete`, `assessment_shown`, and `setup_complete`.

## 1. Find The Data Root

Production web and gateway services should use the same per-user data root.

```bash
systemctl cat finance-web | grep -E 'FINANCE_WEB_DATA_ROOT|EnvironmentFile'
systemctl cat finance-gateway | grep -E 'FINANCE_GATEWAY_DATA_ROOT|EnvironmentFile'
```

If both services load a shared env file, inspect it:

```bash
sudo grep -E 'FINANCE_WEB_DATA_ROOT|FINANCE_GATEWAY_DATA_ROOT' /etc/cashnerd/finance.env
```

For local development, the default is usually `finance-web/data/users`.

## 2. Audit Legacy Mid-Flow State

List user state files that still mention retired flags:

```bash
DATA_ROOT=/path/to/users
grep -RslE '"(profile_complete|assessment_shown|setup_complete)"' "$DATA_ROOT"/*/skill_state.json
```

For each match, check whether the user is already complete:

```bash
jq '.onboarding | {complete, user_type, income_stability, priority, setup_acknowledged, profile_complete, assessment_shown, setup_complete}' \
  "$DATA_ROOT/USER_ID/skill_state.json"
```

Safe cases:

- `complete: true`: legacy completed users pass through the back-compat complete check.
- Owner/test users: reset directly if you intend to replay onboarding.

Pause cases:

- External users with retired flags but no `complete: true`.
- Any state file that is malformed JSON.

## 3. Reset Owner/Test Onboarding State

Preferred direct-file reset:

```bash
STATE_FILE="$DATA_ROOT/USER_ID/skill_state.json"
tmp="$(mktemp)"
jq 'del(.onboarding)' "$STATE_FILE" > "$tmp"
sudo install -m 600 -o "$(stat -c %U "$STATE_FILE")" -g "$(stat -c %G "$STATE_FILE")" "$tmp" "$STATE_FILE"
rm -f "$tmp"
```

Alternative via MCP when pointing the server at the target user data:

```text
skill_state_clear(name="onboarding")
```

## 4. Verify Runtime Cleanup

From the repo root:

```bash
rg -n "profile_complete|assessment_shown|setup_complete" \
  finance-web/frontend/src finance-web/server finance_cli \
  -g '!finance_cli/telegram_bot/**'
```

Expected result: only tests, historical docs, migration notes, and compatibility fixtures. Any runtime write outside the intentionally deferred polling bot blocks deploy.

Then run the onboarding contract and gateway prompt checks:

```bash
PYTHONPATH=/path/to/AI-excel-addin/api:$PYTHONPATH \
python3 -m pytest -q finance_cli/tests/test_gateway_prompt.py -k onboarding finance_cli/tests/test_onboarding_contract.py
```

## 5. Smoke After Deploy

Run three checks:

- Fresh user: `/welcome` opens, completes connect/profile/focus/setup, then dashboard shows the welcome surface.
- Skipped user: after connect, skip to dashboard and confirm the finish-setup banner routes back to chat with onboarding context.
- Legacy completed user: logs in directly to dashboard with no forced onboarding.

Keep the audit output and deploy SHA with the release note.
