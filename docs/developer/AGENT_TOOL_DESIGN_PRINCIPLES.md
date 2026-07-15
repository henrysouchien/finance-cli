# Agent Tool Design Principles

Why some tools are effortless for AI agents and others fight them at every step. Derived from working patterns (AWS CLI, finance-cli MCP, shell tools) — intended as a rubric for auditing and improving tool interfaces across repos.

Applies to: MCP servers, CLI tools, API wrappers, anything an agent invokes.

---

## The Core Insight

AWS CLI is one of the easiest tools for agents to use — not because of any AI-specific design, but because it follows principles that happen to be exactly what agents need. The same principles that make a tool good for scripting make it good for agents. Agents are just programs that pick their own arguments.

---

## Principles

### 1. Consistent Grammar

**What it means:** Every command follows the same pattern. No surprises.

**AWS example:** Always `aws <service> <action> --flag value`. Whether it's S3, EC2, Lambda, or IAM — same shape. An agent that learns one service can use all of them.

**Anti-pattern:** Tools where `list` is a subcommand for one resource but a `--list` flag for another. Or where some commands use positional args and others use named flags for the same concept.

**Audit question:** Can an agent predict the syntax of a command it hasn't seen before, based on commands it has?

### 2. Structured, Parseable Output

**What it means:** JSON (or similar) by default. No output that requires visual parsing or regex to extract values.

**AWS example:** Every command returns JSON. `--output table` for humans, `--query` (JMESPath) for filtering. An agent never has to scrape a formatted table to get an instance ID.

**Finance-cli MCP example:** Every tool returns `{data, summary}` dicts. `summary_only=True` gives the agent a capped response; `summary_only=False` gives full data when needed.

**Anti-pattern:** Tools that return pretty-printed human text with no structured option. Or tools where the output shape changes depending on the number of results (single object vs. array).

**Audit question:** Can an agent extract any value from the output without regex or string splitting?

### 3. Predictable Error Surfaces

**What it means:** When something fails, the error tells you exactly what went wrong and what to do about it. Consistent error shape.

**AWS example:** `AccessDenied` → you know it's IAM. `InvalidParameterValue` → you know which param. The error structure is always `{Code, Message}`, so the agent can branch on error type programmatically.

**Anti-pattern:** Tools that return exit code 1 with no message. Or tools where the same error produces different messages depending on context. Or — worst — tools that return exit code 0 with an error buried in stdout.

**Audit question:** Can an agent distinguish between "bad input", "missing auth", "resource not found", and "server error" without pattern-matching on prose?

### 4. Safe Exploration (Dry-Run / Preview)

**What it means:** The agent can ask "what would happen?" before committing to a side effect.

**AWS example:** `--dry-run` on EC2 operations. `aws s3 sync --dryrun`. The agent tries the scary thing safely, reads the output, then decides.

**Finance-cli example:** `dedup cross-format` and `db restore` are dry-run by default, require `--commit` / `--yes` to apply. `cat auto-categorize --dry-run` previews without writing.

**Anti-pattern:** Write operations with no preview mode. The agent either commits blind or has to build its own simulation — which it won't.

**Audit question:** For every write/delete/modify operation, is there a way to preview the effect without executing it?

### 5. Self-Documenting at Runtime

**What it means:** The tool describes itself — available commands, expected args, valid values — without the agent needing external docs.

**AWS example:** `aws help`, `aws s3 help`, `aws s3 cp help` — three levels deep. Every command lists its flags and valid values inline.

**Anti-pattern:** Tools where the only documentation is a wiki page or README. The agent can read those, but it's a separate lookup step that breaks flow and may be stale.

**Audit question:** Can the agent discover available actions, required params, and valid enum values from the tool itself, without reading external files?

### 6. Idempotent Where Possible

**What it means:** Running the same command twice produces the same result, not a duplicate or an error.

**AWS example:** `aws s3 sync` only copies changed files. Tagging an already-tagged resource succeeds silently. Creating a resource that exists returns the existing one (for many services).

**Finance-cli example:** `import_batches` tracks file hashes — re-importing the same statement is a no-op.

**Anti-pattern:** Tools where re-running a create operation produces duplicates or errors. The agent may retry on timeout, and duplicates are expensive to clean up.

**Audit question:** If the agent runs the same command twice (e.g., after a timeout with unclear result), does anything break?

### 7. Composable Scoping

**What it means:** Filters and scopes are flags, not separate commands. You narrow results by adding constraints, not by navigating to a different context.

**AWS example:** `--filters`, `--query`, `--instance-ids`, `--region` — all composable on any command. No need to "select a region" then "select an instance" in a stateful session.

**Finance-cli MCP example:** `txn_list` takes `--from`, `--to`, `--category`, `--account-id`, `--uncategorized` — all combinable in one call.

**Anti-pattern:** Stateful tool sessions where you `cd` into a context before querying. Or tools where you can filter by category OR by date, but not both.

**Audit question:** Can the agent get exactly the data it needs in one call, or does it need multi-step navigation?

### 8. Bounded Output

**What it means:** The tool never dumps unbounded data at the agent. Pagination, limits, or summary modes keep responses manageable.

**AWS example:** Automatic pagination with `--max-items`. `--query` prunes response fields.

**Finance-cli MCP example:** `summary_only=True` (default) caps response size. `--limit` on list operations. Large result sets return counts + sample, not full dumps.

**Anti-pattern:** A "list all" command that returns 50,000 rows with no pagination. The agent's context window fills up, it loses track of the task, and the conversation degrades.

**Audit question:** What happens when the agent calls a list/search command on a large dataset? Does the tool protect the agent from itself?

### 9. Clear Naming That Implies Behavior

**What it means:** Command and parameter names tell you what they do without reading docs. Verbs are honest.

**AWS example:** `describe-instances`, `create-bucket`, `delete-function`, `put-object` — the verb tells you the HTTP method and the side effect.

**Anti-pattern:** `process`, `handle`, `run`, `execute` — verbs that could mean anything. Or names where `update` sometimes creates and `set` sometimes deletes.

**Audit question:** Can the agent infer whether a command reads or writes, and what it operates on, from the name alone?

### 10. Auth and Context Are Ambient

**What it means:** Credentials and context (region, project, profile) are set once and inherited. The agent doesn't pass a token on every call.

**AWS example:** `~/.aws/credentials` + `AWS_PROFILE` env var. Every command inherits auth without `--access-key` flags.

**MCP example:** The MCP server inherits the working directory, database path, and API keys from its environment. Tools don't take credentials as parameters.

**Anti-pattern:** Tools that require an API key or session token on every invocation. The agent either leaks secrets into prompts or needs complex credential-passing logic.

**Audit question:** Does the agent need to manage or pass auth for every call, or is it handled by the environment?

---

## Using This as an Audit Rubric

For each tool/MCP server, score each principle 0-2:

| Score | Meaning |
|-------|---------|
| 0 | Violates — agent will struggle or fail |
| 1 | Partial — works but with friction or workarounds |
| 2 | Clean — agent uses it naturally |

**20/20** = tool is agent-native. **Below 14** = expect agent errors, retries, or task failures. Focus remediation on the 0s first — those are the blockers.

### Quick-Audit Template

```
Tool/MCP: _______________
Date: _______________

 1. Consistent grammar       [ /2 ]  Notes:
 2. Structured output        [ /2 ]  Notes:
 3. Predictable errors       [ /2 ]  Notes:
 4. Dry-run / preview        [ /2 ]  Notes:
 5. Self-documenting         [ /2 ]  Notes:
 6. Idempotent               [ /2 ]  Notes:
 7. Composable scoping       [ /2 ]  Notes:
 8. Bounded output           [ /2 ]  Notes:
 9. Clear naming             [ /2 ]  Notes:
10. Ambient auth             [ /2 ]  Notes:

Total: [ /20 ]
Priority fixes:
```

---

## Repo Applicability

| Repo | Primary Tool Surface | Key Audit Targets |
|------|---------------------|-------------------|
| finance_cli | MCP server (183 tools) + CLI | Output bounding, error consistency, dry-run coverage |
| risk_module | (TBD) | Apply principles when designing tool interfaces |
| investment_tools | (TBD) | Apply principles when designing tool interfaces |

---

## Audits

- [finance-cli MCP Server (2026-03-25)](../audits/AGENT_TOOL_AUDIT_FINANCE_CLI.md) — 183 tools, score 15/20 → 20/20 (remediated 2026-03-26)

---

## Related

- `docs/AGENT_WORKFLOWS.md` — operational playbooks that assume these tool properties
- `finance_cli/mcp_server.py` — current MCP implementation (183 tools)
- AWS CLI docs — the reference implementation for "agent-friendly by accident"
