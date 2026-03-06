# finance-cli

Personal finance CLI and MCP server for AI-assisted financial management.

Import bank statements (Plaid, CSV, PDF), categorize transactions via rules + AI,
track budgets, subscriptions, debt, and net worth. SQLite backend, 130 MCP tools
for Claude Code integration.

## Features

- **Multi-source import**: Plaid API, CSV statements, PDF statements (AI-parsed)
- **Smart categorization**: Keyword rules → vendor memory → Plaid PFC → AI fallback
- **Budget tracking**: Per-category budgets with alerts and forecasting
- **Debt management**: Dashboard, paydown simulator, spending impact analysis
- **Subscription detection**: Fixed + metered recurring charge detection
- **Business accounting**: P&L, Schedule C, estimated tax, 1099 tracking
- **Net worth tracking**: Balance snapshots, investment accounts, projections
- **130 MCP tools**: Full Claude Code integration for AI-assisted workflows

## Quick Start

```bash
pip install -e ".[all]"     # Install with all provider extras
finance-cli setup init       # Initialize database and seed categories
finance-cli setup connect    # Link bank accounts via Plaid
finance-cli plaid sync       # Sync transactions
finance-cli daily            # Today's spending summary
```

## Installation

```bash
# Core only (categorization, budgets, reports)
pip install -e .

# With specific providers
pip install -e ".[plaid]"       # Plaid bank sync
pip install -e ".[stripe]"      # Stripe revenue tracking
pip install -e ".[mcp]"         # Claude Code MCP server
pip install -e ".[all]"         # Everything
```

## Configuration

Set environment variables (or use a `.env` file):

```bash
# Plaid (optional)
PLAID_CLIENT_ID=...
PLAID_SECRET=...
PLAID_ENV=production

# Stripe (optional)
STRIPE_API_KEY=...

# AI categorization (optional)
OPENAI_API_KEY=...
ANTHROPIC_API_KEY=...
```

## MCP Server (Claude Code)

```bash
claude mcp add finance-cli -- python3 -m finance_cli.mcp_server
```

## Documentation

- [How It Works](docs/overview/HOW_IT_WORKS.md) — Architecture overview
- [Project Guide](docs/overview/PROJECT_GUIDE.md) — Detailed project guide
- [Agent Workflows](docs/AGENT_WORKFLOWS.md) — AI agent operational playbooks
- [Import Workflow](docs/ingest/INGEST_WORKFLOW.md) — Bank statement import guide
- [Adding Institutions](docs/developer/ADD_INSTITUTION_RUNBOOK.md) — CSV normalizer guide

## Requirements

- Python 3.11+
- SQLite (included with Python)

## License

MIT
