"""Claude finance agent wrapper for Telegram."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any

import anthropic
from anthropic import AuthenticationError
from claude_gateway import AgentRunner, EventLog, McpClientManager, ToolDispatcher

from .config import BotConfig
from .store import BotStore, RequestMetrics

log = logging.getLogger(__name__)

# Only these tools are available in the Telegram bot (read-only + safe writes).
# All other MCP tools are excluded via AgentRunner's excluded_tools parameter.
# This is an allowlist approach - safer than a denylist since new tools added
# to the MCP server are excluded by default until explicitly allowed here.
_ALLOWED_TOOLS: frozenset[str] = frozenset(
    {
        "get_workflow",
        "db_status",
        "setup_check",
        "setup_status",
        "account_list",
        "account_show",
        "balance_show",
        "balance_net_worth",
        "balance_history",
        "liquidity",
        "daily_summary",
        "weekly_summary",
        "financial_summary",
        "spending_trends",
        "net_worth_projection",
        "debt_dashboard",
        "debt_interest",
        "debt_simulate",
        "debt_impact",
        "liability_show",
        "liability_upcoming",
        "liability_obligations",
        "txn_list",
        "txn_search",
        "txn_show",
        "txn_explain",
        "txn_coverage",
        "budget_list",
        "budget_status",
        "budget_forecast",
        "budget_alerts",
        "goal_list",
        "goal_status",
        "subs_list",
        "subs_total",
        "subs_audit",
        "subs_recurring",
        "biz_pl",
        "biz_cashflow",
        "biz_tax",
        "biz_estimated_tax",
        "biz_forecast",
        "biz_runway",
        "biz_seasonal",
        "biz_budget_status",
        "cat_list",
        "cat_tree",
        "cat_memory_list",
        "rules_show",
        "rules_test",
        "rules_list",
        "txn_categorize",
        "txn_review",
        "txn_tag",
        "budget_set",
        "goal_set",
    }
)

SYSTEM_PROMPT = """You are a personal finance assistant replying inside Telegram.

Keep replies concise and mobile-friendly:
- Prefer short bullets over long prose.
- Do not narrate tool usage or internal reasoning.
- Use real numbers from tools, not placeholders.
- Keep tables narrow: 3-4 columns max.
- If the user asks about spending, budgets, or burn rate, proactively check budget status/forecast/alerts when useful.

Known targets and context:
- Personal discretionary budget targets: Dining $400/month, Shopping $150/month, Entertainment $100/month.
- Optimize for closing the monthly gap and watching for debt-trap situations where minimum payments do not cover interest.
- If data is missing or inconsistent, say that plainly and ask a targeted follow-up.

You have access to documented workflows via the get_workflow tool. Workflows are advisory guides written with CLI command names — map them to your available MCP tools. If a workflow step requires a tool you don't have, skip it or tell the user it needs the CLI.

Trigger workflows when the user asks for structured processes:
- "monthly review" / "how am I doing" -> get_workflow("monthly_review")
- "help with debt" / "payoff plan" -> get_workflow("debt_planning")
- "audit subscriptions" -> get_workflow("subscription_audit")
- "set budgets" -> get_workflow("budget_setting")
- "check budgets" / "budget alerts" -> get_workflow("budget_monitoring")
- "financial overview" / "where do I stand" -> get_workflow("gap_analysis")
- "business taxes" / "schedule c" -> get_workflow("business_tax")
"""


class FinanceAgent:
    """Stateful finance agent with in-memory conversation history."""

    def __init__(self, config: BotConfig, store: BotStore | None = None) -> None:
        self._config = config
        self._store = store
        self._mcp: McpClientManager | None = None
        self._excluded_tools: set[str] = set()
        self.history: list[dict[str, str]] = []
        self.model_override: str | None = None

    async def startup(self) -> None:
        if self._mcp is not None:
            return

        mcp = McpClientManager(
            allowed_servers={"finance-cli"},
            default_tool_timeout=120,
            timeout_overrides={"finance-cli": 120},
        )
        try:
            await mcp.startup()

            tool_definitions = mcp.get_tool_definitions()
            if not tool_definitions:
                raise RuntimeError("finance-cli MCP server is unavailable or exposed no tools")

            self._excluded_tools = {tool["name"] for tool in tool_definitions if isinstance(tool.get("name"), str)}
            self._excluded_tools.difference_update(_ALLOWED_TOOLS)

            await asyncio.to_thread(self._validate_anthropic_api_key)
        except Exception:
            self._excluded_tools = set()
            await mcp.shutdown()
            raise

        self._mcp = mcp

    async def shutdown(self) -> None:
        if self._mcp is not None:
            await self._mcp.shutdown()
            self._mcp = None
            self._excluded_tools = set()

    def reset_history(self) -> None:
        self.history.clear()
        self.model_override = None
        if self._store is not None:
            self._store.mark_history_reset()

    async def run(self, user_message: str) -> str:
        if self._mcp is None:
            raise RuntimeError("FinanceAgent.startup() must be called before run()")

        request_id = uuid.uuid4().hex
        session_id = f"telegram-{request_id}"
        auth_config = self._build_auth_config()
        metrics = RequestMetrics(
            request_id=request_id,
            session_id=session_id,
            model=str(auth_config["model"]),
            start_time=time.time(),
        )
        user_entry = {"role": "user", "content": user_message}

        if self._store is not None:
            self._store.save_user_message(user_message, request_id)

        self.history.append(user_entry)
        self._trim_history()

        text = ""
        try:
            event_log = EventLog(session_id=session_id)
            dispatcher = ToolDispatcher(mcp_client=self._mcp)
            runner = AgentRunner(
                event_log=event_log,
                dispatcher=dispatcher,
                session_id=session_id,
                auth_config=auth_config,
                mcp_client=self._mcp,
                excluded_tools=self._excluded_tools,
                per_turn_timeout=120.0,
            )

            try:
                await runner.run(
                    messages=list(self.history),
                    system_prompt=SYSTEM_PROMPT,
                    max_turns=self._config.max_turns,
                )
            except asyncio.CancelledError:
                metrics.error = "cancelled"
                raise
            except Exception as exc:
                metrics.error = str(exc)
                raise
            finally:
                text = self._extract_response_text(event_log, metrics)

            if text.startswith("Stub response"):
                metrics.error = "Agent returned stub response - check API key"
                raise RuntimeError(metrics.error)

            if metrics.error:
                if text:
                    text = f"{text}\n\nWarning: response may be incomplete (agent error: {metrics.error})"
                else:
                    text = f"Error: {metrics.error}"
            elif not text:
                text = "No response generated."
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if metrics.error is None:
                metrics.error = str(exc)
            raise
        finally:
            if metrics.error is None:
                self.history.append({"role": "assistant", "content": text})
                self._trim_history()
                if self._store is not None:
                    self._store.save_assistant_message(text, request_id)
            else:
                self._remove_message(user_entry)

            if self._store is not None:
                self._store.save_request(metrics)

            self._log_request(metrics)

        return text

    def _trim_history(self) -> None:
        max_messages = max(1, self._config.history_max_turns * 2)
        while len(self.history) > max_messages:
            self.history.pop(0)

    def _remove_message(self, message: dict[str, str]) -> None:
        for index in range(len(self.history) - 1, -1, -1):
            if self.history[index] == message:
                self.history.pop(index)
                return

    def _extract_response_text(self, event_log: EventLog, metrics: RequestMetrics) -> str:
        text_parts: list[str] = []
        for entry in event_log.entries:
            event = entry.event
            event_type = event.get("type")
            if event_type == "text_delta":
                text_parts.append(str(event.get("text", "")))
            elif event_type == "tool_call_complete":
                result = event.get("result")
                metrics.tool_calls.append(
                    {
                        "tool_name": str(event.get("tool_name", "")),
                        "server": event.get("server"),
                        "duration_ms": int(event.get("duration_ms", 0) or 0),
                        "is_error": event.get("error") is not None,
                        "result_bytes": len(json.dumps(result, default=str)) if result is not None else 0,
                    }
                )
                metrics.tool_call_count = len(metrics.tool_calls)
            elif event_type == "stream_complete":
                usage = event.get("usage")
                if isinstance(usage, dict):
                    metrics.input_tokens = int(usage.get("input_tokens", 0) or 0)
                    metrics.output_tokens = int(usage.get("output_tokens", 0) or 0)
                    metrics.cache_creation_tokens = int(
                        usage.get("cache_creation_input_tokens", 0) or 0
                    )
                    metrics.cache_read_tokens = int(usage.get("cache_read_input_tokens", 0) or 0)
                    metrics.estimated_cost = float(usage.get("estimated_cost", 0.0) or 0.0)
            elif event_type == "error" and metrics.error is None:
                metrics.error = str(event.get("error", "Agent error"))

        return "".join(text_parts).strip()

    def _is_oauth(self) -> bool:
        return self._config.anthropic_api_key.startswith("sk-ant-oat")

    def _build_auth_config(self) -> dict[str, Any]:
        base = {
            "model": self.model_override or self._config.model,
            "max_tokens": self._config.max_tokens,
            "thinking": self._config.thinking,
        }
        if self._is_oauth():
            base["auth_mode"] = "oauth"
            base["auth_token"] = self._config.anthropic_api_key
        else:
            base["auth_mode"] = "api"
            base["api_key"] = self._config.anthropic_api_key
        return base

    def _validate_anthropic_api_key(self) -> None:
        if self._is_oauth():
            # OAuth tokens can't be validated via models.list() —
            # they'll be validated on first real API call.
            return
        client = anthropic.Anthropic(api_key=self._config.anthropic_api_key)
        try:
            client.models.list(limit=1)
        except AuthenticationError as exc:
            raise RuntimeError("Invalid ANTHROPIC_API_KEY") from exc

    def _log_request(self, metrics: RequestMetrics) -> None:
        summary = (
            "request=%s model=%s tokens=%s/%s cost=$%.4f tools=%s latency=%sms"
        )
        if metrics.error is None:
            log.info(
                summary,
                metrics.request_id,
                metrics.model,
                metrics.input_tokens,
                metrics.output_tokens,
                metrics.estimated_cost,
                metrics.tool_call_count,
                metrics.latency_ms,
            )
            return

        log.warning(
            summary + " error=%s",
            metrics.request_id,
            metrics.model,
            metrics.input_tokens,
            metrics.output_tokens,
            metrics.estimated_cost,
            metrics.tool_call_count,
            metrics.latency_ms,
            metrics.error,
        )
