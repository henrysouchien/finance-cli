"""Finance gateway server — FastAPI app using agent-gateway."""
from __future__ import annotations

import asyncio
import copy
import itertools
import json
import logging
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional
from uuid import uuid4

from agent_gateway import (
    AgentRunner,
    AnthropicProvider,
    AuthConfig,
    CodeExecutionConfig,
    EventLog,
    MissingUserIdError,
    McpClientManager,
    NoCredentialError,
    ResolverResult,
    cleanup_code_execution,
    resolve_auth_config,
    strip_code_execute_base64_hook,
)
from agent_gateway.runner import ToolResultContext
from agent_gateway.server import (
    ChatRequest,
    ChatRuntime,
    GatewayServerConfig,
    RequestContext,
    _make_request_approval,
    create_gateway_app,
)
from agent_gateway.session import AuthManager, GatewaySession
from agent_gateway.tool_dispatcher import ToolDispatcher
from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from .code_exec_preamble import build_finance_preamble
from .config import GatewaySettings, load_settings
from .user_keys import GatewayUserKeySet, load_gateway_user_key_set
from .interceptors import make_input_size_interceptor, make_rate_limit_interceptor
from .prompt import build_code_execution_prompt, build_system_prompt
from .secure_code_execution import build_secure_code_execution as build_code_execution
from .socket_bridge import (
    FinanceBridgeServer,
    build_client_module_source,
    build_tool_catalog,
)
from .tools import (
    _NON_ACTIVATABLE_SKILLS,
    BRIDGE_TOOLS,
    COACH_DEBT_PAYOFF_AUTO_APPROVED,
    COACH_EMERGENCY_FUND_AUTO_APPROVED,
    COACH_ESTATE_DOCUMENT_READINESS_AUTO_APPROVED,
    COACH_FINANCIAL_PLAN_INTAKE_AUTO_APPROVED,
    COACH_ADVISOR_HANDOFF_READINESS_AUTO_APPROVED,
    COACH_HOMEBUYING_READINESS_AUTO_APPROVED,
    COACH_INVESTMENT_READINESS_AUTO_APPROVED,
    COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED,
    COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED,
    COACH_RISK_INSURANCE_READINESS_AUTO_APPROVED,
    COACH_SAVINGS_GOAL_AUTO_APPROVED,
    COACH_SPENDING_PLAN_AUTO_APPROVED,
    COACH_TAX_READINESS_AUTO_APPROVED,
    EXCLUDED_TOOLS,
    ONBOARDING_AUTO_APPROVED,
    REGULATED_SCOPE_EXCLUDED_TOOLS,
    VALID_SKILLS,
    WEB_IMPORT_TOOLS,
    needs_approval,
    web_excluded_tools,
)
from ..skills import load_skill_profile
from ..ai_egress import ai_egress_blocked_message, normalize_ai_egress_mode
from ..billing import RequestResolution, resolve_request
from ..cost_tracking import dollars_to_usd6, record_and_settle_cost
from ..db_keys import begin_request_cache, end_request_cache
from ..error_capture import capture_error
from ..config import get_db_path
from ..perf import (
    _conversation_id_var,
    _record_perf_sample,
    _request_id_var,
    _session_id_var,
    get_conversation_id,
    get_request_id,
    get_session_id,
    set_conversation_id,
    set_request_id,
    set_session_id,
)
from ..storage_lease import (
    LeaseHeartbeat,
    LeaseScope,
    LeaseUnavailableError,
    Queued,
    acquire_or_route,
    lease_enforcement_enabled,
    optional_lease_scope,
    release_user_lease,
)
from ..user_provisioning import provision_user, user_db_path, user_rules_path

if TYPE_CHECKING:
    from agent_gateway.multi_user.billing import UsageEvent

log = logging.getLogger(__name__)
_SERVER_RESERVED_ARG_KEYS = {
    "_approval_reason",
    "_request_id",
    "_session_id",
    "_storage_lease_id",
    "_storage_mode",
}
_CONTEXT_SANITIZE_RE = re.compile(r"[<>\n\r\x00-\x1f\u2028\u2029]")
_CODE_EXEC_SEMAPHORE = asyncio.Semaphore(2)
_COMPACTION_ALLOWED_TOOLS = frozenset(
    {
        "agent_session_write",
        "agent_session_search",
        "agent_session_read",
        "agent_memory_read",
    }
)
_COMPACTION_SYSTEM_PROMPT = (
    "You are performing conversation maintenance. Follow the instructions precisely."
)
_CHAT_INIT_PATH = "/api/chat/init"


def _is_credentials_unavailable_body(body: bytes) -> bool:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    error = payload.get("error")
    if error == "credentials_unavailable":
        return True
    detail = payload.get("detail")
    return isinstance(detail, dict) and detail.get("error") == "credentials_unavailable"


def _truthy_tool_input(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes"}
    return bool(value)


def _dedup_cross_format_requires_key_only_approval(
    tool_name: str,
    tool_input: dict | None,
) -> bool:
    if tool_name != "dedup_cross_format" or not isinstance(tool_input, dict):
        return False
    include_key_only = _truthy_tool_input(tool_input.get("include_key_only"))
    dry_run = _truthy_tool_input(tool_input.get("dry_run"), default=True)
    return include_key_only and not dry_run


class GatewayInitCredentialStatusMiddleware:
    """Rewrite gateway credential-missing init responses to product error status."""

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or scope.get("path") != _CHAT_INIT_PATH:
            await self.app(scope, receive, send)
            return

        start_message: dict[str, Any] | None = None
        body_parts: list[bytes] = []

        async def send_wrapper(message: dict[str, Any]) -> None:
            nonlocal start_message
            if message["type"] == "http.response.start":
                start_message = message
                return

            if message["type"] != "http.response.body" or start_message is None:
                await send(message)
                return

            body_parts.append(message.get("body", b""))
            if message.get("more_body", False):
                return

            body = b"".join(body_parts)
            response_start = dict(start_message)
            if (
                response_start.get("status") == 401
                and _is_credentials_unavailable_body(body)
            ):
                response_start["status"] = 402
            await send(response_start)
            await send({**message, "body": body})
            start_message = None

        await self.app(scope, receive, send_wrapper)
        if start_message is not None:
            await send(start_message)


async def _stream_cleanup_wrapper(body_iterator, *, cleanup):
    try:
        async for chunk in body_iterator:
            yield chunk
    finally:
        cleanup()


async def _on_tool_result(ctx: ToolResultContext) -> None:
    strip_code_execute_base64_hook(ctx)


def _make_activate_skill_hook(
    session: Any,
    chain: Any | None = None,
) -> Any:
    """Return an on_tool_result hook that persists activate_skill() calls."""

    async def _hook(ctx: ToolResultContext) -> list[dict[str, Any]] | None:
        if chain is not None:
            await chain(ctx)
        if ctx.tool_name != "activate_skill" or ctx.error:
            return None

        skill_name = (ctx.tool_input or {}).get("name")
        if not skill_name or skill_name in _NON_ACTIVATABLE_SKILLS:
            return None

        profile = load_skill_profile(skill_name)
        if profile is None or not profile.tool_packs or not profile.tool_packs_enabled:
            return None

        conv_id = get_conversation_id()
        if conv_id:
            activated: dict[str, str] = getattr(session, "_activated_skills", {})
            activated[conv_id] = skill_name
            session._activated_skills = activated
        return None

    return _hook


class UserScopedDispatcher(ToolDispatcher):
    """Inject server-derived user paths and strip model-supplied overrides."""

    def __init__(self, *, user_paths: dict[str, str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._user_paths = user_paths

    async def dispatch(
        self,
        tool_call_id: str,
        tool_name: str,
        tool_input: Dict[str, Any],
        *,
        call_index: int = 0,
    ):
        clean_input = {
            key: value
            for key, value in dict(tool_input or {}).items()
            if not str(key).startswith("_user_") and key not in _SERVER_RESERVED_ARG_KEYS
        }
        if self._user_paths:
            clean_input.update(self._user_paths)
        try:
            return await super().dispatch(
                tool_call_id,
                tool_name,
                clean_input,
                call_index=call_index,
            )
        except Exception as exc:
            db_path = self._user_paths.get("_user_db_path") if self._user_paths else None
            capture_error(
                exc,
                source="gateway",
                endpoint=tool_name,
                context={
                    "request_id": get_request_id() or "",
                    "tool_name": tool_name,
                },
                db_path=db_path,
            )
            raise


def _credit_purchase_url(settings: Any) -> str:
    base = (
        str(getattr(settings, "public_base_url", "") or "").strip()
        or str(os.getenv("CASHNERD_PUBLIC_BASE_URL", "") or "").strip()
        or str(os.getenv("FRONTEND_ORIGIN", "") or "").strip()
    ).rstrip("/")
    return f"{base}/settings/billing" if base else "/settings/billing"


def _credit_cta_message(settings: Any, prefix: str = "AI usage limit reached.") -> str:
    return f"{prefix} Buy credits in Billing settings: {_credit_purchase_url(settings)}"


def _billing_settings(settings: Any) -> Any:
    if hasattr(settings, "stripe_price_lite"):
        return settings
    return SimpleNamespace(stripe_price_lite=os.getenv("STRIPE_PRICE_LITE", ""))


def _load_user_billing_snapshot(settings: GatewaySettings, user_id: str) -> dict[str, Any]:
    """Load the Postgres billing fields needed by resolve_request()."""
    if not settings.database_url:
        return {"id": user_id, "user_id": user_id, "tier": "paid"}

    import psycopg2
    import psycopg2.extras

    with psycopg2.connect(
        settings.database_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id,
                       tier,
                       trial_ends_at,
                       lifetime_deal,
                       stripe_price_id,
                       anthropic_api_key_secret_ref,
                       anthropic_api_key_enc,
                       ai_egress_mode
                  FROM users
                 WHERE id = %s
                   AND deleted_at IS NULL
                """,
                (user_id,),
            )
            row = cursor.fetchone()
    if row is None:
        return {"id": user_id, "user_id": user_id, "tier": "paid"}
    return dict(row)


def _session_billing_mode(session: Any) -> str | None:
    auth_config = getattr(session, "auth_config", None)
    if isinstance(auth_config, dict):
        raw = auth_config.get("billing_mode")
    else:
        raw = getattr(auth_config, "billing_mode", None)
        if raw is None and hasattr(auth_config, "to_dict"):
            try:
                raw = auth_config.to_dict().get("billing_mode")
            except Exception:
                raw = None
    billing_mode = str(raw or "").strip().lower()
    return billing_mode or None


def _make_web_usage_hook(
    db_path: str,
    *,
    request_id: str | None,
    model: str | None,
    is_byok: bool,
):
    turn_counter = itertools.count()

    def _on_usage(event: "UsageEvent") -> None:
        turn = next(turn_counter)
        effective_request_id = request_id or get_request_id() or str(uuid4())
        record_and_settle_cost(
            db_path,
            "claude",
            "web_chat",
            dollars_to_usd6(getattr(event, "cost_usd", 0.0) or 0.0),
            idempotency_key=f"web_chat_{effective_request_id}_t{turn}",
            is_byok=is_byok,
            input_tokens=int(getattr(event, "input_tokens", 0) or 0),
            output_tokens=int(getattr(event, "output_tokens", 0) or 0),
            cache_creation_tokens=int(getattr(event, "cache_creation_tokens", 0) or 0),
            cache_read_tokens=int(getattr(event, "cache_read_tokens", 0) or 0),
            model=getattr(event, "model", None) or model,
            request_id=effective_request_id,
        )

    return _on_usage


def _make_web_guardrail_post_runner_init(
    *,
    resolution: RequestResolution,
    settings: GatewaySettings,
):
    def _post_runner_init(runner: Any) -> None:
        original_run = runner.run

        def _terminate_with_error(message: str, *, status_code: int | None = None) -> None:
            event: dict[str, Any] = {"type": "error", "error": message}
            if status_code is not None:
                event["status_code"] = status_code
                event["code"] = "payment_required"
            runner._append(event)
            runner._append({"type": "stream_complete", "usage": {}})

        async def _guarded_run(*args, **kwargs):
            if resolution.action == "block":
                log.warning(
                    "web chat blocked by plan cap",
                    extra={
                        "request_id": get_request_id(),
                        "credits_available": resolution.credits_available,
                    },
                )
                _terminate_with_error(_credit_cta_message(settings), status_code=402)
                return None

            return await original_run(*args, **kwargs)

        runner.run = _guarded_run

    return _post_runner_init


def _build_auth_config(settings: GatewaySettings) -> Dict[str, Any]:
    # Keep gateway auth isolated from process env because finance_cli also uses
    # ANTHROPIC_API_KEY for a separate categorization client.
    return resolve_auth_config(
        auth_token=settings.anthropic_auth_token,
        read_env=False,
        model=settings.model,
        max_tokens=settings.max_tokens,
        thinking=settings.thinking,
    )


def _make_credentials_resolver(
    get_session_fn: Callable | None,
    session_secret: str,
    fallback_auth_token: str,
    model: str,
    max_tokens: int,
    thinking: bool,
    key_set: GatewayUserKeySet,
):
    from finance_cli.crypto import decrypt_api_key
    from finance_cli.secrets_store import get_user_api_key, migrate_provider_ref_to_vault

    def _classify_credential(raw: str) -> dict[str, str]:
        """Inline credential classification (avoids private API import)."""
        if raw.startswith("sk-ant-oat01-"):
            return {"auth_mode": "oauth", "auth_token": raw}
        return {"auth_mode": "api", "api_key": raw}

    async def _resolve(api_key: str, init_request: Any) -> ResolverResult:
        key_entry = key_set.entry_for_key(api_key)
        if key_entry is None:
            raise MissingUserIdError("Unknown gateway user key.")

        user_id = key_entry.user_id
        risk_user_id = key_entry.risk_user_id

        raw_user_id = getattr(init_request, "user_id", None)
        claimed_user_id = str(raw_user_id).strip() if raw_user_id is not None else ""
        if claimed_user_id and claimed_user_id != user_id:
            raise MissingUserIdError("Gateway user key is bound to a different user_id.")

        claimed_channel = None
        ctx = getattr(init_request, "context", None)
        if isinstance(ctx, dict):
            raw_channel = ctx.get("channel")
            if isinstance(raw_channel, str):
                claimed_channel = raw_channel.strip().lower() or None
        channel = key_entry.channel
        if claimed_channel and claimed_channel != channel:
            raise MissingUserIdError("Gateway user key is bound to a different channel.")

        def _query():
            if get_session_fn is None:
                return None
            import psycopg2.extras

            with get_session_fn() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT anthropic_api_key_secret_ref, anthropic_api_key_enc
                      FROM users
                     WHERE id = %s
                    """,
                    (user_id,),
                )
                return cursor.fetchone()

        row = await asyncio.to_thread(_query)
        stored_ref = row.get("anthropic_api_key_secret_ref") if row else None
        api_key = get_user_api_key(
            str(user_id),
            "anthropic",
            secret_ref=None if stored_ref is None else str(stored_ref),
        )
        if api_key and stored_ref and not str(stored_ref).startswith("vault://"):
            new_ref = migrate_provider_ref_to_vault(
                str(user_id),
                provider="anthropic",
                path=("api_key",),
                plaintext=api_key,
                old_sm_ref=str(stored_ref),
            )

            def _update_ref():
                with get_session_fn() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        UPDATE users
                           SET anthropic_api_key_secret_ref = %s
                         WHERE id = %s
                           AND anthropic_api_key_secret_ref = %s
                        """,
                        (new_ref, user_id, str(stored_ref)),
                    )
                    conn.commit()

            await asyncio.to_thread(_update_ref)
        encrypted_key = row.get("anthropic_api_key_enc") if row else None

        if api_key is None and encrypted_key:
            if not session_secret:
                raise NoCredentialError("SESSION_SECRET is required to decrypt stored user keys.")
            api_key = decrypt_api_key(str(encrypted_key), session_secret)
            if api_key is None:
                raise NoCredentialError(
                    f"Stored key for user {user_id} could not be decrypted. "
                    "The key may be corrupted or SESSION_SECRET may have changed."
                )

        if api_key:
            auth_config = AuthConfig.from_dict(
                {
                    "provider": "anthropic",
                    "billing_mode": "byok",
                    "model": model,
                    "max_tokens": max_tokens,
                    "thinking": thinking,
                    **_classify_credential(api_key),
                }
            )
        elif fallback_auth_token:
            auth_config = AuthConfig.from_dict(
                {
                    "provider": "anthropic",
                    "billing_mode": "metered",
                    "model": model,
                    "max_tokens": max_tokens,
                    "thinking": thinking,
                    **_classify_credential(fallback_auth_token),
                }
            )
        else:
            raise NoCredentialError(
                f"No credential available for user {user_id}. "
                "Configure a BYOK key or set ANTHROPIC_AUTH_TOKEN as fallback."
            )

        return ResolverResult(
            user_id=user_id,
            channel=channel,
            auth_config=auth_config,
            risk_user_id=risk_user_id,
            role=key_entry.role,
            user_email=key_entry.email,
        )

    return _resolve


def _make_startup(settings: GatewaySettings, mcp: McpClientManager):
    """Return a startup callback closed over settings and the MCP manager.

    The MCP manager is created before GatewayServerConfig so the config can
    reference the live object, but startup() is deferred to FastAPI lifespan.
    """

    async def _startup() -> None:
        if settings.anthropic_auth_token:
            _build_auth_config(settings)

        await mcp.startup()
        tool_defs = mcp.get_tool_definitions()
        if not tool_defs:
            await mcp.shutdown()
            raise RuntimeError("finance-cli MCP server unavailable or exposed no tools")
        log.info("Finance gateway started: %d tools loaded", len(tool_defs))

    return _startup


def _make_shutdown(mcp: McpClientManager, pg_connection_pool: Any | None = None):
    """Return a shutdown callback closed over the MCP manager."""

    async def _shutdown() -> None:
        await mcp.shutdown()
        if pg_connection_pool is not None:
            pg_connection_pool.closeall()
        log.info("Finance gateway stopped")

    return _shutdown


class _GatewayLeaseSessionManager:
    def __init__(self, get_session_fn: Callable[[], Any]) -> None:
        self._get_session_fn = get_session_fn

    def get_db_session(self):
        return self._get_session_fn()


def _ensure_gateway_storage_lease(
    session: GatewaySession,
    user_id: str,
    *,
    session_manager,
):
    lease_state = session.background_tasks.get("storage_lease")
    if lease_state is not None and str(lease_state.get("user_id")) == str(user_id):
        return lease_state["lease"]

    result = acquire_or_route(
        user_id,
        session_manager=session_manager,
        operation="gateway",
        metadata={"source": "gateway.server", "session_id": session.session_id},
    )
    if isinstance(result, Queued):
        raise HTTPException(status_code=503, detail="Maintenance in progress, please try again shortly")
    session.background_tasks["storage_lease"] = {
        "user_id": str(user_id),
        "lease": result,
        "session_manager": session_manager,
    }
    LeaseHeartbeat.instance().register(result.lease_id, session_manager=session_manager)
    return result


def _load_onboarding_state_for_prompt(
    *,
    data_dir: Path,
    user_id: str,
    session_manager,
) -> dict[str, Any]:
    from finance_cli.skill_state import SkillStateStore
    from finance_cli.storage_client import _dispatch as storage_dispatch
    from finance_cli import storage_files

    remote_target = storage_dispatch.remote_file_target_for_user(
        user_id,
        session_manager=session_manager,
    )
    if remote_target:
        try:
            with optional_lease_scope(
                user_id,
                session_manager=session_manager,
                operation="gateway.onboarding_state",
                metadata={"source": "gateway.onboarding_prompt"},
                heartbeat=True,
            ):
                if "skill_state.json" not in storage_files.list_files(
                    remote_target,
                    user_id=user_id,
                    product="finance_cli",
                ):
                    return {}
                payload = json.loads(
                    storage_files.read_file(
                        remote_target,
                        user_id=user_id,
                        product="finance_cli",
                        relative_path="skill_state.json",
                    ).decode("utf-8")
                )
        except Exception:
            log.warning("gateway_onboarding_state_remote_read_failed", exc_info=True)
            return {}
        state = payload.get("onboarding") if isinstance(payload, dict) else None
        return dict(state) if isinstance(state, dict) else {}

    return SkillStateStore(data_dir / "skill_state.json").get("onboarding") or {}


def _load_onboarding_phase_fragment(relative_path: str) -> str | None:
    try:
        fragment_path = (Path(__file__).resolve().parents[1] / relative_path).resolve()
        prompts_root = (Path(__file__).resolve().parents[1] / "prompts" / "onboarding").resolve()
        fragment_path.relative_to(prompts_root)
        return fragment_path.read_text(encoding="utf-8").strip()
    except Exception:
        log.warning("gateway_onboarding_phase_fragment_load_failed", extra={"path": relative_path}, exc_info=True)
        return None


async def _cleanup_gateway_storage_lease(session: GatewaySession) -> None:
    lease_state = session.background_tasks.pop("storage_lease", None)
    if not lease_state:
        return
    lease = lease_state.get("lease")
    session_manager = lease_state.get("session_manager")
    lease_id = getattr(lease, "lease_id", None)
    LeaseHeartbeat.instance().unregister(lease_id)
    release_user_lease(lease_id, session_manager=session_manager)


def _make_build_chat_runtime(
    settings: GatewaySettings,
    mcp: McpClientManager,
    lease_session_manager=None,
):
    """Return a build_chat_runtime callback closed over settings and MCP manager."""
    provider = AnthropicProvider()
    auth_config = _build_auth_config(settings) if settings.anthropic_auth_token else {}
    interceptors = [
        make_rate_limit_interceptor(settings.interceptor_rate_limit_rpm),
        make_input_size_interceptor(settings.interceptor_max_input_bytes),
    ]

    async def _build_chat_runtime(
        session: GatewaySession,
        request: ChatRequest,
        channel: Optional[str],
        auth_manager: AuthManager,
    ) -> ChatRuntime:
        _ = auth_manager

        request_context = dict(request.context or {})
        compaction = bool(request_context.get("compaction"))
        if channel == "web" and not request_context.get("skill"):
            conv_id = get_conversation_id()
            activated_skills: dict[str, str] = getattr(session, "_activated_skills", {})
            activated_skill = activated_skills.get(conv_id) if conv_id else None
            if activated_skill:
                request_context["skill"] = activated_skill
        raw_skill = request_context.get("skill")
        skill = (raw_skill.strip() or None) if isinstance(raw_skill, str) else None
        if skill is not None and skill not in VALID_SKILLS:
            log.warning("Rejected unknown skill %r from request context", skill)
            skill = None
        skill_profile = None
        onboarding = False
        coach_debt_payoff = False
        coach_emergency_fund = False
        coach_savings_goal = False
        coach_spending_plan = False
        coach_tax_readiness = False
        coach_homebuying_readiness = False
        coach_retirement_contribution_readiness = False
        coach_retirement_income_readiness = False
        coach_investment_readiness = False
        coach_estate_document_readiness = False
        coach_financial_plan_intake = False
        coach_risk_insurance_readiness = False
        user_paths: dict[str, str] | None = None
        prompt_data_dir = None
        excluded_tools = set(EXCLUDED_TOOLS) | set(REGULATED_SCOPE_EXCLUDED_TOOLS)
        runtime_on_usage = None
        post_runner_init = None
        local_tool_handlers: Dict[str, Any] = {}
        extra_tool_defs: list[dict[str, Any]] = []
        code_exec_qualifier = None
        on_tool_result_hook = None
        on_tool_timing_hook = None
        onboarding_phase_fragment: str | None = None
        runner_timeout = settings.per_turn_timeout
        compaction_trigger = None
        compaction_instructions = None
        max_budget_usd = None
        timing_db_path = str(get_db_path().resolve())
        model_override = request.model
        web_resolution: RequestResolution | None = None
        user_snapshot: dict[str, Any] | None = None
        ai_egress_mode = "full"
        usage_user_id = str(getattr(session, "user_id", "") or "").strip()

        user_scoped_channel = channel in {"web", "telegram", "cli"}

        if channel == "web" and compaction:
            raise HTTPException(status_code=400, detail="Compaction not supported on web channel")

        if user_scoped_channel:
            raw_user_id = getattr(request, "user_id", None) or request_context.get("user_id")
            claimed_user_id = str(raw_user_id).strip() if raw_user_id is not None else ""
            session_user_id = str(getattr(session, "user_id", "") or "").strip()
            if session_user_id and claimed_user_id and claimed_user_id != session_user_id:
                raise HTTPException(
                    status_code=403,
                    detail="Request user_id does not match authenticated gateway session",
                )
            user_id = session_user_id or claimed_user_id
            if not user_id:
                raise HTTPException(
                    status_code=400,
                    detail=f"user_id is required for {channel or 'chat'} chat",
                )
            if not usage_user_id:
                usage_user_id = user_id
            storage_lease = None
            if lease_session_manager is not None:
                try:
                    storage_lease = _ensure_gateway_storage_lease(
                        session,
                        user_id,
                        session_manager=lease_session_manager,
                    )
                except LeaseUnavailableError:
                    if lease_enforcement_enabled():
                        raise
                    storage_lease = None
            try:
                if storage_lease is not None:
                    with LeaseScope(
                        user_id=user_id,
                        lease=storage_lease,
                        session_manager=lease_session_manager,
                        owns_lease=False,
                    ):
                        provision_user(
                            data_root=settings.data_root,
                            user_id=user_id,
                            template_rules_path=settings.template_rules_path,
                            ensure_canonical_categories=True,
                        )
                        db_path = user_db_path(settings.data_root, user_id)
                        rules_path = user_rules_path(settings.data_root, user_id)
                else:
                    provision_user(
                        data_root=settings.data_root,
                        user_id=user_id,
                        template_rules_path=settings.template_rules_path,
                        ensure_canonical_categories=True,
                    )
                    db_path = user_db_path(settings.data_root, user_id)
                    rules_path = user_rules_path(settings.data_root, user_id)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

            prompt_data_dir = db_path.parent
            if skill == "onboarding":
                from finance_cli import db as finance_db
                from finance_cli.onboarding_contract import PhaseEvaluation, is_fully_onboarded

                ob_state = _load_onboarding_state_for_prompt(
                    data_dir=prompt_data_dir,
                    user_id=user_id,
                    session_manager=lease_session_manager,
                )
                evaluation: PhaseEvaluation | None = None
                try:
                    if storage_lease is not None:
                        with LeaseScope(
                            user_id=user_id,
                            lease=storage_lease,
                            session_manager=lease_session_manager,
                            owns_lease=False,
                        ):
                            with finance_db.connect(
                                db_path,
                                busy_timeout=5000,
                                expected_user_id=user_id,
                            ) as conn:
                                evaluation = PhaseEvaluation.build(conn, ob_state)
                                completed = is_fully_onboarded(conn, ob_state)
                    else:
                        with finance_db.connect(
                            db_path,
                            busy_timeout=5000,
                            expected_user_id=user_id,
                        ) as conn:
                            evaluation = PhaseEvaluation.build(conn, ob_state)
                            completed = is_fully_onboarded(conn, ob_state)
                except Exception:
                    log.warning("gateway_onboarding_completion_check_failed", exc_info=True)
                    completed = False
                if completed:
                    log.info("Stripped onboarding skill for completed user")
                    skill = None
                elif evaluation is not None:
                    onboarding_phase_fragment = _load_onboarding_phase_fragment(
                        evaluation.current_phase.system_prompt_fragment_path
                    )
            skill_profile = load_skill_profile(skill) if skill else None
            onboarding = skill == "onboarding"
            coach_debt_payoff = skill == "coach_debt_payoff"
            coach_emergency_fund = skill == "coach_emergency_fund"
            coach_savings_goal = skill == "coach_savings_goal"
            coach_spending_plan = skill == "coach_spending_plan"
            coach_tax_readiness = skill == "coach_tax_readiness"
            coach_homebuying_readiness = skill == "coach_homebuying_readiness"
            coach_retirement_contribution_readiness = (
                skill == "coach_retirement_contribution_readiness"
            )
            coach_retirement_income_readiness = (
                skill == "coach_retirement_income_readiness"
            )
            coach_investment_readiness = skill == "coach_investment_readiness"
            coach_estate_document_readiness = (
                skill == "coach_estate_document_readiness"
            )
            coach_financial_plan_intake = skill == "coach_financial_plan_intake"
            coach_risk_insurance_readiness = (
                skill == "coach_risk_insurance_readiness"
            )
            coach_advisor_handoff_readiness = (
                skill == "coach_advisor_handoff_readiness"
            )
            timing_db_path = str(db_path)
            uploads_dir = db_path.parent / "uploads"
            uploads_dir.mkdir(parents=True, exist_ok=True)
            user_paths = {
                "_user_id": user_id,
                "_user_db_path": str(db_path),
                "_user_rules_path": str(rules_path),
                "_user_uploads_dir": str(uploads_dir),
            }
            if storage_lease is not None:
                user_paths["_storage_mode"] = str(storage_lease.storage_mode)
                user_paths["_storage_lease_id"] = str(storage_lease.lease_id)
            request_id = get_request_id()
            session_id = get_session_id()
            if request_id:
                user_paths["_request_id"] = request_id
            if session_id:
                user_paths["_session_id"] = session_id
            try:
                user_snapshot = _load_user_billing_snapshot(settings, user_id)
            except Exception as exc:
                if channel == "web":
                    capture_error(
                        exc,
                        source="gateway",
                        endpoint="user_policy_load",
                        context={"request_id": request_id or "", "user_id": user_id},
                        db_path=str(db_path),
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Chat is temporarily unavailable. Please try again shortly.",
                    ) from exc
                log.warning("Failed loading user policy for %s chat user_id=%s", channel, user_id, exc_info=True)
                user_snapshot = None
            ai_egress_mode = normalize_ai_egress_mode(
                (user_snapshot or {}).get("ai_egress_mode")
                or request_context.get("ai_egress_mode")
            )
            if ai_egress_mode != "full" and compaction:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error": "ai_egress_blocked",
                        "mode": ai_egress_mode,
                        "surface": "chat compaction",
                        "message": ai_egress_blocked_message(ai_egress_mode, "Chat compaction"),
                    },
                )
            if ai_egress_mode == "off":
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error": "ai_egress_blocked",
                        "mode": ai_egress_mode,
                        "surface": "chat",
                        "message": ai_egress_blocked_message(ai_egress_mode, "Chat"),
                    },
                )
            if ai_egress_mode == "redacted":
                excluded_tools |= {
                    tool["name"] if isinstance(tool, dict) else getattr(tool, "name")
                    for tool in mcp.get_tool_definitions()
                }
                prompt_data_dir = None
            if channel == "web":
                try:
                    if user_snapshot is None:
                        user_snapshot = _load_user_billing_snapshot(settings, user_id)
                    billing_mode = _session_billing_mode(session)
                    if billing_mode:
                        user_snapshot["billing_mode"] = billing_mode
                    requested_model = request.model or (
                        skill_profile.model if skill_profile is not None else settings.model
                    )
                    web_resolution = resolve_request(
                        user_snapshot,
                        db_path,
                        _billing_settings(settings),
                        explicit_model=requested_model,
                    )
                except Exception as exc:
                    capture_error(
                        exc,
                        source="gateway",
                        endpoint="cost_resolve",
                        context={"request_id": request_id or "", "user_id": user_id},
                        db_path=str(db_path),
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Chat is temporarily unavailable. Please try again shortly.",
                    ) from exc
                model_override = web_resolution.effective_model
                runtime_on_usage = _make_web_usage_hook(
                    str(db_path),
                    request_id=request_id,
                    model=model_override,
                    is_byok=web_resolution.mode == "byok",
                )
                post_runner_init = _make_web_guardrail_post_runner_init(
                    resolution=web_resolution,
                    settings=settings,
                )
                excluded_tools |= set(web_excluded_tools(skill))
                if ai_egress_mode == "full":
                    excluded_tools -= set(WEB_IMPORT_TOOLS)
                on_tool_result_hook = _make_activate_skill_hook(session)
        else:
            skill_profile = load_skill_profile(skill) if skill else None
            onboarding = skill == "onboarding"
            coach_debt_payoff = skill == "coach_debt_payoff"
            coach_emergency_fund = skill == "coach_emergency_fund"
            coach_savings_goal = skill == "coach_savings_goal"
            coach_spending_plan = skill == "coach_spending_plan"
            coach_tax_readiness = skill == "coach_tax_readiness"
            coach_homebuying_readiness = skill == "coach_homebuying_readiness"
            coach_retirement_contribution_readiness = (
                skill == "coach_retirement_contribution_readiness"
            )
            coach_retirement_income_readiness = (
                skill == "coach_retirement_income_readiness"
            )
            coach_investment_readiness = skill == "coach_investment_readiness"
            coach_estate_document_readiness = (
                skill == "coach_estate_document_readiness"
            )
            coach_financial_plan_intake = skill == "coach_financial_plan_intake"
            coach_risk_insurance_readiness = (
                skill == "coach_risk_insurance_readiness"
            )
            coach_advisor_handoff_readiness = (
                skill == "coach_advisor_handoff_readiness"
            )

        if compaction:
            all_tools = {
                tool["name"] if isinstance(tool, dict) else getattr(tool, "name")
                for tool in mcp.get_tool_definitions()
            }
            excluded_tools = all_tools - _COMPACTION_ALLOWED_TOOLS
            system_prompt = _COMPACTION_SYSTEM_PROMPT
            max_turns = 3

            def approval_gate(_tool_name):
                return False

            request_approval = None
            request_interceptors = []
        else:
            request_interceptors = interceptors
            if onboarding:
                compaction_trigger = 150_000
                compaction_instructions = (
                    "Preserve all onboarding markers, user type, current phase, and financial "
                    "profile. Summarize tool results, not tool names."
                )
                max_budget_usd = 3.0
            elif channel == "web":
                compaction_trigger = settings.web_compaction_trigger
                max_budget_usd = settings.web_max_budget_usd
            if onboarding:
                def onboarding_tool_timing(
                    hook_session_id: str,
                    tool_name: str,
                    server_name: str | None,
                    duration_ms: int,
                    is_error: bool,
                    result_bytes: int,
                ) -> None:
                    _record_perf_sample(
                        timing_db_path,
                        "onboarding",
                        f"onboarding.tool.{tool_name}",
                        duration_ms,
                        tags={
                            "tool_name": tool_name,
                            "server": server_name or "unknown",
                            "session_id": hook_session_id,
                            "result_bytes": int(result_bytes),
                        },
                        is_error=is_error,
                    )

                on_tool_timing_hook = onboarding_tool_timing
            skill_context: dict[str, object] | None = None
            if skill:
                sc: dict[str, object] = {}
                for key in ("upload_path", "sample_rows"):
                    val = request_context.get(key)
                    if val is not None:
                        sc[key] = val
                skill_context = sc or None
            upload_context: dict[str, object] | None = None
            upload_ctx: dict[str, object] = {}
            for key in ("upload_path", "upload_filename", "upload_file_type"):
                val = request_context.get(key)
                if isinstance(val, str):
                    upload_ctx[key] = _CONTEXT_SANITIZE_RE.sub("_", val)[:512]
            if upload_ctx:
                upload_context = upload_ctx
            interventions = ()
            if user_scoped_channel and not onboarding and ai_egress_mode == "full":
                try:
                    from finance_cli import db as finance_db
                    from finance_cli.intervention_engine import evaluate_for_surface

                    if storage_lease is not None:
                        with LeaseScope(
                            user_id=user_id,
                            lease=storage_lease,
                            session_manager=lease_session_manager,
                            owns_lease=False,
                        ):
                            with finance_db.connect(db_path, busy_timeout=5000) as conn:
                                _, interventions = evaluate_for_surface(
                                    conn,
                                    "agent_prompt",
                                    rules_path=rules_path,
                                    log_to_surface="agent_prompt",
                                )
                    else:
                        with finance_db.connect(db_path, busy_timeout=5000) as conn:
                            _, interventions = evaluate_for_surface(
                                conn,
                                "agent_prompt",
                                rules_path=rules_path,
                                log_to_surface="agent_prompt",
                            )
                except Exception:
                    log.exception(
                        "intervention injection failed for user %s",
                        user_id,
                    )
                    interventions = ()
            system_prompt = build_system_prompt(
                channel=channel,
                data_dir=prompt_data_dir,
                skill=skill,
                skill_context=skill_context,
                upload_context=upload_context,
                interventions=interventions,
                onboarding_phase_fragment=onboarding_phase_fragment,
            )
            if ai_egress_mode == "redacted":
                privacy_prompt = (
                    "\n\n<privacy>\n"
                    "AI privacy mode is redacted. Do not claim access to linked "
                    "accounts, transactions, balances, memory, or stored financial "
                    "records. No finance tools or code execution are available in "
                    "this mode; answer only from the current user message and "
                    "general financial knowledge.\n"
                    "</privacy>"
                )
                if isinstance(system_prompt, list):
                    system_prompt = [*system_prompt, (privacy_prompt, False)]
                else:
                    system_prompt += privacy_prompt
            if settings.code_execution_enabled and ai_egress_mode == "full":
                code_execution_prompt = build_code_execution_prompt(
                    build_tool_catalog(mcp, BRIDGE_TOOLS)
                )
                _build_finance_preamble = build_finance_preamble

                if isinstance(system_prompt, list):
                    system_prompt = [*system_prompt, (code_execution_prompt, False)]
                else:
                    system_prompt += code_execution_prompt
                bundle_config = CodeExecutionConfig(
                    docker_image=settings.code_exec_docker_image,
                    register_subprocess=settings.env != "production",
                    build_preamble=_build_finance_preamble,
                )
                bundle = build_code_execution(
                    session,
                    config=bundle_config,
                )

                original_handler = bundle.handlers["code_execute"]

                async def _guarded_code_execute(tool_input, **kwargs):
                    if tool_input.get("background"):
                        return None, {
                            "code": "invalid_input",
                            "message": "Background execution is not supported",
                        }
                    async with _CODE_EXEC_SEMAPHORE:
                        if not session.code_execution_work_dir:
                            session.code_execution_work_dir = tempfile.mkdtemp(
                                prefix=bundle_config.work_dir_prefix,
                                dir=bundle_config.work_dir_root,
                            )
                        else:
                            os.makedirs(session.code_execution_work_dir, exist_ok=True)

                        client_module_path = os.path.join(
                            session.code_execution_work_dir,
                            "finance_client.py",
                        )
                        with open(client_module_path, "w", encoding="utf-8") as handle:
                            handle.write(build_client_module_source())

                        bridge = FinanceBridgeServer(
                            socket_path=os.path.join(
                                session.code_execution_work_dir,
                                "_finance.sock",
                            ),
                            mcp_client=mcp,
                            user_paths=user_paths,
                            allowed_tools=BRIDGE_TOOLS,
                        )
                        await bridge.start()
                        try:
                            return await original_handler(tool_input, **kwargs)
                        finally:
                            await bridge.stop()

                filtered_tool_defs: list[dict[str, Any]] = []
                for tool_def in bundle.tool_definitions:
                    if tool_def.get("name") == "code_execute_status":
                        continue
                    next_tool_def = copy.deepcopy(tool_def)
                    if next_tool_def.get("name") == "code_execute":
                        schema = next_tool_def.get("input_schema")
                        if isinstance(schema, dict):
                            properties = schema.get("properties")
                            if isinstance(properties, dict):
                                properties.pop("background", None)
                                properties.pop("host", None)
                            required = schema.get("required")
                            if isinstance(required, list):
                                schema["required"] = [
                                    value for value in required if value not in {"background", "host"}
                                ]
                    filtered_tool_defs.append(next_tool_def)

                local_tool_handlers["code_execute"] = _guarded_code_execute
                extra_tool_defs.extend(filtered_tool_defs)
                code_exec_qualifier = bundle.approval_qualifier
                if channel == "web":
                    on_tool_result_hook = _make_activate_skill_hook(session, chain=_on_tool_result)
                else:
                    on_tool_result_hook = _on_tool_result

                base_needs_approval = needs_approval
                bundle_needs_approval = bundle.needs_approval

                def merged_needs_approval(
                    tool_name: str,
                    tool_input: dict | None = None,
                    qualifier: str = "",
                ) -> bool:
                    if tool_name in {"code_execute", "code_execute_status"}:
                        return bundle_needs_approval(tool_name, tool_input or {}, qualifier)
                    if _dedup_cross_format_requires_key_only_approval(tool_name, tool_input):
                        return base_needs_approval(tool_name)
                    if onboarding and tool_name in ONBOARDING_AUTO_APPROVED:
                        return False
                    if coach_debt_payoff and tool_name in COACH_DEBT_PAYOFF_AUTO_APPROVED:
                        return False
                    if coach_emergency_fund and tool_name in COACH_EMERGENCY_FUND_AUTO_APPROVED:
                        return False
                    if coach_savings_goal and tool_name in COACH_SAVINGS_GOAL_AUTO_APPROVED:
                        return False
                    if coach_spending_plan and tool_name in COACH_SPENDING_PLAN_AUTO_APPROVED:
                        return False
                    if coach_tax_readiness and tool_name in COACH_TAX_READINESS_AUTO_APPROVED:
                        return False
                    if (
                        coach_homebuying_readiness
                        and tool_name in COACH_HOMEBUYING_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_retirement_contribution_readiness
                        and tool_name
                        in COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_retirement_income_readiness
                        and tool_name
                        in COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_investment_readiness
                        and tool_name in COACH_INVESTMENT_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_estate_document_readiness
                        and tool_name in COACH_ESTATE_DOCUMENT_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_financial_plan_intake
                        and tool_name in COACH_FINANCIAL_PLAN_INTAKE_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_risk_insurance_readiness
                        and tool_name
                        in COACH_RISK_INSURANCE_READINESS_AUTO_APPROVED
                    ):
                        return False
                    if (
                        coach_advisor_handoff_readiness
                        and tool_name
                        in COACH_ADVISOR_HANDOFF_READINESS_AUTO_APPROVED
                    ):
                        return False
                    return base_needs_approval(tool_name)

                approval_gate = merged_needs_approval
            else:
                if (
                    onboarding
                    or coach_debt_payoff
                    or coach_emergency_fund
                    or coach_savings_goal
                    or coach_spending_plan
                    or coach_tax_readiness
                    or coach_homebuying_readiness
                    or coach_retirement_contribution_readiness
                    or coach_retirement_income_readiness
                    or coach_investment_readiness
                    or coach_estate_document_readiness
                    or coach_financial_plan_intake
                    or coach_risk_insurance_readiness
                    or coach_advisor_handoff_readiness
                ):
                    base_needs_approval = needs_approval

                    def onboarding_needs_approval(
                        tool_name: str,
                        tool_input: dict | None = None,
                        qualifier: str = "",
                    ) -> bool:
                        del qualifier
                        if _dedup_cross_format_requires_key_only_approval(tool_name, tool_input):
                            return base_needs_approval(tool_name)
                        if tool_name in ONBOARDING_AUTO_APPROVED:
                            if onboarding:
                                return False
                        if tool_name in COACH_DEBT_PAYOFF_AUTO_APPROVED:
                            if coach_debt_payoff:
                                return False
                        if tool_name in COACH_EMERGENCY_FUND_AUTO_APPROVED:
                            if coach_emergency_fund:
                                return False
                        if tool_name in COACH_SAVINGS_GOAL_AUTO_APPROVED:
                            if coach_savings_goal:
                                return False
                        if tool_name in COACH_SPENDING_PLAN_AUTO_APPROVED:
                            if coach_spending_plan:
                                return False
                        if tool_name in COACH_TAX_READINESS_AUTO_APPROVED:
                            if coach_tax_readiness:
                                return False
                        if tool_name in COACH_HOMEBUYING_READINESS_AUTO_APPROVED:
                            if coach_homebuying_readiness:
                                return False
                        if (
                            tool_name
                            in COACH_RETIREMENT_CONTRIBUTION_READINESS_AUTO_APPROVED
                        ):
                            if coach_retirement_contribution_readiness:
                                return False
                        if (
                            tool_name
                            in COACH_RETIREMENT_INCOME_READINESS_AUTO_APPROVED
                        ):
                            if coach_retirement_income_readiness:
                                return False
                        if tool_name in COACH_INVESTMENT_READINESS_AUTO_APPROVED:
                            if coach_investment_readiness:
                                return False
                        if tool_name in COACH_ESTATE_DOCUMENT_READINESS_AUTO_APPROVED:
                            if coach_estate_document_readiness:
                                return False
                        if tool_name in COACH_FINANCIAL_PLAN_INTAKE_AUTO_APPROVED:
                            if coach_financial_plan_intake:
                                return False
                        if tool_name in COACH_RISK_INSURANCE_READINESS_AUTO_APPROVED:
                            if coach_risk_insurance_readiness:
                                return False
                        if (
                            tool_name
                            in COACH_ADVISOR_HANDOFF_READINESS_AUTO_APPROVED
                        ):
                            if coach_advisor_handoff_readiness:
                                return False
                        return base_needs_approval(tool_name)

                    approval_gate = onboarding_needs_approval
                else:
                    approval_gate = needs_approval
            skill_max_turns = skill_profile.max_turns if skill_profile is not None else None
            max_turns = skill_max_turns or (40 if onboarding else settings.max_turns)
            request_approval = "use-request-context"
            runner_timeout = (
                skill_profile.timeout
                if skill_profile and skill_profile.timeout
                else settings.per_turn_timeout
            )
            if channel == "telegram" and not compaction:
                runner_timeout = max(runner_timeout, settings.telegram_per_turn_timeout)

        def _get_tool_definitions() -> list[dict[str, Any]]:
            return mcp.get_tool_definitions() + extra_tool_defs

        def _build_dispatcher(req_ctx: RequestContext) -> ToolDispatcher:
            return UserScopedDispatcher(
                mcp_client=req_ctx.mcp_client or mcp,
                local_tool_handlers=local_tool_handlers,
                needs_approval=approval_gate,
                request_approval=None if request_approval is None else req_ctx.request_approval,
                approved_tool_types=session.approved_tool_types,
                user_paths=user_paths,
                event_log=req_ctx.event_log,
                approval_key_qualifier=code_exec_qualifier,
                interceptors=request_interceptors,
                session_id=session.session_id,
            )

        def _build_runner(event_log: EventLog, session_id: str) -> AgentRunner:
            request_approval_cb = _make_request_approval(session, event_log)
            req_ctx = RequestContext(
                session,
                event_log,
                request_approval_cb,
                session.result_queue,
                mcp,
            )
            dispatcher = _build_dispatcher(req_ctx)
            effective_auth = session.auth_config or auth_config
            if isinstance(effective_auth, dict):
                effective_auth_data = effective_auth
            elif hasattr(effective_auth, "to_dict"):
                effective_auth_data = effective_auth.to_dict()
            else:
                effective_auth_data = {}
            rate_table = getattr(provider, "_rate_table", None)
            resolved_rate_table_version = str(
                effective_auth_data.get("rate_table_version")
                or getattr(rate_table, "version", "unknown")
                or "unknown"
            )
            resolved_billing_mode = str(effective_auth_data.get("billing_mode") or "metered")
            runner = AgentRunner(
                event_log,
                dispatcher,
                session_id,
                provider=provider,
                auth_config=effective_auth,
                client_timeout=settings.client_timeout,
                per_turn_timeout=runner_timeout if not compaction else settings.per_turn_timeout,
                mcp_client=mcp,
                excluded_tools=excluded_tools,
                get_tool_definitions=_get_tool_definitions,
                on_tool_result=on_tool_result_hook,
                on_usage=runtime_on_usage,
                on_tool_timing=on_tool_timing_hook if not compaction else None,
                compaction_trigger=compaction_trigger if not compaction else None,
                compaction_instructions=compaction_instructions if not compaction else None,
                max_budget_usd=max_budget_usd if not compaction else None,
                user_id=usage_user_id,
                request_id=get_request_id(),
                billing_mode=resolved_billing_mode,
                rate_table_version=resolved_rate_table_version,
                channel=getattr(session, "channel", None) or channel,
            )
            if post_runner_init is not None:
                post_runner_init(runner)
            return runner

        return ChatRuntime(
            system_prompt=system_prompt,
            build_runner=_build_runner,
            get_tool_definitions=_get_tool_definitions,
            build_dispatcher=_build_dispatcher,
            model_override=model_override
            or (skill_profile.model if skill_profile is not None else None),
            excluded_tools=excluded_tools,
            on_usage=runtime_on_usage,
            post_runner_init=post_runner_init,
            max_turns=max_turns,
        )

    return _build_chat_runtime


def create_app(settings: GatewaySettings | None = None) -> Any:
    """Create and return the FastAPI gateway app."""
    if settings is None:
        settings = load_settings()

    mcp_kwargs: Dict[str, Any] = dict(
        allowed_servers={"finance-cli"},
        default_tool_timeout=120,
        timeout_overrides={"finance-cli": 120},
    )
    if settings.mcp_config_path:
        mcp_kwargs["config_path"] = Path(settings.mcp_config_path).expanduser()
    mcp = McpClientManager(**mcp_kwargs)
    gateway_auth_config: Dict[str, Any] = (
        _build_auth_config(settings) if settings.anthropic_auth_token else {}
    )
    gateway_user_key_set = load_gateway_user_key_set(settings.gateway_user_keys)
    get_pg_session = None
    pg_connection_pool = None
    lease_session_manager = None
    if settings.database_url:
        from psycopg2 import pool as pg_pool

        pg_connection_pool = pg_pool.ThreadedConnectionPool(0, 5, dsn=settings.database_url)

        @contextmanager
        def _get_pg_session():
            conn = pg_connection_pool.getconn()
            try:
                yield conn
            finally:
                pg_connection_pool.putconn(conn)

        get_pg_session = _get_pg_session
        lease_session_manager = _GatewayLeaseSessionManager(_get_pg_session)

    credentials_resolver = _make_credentials_resolver(
        get_session_fn=get_pg_session,
        session_secret=settings.session_secret,
        fallback_auth_token=settings.anthropic_auth_token,
        model=settings.model,
        max_tokens=settings.max_tokens,
        thinking=settings.thinking,
        key_set=gateway_user_key_set,
    )

    def _on_gateway_event(event: Dict[str, Any], session_id: str) -> None:
        event_type = event.get("type", "unknown")
        short_session_id = session_id[:12]
        if event_type == "interceptor_decision":
            log.warning(
                "gateway event: %s session=%s action=%s code=%s message=%s",
                event_type,
                short_session_id,
                event.get("action", ""),
                event.get("code", ""),
                str(event.get("message", ""))[:200],
                extra={"event": event, "session_id": session_id},
            )
        elif event_type == "budget_exceeded":
            log.warning(
                "gateway event: %s session=%s total_cost=%s budget=%s",
                event_type,
                short_session_id,
                event.get("total_cost", ""),
                event.get("budget", ""),
                extra={"event": event, "session_id": session_id},
            )
        elif event_type == "error":
            log.warning(
                "gateway event: %s session=%s error=%s",
                event_type,
                short_session_id,
                str(event.get("error", ""))[:200],
                extra={"event": event, "session_id": session_id},
            )
        elif event_type in ("tool_call_start", "tool_call_complete"):
            log.debug(
                "gateway event: %s session=%s tool=%s",
                event_type,
                short_session_id,
                event.get("tool_name", "?"),
            )

    cors_list = [origin.strip() for origin in settings.cors_origins if origin.strip()]

    kwargs: Dict[str, Any] = dict(
        jwt_secret=settings.jwt_secret,
        session_ttl=settings.session_ttl,
        valid_api_keys=gateway_user_key_set.valid_api_keys,
        cors_origins=cors_list,
        auth_config=gateway_auth_config,
        mcp_client=mcp,
        per_turn_timeout=settings.per_turn_timeout,
        build_chat_runtime=_make_build_chat_runtime(settings, mcp, lease_session_manager),
        on_event=_on_gateway_event,
        on_startup=_make_startup(settings, mcp),
        on_shutdown=_make_shutdown(mcp, pg_connection_pool),
        log_name="finance_cli.gateway",
    )
    kwargs["credentials_resolver"] = credentials_resolver
    kwargs["resolver_timeout_seconds"] = settings.resolver_timeout_seconds
    if settings.allowed_models:
        kwargs["allowed_models"] = {model.strip() for model in settings.allowed_models if model.strip()}

    config = GatewayServerConfig(**kwargs)
    app = create_gateway_app(config)
    app.add_middleware(GatewayInitCredentialStatusMiddleware)

    auth_manager = getattr(app.state, "auth", None)
    session_store = getattr(auth_manager, "session_store", None)
    if session_store is not None:
        if lease_session_manager is None:
            if settings.code_execution_enabled:
                session_store.set_on_expiry(cleanup_code_execution)
        else:
            async def _on_session_expiry(session: GatewaySession) -> None:
                await _cleanup_gateway_storage_lease(session)
                if settings.code_execution_enabled:
                    await cleanup_code_execution(session)

            session_store.set_on_expiry(_on_session_expiry)

    @app.middleware("http")
    async def set_request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or get_request_id() or str(uuid4())
        session_id = request.headers.get("X-Session-ID") or None
        conversation_id = request.headers.get("X-Conversation-ID") or None

        request_token = set_request_id(request_id)
        session_token = set_session_id(session_id)
        conversation_token = set_conversation_id(conversation_id)
        db_key_cache_token = begin_request_cache()
        request.state.request_id = request_id
        request.state.session_id = session_id
        request.state.conversation_id = conversation_id

        cleanup_done = False

        def _finish_request_scope() -> None:
            nonlocal cleanup_done
            if cleanup_done:
                return
            cleanup_done = True
            end_request_cache(db_key_cache_token)
            _conversation_id_var.reset(conversation_token)
            _session_id_var.reset(session_token)
            _request_id_var.reset(request_token)

        try:
            response = await call_next(request)
        except Exception:
            _finish_request_scope()
            raise

        if isinstance(response, StreamingResponse):
            response.body_iterator = _stream_cleanup_wrapper(
                response.body_iterator,
                cleanup=_finish_request_scope,
            )
            return response

        _finish_request_scope()
        return response

    return app
