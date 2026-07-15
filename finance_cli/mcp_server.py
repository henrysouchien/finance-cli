#!/usr/bin/env python3
"""Shared Finance CLI MCP tool definitions wrapping existing CLI handlers.

Local Claude Code clients should register ``finance_cli.mcp_local``.
Server-side stdio consumers should use a dedicated entrypoint such as
``finance_cli.mcp_gateway``.
"""

# stdout redirect (required for MCP JSON-RPC over stdio — handler print()
# must not corrupt the transport).
import functools
import hashlib
import inspect
import json
import logging
import os
import re
import sqlite3
import sys
import time
import uuid

_real_stdout = sys.stdout
sys.stdout = sys.stderr
logger = logging.getLogger(__name__)

from argparse import Namespace  # noqa: E402
from dataclasses import asdict, dataclass, is_dataclass  # noqa: E402
from datetime import date, datetime, time as datetime_time, timedelta, timezone  # noqa: E402
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any, Optional  # noqa: E402

import yaml  # noqa: E402
from finance_cli import tool_registry  # noqa: E402
from finance_cli.advisory import (  # noqa: E402
    SUPPORTED_LIMIT_YEARS,
    annuity_surrender_analysis,
    bracket_room,
    contribution_priority,
    debt_vs_invest,
    federal_tax,
    fee_impact,
    fica_tax,
    fund_fee_comparison,
    future_value,
    roth_conversion_analysis,
    roth_vs_traditional,
    runway_projection,
    target_allocation,
    taxable_income_from_gross,
    time_to_goal,
)
from finance_cli.advisory.tax_brackets_data import STANDARD_DEDUCTION  # noqa: E402
from finance_cli.config import PROJECT_ROOT, get_db_path, load_dotenv  # noqa: E402,F401
from finance_cli.db import connect, initialize_connection  # noqa: E402
from finance_cli.error_capture import capture_error  # noqa: E402
from finance_cli.exceptions import NotFoundError, ValidationError  # noqa: E402
from finance_cli.storage_lease import (  # noqa: E402
    LeaseUnavailableError,
    LeaseScope,
    LocalLease,
    Queued,
    RemoteLease,
    optional_lease_scope,
)
from finance_cli.importers import detect_csv_institution, normalize_csv  # noqa: E402
from finance_cli.importers.csv_normalizers import validate_normalize_result  # noqa: E402
from finance_cli.importers.normalizer_sandbox import validate_normalizer_source  # noqa: E402
from finance_cli.importers.normalizers import (  # noqa: E402
    BUILT_IN_TIER,
    USER_TIER,
    get_normalizer_loader,
    load_user_module_metadata,
    normalizer_file_content_hash,
    normalizer_file_exists,
    normalize_registry_key,
    read_normalizer_text,
    replace_normalizer_file,
    reset_normalizer_loader_cache,
    resolve_user_normalizers_dir,
    run_user_normalizer_normalize_with_validation,
    staging_normalizers_dir,
    write_normalizer_text,
)
from finance_cli.interventions.helpers import bounded_whole_percent, income_by_stream  # noqa: E402
from finance_cli.institution_names import is_known, register_user_institution  # noqa: E402
from finance_cli.lazy_imports import LazyModule  # noqa: E402
from finance_cli.skill_state import SkillStateStore  # noqa: E402
from finance_cli.skill_constants import NON_ACTIVATABLE_SKILLS  # noqa: E402
from finance_cli.response_sanitizer import (  # noqa: E402
    _PATH_SANITIZE_EXACT_KEYS,  # noqa: F401
    _PATH_SANITIZE_SUFFIXES,  # noqa: F401
    _SCRUB_STRING_KEYS,  # noqa: F401
    _is_path_field,  # noqa: F401
    _sanitize_cache_payload,
    _scrub_server_paths,  # noqa: F401
    sanitize_envelope,
)
from finance_cli.skills import load_skill, load_skill_profile  # noqa: E402
from finance_cli.perf import (  # noqa: E402
    _record_perf_sample,
    _request_id_var,
    _session_id_var,
    get_request_id,
    get_session_id,
    set_request_id,
    set_session_id,
)
from finance_cli.operation_log import (  # noqa: E402
    current_changelog_id,
    exception_error_metadata,
    operation_error_metadata,
    operation_result_metadata,
    record_operation_log,
    tool_request_metadata,
    utc_now_iso,
)
from finance_cli.sensitive_audit import record_sqlite_sensitive_audit_event  # noqa: E402
from finance_cli.user_context import (  # noqa: E402
    UserContext,
    current_db_path,
    current_local_mode,
    current_rules_path,
    current_uploads_dir,
    get_user_context,
    reset_user_context,
    set_user_context,
)
from finance_cli.spending_analysis import (  # noqa: E402
    category_spending_averages,
    load_essential_categories,
)
from finance_cli.tool_registry import ToolMetadata  # noqa: E402


storage_files = LazyModule("finance_cli.storage_files")
storage_dispatch = LazyModule("finance_cli.storage_client._dispatch")
account_cmd = LazyModule("finance_cli.commands.account_cmd")
balance_cmd = LazyModule("finance_cli.commands.balance_cmd")
biz_cmd = LazyModule("finance_cli.commands.biz_cmd")
budget = LazyModule("finance_cli.commands.budget")
cat = LazyModule("finance_cli.commands.cat")
daily = LazyModule("finance_cli.commands.daily")
debt_cmd = LazyModule("finance_cli.commands.debt_cmd")
db_cmd = LazyModule("finance_cli.commands.db_cmd")
dedup_cmd = LazyModule("finance_cli.commands.dedup_cmd")
export_cmd = LazyModule("finance_cli.commands.export")
goal_cmd = LazyModule("finance_cli.commands.goal_cmd")
ingest = LazyModule("finance_cli.commands.ingest")
intervention_cmd = LazyModule("finance_cli.commands.intervention_cmd")
liability_cmd = LazyModule("finance_cli.commands.liability_cmd")
loan_cmd = LazyModule("finance_cli.commands.loan_cmd")
liquidity_cmd = LazyModule("finance_cli.commands.liquidity_cmd")
memory_cmd = LazyModule("finance_cli.commands.memory_cmd")
monthly_cmd = LazyModule("finance_cli.commands.monthly_cmd")
notify_cmd = LazyModule("finance_cli.commands.notify_cmd")
plaid_cmd = LazyModule("finance_cli.commands.plaid_cmd")
plan = LazyModule("finance_cli.commands.plan")
projection_cmd = LazyModule("finance_cli.commands.projection_cmd")
provider_cmd = LazyModule("finance_cli.commands.provider_cmd")
reminder_cmd = LazyModule("finance_cli.commands.reminder_cmd")
account_alerts = LazyModule("finance_cli.account_alerts")
contractor_tax_prep = LazyModule("finance_cli.contractor_tax_prep")
spending_freeze = LazyModule("finance_cli.spending_freeze")
late_month_buffer = LazyModule("finance_cli.late_month_buffer")
card_paydown_flags = LazyModule("finance_cli.card_paydown_flags")
business_bulk_actions = LazyModule("finance_cli.business_bulk_actions")
home_office_tracking = LazyModule("finance_cli.home_office_tracking")
retirement_targets = LazyModule("finance_cli.retirement_targets")
hysa_transfer_flags = LazyModule("finance_cli.hysa_transfer_flags")
savings_automations = LazyModule("finance_cli.savings_automations")
transaction_disputes = LazyModule("finance_cli.transaction_disputes")
starter_setup = LazyModule("finance_cli.starter_setup")
rules = LazyModule("finance_cli.commands.rules")
schwab_cmd = LazyModule("finance_cli.commands.schwab_cmd")
setup_cmd = LazyModule("finance_cli.commands.setup_cmd")
spending_cmd = LazyModule("finance_cli.commands.spending_cmd")
stripe_cmd = LazyModule("finance_cli.commands.stripe_cmd")
subs = LazyModule("finance_cli.commands.subs")
summary_cmd = LazyModule("finance_cli.commands.summary_cmd")
txn = LazyModule("finance_cli.commands.txn")
weekly = LazyModule("finance_cli.commands.weekly")

sys.stdout = _real_stdout
import mcp.types as mt  # noqa: E402
from fastmcp import FastMCP  # noqa: E402
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext  # noqa: E402
from fastmcp.tools.tool import ToolResult  # noqa: E402

mcp = FastMCP(
    "finance-cli",
    instructions=(
        "Personal finance tools that build understanding over time. "
        "Accumulated vendor memory, spending patterns, and categorization rules "
        "make insights more accurate as financial history grows."
    ),
)


# ---------------------------------------------------------------------------
# Auto-coerce string params → int/bool (MCP-001 fix)
# ---------------------------------------------------------------------------
# FastMCP validates JSON Schema strictly — e.g. "20" fails for int params.
# Patch mcp.tool() so every registered tool auto-coerces string values.


@dataclass
class ToolError(Exception):
    """Structured MCP tool error payload."""

    error_class: str
    message: str
    names_correction: dict[str, Any] | None = None
    suggested_tool_calls: list[dict[str, Any]] | None = None
    recoverable: bool = True

    def __str__(self) -> str:
        return self.message

    def to_envelope(self) -> dict[str, Any]:
        return {
            "status": "error",
            "error": self.message,
            "error_class": self.error_class,
            "message": self.message,
            "names_correction": self.names_correction or {},
            "suggested_tool_calls": self.suggested_tool_calls or [],
            "recoverable": self.recoverable,
        }


def _bind_tool_args(
    sig: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> dict[str, Any]:
    try:
        return dict(sig.bind_partial(*args, **kwargs).arguments)
    except TypeError:
        return dict(kwargs)


def _tool_names_correction(
    tool_name: str,
    sig: inspect.Signature,
    bound_args: dict[str, Any],
) -> dict[str, Any]:
    return {
        "tool": tool_name,
        "valid_arguments": list(sig.parameters),
        "received_arguments": sorted(bound_args),
    }


def _suggested_tool_calls(
    tool_name: str,
    sig: inspect.Signature,
    bound_args: dict[str, Any],
) -> list[dict[str, Any]]:
    params = set(sig.parameters) | set(bound_args)
    suggestions: list[dict[str, Any]] = []

    def add(name: str, args: dict[str, Any] | None = None) -> None:
        call = {"name": name, "args": args or {}}
        if call not in suggestions:
            suggestions.append(call)

    if {"account_id", "account", "account_name", "institution"} & params:
        add("account_list", {"include_inactive": True})
    if {"txn_id", "transaction_id", "transaction_ids"} & params:
        add("txn_list", {"limit": 20})
    if {"category", "categories"} & params:
        add("rules_list", {"limit": 200})
        add("cat_memory_list", {"limit": 20})
    if {"budget_id", "period"} & params or tool_name.startswith("budget_"):
        add("budget_list", {"view": "all"})
    if {"provider", "institution"} & params or tool_name.startswith("provider_"):
        add("provider_status")
    if "plaid" in tool_name or "item_id" in params:
        add("plaid_status")
    if "normalizer" in tool_name or {"key", "canonical_name"} & params:
        add("statement_normalizer_list")
    if tool_name.startswith("interventions_") or "log_id" in params:
        add("interventions_get", {"surface": "agent_prompt"})
    if "error_id" in params:
        add("error_list", {"status": "open", "limit": 20})
    if "issue_id" in params:
        add("issue_list", {"status": "open", "summary_only": True})
    if not suggestions:
        add("setup_status")
    return suggestions[:4]


def _exception_envelope(
    exc: Exception,
    *,
    tool_name: str,
    sig: inspect.Signature,
    bound_args: dict[str, Any],
) -> dict[str, Any]:
    message = str(exc) or exc.__class__.__name__
    return ToolError(
        error_class=exc.__class__.__name__,
        message=message,
        names_correction=_tool_names_correction(tool_name, sig, bound_args),
        suggested_tool_calls=_suggested_tool_calls(tool_name, sig, bound_args),
    ).to_envelope()


def _normalize_error_result(
    result: Any,
    *,
    tool_name: str,
    sig: inspect.Signature,
    bound_args: dict[str, Any],
) -> Any:
    if not isinstance(result, dict) or result.get("status") != "error":
        return result
    if (
        "error_class" in result
        and "message" in result
        and "suggested_tool_calls" in result
    ):
        return result

    message = str(
        result.get("error") or result.get("message") or "Tool returned an error"
    )
    envelope = ToolError(
        error_class=str(result.get("error_class") or "ToolReturnedError"),
        message=message,
        names_correction=result.get("names_correction")
        or _tool_names_correction(tool_name, sig, bound_args),
        suggested_tool_calls=result.get("suggested_tool_calls")
        or _suggested_tool_calls(tool_name, sig, bound_args),
    ).to_envelope()
    merged = dict(result)
    for key, value in envelope.items():
        merged.setdefault(key, value)
    return merged


def _with_structured_error_envelope(fn, *, tool_name: str):
    sig = inspect.signature(fn)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        bound_args = _bind_tool_args(sig, args, kwargs)
        try:
            result = fn(*args, **kwargs)
        except ToolError as exc:
            return exc.to_envelope()
        except Exception as exc:
            return _exception_envelope(
                exc,
                tool_name=tool_name,
                sig=sig,
                bound_args=bound_args,
            )
        return _normalize_error_result(
            result,
            tool_name=tool_name,
            sig=sig,
            bound_args=bound_args,
        )

    wrapper.__signature__ = getattr(fn, "__signature__", sig)
    return wrapper


def _coerce_params(fn):
    """Broaden int/bool params to also accept strings, with auto-coercion."""
    import typing

    sig = inspect.signature(fn)
    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        hints = {}
    coercions: dict[str, type] = {}
    new_params = []
    for name, param in sig.parameters.items():
        ann = hints.get(name, param.annotation)
        if ann is int:
            coercions[name] = int
            new_params.append(param.replace(annotation=int | str))
        elif ann is bool:
            coercions[name] = bool
            new_params.append(param.replace(annotation=bool | str))
        else:
            new_params.append(param)
    if not coercions:
        return fn

    @functools.wraps(fn)
    def wrapper(**kwargs):
        for name, target in coercions.items():
            val = kwargs.get(name)
            if val is None or isinstance(val, target):
                continue
            if target is bool and isinstance(val, str):
                kwargs[name] = val.lower() in ("true", "1", "yes")
            elif target is int and isinstance(val, str):
                kwargs[name] = int(val)
        return fn(**kwargs)

    wrapper.__signature__ = sig.replace(parameters=new_params)
    return wrapper


_orig_mcp_tool = mcp.tool
tool_registry.clear()
_REGISTRY_KWARGS = frozenset(
    {
        "sync_behavior",
        "read_only",
        "approval_required",
        "excluded_from_agent",
        "normalizer",
        "onboarding_auto_approved",
        "coach_debt_payoff_auto_approved",
        "coach_emergency_fund_auto_approved",
        "coach_savings_goal_auto_approved",
        "coach_spending_plan_auto_approved",
        "coach_tax_readiness_auto_approved",
        "coach_homebuying_readiness_auto_approved",
        "coach_retirement_contribution_readiness_auto_approved",
        "coach_retirement_income_readiness_auto_approved",
        "coach_investment_readiness_auto_approved",
        "coach_estate_document_readiness_auto_approved",
        "coach_financial_plan_intake_auto_approved",
        "coach_risk_insurance_readiness_auto_approved",
        "coach_advisor_handoff_readiness_auto_approved",
    }
)


def _tool_with_coercion(*args, **kwargs):
    meta_kwargs = {
        key: kwargs.pop(key) for key in list(kwargs) if key in _REGISTRY_KWARGS
    }
    name_override = kwargs.get("name")
    orig_decorator = _orig_mcp_tool(*args, **kwargs)

    def new_decorator(fn):
        tool_name = name_override or fn.__name__
        wrapped = orig_decorator(
            _with_structured_error_envelope(_coerce_params(fn), tool_name=tool_name)
        )
        tool_registry._register_name(tool_name)
        if meta_kwargs:
            tool_registry.register(tool_name, ToolMetadata(**meta_kwargs))
        return wrapped

    return new_decorator


mcp.tool = _tool_with_coercion  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SENSITIVE_MCP_TOOL_AUDIT_EVENTS: dict[str, tuple[str, str]] = {
    "export_sheets": ("data_export.google_sheets", "export"),
    "export_csv": ("data_export.csv", "export"),
    "export_summary": ("data_export.summary", "export"),
    "export_wave": ("data_export.wave", "export"),
    "db_export_preferences": ("data_export.preferences", "preferences_bundle"),
    "plaid_link": ("plaid.link_started", "plaid_item"),
    "plaid_exchange": ("plaid.link_completed", "plaid_item"),
    "plaid_unlink": ("plaid.unlink", "plaid_item"),
}
_INTERNAL_CONTEXT_ARG_KEYS = {
    "_request_id",
    "_session_id",
    "_storage_lease_id",
    "_storage_mode",
}


def _ns(**kwargs) -> Namespace:
    """Build an argparse Namespace with sensible defaults."""
    defaults = {"format": "json", "verbose": False}
    defaults.update(kwargs)
    return Namespace(**defaults)


def _initialized_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    try:
        initialize_connection(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _get_conn() -> sqlite3.Connection:
    """Get a DB connection using the user-scoped path when present."""
    user_context = get_user_context()
    if user_context is not None and user_context.expected_user_id is not None:
        return _initialized_conn(
            connect(
                db_path=Path(user_context.db_path).expanduser().resolve(),
                busy_timeout=5000,
                expected_user_id=user_context.expected_user_id,
            )
        )
    if user_context is not None:
        resolved = Path(user_context.db_path).expanduser().resolve()
        return _initialized_conn(
            connect(
                db_path=resolved,
                busy_timeout=5000,
            )
        )
    load_dotenv()
    return _initialized_conn(connect(busy_timeout=5000))


def _gateway_storage_scope(
    *,
    user_id: Any,
    storage_mode: Any,
    storage_lease_id: Any,
) -> LeaseScope | None:
    user_id_str = str(user_id or "").strip()
    lease_id = str(storage_lease_id or "").strip()
    mode = str(storage_mode or "").strip().lower()
    if not user_id_str or not lease_id:
        return None
    if mode == "local":
        lease = LocalLease(lease_id)
    elif mode == "remote":
        lease = RemoteLease(lease_id)
    else:
        return None
    return LeaseScope(
        user_id=user_id_str,
        lease=lease,
        session_manager=None,
        owns_lease=False,
    )


def _get_db_path() -> Path:
    """Get the DB path used by the current request context."""
    db_path_str = current_db_path()
    if db_path_str:
        return Path(db_path_str).expanduser().resolve()
    load_dotenv()
    return get_db_path().expanduser().resolve()


def _get_rules_path() -> Path | None:
    """Get a user-scoped rules path when present."""
    rules_path_str = current_rules_path()
    return Path(rules_path_str).expanduser().resolve() if rules_path_str else None


def _get_uploads_dir() -> Path | None:
    """Get a user-scoped uploads directory when present."""
    uploads_dir_str = current_uploads_dir()
    return Path(uploads_dir_str).expanduser().resolve() if uploads_dir_str else None


def _validate_upload_path(file_path: str) -> Path:
    """Resolve a file path and constrain it to the user uploads dir when present.

    In local MCP mode, accepts any readable local path —
    the local stdio MCP runs on the user's own machine so there is no
    cross-tenant path-traversal risk.
    """
    path = Path(file_path).expanduser().resolve()
    if current_local_mode():
        return path
    uploads_dir = _get_uploads_dir()
    if uploads_dir is None:
        return path
    try:
        path.relative_to(uploads_dir)
    except ValueError as exc:
        raise ValueError("file path must be within the user uploads directory") from exc
    return path


def _get_data_dir() -> Path | None:
    """Get the user-scoped data directory when present."""
    db_path_str = current_db_path()
    if db_path_str:
        return Path(db_path_str).expanduser().resolve().parent
    load_dotenv()
    return None


class _RemoteAwareSkillStateStore:
    """SkillStateStore-compatible wrapper that writes remote user state via gRPC."""

    def __init__(self, data_dir: Path, *, user_id: str, target: str) -> None:
        self._user_id = user_id
        self._target = target

    def _read_all(self) -> dict[str, Any]:
        if "skill_state.json" not in storage_files.list_files(
            self._target,
            user_id=self._user_id,
            product="finance_cli",
        ):
            return {}
        payload = json.loads(
            storage_files.read_file(
                self._target,
                user_id=self._user_id,
                product="finance_cli",
                relative_path="skill_state.json",
            ).decode("utf-8")
        )
        return payload if isinstance(payload, dict) else {}

    def _write_all(self, payload: dict[str, Any]) -> None:
        storage_files.write_file(
            self._target,
            user_id=self._user_id,
            product="finance_cli",
            relative_path="skill_state.json",
            content=(json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(
                "utf-8"
            ),
        )

    def get(self, skill_name: str) -> dict[str, Any]:
        payload = self._read_all()
        state = payload.get(skill_name)
        return dict(state) if isinstance(state, dict) else {}

    def set(self, skill_name: str, state: dict[str, Any]) -> None:
        if not isinstance(state, dict):
            raise TypeError("state must be a dict")
        payload = self._read_all()
        payload[str(skill_name)] = dict(state)
        self._write_all(payload)

    def clear(self, skill_name: str) -> None:
        payload = self._read_all()
        if skill_name not in payload:
            return
        payload.pop(skill_name, None)
        self._write_all(payload)


class _LeasedSkillStateStore:
    def __init__(self, inner, *, user_id: str) -> None:
        self._inner = inner
        self._user_id = user_id

    def get(self, skill_name: str) -> dict[str, Any]:
        with optional_lease_scope(
            self._user_id,
            operation="mcp",
            metadata={"source": "mcp.skill_state.get"},
            heartbeat=True,
        ):
            return self._inner.get(skill_name)

    def set(self, skill_name: str, state: dict[str, Any]) -> None:
        with optional_lease_scope(
            self._user_id,
            operation="mcp",
            metadata={"source": "mcp.skill_state.set"},
            heartbeat=True,
        ):
            self._inner.set(skill_name, state)

    def clear(self, skill_name: str) -> None:
        with optional_lease_scope(
            self._user_id,
            operation="mcp",
            metadata={"source": "mcp.skill_state.clear"},
            heartbeat=True,
        ):
            self._inner.clear(skill_name)


def _get_skill_state_store():
    """Return the current user-scoped skill state store."""
    data_dir = _get_data_dir() or _get_db_path().parent
    user_id = storage_dispatch.user_id_from_data_dir(data_dir)
    remote_target = (
        storage_dispatch.remote_file_target_for_user(user_id)
        if user_id is not None
        else None
    )
    if remote_target and user_id is not None:
        return _LeasedSkillStateStore(
            _RemoteAwareSkillStateStore(
                data_dir, user_id=user_id, target=remote_target
            ),
            user_id=user_id,
        )
    store = SkillStateStore(data_dir / "skill_state.json")
    return (
        _LeasedSkillStateStore(store, user_id=user_id) if user_id is not None else store
    )


def _result_envelope(result: dict[str, Any]) -> dict:
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


def _current_audit_user_id() -> str:
    user_context = get_user_context()
    if user_context is not None and str(user_context.expected_user_id or "").strip():
        return str(user_context.expected_user_id).strip()

    from finance_cli import config as config_module

    return str(config_module.default_user_id)


def _record_mcp_sensitive_audit_event(
    *,
    event_type: str,
    target_type: str,
    target_id: object | None = None,
    outcome: str = "succeeded",
    details: dict[str, Any] | None = None,
) -> None:
    try:
        with _get_conn() as conn:
            record_sqlite_sensitive_audit_event(
                conn,
                user_id=_current_audit_user_id(),
                actor_type="agent",
                event_type=event_type,
                target_type=target_type,
                target_id=target_id,
                surface="mcp",
                outcome=outcome,
                request_id=get_request_id(),
                session_id=get_session_id(),
                details=details,
            )
    except Exception:
        logger.warning(
            "mcp_sensitive_audit_write_failed event_type=%s", event_type, exc_info=True
        )


_DEBT_PAYOFF_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "smart_goal",
        "strategy",
        "action_steps",
        "monthly_commitment_cents",
        "debts_in_scope",
    }
)

_DEBT_PAYOFF_ARTIFACT_TEMPLATE = """# Debt Payoff Action Plan
**Generated:** {generated_date}
**SMART goal:** {smart_goal}

## Strategy
- **Chosen strategy:** {strategy_name}
- **Why:** {strategy_why}

## Action Steps
{action_steps}

## Known Obstacles + Mitigations
{obstacles}

## Referrals
{referrals}

## Targets
- **Target debt-free date:** {target_debt_free_date}
- **Monthly commitment:** {monthly_commitment} (constant - does not shrink as balances fall)

## Monitoring
- **Cadence:** {monitoring_cadence}
- **Next check-in:** {next_check_in}

## Generated machine-readable footer (for the constant-payment-rule intervention)
```yaml
{yaml_footer}```
"""


def _debt_payoff_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_debt_payoff"


_ARTIFACT_FILENAME_RE = re.compile(r"^(\d{8})(?:-r(\d+))?\.md$")


def _latest_artifact_path(artifact_dir: Path) -> Path | None:
    """Return the chronologically + revision-latest .md artifact in ``artifact_dir``.

    Filenames follow ``YYYYMMDD.md`` (revision 1) or ``YYYYMMDD-rN.md`` (N >= 2).
    Lexicographic sort over filenames is wrong here because ``-`` (0x2D) sorts
    before ``.`` (0x2E), so ``20260607-r2.md`` would compare less than
    ``20260607.md`` and the base file would (incorrectly) be picked as latest.
    Sort by (date_stem, revision_int) instead.

    Returns ``None`` when the directory contains no matching artifact files.
    Non-matching files (e.g., stray ``notes.md``) are ignored.
    """
    candidates: list[tuple[str, int, Path]] = []
    for path in artifact_dir.glob("*.md"):
        match = _ARTIFACT_FILENAME_RE.match(path.name)
        if match is None:
            continue
        date_stem = match.group(1)
        revision = int(match.group(2)) if match.group(2) else 1
        candidates.append((date_stem, revision, path))
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1][2]


def _format_dollars_from_cents(cents: Any) -> str:
    try:
        amount = int(cents) / 100
    except (TypeError, ValueError):
        return "$0"
    return f"${amount:,.2f}"


def _complete_month_labels(as_of: date, months: int) -> list[str]:
    end_of_last_complete_month = as_of.replace(day=1) - timedelta(days=1)
    cursor = end_of_last_complete_month.replace(day=1)
    labels: list[str] = []
    for _ in range(months):
        labels.append(cursor.strftime("%Y-%m"))
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    labels.reverse()
    return labels


def _income_mix_report(
    *,
    months: int,
    month_labels: list[str],
    sources: list[dict[str, Any]],
    total_income_cents: int,
) -> str:
    lines = [f"Income Mix - Last {months} complete month(s)", ""]
    if month_labels:
        lines.append("Months: " + ", ".join(month_labels))
    lines.append(f"Total income: {_format_dollars_from_cents(total_income_cents)}")
    lines.append("")
    lines.append("Sources")
    if not sources:
        lines.append("  (none)")
        return "\n".join(lines)

    for source in sources:
        lines.append(
            "  "
            f"{str(source['name'])[:24]:<24}"
            f"{_format_dollars_from_cents(source['total_cents']):>14}"
            f"  {int(source['share_pct']):>3}%"
        )
    return "\n".join(lines)


def _render_list_items(value: Any, *, numbered: bool = False) -> str:
    if not value:
        return "- None noted"
    if not isinstance(value, list):
        value = [value]
    lines: list[str] = []
    for index, item in enumerate(value, start=1):
        prefix = f"{index}." if numbered else "-"
        if isinstance(item, dict):
            if "step" in item:
                text = str(item.get("step") or "").strip()
                timeline = str(item.get("timeline") or "").strip()
                if timeline:
                    text = f"{text} ({timeline})"
                if item.get("quick_win"):
                    text = f"{text} [QUICK WIN]"
            elif "description" in item:
                text = str(item.get("description") or "").strip()
                mitigation = str(item.get("mitigation") or "").strip()
                if mitigation:
                    text = f"{text} -> {mitigation}"
            else:
                text = ", ".join(f"{key}: {val}" for key, val in item.items())
        else:
            text = str(item)
        lines.append(f"{prefix} {text or 'TBD'}")
    return "\n".join(lines)


def _normalize_debt_payoff_payload(
    action_plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(action_plan_payload, dict):
        raise ValueError("action_plan_payload must be a dict")
    missing = sorted(_DEBT_PAYOFF_ARTIFACT_REQUIRED_KEYS - set(action_plan_payload))
    if missing:
        raise ValueError(
            f"Missing required action_plan_payload keys: {', '.join(missing)}"
        )
    payload = dict(action_plan_payload)
    generated_at = payload.get("generated_at")
    if not generated_at:
        generated_at = utc_now_iso()
        payload["generated_at"] = generated_at
    return payload


def _generated_at_date(generated_at: Any) -> date:
    raw = str(generated_at or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).date()
    except ValueError:
        return date.fromisoformat(str(generated_at)[:10])


def _render_debt_payoff_artifact(payload: dict[str, Any]) -> str:
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        strategy_name = strategy.get("name") or strategy.get("chosen_strategy") or "TBD"
        strategy_why = strategy.get("why") or strategy.get("rationale") or "TBD"
    else:
        strategy_name = strategy or "TBD"
        strategy_why = payload.get("rationale") or "TBD"

    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _DEBT_PAYOFF_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        smart_goal=payload.get("smart_goal") or "TBD",
        strategy_name=strategy_name,
        strategy_why=strategy_why,
        action_steps=_render_list_items(payload.get("action_steps"), numbered=True),
        obstacles=_render_list_items(payload.get("obstacles")),
        referrals=_render_list_items(payload.get("referrals")),
        target_debt_free_date=payload.get("target_debt_free_date") or "TBD",
        monthly_commitment=_format_dollars_from_cents(
            payload.get("monthly_commitment_cents")
        ),
        monitoring_cadence=payload.get("monitoring_cadence") or "TBD",
        next_check_in=payload.get("next_check_in") or "TBD",
        yaml_footer=footer + "\n",
    )


def _parse_debt_payoff_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _summarize_result(
    result: dict[str, Any], extra_data: dict[str, Any] | None = None
) -> dict:
    """Return a compact summary: cli_report + summary dict + optional extra scalar fields.

    Used by ``summary_only`` modes to avoid sending unbounded match/item lists
    over MCP (which can break Telegram streaming at ~70 KB+).
    """
    data: dict[str, Any] = {"cli_report": result.get("cli_report", "")}
    if extra_data:
        data.update(extra_data)
    return {"data": data, "summary": result.get("summary", {})}


def _bulk_envelope(action: str, results: list[dict[str, Any]]) -> dict:
    failed = sum(1 for item in results if item.get("status") == "error")
    succeeded = len(results) - failed
    return {
        "data": {
            "action": action,
            "results": results,
        },
        "summary": {
            "total": len(results),
            "succeeded": succeeded,
            "failed": failed,
            "status": "success" if failed == 0 else "partial_error",
        },
    }


def _bulk_error(item: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "item": item,
        "error": str(exc),
        "error_class": exc.__class__.__name__,
    }


def _bulk_dict_item(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {"value": value}


def _advisory_pct_to_fraction(value: float) -> Decimal:
    return Decimal(str(value)) / Decimal("100")


def _advisory_decimal_to_str(value: Decimal) -> str:
    return format(value, "f")


def _advisory_fraction_to_pct_str(value: Decimal) -> str:
    return format(value * Decimal("100"), "f").rstrip("0").rstrip(".")


def _advisory_currency(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    whole_dollars, remainder = divmod(abs(cents), 100)
    return f"{sign}${whole_dollars:,}.{remainder:02d}"


def _advisory_nonnegative_int(value: int, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a non-negative integer")
    if isinstance(value, int):
        coerced = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field_name} must be a non-negative integer")
        coerced = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped.isdecimal():
            raise ValueError(f"{field_name} must be a non-negative integer")
        coerced = int(stripped)
    else:
        raise ValueError(f"{field_name} must be a non-negative integer")
    if coerced < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return coerced


def _advisory_round_cents(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _advisory_ratio_pct(numerator_cents: int, denominator_cents: int) -> Decimal:
    return (Decimal(numerator_cents) / Decimal(denominator_cents) * Decimal("100")).quantize(
        Decimal("0.1"),
        rounding=ROUND_HALF_UP,
    )


def _json_safe_advisory(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Decimal):
        return _advisory_decimal_to_str(value)
    if isinstance(value, dict):
        return {key: _json_safe_advisory(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_advisory(item) for item in value]
    return value


def _advisory_envelope(data: dict[str, Any], text: str, **summary: Any) -> dict:
    return {
        "data": _json_safe_advisory(data),
        "summary": {
            "text": text,
            **_json_safe_advisory(summary),
        },
    }


def _retirement_limits_source_metadata(tax_year: int) -> dict[str, Any]:
    sources_by_year: dict[int, dict[str, str]] = {
        2025: {
            "retirement_limits": "IRS Notice 2024-80",
            "hsa_limits": "IRS Rev. Proc. 2024-25",
            "roth_ira_worksheet": "IRS Pub. 590-A Worksheet 2-2",
        },
        2026: {
            "retirement_limits": "IRS Notice 2025-67",
            "hsa_limits": "IRS Rev. Proc. 2025-19",
            "roth_ira_worksheet": "IRS Pub. 590-A Worksheet 2-2",
        },
    }
    return sources_by_year.get(tax_year, {})


def _call_full(
    handler,
    ns_kwargs: dict,
    *,
    pass_rules: bool = False,
    pass_data_dir: bool = False,
    allow_missing_user_db: bool = False,
    _caller_name: str | None = None,
) -> dict[str, Any]:
    """Open a DB connection, call *handler*, and return the raw result dict."""
    extra: dict[str, Any] = {}
    if pass_rules:
        rules_path = _get_rules_path()
        if rules_path is not None:
            extra["rules_path"] = rules_path
    if pass_data_dir:
        data_dir = _get_data_dir()
        if data_dir is not None:
            extra["data_dir"] = data_dir
    if _caller_name is not None:
        tool_name = _caller_name
    else:
        frame = inspect.currentframe()
        try:
            tool_name = (
                frame.f_back.f_code.co_name
                if frame is not None and frame.f_back is not None
                else getattr(handler, "__name__", "unknown")
            )
        finally:
            del frame
    db_path = _get_db_path()
    start = time.perf_counter()
    is_error = False
    request_token = None
    if get_request_id() is None:
        request_token = set_request_id(str(uuid.uuid4()))
    try:
        try:
            conn = _get_conn()
        except ValueError as exc:
            if (
                not allow_missing_user_db
                or get_user_context() is not None
                or "DB path does not live under FINANCE_WEB_DATA_ROOT" not in str(exc)
            ):
                raise
            result = handler(_ns(**ns_kwargs), None, **extra)
        else:
            with conn:
                result = handler(_ns(**ns_kwargs), conn, **extra)
        return result
    except Exception as exc:
        is_error = True
        capture_error(
            exc,
            source="mcp",
            endpoint=tool_name,
            context={
                "request_id": get_request_id() or "",
                "tool_name": tool_name,
                "tool_input_keys": sorted(str(key) for key in ns_kwargs.keys()),
            },
            db_path=db_path,
        )
        raise
    finally:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        _record_perf_sample(
            db_path,
            "tool",
            f"tool.{tool_name}",
            elapsed_ms,
            is_error=is_error,
        )
        audit_meta = _SENSITIVE_MCP_TOOL_AUDIT_EVENTS.get(tool_name)
        if audit_meta is not None:
            event_type, target_type = audit_meta
            _record_mcp_sensitive_audit_event(
                event_type=event_type,
                target_type=target_type,
                outcome="failed" if is_error else "succeeded",
                details={"tool_name": tool_name, "arguments": ns_kwargs},
            )
        if request_token is not None:
            _request_id_var.reset(request_token)


def _call(
    handler,
    ns_kwargs: dict,
    *,
    pass_rules: bool = False,
    pass_data_dir: bool = False,
) -> dict:
    """Open a DB connection, call *handler*, return {data, summary}."""
    frame = inspect.currentframe()
    try:
        caller = (
            frame.f_back.f_code.co_name
            if frame is not None and frame.f_back is not None
            else None
        )
    finally:
        del frame
    return _result_envelope(
        _call_full(
            handler,
            ns_kwargs,
            pass_rules=pass_rules,
            pass_data_dir=pass_data_dir,
            _caller_name=caller,
        )
    )


class UserContextMiddleware(Middleware):
    """Set per-call DB/rules context from server-injected tool arguments."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        args = dict(context.message.arguments or {})
        user_id_arg = args.get("_user_id")
        db_path = args.get("_user_db_path")
        rules_path = args.get("_user_rules_path")
        uploads_dir = args.get("_user_uploads_dir")
        request_id = args.get("_request_id")
        session_id = args.get("_session_id")
        storage_mode = args.get("_storage_mode")
        storage_lease_id = args.get("_storage_lease_id")

        clean_message = mt.CallToolRequestParams(
            name=context.message.name,
            arguments={
                key: value
                for key, value in args.items()
                if not str(key).startswith("_user_")
                and key not in _INTERNAL_CONTEXT_ARG_KEYS
            },
            task=context.message.task,
            meta=context.message.meta,
        )
        clean_context = context.copy(message=clean_message)

        resolved_db_path = (
            Path(str(db_path)).expanduser().resolve() if db_path else None
        )
        normalized_storage_mode = str(storage_mode or "").strip().lower()
        user_context_storage_mode = (
            normalized_storage_mode
            if normalized_storage_mode in {"local", "remote"}
            else None
        )
        token_user_context = (
            set_user_context(
                UserContext.from_paths(
                    db_path=resolved_db_path,
                    expected_user_id=str(user_id_arg) if user_id_arg else None,
                    rules_path=rules_path,
                    uploads_dir=uploads_dir,
                    local_mode=False,
                    storage_mode=user_context_storage_mode,
                )
            )
            if resolved_db_path is not None
            else None
        )
        storage_scope = _gateway_storage_scope(
            user_id=user_id_arg,
            storage_mode=storage_mode,
            storage_lease_id=storage_lease_id,
        )
        token_request = set_request_id(str(request_id)) if request_id else None
        token_session = set_session_id(str(session_id)) if session_id else None
        try:
            if storage_scope is not None:
                storage_scope.__enter__()
            return await call_next(clean_context)
        finally:
            if token_user_context is not None:
                reset_user_context(token_user_context)
            if token_request is not None:
                _request_id_var.reset(token_request)
            if token_session is not None:
                _session_id_var.reset(token_session)
            if storage_scope is not None:
                storage_scope.close()


def _tool_result_payload(result: ToolResult) -> dict[str, Any]:
    if isinstance(result.structured_content, dict):
        payload = dict(result.structured_content)
        payload.setdefault("data", {})
        payload.setdefault("summary", {})
        return payload
    for item in result.content:
        if isinstance(item, mt.TextContent):
            try:
                payload = json.loads(item.text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                payload.setdefault("data", {})
                payload.setdefault("summary", {})
                return payload
    return {"data": {}, "summary": {}}


def _tool_is_mutating(tool_name: str) -> bool:
    for registered_name, metadata in tool_registry.iter_registry():
        if registered_name == tool_name:
            if metadata.sync_behavior == "db_write":
                return True
            if metadata.sync_behavior == "server_proxied" and not metadata.read_only:
                return True
            return False
    return False


def _operation_log_table_exists(conn: Any) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_operation_log'"
        ).fetchone()
        is not None
    )


def _operation_changelog_id() -> int:
    with _get_conn() as conn:
        if not _operation_log_table_exists(conn):
            return 0
        return current_changelog_id(conn)


def _record_mcp_operation(
    *,
    tool_name: str,
    status: str,
    started_at: str,
    started_monotonic: float,
    start_changelog_id: int,
    end_changelog_id: int,
    arguments: dict[str, Any],
    result_metadata: dict[str, Any] | None = None,
    error_metadata: dict[str, Any] | None = None,
) -> None:
    with _get_conn() as conn:
        if not _operation_log_table_exists(conn):
            return
        record_operation_log(
            conn,
            op_type="tool_invocation",
            surface="remote_mcp",
            tool_name=tool_name,
            status=status,
            started_at=started_at,
            started_monotonic=started_monotonic,
            start_changelog_id=start_changelog_id,
            end_changelog_id=end_changelog_id,
            request_metadata=tool_request_metadata(
                arguments=arguments,
                mutating=_tool_is_mutating(tool_name),
            ),
            result_metadata=result_metadata,
            error_metadata=error_metadata,
            idempotency_key=get_request_id(),
        )
        conn.commit()


class OperationLogMiddleware(Middleware):
    """Record remote MCP/gateway tool invocations in the user's operation log."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        user_context = get_user_context()
        if user_context is None or user_context.local_mode:
            return await call_next(context)

        tool_name = str(context.message.name or "")
        arguments = dict(context.message.arguments or {})
        started_at = utc_now_iso()
        started_monotonic = time.monotonic()
        start_op_id = _operation_changelog_id()
        try:
            result = await call_next(context)
        except Exception as exc:
            end_op_id = _operation_changelog_id()
            try:
                _record_mcp_operation(
                    tool_name=tool_name,
                    status="error",
                    started_at=started_at,
                    started_monotonic=started_monotonic,
                    start_changelog_id=start_op_id,
                    end_changelog_id=end_op_id,
                    arguments=arguments,
                    error_metadata=exception_error_metadata(exc),
                )
            except Exception:
                logger.warning("mcp_operation_log_write_failed", exc_info=True)
            raise

        end_op_id = _operation_changelog_id()
        payload = _tool_result_payload(result)
        error_metadata = operation_error_metadata(payload)
        try:
            _record_mcp_operation(
                tool_name=tool_name,
                status="error" if error_metadata else "success",
                started_at=started_at,
                started_monotonic=started_monotonic,
                start_changelog_id=start_op_id,
                end_changelog_id=end_op_id,
                arguments=arguments,
                result_metadata=operation_result_metadata(payload),
                error_metadata=error_metadata,
            )
        except Exception:
            logger.warning("mcp_operation_log_write_failed", exc_info=True)
        return result


class PathSanitizeMiddleware(Middleware):
    """Strip server directory prefixes from gateway tool results and errors."""

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        try:
            result = await call_next(context)
        except Exception as exc:
            if current_db_path() is not None:
                if exc.args and isinstance(exc.args[0], str):
                    scrubbed = _scrub_server_paths(exc.args[0])
                    if scrubbed != exc.args[0]:
                        exc.args = (scrubbed,) + exc.args[1:]
                if isinstance(exc, OSError):
                    if isinstance(exc.filename, str):
                        exc.filename = Path(exc.filename).name
                    if isinstance(exc.filename2, str):
                        exc.filename2 = Path(exc.filename2).name
            raise

        if current_db_path() is None:
            return result

        return _sanitize_tool_result(result)


mcp.add_middleware(UserContextMiddleware())
mcp.add_middleware(OperationLogMiddleware())
mcp.add_middleware(PathSanitizeMiddleware())


_TXN_STRIP_FIELDS = {
    "raw_plaid_json",
    "dedupe_key",
    "split_group_id",
    "parent_transaction_id",
    "split_pct",
    "split_note",
    "removed_at",
    "created_at",
    "updated_at",
}

_CACHE_TTL_HOURS = 24
_READTHROUGH_BYTE_CAP = 50 * 1024
_READTHROUGH_LIMIT_MAX = 50
_READTHROUGH_SUFFIX = ".readthrough.json"
_CACHE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
_AVAILABLE_KEYS_CAP = 50
_CACHE_NOT_FOUND_ERROR = (
    "Cache file not found. It may have expired (24h TTL). "
    "Re-run the original tool to generate a fresh cache."
)
_CACHE_SECURITY_ERROR = "Invalid cache_id."


def _cache_dir() -> Path:
    db_path_str = current_db_path()
    if db_path_str:
        return Path(db_path_str).expanduser().resolve().parent / "mcp_cache"
    return Path(__file__).resolve().parent.parent / "exports" / "mcp_cache"


def _cleanup_cache(cache_dir: Path, max_age_hours: int = _CACHE_TTL_HOURS) -> None:
    """Best-effort purge for stale cache files across both cache suffixes."""
    if not cache_dir.exists():
        return

    cutoff_ts = datetime.now().timestamp() - (max_age_hours * 3600)
    for cache_file in cache_dir.glob("*.json"):
        try:
            if not cache_file.is_file() and not cache_file.is_symlink():
                continue
            if cache_file.stat().st_mtime < cutoff_ts:
                cache_file.unlink()
        except Exception:
            continue


def _cache_slug(tool_name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    short_id = uuid.uuid4().hex[:8]
    return f"{tool_name}_{timestamp}_{short_id}"


def _sanitize_tool_result(result: ToolResult) -> ToolResult:
    content = result.content
    if isinstance(content, list):
        sanitized_content = []
        for block in content:
            if isinstance(block, mt.TextContent):
                try:
                    payload = json.loads(block.text)
                except (TypeError, ValueError):
                    sanitized_content.append(block)
                    continue
                sanitized_content.append(
                    block.model_copy(
                        update={
                            "text": json.dumps(sanitize_envelope(payload), default=str)
                        }
                    )
                )
                continue
            sanitized_content.append(block)
    else:
        sanitized_content = content

    structured_content = result.structured_content
    if isinstance(structured_content, dict):
        structured_content = sanitize_envelope(structured_content)

    return result.model_copy(
        update={
            "content": sanitized_content,
            "structured_content": structured_content,
        }
    )


def _write_cache(tool_name: str, data: Any) -> str:
    """Write full MCP tool data to exports/mcp_cache and return file path."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_cache(cache_dir)
    file_path = cache_dir / f"{_cache_slug(tool_name)}.json"
    file_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    return str(file_path)


def _write_cache_safe(tool_name: str, data: Any) -> str:
    """Write a sanitized read-through cache and return its opaque cache slug."""
    cache_dir = _cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_cache(cache_dir)
    cache_id = _cache_slug(tool_name)
    file_path = cache_dir / f"{cache_id}{_READTHROUGH_SUFFIX}"
    payload = (
        sanitize_envelope(data)
        if isinstance(data, dict)
        else _sanitize_cache_payload(data)
    )
    file_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return cache_id


def _summarize_result_with_cache(
    tool_name: str,
    result: dict[str, Any],
    extra_data: dict[str, Any] | None = None,
) -> dict:
    data = dict(extra_data or {})
    data["cache_id"] = _write_cache_safe(tool_name, _result_envelope(result))
    return _summarize_result(result, data)


def _result_with_optional_cache(
    tool_name: str,
    result: dict[str, Any],
    *,
    summary_only: bool,
    extra_data: dict[str, Any] | None = None,
) -> dict:
    if summary_only:
        return _summarize_result_with_cache(tool_name, result, extra_data)
    return _result_envelope(result)


def _readthrough_response_size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, default=str).encode("utf-8"))


def _available_keys_payload(value: Any) -> dict[str, Any]:
    keys = sorted(str(key) for key in value.keys()) if isinstance(value, dict) else []
    payload: dict[str, Any] = {"available_keys": keys[:_AVAILABLE_KEYS_CAP]}
    if len(keys) > _AVAILABLE_KEYS_CAP:
        payload["keys_truncated"] = True
    return payload


def _cache_not_found_response() -> dict:
    return {"data": {}, "summary": {"error": _CACHE_NOT_FOUND_ERROR}}


def _cache_security_rejection(message: str | None = None) -> dict:
    return {"data": {}, "summary": {"error": message or _CACHE_SECURITY_ERROR}}


def _resolved_value_type(value: Any) -> str:
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "list"
    return "scalar"


def _invalid_list_index_error(segment: str, list_length: int) -> str:
    if list_length > 0:
        return (
            f"Key '{segment}' is not a valid list index. "
            f"Use a numeric index (0-{list_length - 1})."
        )
    return f"Key '{segment}' is not a valid list index. Use a numeric index."


def _resolve_cache_value(data: Any, key: str) -> tuple[str, Any] | dict:
    if not key:
        if not isinstance(data, dict):
            return {
                "data": _available_keys_payload({}),
                "summary": {"error": "No top-level list found in data. Specify a key."},
            }

        chosen_key: str | None = None
        chosen_size = -1
        for candidate in sorted(str(cache_key) for cache_key in data.keys()):
            value = data.get(candidate)
            if not isinstance(value, list):
                continue
            size = len(value)
            if size > chosen_size:
                chosen_key = candidate
                chosen_size = size

        if chosen_key is None:
            return {
                "data": _available_keys_payload(data),
                "summary": {"error": "No top-level list found in data. Specify a key."},
            }
        return chosen_key, data[chosen_key]

    current = data
    traversed: list[str] = []
    for segment in key.split("."):
        if isinstance(current, dict):
            if segment not in current:
                return {
                    "data": _available_keys_payload(current),
                    "summary": {"error": f"Key '{segment}' not found in data"},
                }
            current = current[segment]
            traversed.append(segment)
            continue
        if isinstance(current, list):
            if not re.fullmatch(r"-?\d+", segment):
                return {
                    "data": {"available_keys": [], "list_length": len(current)},
                    "summary": {
                        "error": _invalid_list_index_error(segment, len(current))
                    },
                }
            index = int(segment)
            if index < 0 or index >= len(current):
                return {
                    "data": {"available_keys": [], "list_length": len(current)},
                    "summary": {
                        "error": f"Index {index} out of range (list has {len(current)} items)"
                    },
                }
            current = current[index]
            traversed.append(segment)
            continue
        scalar_path = ".".join(traversed) or key
        return {
            "data": {"available_keys": []},
            "summary": {
                "error": f"Cannot traverse into scalar value at '{scalar_path}'"
            },
        }
    return key, current


def _list_page_response(
    key: str,
    items: list[Any],
    *,
    total: int,
    offset: int,
    limit: int,
    truncated: bool = False,
) -> dict:
    data: dict[str, Any] = {"items": items, "key": key}
    if truncated:
        data["truncated"] = True
    return {
        "data": data,
        "summary": {
            "total": total,
            "offset": offset,
            "limit": limit,
            "returned": len(items),
        },
    }


def _oversized_value_response(key: str, value: Any, size_bytes: int) -> dict:
    payload = _available_keys_payload(value)
    if isinstance(value, dict):
        error = (
            f"Value too large ({size_bytes} bytes). Use a more specific dot-path key."
        )
    else:
        error = (
            f"Value too large ({size_bytes} bytes). "
            "Value is a scalar — no further drill-down available."
        )
    return {"data": payload, "summary": {"error": error, "key": key}}


def _oversized_list_item_response(
    key: str, item: Any, item_index: int, size_bytes: int
) -> dict:
    payload: dict[str, Any] = {"item_index": item_index}
    payload.update(_available_keys_payload(item))
    if isinstance(item, dict):
        error = (
            f"Single item at index {item_index} too large ({size_bytes} bytes). "
            f"Drill deeper with key '{key}.{item_index}.<sub_key>'."
        )
    elif isinstance(item, list):
        payload["item_type"] = "list"
        payload["item_length"] = len(item)
        error = (
            f"Single item at index {item_index} too large ({size_bytes} bytes). "
            f"Item is a list ({len(item)} items) — drill deeper with key "
            f"'{key}.{item_index}.<index>'."
        )
    else:
        payload["item_type"] = "scalar"
        error = (
            f"Single item at index {item_index} too large ({size_bytes} bytes). "
            "Item is a scalar — no further drill-down available."
        )
    return {"data": payload, "summary": {"error": error, "key": key}}


def _export_output_path(prefix: str) -> str:
    """Build a timestamped CSV output path under exports/."""
    export_dir = Path(__file__).resolve().parent.parent / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return str(export_dir / f"{prefix}_{timestamp}.csv")


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalizer_test_state_path() -> Path:
    staging_dir = staging_normalizers_dir()
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / ".test_passes.json"


def _load_normalizer_test_state() -> dict[str, dict[str, Any]]:
    path = _normalizer_test_state_path()
    if not normalizer_file_exists(path):
        return {}
    try:
        payload = json.loads(read_normalizer_text(path))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(key): value for key, value in payload.items() if isinstance(value, dict)
    }


def _write_normalizer_test_state(state: dict[str, dict[str, Any]]) -> None:
    path = _normalizer_test_state_path()
    write_normalizer_text(path, json.dumps(state, indent=2, sort_keys=True) + "\n")


def _clear_normalizer_test_state(key: str, *, best_effort: bool = False) -> None:
    state = _load_normalizer_test_state()
    if key in state:
        state.pop(key, None)
        try:
            _write_normalizer_test_state(state)
        except Exception as exc:
            if not best_effort:
                raise
            logger.warning(
                "Failed to clear normalizer test state key=%s error=%s",
                key,
                exc,
            )


def _record_normalizer_test_pass(
    key: str, module_path: Path, content_hash: str
) -> None:
    state = _load_normalizer_test_state()
    state[key] = {
        "content_hash": content_hash,
        "module_path": str(module_path),
        "tested_at": datetime.now().isoformat(),
    }
    _write_normalizer_test_state(state)


def _active_user_normalizer_path(key: str) -> Path:
    return resolve_user_normalizers_dir() / f"{key}.py"


def _staged_user_normalizer_path(key: str) -> Path:
    return staging_normalizers_dir() / f"{key}.py"


def _validate_normalizer_source_for_key(key: str, source: str):
    metadata = validate_normalizer_source(source, filename=f"{key}.py")
    if normalize_registry_key(metadata.primary_key) != key:
        raise ValueError(
            f"PRIMARY_KEY '{metadata.primary_key}' does not match requested key '{key}'"
        )
    if not is_known(metadata.source_name):
        raise ValueError(
            f"SOURCE_NAME '{metadata.source_name}' is not registered; use normalizer_register_institution first"
        )
    return metadata


def _normalize_source_blob(source: str) -> str:
    stripped = source.rstrip()
    return f"{stripped}\n" if stripped else ""


def _assert_normalizer_key_conflicts(key: str, metadata) -> None:
    loader = get_normalizer_loader()
    active_entry = loader.get_entry(key)
    allowed_keys = {key}
    if active_entry and active_entry.tier == USER_TIER:
        allowed_keys.update(active_entry.keys)

    conflicts: list[str] = []
    for raw_key in [metadata.primary_key, *metadata.aliases]:
        normalized = normalize_registry_key(raw_key)
        entry = loader.get_entry(normalized)
        if entry is None:
            continue
        if normalized in allowed_keys:
            continue
        conflicts.append(normalized)

    if conflicts:
        raise ValueError(
            f"normalizer '{key}' conflicts with existing registered keys: {', '.join(sorted(set(conflicts)))}"
        )


def _run_normalizer_preview(key: str, file_path: Path) -> dict[str, Any]:
    normalized_key = normalize_registry_key(key)
    staged_path = _staged_user_normalizer_path(normalized_key)
    active_path = _active_user_normalizer_path(normalized_key)
    loader = get_normalizer_loader()

    if normalizer_file_exists(staged_path):
        metadata = load_user_module_metadata(staged_path)
        result, validation = run_user_normalizer_normalize_with_validation(
            staged_path,
            file_path,
            expected_source_name=metadata.source_name,
        )
        source_text = read_normalizer_text(staged_path)
        if validation["valid"]:
            _record_normalizer_test_pass(
                normalized_key, staged_path, _sha256_text(source_text)
            )
        return {
            "content_hash": _sha256_text(source_text),
            "module_path": staged_path,
            "result": result,
            "source_name": metadata.source_name,
            "tier": "staged_user",
            "validation": validation,
        }

    entry = loader.get_entry(normalized_key)
    if entry is None:
        raise ValueError(f"unsupported normalizer '{key}'")

    if entry.tier == USER_TIER:
        result, validation = run_user_normalizer_normalize_with_validation(
            active_path,
            file_path,
            expected_source_name=entry.source_name,
        )
        content_hash = normalizer_file_content_hash(active_path)
    else:
        result = normalize_csv(file_path, normalized_key)
        validation = validate_normalize_result(
            result, expected_source_name=entry.source_name
        )
        content_hash = ""

    return {
        "content_hash": content_hash,
        "module_path": active_path if entry.tier == USER_TIER else entry.file_path,
        "result": result,
        "source_name": entry.source_name,
        "tier": entry.tier,
        "validation": validation,
    }


def _strip_txn_fields(txn_dict: dict[str, Any]) -> dict[str, Any]:
    """Return a new transaction dict with verbose/large fields removed."""
    return {
        key: value for key, value in txn_dict.items() if key not in _TXN_STRIP_FIELDS
    }


_SIMULATION_CAP_CAVEAT = (
    "Baseline minimum-payment simulation did not fully pay off within the 360-month cap; "
    "time/interest savings estimates are approximate."
)

_WORKFLOW_SECTIONS = {
    "gap_analysis": "Financial Gap Analysis & Action Planning",
    "monthly_review": "Monthly Financial Review",
    "debt_planning": "Debt Payoff Planning",
    "goal_tracking": "Goal Setting & Tracking",
    "business_tax": "Business Accounting & Tax Compliance",
    "category_design": "Category Taxonomy Design",
    "subscription_audit": "Subscription Audit",
    "budget_setting": "Expense Budget Setting",
    "budget_monitoring": "Budget Monitoring & Alerts",
    "category_cleanup": "Category Data Quality Cleanup",
    "post_import_qa": "Post-Import QA",
    "new_user_onboarding": "AI-Driven Onboarding",
}


def _attach_simulation_cap_caveat(result: dict[str, Any]) -> dict[str, Any]:
    data = dict(result.get("data", {}))
    baseline = data.get("baseline")
    if isinstance(baseline, dict) and baseline.get("fully_paid_off") is False:
        existing_caveat = str(data.get("caveat") or "").strip()
        data["caveat"] = (
            f"{existing_caveat} {_SIMULATION_CAP_CAVEAT}".strip()
            if existing_caveat
            else _SIMULATION_CAP_CAVEAT
        )
    return {"data": data, "summary": result.get("summary", {})}


def _unknown_workflow_response() -> dict:
    available = list(_WORKFLOW_SECTIONS.keys())
    return {
        "data": {"available": available},
        "summary": {"error": "Unknown workflow", "available": available},
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def read_mcp_cache(
    cache_id: str, key: str = "", offset: int = 0, limit: int = 20
) -> dict:
    """Read a paginated page from a sanitized MCP cache created by summary-only tools.

    Args:
        cache_id: Opaque cache slug returned by another MCP tool.
        key: Optional dot-path into cached data. Empty auto-selects the largest top-level list.
        offset: Starting item offset for list pagination.
        limit: Requested page size, clamped to 1-50.

    Discovery: cache_id is returned by summary-only MCP tools that spill large sanitized payloads to cache.
    """
    if "/" in cache_id or "\\" in cache_id or ".." in cache_id:
        return _cache_security_rejection()
    if not _CACHE_ID_PATTERN.fullmatch(cache_id):
        return _cache_security_rejection()

    cache_dir = _cache_dir()
    resolved_cache_dir = cache_dir.resolve(strict=False)
    cache_path = cache_dir / f"{cache_id}{_READTHROUGH_SUFFIX}"
    resolved_path = cache_path.resolve(strict=False)
    if resolved_path.parent != resolved_cache_dir:
        return _cache_security_rejection()
    if cache_path.is_symlink():
        return _cache_security_rejection()
    if not cache_path.exists():
        return _cache_not_found_response()

    cutoff_ts = datetime.now().timestamp() - (_CACHE_TTL_HOURS * 3600)
    try:
        stat_result = cache_path.stat()
    except OSError:
        return _cache_not_found_response()
    if stat_result.st_mtime < cutoff_ts:
        try:
            cache_path.unlink()
        except OSError:
            pass
        return _cache_not_found_response()

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return _cache_not_found_response()

    root_data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if not isinstance(root_data, dict):
        root_data = {}

    resolved = _resolve_cache_value(root_data, key)
    if isinstance(resolved, dict):
        return resolved

    resolved_key, resolved_value = resolved
    offset = max(0, offset)
    limit = max(1, min(limit, _READTHROUGH_LIMIT_MAX))

    if isinstance(resolved_value, list):
        total = len(resolved_value)
        if offset >= total:
            return _list_page_response(
                resolved_key,
                [],
                total=total,
                offset=offset,
                limit=limit,
            )

        natural_page = min(limit, total - offset)
        best_size = 0
        low = 1
        high = natural_page
        while low <= high:
            mid = (low + high) // 2
            candidate = _list_page_response(
                resolved_key,
                resolved_value[offset : offset + mid],
                total=total,
                offset=offset,
                limit=limit,
                truncated=mid < natural_page,
            )
            if _readthrough_response_size(candidate) <= _READTHROUGH_BYTE_CAP:
                best_size = mid
                low = mid + 1
            else:
                high = mid - 1

        if best_size == 0:
            single_item = resolved_value[offset]
            oversized_single = _list_page_response(
                resolved_key,
                resolved_value[offset : offset + 1],
                total=total,
                offset=offset,
                limit=limit,
                truncated=1 < natural_page,
            )
            return _oversized_list_item_response(
                resolved_key,
                single_item,
                offset,
                _readthrough_response_size(oversized_single),
            )

        return _list_page_response(
            resolved_key,
            resolved_value[offset : offset + best_size],
            total=total,
            offset=offset,
            limit=limit,
            truncated=best_size < natural_page,
        )

    response = {
        "data": {"value": resolved_value, "key": resolved_key},
        "summary": {"type": _resolved_value_type(resolved_value)},
    }
    response_size = _readthrough_response_size(response)
    if response_size > _READTHROUGH_BYTE_CAP:
        return _oversized_value_response(resolved_key, resolved_value, response_size)
    return response


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_future_value(
    principal_cents: int,
    annual_rate_pct: float,
    years: int,
    monthly_contribution_cents: int = 0,
) -> dict:
    """Project the future value of savings at an annual return rate.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `future_value` directly from `finance_cli.advisory`.
    """
    annual_rate = _advisory_pct_to_fraction(annual_rate_pct)
    projected_cents = future_value(
        principal_cents=principal_cents,
        annual_rate=annual_rate,
        years=years,
        monthly_contribution_cents=monthly_contribution_cents,
    )
    data = {
        "future_value_cents": projected_cents,
        "principal_cents": principal_cents,
        "annual_rate_pct": str(Decimal(str(annual_rate_pct))),
        "years": years,
        "monthly_contribution_cents": monthly_contribution_cents,
    }
    return _advisory_envelope(
        data,
        (
            f"Projected value after {years} years at {annual_rate_pct}% is "
            f"{_advisory_currency(projected_cents)}."
        ),
        future_value_cents=projected_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_fee_impact(
    balance_cents: int,
    current_fee_pct: float,
    proposed_fee_pct: float,
    years: int,
    annual_return_pct: float = 8.0,
    monthly_contribution_cents: int = 0,
) -> dict:
    """Compare compounded portfolio outcomes under two fee levels.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `fee_impact` directly from `finance_cli.advisory`.
    """
    result = fee_impact(
        balance_cents=balance_cents,
        current_fee_pct=_advisory_pct_to_fraction(current_fee_pct),
        proposed_fee_pct=_advisory_pct_to_fraction(proposed_fee_pct),
        years=years,
        annual_return=_advisory_pct_to_fraction(annual_return_pct),
        monthly_contribution_cents=monthly_contribution_cents,
    )
    data = {
        "balance_cents": balance_cents,
        "current_total_cents": result.current_total_cents,
        "proposed_total_cents": result.proposed_total_cents,
        "savings_cents": result.savings_cents,
        "years": years,
        "annual_return_pct": str(Decimal(str(annual_return_pct))),
        "current_fee_pct": str(Decimal(str(current_fee_pct))),
        "proposed_fee_pct": str(Decimal(str(proposed_fee_pct))),
        "monthly_contribution_cents": monthly_contribution_cents,
    }
    return _advisory_envelope(
        data,
        (
            f"Switching from {current_fee_pct}% to {proposed_fee_pct}% saves "
            f"{_advisory_currency(result.savings_cents)} over {years} years."
        ),
        savings_cents=result.savings_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_time_to_goal(
    current_cents: int,
    goal_cents: int,
    monthly_contribution_cents: int,
    annual_rate_pct: float = 8.0,
) -> dict:
    """Estimate months needed to reach a savings goal.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `time_to_goal` directly from `finance_cli.advisory`.
    """
    months = time_to_goal(
        current_cents=current_cents,
        goal_cents=goal_cents,
        monthly_contribution_cents=monthly_contribution_cents,
        annual_rate=_advisory_pct_to_fraction(annual_rate_pct),
    )
    data = {
        "current_cents": current_cents,
        "goal_cents": goal_cents,
        "monthly_contribution_cents": monthly_contribution_cents,
        "annual_rate_pct": str(Decimal(str(annual_rate_pct))),
        "months_to_goal": months,
    }
    if months is None:
        text = "The goal is unreachable within the advisory helper's 60-year cap."
    elif months == 0:
        text = "The current balance already meets the goal."
    else:
        text = f"At {annual_rate_pct}% and the current contribution rate, the goal takes {months} months."
    return _advisory_envelope(data, text, months_to_goal=months)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_runway(
    balance_cents: int,
    monthly_spend_cents: int,
    annual_return_pct: float = 4.0,
) -> dict:
    """Estimate how long a portfolio can fund a monthly spend rate.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `runway_projection` directly from `finance_cli.advisory`.
    """
    months = runway_projection(
        balance_cents=balance_cents,
        monthly_spend_cents=monthly_spend_cents,
        annual_return=_advisory_pct_to_fraction(annual_return_pct),
    )
    data = {
        "balance_cents": balance_cents,
        "monthly_spend_cents": monthly_spend_cents,
        "annual_return_pct": str(Decimal(str(annual_return_pct))),
        "runway_months": months,
    }
    if months is None:
        text = "At this spend and return rate, the balance does not exhaust within the helper horizon."
    else:
        text = f"The balance lasts about {months} months at the current spend and return assumptions."
    return _advisory_envelope(data, text, runway_months=months)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_taxable_income_from_gross(
    gross_income_cents: int,
    filing_status: str = "single",
    tax_year: int = 2026,
    itemized_deductions_cents: int = 0,
    above_the_line_adjustments_cents: int = 0,
) -> dict:
    """Convert gross income into post-deduction taxable income.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `taxable_income_from_gross` directly from
    `finance_cli.advisory`.
    """
    taxable_income_cents = taxable_income_from_gross(
        gross_income_cents=gross_income_cents,
        filing_status=filing_status,
        tax_year=tax_year,
        itemized_deductions_cents=itemized_deductions_cents,
        above_the_line_adjustments_cents=above_the_line_adjustments_cents,
    )
    agi_cents = max(gross_income_cents - above_the_line_adjustments_cents, 0)
    standard_deduction_cents = STANDARD_DEDUCTION[tax_year][filing_status]
    deduction_applied_cents = max(standard_deduction_cents, itemized_deductions_cents)
    deduction_type = (
        "itemized"
        if itemized_deductions_cents > standard_deduction_cents
        else "standard"
    )
    data = {
        "gross_income_cents": gross_income_cents,
        "agi_cents": agi_cents,
        "taxable_income_cents": taxable_income_cents,
        "deduction_applied_cents": deduction_applied_cents,
        "deduction_type": deduction_type,
        "filing_status": filing_status,
        "tax_year": tax_year,
        "itemized_deductions_cents": itemized_deductions_cents,
        "above_the_line_adjustments_cents": above_the_line_adjustments_cents,
    }
    return _advisory_envelope(
        data,
        (
            f"Taxable income after {deduction_type} deduction is "
            f"{_advisory_currency(taxable_income_cents)} for tax year {tax_year}."
        ),
        taxable_income_cents=taxable_income_cents,
        deduction_type=deduction_type,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_federal_tax(
    taxable_income_cents: int,
    filing_status: str = "single",
    tax_year: int = 2026,
    include_fica: bool = False,
    gross_wages_cents: int = 0,
    net_se_earnings_cents: int = 0,
) -> dict:
    """Estimate federal income tax, bracket room, and optional FICA.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `federal_tax` directly from `finance_cli.advisory`.
    """
    income_tax = federal_tax(
        taxable_income_cents=taxable_income_cents,
        filing_status=filing_status,
        tax_year=tax_year,
    )
    bracket_room_cents = bracket_room(
        taxable_income_cents=taxable_income_cents,
        filing_status=filing_status,
        tax_year=tax_year,
    )
    data = {
        "taxable_income_cents": taxable_income_cents,
        "filing_status": filing_status,
        "tax_year": tax_year,
        "tax_owed_cents": income_tax.tax_owed_cents,
        "marginal_rate_pct": _advisory_decimal_to_str(income_tax.marginal_rate_pct),
        "effective_rate_pct": _advisory_decimal_to_str(income_tax.effective_rate_pct),
        "bracket_room_cents": bracket_room_cents,
        "include_fica": include_fica,
        "gross_wages_cents": gross_wages_cents,
        "net_se_earnings_cents": net_se_earnings_cents,
    }
    total_tax_cents = income_tax.tax_owed_cents
    if include_fica:
        fica_result = fica_tax(
            gross_wages_cents=gross_wages_cents,
            net_se_earnings_cents=net_se_earnings_cents,
            filing_status=filing_status,
            tax_year=tax_year,
        )
        data["fica"] = _json_safe_advisory(fica_result)
        total_tax_cents += fica_result.total_cents
        data["total_tax_cents"] = total_tax_cents
    return _advisory_envelope(
        data,
        (
            f"Federal income tax on {_advisory_currency(taxable_income_cents)} taxable income is "
            f"{_advisory_currency(income_tax.tax_owed_cents)} at a "
            f"{income_tax.marginal_rate_pct}% marginal rate."
        ),
        tax_owed_cents=income_tax.tax_owed_cents,
        total_tax_cents=total_tax_cents,
        marginal_rate_pct=_advisory_decimal_to_str(income_tax.marginal_rate_pct),
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_debt_vs_invest(
    debt_balance_cents: int,
    debt_apr_pct: float,
    monthly_extra_payment_cents: int,
    debt_minimum_payment_cents: int,
    expected_market_return_pct: float = 8.0,
    marginal_tax_rate_pct: float = 0.0,
    is_tax_deductible: bool = False,
    risk_tolerance: str = "moderate",
) -> dict:
    """Compare extra debt payoff against investing the same monthly dollars.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `debt_vs_invest` directly from `finance_cli.advisory`.
    """
    result = debt_vs_invest(
        debt_balance_cents=debt_balance_cents,
        debt_apr=_advisory_pct_to_fraction(debt_apr_pct),
        monthly_extra_payment_cents=monthly_extra_payment_cents,
        debt_minimum_payment_cents=debt_minimum_payment_cents,
        expected_market_return=_advisory_pct_to_fraction(expected_market_return_pct),
        marginal_tax_rate=_advisory_pct_to_fraction(marginal_tax_rate_pct),
        is_tax_deductible=is_tax_deductible,
        risk_tolerance=risk_tolerance,
    )
    data = {
        "debt_balance_cents": debt_balance_cents,
        "debt_apr_pct": str(Decimal(str(debt_apr_pct))),
        "debt_effective_apr_pct": _advisory_fraction_to_pct_str(
            result.debt_effective_apr
        ),
        "expected_market_return_pct": str(Decimal(str(expected_market_return_pct))),
        "marginal_tax_rate_pct": str(Decimal(str(marginal_tax_rate_pct))),
        "monthly_extra_payment_cents": monthly_extra_payment_cents,
        "debt_minimum_payment_cents": debt_minimum_payment_cents,
        "is_tax_deductible": is_tax_deductible,
        "risk_tolerance": risk_tolerance,
        "recommendation": result.recommendation,
        "reason": result.reason,
        "debt_payoff_months": result.debt_payoff_months,
        "debt_interest_saved_cents": result.debt_interest_saved_cents,
        "investment_value_at_debt_payoff_cents": result.investment_value_at_debt_payoff_cents,
        "difference_cents": result.difference_cents,
    }
    return _advisory_envelope(
        data,
        result.reason,
        recommendation=result.recommendation,
        difference_cents=result.difference_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_roth_vs_traditional(
    contribution_cents: int,
    current_marginal_rate_pct: float,
    estimated_retirement_marginal_rate_pct: float,
    years_to_retirement: int,
    expected_annual_return_pct: float = 8.0,
) -> dict:
    """Compare Roth and Traditional retirement contributions.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `roth_vs_traditional` directly from `finance_cli.advisory`.

    Related tools: advisory_roth_conversion_analysis.
    """
    result = roth_vs_traditional(
        contribution_cents=contribution_cents,
        current_marginal_rate_pct=Decimal(str(current_marginal_rate_pct)),
        estimated_retirement_marginal_rate_pct=Decimal(
            str(estimated_retirement_marginal_rate_pct)
        ),
        years_to_retirement=years_to_retirement,
        expected_annual_return=_advisory_pct_to_fraction(expected_annual_return_pct),
    )
    return _advisory_envelope(
        _json_safe_advisory(result),
        result.reason,
        winner=result.winner,
        advantage_cents=result.advantage_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_roth_conversion_analysis(
    conversion_amount_cents: int,
    current_marginal_rate_pct: float,
    estimated_retirement_marginal_rate_pct: float,
    years_to_retirement: int,
    expected_annual_return_pct: float = 8.0,
) -> dict:
    """Analyze a Roth conversion using the helper's future-dollars model.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `roth_conversion_analysis` directly from `finance_cli.advisory`.

    Related tools: advisory_roth_vs_traditional.
    """
    result = roth_conversion_analysis(
        conversion_amount_cents=conversion_amount_cents,
        current_marginal_rate_pct=Decimal(str(current_marginal_rate_pct)),
        estimated_retirement_marginal_rate_pct=Decimal(
            str(estimated_retirement_marginal_rate_pct)
        ),
        years_to_retirement=years_to_retirement,
        expected_annual_return=_advisory_pct_to_fraction(expected_annual_return_pct),
    )
    return _advisory_envelope(
        _json_safe_advisory(result),
        result.reason,
        recommendation=result.recommendation,
        net_advantage_cents=result.net_advantage_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_contribution_priority(
    taxable_income_cents: int,
    filing_status: str = "single",
    modified_agi_cents: int = 0,
    annual_salary_cents: int = 0,
    earned_compensation_cents: int | None = None,
    other_ira_contributions_cents: int = 0,
    tax_year: int = 2026,
    employer_match_pct: float = 0.0,
    employer_match_limit_pct: float = 0.0,
    has_mega_backdoor: bool = False,
    has_hsa_eligible_hdhp: bool = False,
    hsa_family_coverage: bool = False,
    age: int = 40,
    existing_emergency_fund_cents: int = 0,
    monthly_expenses_cents: int = 0,
    target_emergency_months: int = 3,
    starter_emergency_threshold_cents: int = 1_000_00,
    high_interest_debt_cents: int = 0,
    high_interest_apr_pct: float = 0.0,
    high_interest_threshold_pct: float = 8.0,
    low_interest_debt_cents: int = 0,
    low_interest_apr_pct: float = 0.0,
    low_interest_tax_deductible: bool = False,
    expected_market_return_pct: float = 8.0,
) -> dict:
    """Return the coaching-ordered account priority sequence.

    Full parameter semantics live in `account_priority.contribution_priority`.
    For ad-hoc composition inside the code execution sandbox, you can also import
    `contribution_priority` directly from `finance_cli.advisory`.
    """
    supported_tax_years = sorted(SUPPORTED_LIMIT_YEARS)
    limits_source = _retirement_limits_source_metadata(tax_year)
    if tax_year not in SUPPORTED_LIMIT_YEARS:
        return _advisory_envelope(
            {
                "steps": [],
                "source_tax_year": tax_year,
                "supported_tax_years": supported_tax_years,
                "limits_source": limits_source,
                "unsupported_year": True,
                "data_needed": [
                    "Use a supported tax year or gather current plan/payroll/provider contribution figures.",
                    "Do not estimate annual retirement, IRA, or HSA contribution limits from memory.",
                ],
            },
            (
                f"Tax year {tax_year} is not yet supported for retirement contribution "
                f"limits. Supported years: {supported_tax_years}."
            ),
            step_count=0,
            next_account=None,
            source_tax_year=tax_year,
            supported_tax_years=supported_tax_years,
            unsupported_year=True,
        )

    steps = contribution_priority(
        taxable_income_cents=taxable_income_cents,
        filing_status=filing_status,
        modified_agi_cents=modified_agi_cents,
        annual_salary_cents=annual_salary_cents,
        earned_compensation_cents=earned_compensation_cents,
        other_ira_contributions_cents=other_ira_contributions_cents,
        tax_year=tax_year,
        employer_match_pct=_advisory_pct_to_fraction(employer_match_pct),
        employer_match_limit_pct=_advisory_pct_to_fraction(employer_match_limit_pct),
        has_mega_backdoor=has_mega_backdoor,
        has_hsa_eligible_hdhp=has_hsa_eligible_hdhp,
        hsa_family_coverage=hsa_family_coverage,
        age=age,
        existing_emergency_fund_cents=existing_emergency_fund_cents,
        monthly_expenses_cents=monthly_expenses_cents,
        target_emergency_months=target_emergency_months,
        starter_emergency_threshold_cents=starter_emergency_threshold_cents,
        high_interest_debt_cents=high_interest_debt_cents,
        high_interest_apr=_advisory_pct_to_fraction(high_interest_apr_pct),
        high_interest_threshold=_advisory_pct_to_fraction(high_interest_threshold_pct),
        low_interest_debt_cents=low_interest_debt_cents,
        low_interest_apr=_advisory_pct_to_fraction(low_interest_apr_pct),
        low_interest_tax_deductible=low_interest_tax_deductible,
        expected_market_return=_advisory_pct_to_fraction(expected_market_return_pct),
    )
    next_account = steps[0].account if steps else None
    next_action = steps[0].action if steps else "No steps"
    return _advisory_envelope(
        {
            "steps": steps,
            "source_tax_year": tax_year,
            "supported_tax_years": supported_tax_years,
            "limits_source": limits_source,
            "unsupported_year": False,
            "data_needed": [],
        },
        f"Produced {len(steps)} contribution-priority steps; first action is {next_action}.",
        step_count=len(steps),
        next_account=next_account,
        source_tax_year=tax_year,
        supported_tax_years=supported_tax_years,
        unsupported_year=False,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_annuity_surrender_analysis(
    current_value_cents: int,
    surrender_charge_pct: float,
    guaranteed_annual_rate_pct: float,
    years_remaining_guarantee: int,
    alternative_annual_return_pct: float = 8.0,
) -> dict:
    """Scope warning: simple fixed-rate annuities only, not variable, rider, indexed, SPIA, or DIA products.

    For ad-hoc composition inside the code execution sandbox, you can also
    import `annuity_surrender_analysis` directly from `finance_cli.advisory`.
    """
    result = annuity_surrender_analysis(
        current_value_cents=current_value_cents,
        surrender_charge_pct=_advisory_pct_to_fraction(surrender_charge_pct),
        guaranteed_annual_rate=_advisory_pct_to_fraction(guaranteed_annual_rate_pct),
        years_remaining_guarantee=years_remaining_guarantee,
        alternative_annual_return=_advisory_pct_to_fraction(
            alternative_annual_return_pct
        ),
    )
    return _advisory_envelope(
        _json_safe_advisory(result),
        result.reason,
        recommendation=result.recommendation,
        advantage_cents=result.advantage_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_fund_fee_comparison(
    balance_cents: int,
    current_expense_ratio_pct: float,
    proposed_expense_ratio_pct: float,
    years: int,
    annual_return_gross_pct: float = 8.0,
    unrealized_gain_cents: int = 0,
    capital_gains_tax_rate_pct: float = 15.0,
) -> dict:
    """Compare fee drag between two funds over a fixed horizon.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `fund_fee_comparison` directly from `finance_cli.advisory`.
    """
    result = fund_fee_comparison(
        balance_cents=balance_cents,
        current_expense_ratio=_advisory_pct_to_fraction(current_expense_ratio_pct),
        proposed_expense_ratio=_advisory_pct_to_fraction(proposed_expense_ratio_pct),
        years=years,
        annual_return_gross=_advisory_pct_to_fraction(annual_return_gross_pct),
        unrealized_gain_cents=unrealized_gain_cents,
        capital_gains_tax_rate=_advisory_pct_to_fraction(capital_gains_tax_rate_pct),
    )
    return _advisory_envelope(
        _json_safe_advisory(result),
        result.reason,
        recommendation=result.recommendation,
        net_savings_cents=result.net_savings_cents,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_target_allocation(
    age: int,
    retirement_age: int = 65,
    risk_tolerance: str = "moderate",
) -> dict:
    """Return the helper's heuristic stock/bond allocation.

    Pure math only. For ad-hoc composition inside the code execution sandbox,
    you can also import `target_allocation` directly from `finance_cli.advisory`.
    """
    result = target_allocation(
        age=age,
        retirement_age=retirement_age,
        risk_tolerance=risk_tolerance,
    )
    return _advisory_envelope(
        _json_safe_advisory(result),
        result.reasoning,
        total_equities_pct=result.total_equities_pct,
        total_bonds_pct=result.total_bonds_pct,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def advisory_home_affordability(
    home_price_cents: int,
    down_payment_cents: int,
    annual_interest_rate_pct: float,
    term_years: int = 30,
    property_tax_monthly_cents: int = 0,
    insurance_monthly_cents: int = 0,
    hoa_monthly_cents: int = 0,
    pmi_monthly_cents: int = 0,
    maintenance_reserve_monthly_cents: int = 0,
    closing_cost_estimate_cents: int = 0,
    moving_cost_estimate_cents: int = 0,
    liquid_cash_cents: int = 0,
    reserve_target_cents: int = 0,
    other_monthly_debt_payments_cents: int = 0,
    gross_monthly_income_cents: Optional[int] = None,
) -> dict:
    """Compute a deterministic homebuying affordability scenario.

    Pure math only. Rates, taxes, insurance, HOA, PMI, and closing costs are
    caller-provided assumptions; this helper does not quote current market
    rates or determine mortgage approval.
    """
    home_price = _advisory_nonnegative_int(home_price_cents, field_name="home_price_cents")
    down_payment = _advisory_nonnegative_int(
        down_payment_cents,
        field_name="down_payment_cents",
    )
    if down_payment > home_price:
        raise ValueError("down_payment_cents cannot exceed home_price_cents")
    term_years_value = _advisory_nonnegative_int(
        term_years,
        field_name="term_years",
    )
    if term_years_value <= 0:
        raise ValueError("term_years must be positive")
    try:
        annual_rate_pct = Decimal(str(annual_interest_rate_pct))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            "annual_interest_rate_pct must be a non-negative number"
        ) from exc
    if not annual_rate_pct.is_finite() or annual_rate_pct < 0:
        raise ValueError("annual_interest_rate_pct must be a non-negative number")

    assumption_fields = {
        "property_tax_monthly_cents": property_tax_monthly_cents,
        "insurance_monthly_cents": insurance_monthly_cents,
        "hoa_monthly_cents": hoa_monthly_cents,
        "pmi_monthly_cents": pmi_monthly_cents,
        "maintenance_reserve_monthly_cents": maintenance_reserve_monthly_cents,
        "closing_cost_estimate_cents": closing_cost_estimate_cents,
        "moving_cost_estimate_cents": moving_cost_estimate_cents,
        "liquid_cash_cents": liquid_cash_cents,
        "reserve_target_cents": reserve_target_cents,
        "other_monthly_debt_payments_cents": other_monthly_debt_payments_cents,
    }
    assumptions = {
        key: _advisory_nonnegative_int(value, field_name=key)
        for key, value in assumption_fields.items()
    }
    if gross_monthly_income_cents is not None:
        gross_income = _advisory_nonnegative_int(
            gross_monthly_income_cents,
            field_name="gross_monthly_income_cents",
        )
    else:
        gross_income = None

    loan_amount_cents = home_price - down_payment
    monthly_rate = annual_rate_pct / Decimal("100") / Decimal("12")
    term_months = term_years_value * 12
    if loan_amount_cents == 0:
        monthly_principal_interest_cents = 0
    elif monthly_rate == 0:
        monthly_principal_interest_cents = _advisory_round_cents(
            Decimal(loan_amount_cents) / Decimal(term_months)
        )
    else:
        growth = (Decimal("1") + monthly_rate) ** term_months
        monthly_payment = Decimal(loan_amount_cents) * monthly_rate * growth / (growth - Decimal("1"))
        monthly_principal_interest_cents = _advisory_round_cents(monthly_payment)

    monthly_housing_payment_cents = (
        monthly_principal_interest_cents
        + assumptions["property_tax_monthly_cents"]
        + assumptions["insurance_monthly_cents"]
        + assumptions["hoa_monthly_cents"]
        + assumptions["pmi_monthly_cents"]
    )
    monthly_homeownership_cost_cents = (
        monthly_housing_payment_cents + assumptions["maintenance_reserve_monthly_cents"]
    )
    cash_to_close_total_cents = (
        down_payment
        + assumptions["closing_cost_estimate_cents"]
        + assumptions["moving_cost_estimate_cents"]
    )
    reserve_after_close_cents = assumptions["liquid_cash_cents"] - cash_to_close_total_cents
    reserve_gap_cents = max(0, assumptions["reserve_target_cents"] - reserve_after_close_cents)

    ratio_notes: list[str] = []
    ratios: dict[str, Any] = {
        "front_end_ratio_pct": None,
        "back_end_ratio_pct": None,
        "full_homeownership_cost_ratio_pct": None,
        "other_monthly_debt_payments_cents": assumptions["other_monthly_debt_payments_cents"],
        "ratio_notes": ratio_notes,
    }
    if gross_income:
        ratios["front_end_ratio_pct"] = _advisory_ratio_pct(
            monthly_housing_payment_cents,
            gross_income,
        )
        ratios["back_end_ratio_pct"] = _advisory_ratio_pct(
            monthly_housing_payment_cents
            + assumptions["other_monthly_debt_payments_cents"],
            gross_income,
        )
        ratios["full_homeownership_cost_ratio_pct"] = _advisory_ratio_pct(
            monthly_homeownership_cost_cents,
            gross_income,
        )
        ratio_notes.append("Ratios use supplied gross monthly income.")
    else:
        ratio_notes.append(
            "Gross monthly income is missing or zero, so DTI ratio context is omitted."
        )

    data = {
        "home_price_cents": home_price,
        "down_payment_cents": down_payment,
        "loan_amount_cents": loan_amount_cents,
        "annual_interest_rate_pct": str(annual_rate_pct),
        "term_years": term_years_value,
        "term_months": term_months,
        "monthly_principal_interest_cents": monthly_principal_interest_cents,
        "property_tax_monthly_cents": assumptions["property_tax_monthly_cents"],
        "insurance_monthly_cents": assumptions["insurance_monthly_cents"],
        "hoa_monthly_cents": assumptions["hoa_monthly_cents"],
        "pmi_monthly_cents": assumptions["pmi_monthly_cents"],
        "maintenance_reserve_monthly_cents": assumptions["maintenance_reserve_monthly_cents"],
        "monthly_housing_payment_cents": monthly_housing_payment_cents,
        "monthly_homeownership_cost_cents": monthly_homeownership_cost_cents,
        "cash_to_close": {
            "down_payment_cents": down_payment,
            "closing_cost_estimate_cents": assumptions["closing_cost_estimate_cents"],
            "moving_cost_estimate_cents": assumptions["moving_cost_estimate_cents"],
            "cash_to_close_total_cents": cash_to_close_total_cents,
            "liquid_cash_cents": assumptions["liquid_cash_cents"],
            "reserve_after_close_cents": reserve_after_close_cents,
            "reserve_target_cents": assumptions["reserve_target_cents"],
            "reserve_gap_cents": reserve_gap_cents,
        },
        "ratios": ratios,
    }
    text = (
        f"Estimated monthly housing payment is "
        f"{_advisory_currency(monthly_housing_payment_cents)}; full monthly "
        f"homeownership cost with maintenance reserve is "
        f"{_advisory_currency(monthly_homeownership_cost_cents)}."
    )
    return _advisory_envelope(
        data,
        text,
        monthly_housing_payment_cents=monthly_housing_payment_cents,
        monthly_homeownership_cost_cents=monthly_homeownership_cost_cents,
        cash_to_close_total_cents=cash_to_close_total_cents,
        reserve_gap_cents=reserve_gap_cents,
    )


# ===================================================================
# 1. Status & Overview (4 tools, read-only)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def get_workflow(name: str) -> dict:
    """Get a documented agent workflow by name. Available workflows:
    - gap_analysis: Financial gap analysis & action planning
    - monthly_review: Monthly financial review check-in
    - debt_planning: Debt payoff planning & optimization
    - goal_tracking: Goal setting & tracking
    - business_tax: Business accounting & tax compliance
    - category_design: Category taxonomy design
    - subscription_audit: Subscription audit & cleanup
    - budget_setting: Expense budget setting
    - budget_monitoring: Budget monitoring & alerts
    - category_cleanup: Category data quality cleanup
    - post_import_qa: Post-import transaction QA
    - new_user_onboarding: AI-driven onboarding

    Args:
        name: Workflow name (e.g., "monthly_review", "debt_planning")

    Discovery: use list_workflows to choose name before reading workflow details.
    """
    title = _WORKFLOW_SECTIONS.get(name)
    if title is None:
        return _unknown_workflow_response()

    workflows_path = Path(__file__).resolve().parent / "data" / "AGENT_WORKFLOWS.md"
    text = workflows_path.read_text(encoding="utf-8")

    for section in re.split(r"^## ", text, flags=re.MULTILINE):
        if not section.strip():
            continue
        section_title, separator, body = section.partition("\n")
        if not separator:
            continue
        section_title = section_title.strip()
        if section_title != title:
            continue

        section_text = f"## {section_title}\n{body}".strip()
        return {
            "data": {"name": name, "content": section_text},
            "summary": {
                "workflow": name,
                "title": section_title,
                "lines": len(section_text.splitlines()),
            },
        }

    return _unknown_workflow_response()


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def get_skill(name: str) -> dict:
    """Get a skill playbook by name. Skills are structured guides the agent follows
    for specialized tasks. Available skills:
    - normalizer_builder: Build a CSV normalizer for an unsupported bank/institution
    - onboarding: Run the AI-driven onboarding conversation playbook
    - coach_debt_payoff: Walk through diagnosis, payoff strategy, action plan, and monitoring
    - coach_emergency_fund: Build a 3-6 month emergency fund with diagnosis, target,
      account placement, funding mechanism, drawdown rules, and monitoring

    Args:
        name: Skill name (e.g., "normalizer_builder")

    Discovery: use list_skills or skill_state_get to choose name before reading or activating skill state.
    """
    return load_skill(name)


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def activate_skill(name: str) -> dict:
    """Activate a skill mid-conversation, unlocking its gated tools and
    returning the skill playbook. Use this when you need specialized tools
    that are not available in regular chat.

    Available skills:
    - normalizer_builder: Build a CSV normalizer for an unsupported bank

    Note: onboarding requires session-start context and cannot be activated
    mid-conversation. Use get_skill("onboarding") for reference only.

    Unlocked tools become available on the user's next message.

    Discovery: use list_skills or skill_state_get to choose name before reading or activating skill state.
    """
    if name in NON_ACTIVATABLE_SKILLS:
        return {
            "data": {"activated": False, "skill": name},
            "summary": {
                "activated": False,
                "reason": (
                    f"'{name}' cannot be activated mid-conversation "
                    f"(requires session-start context). Use get_skill('{name}') "
                    "to read the playbook without unlocking tools."
                ),
            },
        }

    result = load_skill(name)
    if "error" in result.get("summary", {}):
        return result

    profile = load_skill_profile(name)
    has_packs = (
        profile is not None and bool(profile.tool_packs) and profile.tool_packs_enabled
    )
    result["data"]["activated"] = has_packs
    result["summary"]["activated"] = has_packs
    if not has_packs:
        result["summary"]["note"] = (
            f"Skill '{name}' loaded but has no tool_packs - "
            "no tools were unlocked. The playbook is still available."
        )
    return result


@mcp.tool(sync_behavior="no_sync", read_only=True)
def skill_state_get(name: str) -> dict:
    """Read structured state for a named skill.

    Discovery: use list_skills or skill_state_get to choose name before reading or activating skill state.
    Related tools: skill_state_clear, skill_state_set.
    """
    state = _get_skill_state_store().get(name)
    return {
        "data": {"name": name, "state": state},
        "summary": {"name": name, "keys": len(state)},
    }


@mcp.tool(
    sync_behavior="db_write",
    approval_required=True,
    onboarding_auto_approved=True,
    coach_debt_payoff_auto_approved=True,
    coach_emergency_fund_auto_approved=True,
    coach_savings_goal_auto_approved=True,
    coach_spending_plan_auto_approved=True,
    coach_tax_readiness_auto_approved=True,
    coach_homebuying_readiness_auto_approved=True,
    coach_retirement_contribution_readiness_auto_approved=True,
    coach_retirement_income_readiness_auto_approved=True,
    coach_investment_readiness_auto_approved=True,
    coach_estate_document_readiness_auto_approved=True,
    coach_financial_plan_intake_auto_approved=True,
    coach_risk_insurance_readiness_auto_approved=True,
    coach_advisor_handoff_readiness_auto_approved=True,
)
def skill_state_set(name: str, state: dict[str, Any]) -> dict:
    """Persist structured state for a named skill.

    Discovery: use list_skills or skill_state_get to choose name before reading or activating skill state.
    Related tools: skill_state_clear, skill_state_get.
    """
    _get_skill_state_store().set(name, state)
    return {
        "data": {"name": name, "state": state},
        "summary": {"name": name, "updated": True, "keys": len(state)},
    }


@mcp.tool(
    sync_behavior="db_write",
    approval_required=True,
    onboarding_auto_approved=True,
    coach_debt_payoff_auto_approved=True,
    coach_emergency_fund_auto_approved=True,
    coach_savings_goal_auto_approved=True,
    coach_spending_plan_auto_approved=True,
    coach_tax_readiness_auto_approved=True,
    coach_homebuying_readiness_auto_approved=True,
    coach_retirement_contribution_readiness_auto_approved=True,
    coach_retirement_income_readiness_auto_approved=True,
    coach_investment_readiness_auto_approved=True,
    coach_estate_document_readiness_auto_approved=True,
    coach_financial_plan_intake_auto_approved=True,
    coach_risk_insurance_readiness_auto_approved=True,
    coach_advisor_handoff_readiness_auto_approved=True,
)
def skill_state_clear(name: str) -> dict:
    """Clear structured state for a named skill.

    Discovery: use list_skills or skill_state_get to choose name before reading or activating skill state.
    Related tools: skill_state_get, skill_state_set.
    """
    _get_skill_state_store().clear(name)
    return {
        "data": {"name": name, "cleared": True},
        "summary": {"name": name, "cleared": True},
    }


def _normalize_chip_options(options: list[Any]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in options:
        if isinstance(item, str):
            value = item.strip()
            if value:
                normalized.append(
                    {"label": value.replace("_", " ").title(), "value": value}
                )
            continue
        if not isinstance(item, dict):
            continue
        raw_value = item.get("value") or item.get("id") or item.get("key")
        raw_label = item.get("label") or item.get("title") or raw_value
        value = str(raw_value or "").strip()
        label = str(raw_label or "").strip()
        if value and label:
            normalized.append({"label": label, "value": value})
    return normalized


@mcp.tool(sync_behavior="no_sync", read_only=True)
def prompt_chip_select(
    question: str,
    options: list[Any],
    field: str,
    allow_free_text: bool = False,
) -> dict:
    """Ask the web client to render a chip-select prompt.

    Use during onboarding when the user should choose one concise option. After
    the user selects a chip, read the existing onboarding state and persist the
    answer with skill_state_set(name="onboarding", state={...}).
    """
    from finance_cli.exceptions import ValidationError

    normalized_options = _normalize_chip_options(options)
    clean_question = " ".join(str(question or "").split())
    clean_field = str(field or "").strip()
    if not clean_question:
        raise ValidationError("question is required")
    if not clean_field:
        raise ValidationError("field is required")
    if not normalized_options:
        raise ValidationError("at least one option is required")
    if len(normalized_options) > 8:
        raise ValidationError("chip select supports at most 8 options")

    return {
        "data": {
            "type": "chip_select",
            "question": clean_question,
            "field": clean_field,
            "options": normalized_options,
            "allow_free_text": bool(allow_free_text),
        },
        "summary": {
            "type": "chip_select",
            "field": clean_field,
            "options": len(normalized_options),
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def ai_setup_batch() -> dict:
    """Return deterministic starter setup proposals for onboarding.

    This tool only reads onboarding state and transaction/account data. Present
    useful proposals to the user, then use the normal approval-required tools
    (`budget_set`, `goal_set`, `rules_add_split`) for accepted changes.
    """
    from finance_cli.onboarding_contract import onboarding_signals

    state = _get_skill_state_store().get("onboarding") or {}
    user_type = state.get("user_type")
    priority = state.get("priority")
    with _get_conn() as conn:
        signals = onboarding_signals(conn)
        batch = starter_setup.build_starter_setup_batch(
            conn,
            user_type=str(user_type) if user_type is not None else None,
            priority=str(priority) if priority is not None else None,
            months_of_history=int(signals.get("months_of_history") or 0),
        )
    summary = dict(batch.get("summary") or {})
    summary["type"] = "ai_setup_batch"
    return {
        "data": batch,
        "summary": summary,
    }


@mcp.tool(
    sync_behavior="db_write",
    approval_required=True,
    onboarding_auto_approved=True,
)
def skip_onboarding() -> dict:
    """Skip optional onboarding setup after the required data-connection phase.

    Phase 1 remains a hard gate: at least one active account plus one month of
    history or an explicit minimal-data acknowledgement must exist first.
    """
    from finance_cli.exceptions import ConflictError
    from finance_cli.onboarding_contract import connect_phase_complete, current_phase

    store = _get_skill_state_store()
    state = store.get("onboarding") or {}
    with _get_conn() as conn:
        if not connect_phase_complete(conn, state):
            raise ConflictError("Connect a bank or upload a CSV before skipping setup.")
        phase = current_phase(conn, state).id.value
    next_state = dict(state)
    next_state["onboarding_skipped"] = True
    store.set("onboarding", next_state)
    return {
        "data": {
            "name": "onboarding",
            "state": next_state,
            "current_phase": phase,
        },
        "summary": {
            "skipped": True,
            "current_phase": phase,
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def onboarding_detect() -> dict:
    """Detect onboarding state: new user, resume checkpoint, or complete.
    Reads skill_state("onboarding") as primary source plus DB signals for
    new-user detection. Use this at the start of each conversation instead
    of calling db_status + plaid_status + budget_list individually."""
    from finance_cli.onboarding import detect_user_state

    with _get_conn() as conn:
        return detect_user_state(conn, skill_state_store=_get_skill_state_store())


@mcp.tool(sync_behavior="no_sync", read_only=True)
def skill_recommendations(limit: int = 3) -> dict:
    """Recommend the next coaching skill for the user.

    Uses onboarding state first. Once onboarding is complete, uses the
    intervention engine, then the saved onboarding profile as a low-confidence
    fallback. Coaching skills require session-start context, so returned actions
    use get_skill plus session_skill_context instead of activate_skill.

    Args:
        limit: Maximum recommendations to return. Values are clamped to 1-5.

    Discovery: call onboarding_detect for setup state or interventions_get for
    raw interventions when you need the underlying signal list.
    """
    from finance_cli.skill_recommendations import recommend_skills

    with _get_conn() as conn:
        return recommend_skills(
            conn,
            skill_state_store=_get_skill_state_store(),
            rules_path=_get_rules_path(),
            limit=limit,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coaching_progress(limit: int = 3) -> dict:
    """Show coaching progress across the core financial-health skills.

    Pure read. Derives status from existing skill_state, agent_session markers,
    saved coaching artifacts, and the current skill_recommendations envelope.
    It does not create a new progress table or mutate skill state.

    Args:
        limit: Number of current recommendations to include. Values are clamped
          by skill_recommendations.

    Discovery: call skill_recommendations for only the next recommended skill,
    or the individual *_artifact_read tools for full plan payloads.
    """
    from finance_cli.coaching_progress import build_coaching_progress

    with _get_conn() as conn:
        return build_coaching_progress(
            conn,
            skill_state_store=_get_skill_state_store(),
            data_dir=_get_data_dir() or _get_db_path().parent,
            rules_path=_get_rules_path(),
            limit=limit,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def strategy_preference_get(domain: str = "") -> dict:
    """Read durable strategy preferences.

    Currently supported domain: "debt". Debt strategies are avalanche,
    snowball, hybrid, and minimum_commitment.
    """
    from finance_cli.strategy_preferences import get_strategy_preferences

    with _get_conn() as conn:
        return get_strategy_preferences(conn, domain=domain or None)


@mcp.tool(
    sync_behavior="db_write",
    approval_required=True,
    coach_debt_payoff_auto_approved=True,
    coach_emergency_fund_auto_approved=True,
    coach_savings_goal_auto_approved=True,
    coach_spending_plan_auto_approved=True,
)
def strategy_preference_set(
    domain: str,
    strategy: str,
    rationale: str = "",
    source: str = "user",
    evidence: dict[str, Any] | None = None,
) -> dict:
    """Persist a user strategy preference.

    Use after the user explicitly chooses or confirms a strategy. Currently
    supported domain: "debt". Valid debt strategies: avalanche, snowball,
    hybrid, minimum_commitment.
    """
    from finance_cli.strategy_preferences import set_strategy_preference

    with _get_conn() as conn:
        return set_strategy_preference(
            conn,
            domain=domain,
            strategy=strategy,
            rationale=rationale,
            source=source,
            evidence=evidence,
        )


@mcp.tool(
    sync_behavior="db_write",
    approval_required=True,
    coach_debt_payoff_auto_approved=True,
    coach_emergency_fund_auto_approved=True,
    coach_savings_goal_auto_approved=True,
    coach_spending_plan_auto_approved=True,
)
def strategy_preference_clear(domain: str) -> dict:
    """Clear a durable strategy preference for a domain."""
    from finance_cli.strategy_preferences import clear_strategy_preference

    with _get_conn() as conn:
        return clear_strategy_preference(conn, domain=domain)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def agent_memory_read() -> dict:
    """Read long-term memory for persistent finance context. This stores
    user goals, preferences, workflow patterns, active projects, and key
    decisions. Do not expect raw financial facts here — query the DB tools
    for budgets, balances, categories, and transactions.

    Related tools: agent_memory_update.
    """
    with _get_conn() as conn:
        result = memory_cmd.handle_read(_ns(), conn, data_dir=_get_data_dir())
    return _result_envelope(result)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def agent_memory_update(content: str) -> dict:
    """Overwrite long-term memory with new content. Read existing memory first,
    edit it, then write back. Keep this small and curated: goals,
    preferences, workflow patterns, active projects, and key decisions.
    Do not store facts queryable from the DB. Markdown format, 12KB / 120
    line limit.

    Args:
        content: Full replacement content (markdown). Must be under 12KB and 120 lines.

    Related tools: agent_memory_read.
    """
    with _get_conn() as conn:
        result = memory_cmd.handle_update(
            _ns(content=content), conn, data_dir=_get_data_dir()
        )
    return _result_envelope(result)


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    onboarding_auto_approved=True,
    coach_debt_payoff_auto_approved=True,
    coach_emergency_fund_auto_approved=True,
    coach_savings_goal_auto_approved=True,
    coach_spending_plan_auto_approved=True,
    coach_tax_readiness_auto_approved=True,
    coach_homebuying_readiness_auto_approved=True,
    coach_retirement_contribution_readiness_auto_approved=True,
    coach_retirement_income_readiness_auto_approved=True,
    coach_investment_readiness_auto_approved=True,
    coach_estate_document_readiness_auto_approved=True,
    coach_financial_plan_intake_auto_approved=True,
    coach_risk_insurance_readiness_auto_approved=True,
    coach_advisor_handoff_readiness_auto_approved=True,
)
def agent_session_write(content: str) -> dict:
    """Save a deliberate session note. Use for specific decisions, follow-ups,
    or insights you want to preserve for future reference.

    Do NOT save: raw data, tool outputs, or facts queryable from the DB.
    DO save: key decisions, user intent, important follow-ups.

    Args:
        content: Note content (markdown). Keep concise — key points only.

    Related tools: agent_session_read, agent_session_search.
    """
    with _get_conn() as conn:
        result = memory_cmd.handle_session_write(
            _ns(content=content), conn, data_dir=_get_data_dir()
        )
    return _result_envelope(result)


# ---------------------------------------------------------------------------
# Coach debt-payoff artifacts
# ---------------------------------------------------------------------------


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_debt_payoff_auto_approved=True,
)
def coach_debt_payoff_artifact_save(
    action_plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a debt-payoff action plan to the user's artifact directory.

    Persistence path: <data_dir>/artifacts/coach_debt_payoff/<YYYYMMDD>.md
    (directory created with parents=True, exist_ok=True on first use).

    Args:
        action_plan_payload: Dict with required keys smart_goal, strategy,
          action_steps, monthly_commitment_cents, debts_in_scope. Optional:
          obstacles, referrals, target_debt_free_date, monitoring_cadence.
          generated_at is filled server-side from utc_now_iso() if absent.
        dry_run: If True, validate the payload but don't write.

    Raises ValueError if a required key is missing.

    Returns:
        Dict with `data: {artifact_path, generated_at, ...}, summary: {...}`.

    Related tools: coach_debt_payoff_artifact_read.
    """
    payload = _normalize_debt_payoff_payload(action_plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _debt_payoff_artifact_dir()
    artifact_path = artifact_dir / f"{generated_date.strftime('%Y%m%d')}.md"
    rendered = _render_debt_payoff_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "dry_run": dry_run,
            "action_plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_debt_payoff_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted debt-payoff action plan.

    Args:
        date: ISO date YYYY-MM-DD. If None, returns the most recent artifact.

    Returns:
        Dict. If artifact exists: {data: {action_plan_payload, artifact_path,
        generated_at}, summary: {found: True, ...}}. If artifact does not
        exist (no directory, or no file for the requested date), returns
        {data: None, summary: {found: False, reason: <"no_directory" |
        "no_artifact_for_date" | "no_artifacts">}}. Does NOT raise on missing.

    Related tools: coach_debt_payoff_artifact_save.
    """
    artifact_dir = _debt_payoff_artifact_dir()
    if not artifact_dir.exists():
        return {"data": None, "summary": {"found": False, "reason": "no_directory"}}

    if date is None:
        artifact_path = _latest_artifact_path(artifact_dir)
        if artifact_path is None:
            return {"data": None, "summary": {"found": False, "reason": "no_artifacts"}}
    else:
        artifact_path = artifact_dir / f"{date.replace('-', '')}.md"
        if not artifact_path.exists():
            return {
                "data": None,
                "summary": {
                    "found": False,
                    "reason": "no_artifact_for_date",
                    "date": date,
                },
            }

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_debt_payoff_artifact(markdown)
    generated_at = payload.get("generated_at")
    return {
        "data": {
            "action_plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
        },
    }


# ---------------------------------------------------------------------------
# Essential spending helper (generic; used by coach_emergency_fund and future
# coaching skills such as coach_spending_plan)
# ---------------------------------------------------------------------------


@mcp.tool(sync_behavior="no_sync", read_only=True)
def spending_essential_monthly(months: int = 3, use_type: Optional[str] = None) -> dict:
    """Average monthly essential vs discretionary spending across the last N complete months.

    Wraps ``finance_cli.spending_analysis.category_spending_averages``. Categories
    are classified ``essential`` / ``discretionary`` / ``excluded`` based on the
    rules.yaml ``essential_categories`` list, falling back to defaults
    (Utilities, Insurance, Health & Wellness, Rent, Housing, Childcare).

    Args:
        months: Number of complete calendar months to average over (default 3).
        use_type: Optional filter ``'Personal'`` (rows tagged Personal OR
          NULL — CLI view default), ``'Business'`` (only Business rows), or
          ``None`` (all classifications). Case-insensitive.

    Returns:
        {
          data: {
            essential_monthly_cents: int,
            discretionary_monthly_cents: int,
            months_in_window: int,
            months_with_essential_data: int,
            essential_categories: [str, ...],
            use_type: str | None,
            breakdown: [
              {category_name, parent_name, classification, avg_monthly_cents, months_with_data},
              ...
            ],
          },
          summary: {essential_monthly_cents, discretionary_monthly_cents, months_in_window, use_type},
        }
    """
    if int(months) < 1:
        raise ValueError("months must be >= 1")
    rules_path = _get_rules_path()
    essential_set = load_essential_categories(rules_path=rules_path)
    with _get_conn() as conn:
        categories = category_spending_averages(
            conn,
            months=int(months),
            rules_path=rules_path,
            use_type=use_type,
        )

    essential_monthly_cents = 0
    discretionary_monthly_cents = 0
    months_with_essential_data = 0
    breakdown: list[dict[str, Any]] = []
    for entry in categories:
        breakdown.append(
            {
                "category_name": entry.category_name,
                "parent_name": entry.parent_name,
                "classification": entry.classification,
                "avg_monthly_cents": entry.avg_monthly_cents,
                "months_with_data": entry.months_with_data,
            }
        )
        if entry.classification == "essential":
            essential_monthly_cents += entry.avg_monthly_cents
            months_with_essential_data = max(
                months_with_essential_data, entry.months_with_data
            )
        elif entry.classification == "discretionary":
            discretionary_monthly_cents += entry.avg_monthly_cents

    return {
        "data": {
            "essential_monthly_cents": essential_monthly_cents,
            "discretionary_monthly_cents": discretionary_monthly_cents,
            "months_in_window": int(months),
            "months_with_essential_data": months_with_essential_data,
            "essential_categories": sorted(essential_set),
            "use_type": use_type,
            "breakdown": breakdown,
        },
        "summary": {
            "essential_monthly_cents": essential_monthly_cents,
            "discretionary_monthly_cents": discretionary_monthly_cents,
            "months_in_window": int(months),
            "use_type": use_type,
        },
    }


# ---------------------------------------------------------------------------
# Coach emergency-fund artifacts
# ---------------------------------------------------------------------------

_EMERGENCY_FUND_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "smart_goal",
        "target_phase",
        "target_balance_cents",
        "monthly_commitment_cents",
        "essential_monthly_expenses_cents",
        "target_multiplier_months",
        "account_ids_in_fund",
        "tier_balances_target",
        "action_steps",
        "drawdown_rules_user_defined",
        "replenishment_commitment",
    }
)

_EMERGENCY_FUND_ARTIFACT_TEMPLATE = """# Emergency Fund Plan
**Generated:** {generated_date}
**SMART goal:** {smart_goal}
**Target phase:** {target_phase}

## Target
- **Essential monthly expenses:** {essential_monthly_expenses}
- **Target multiplier:** {target_multiplier_months} months
- **Total target:** {target_balance}
- **Current liquid balance:** {current_liquid_balance}
- **Gap:** {gap}

## Target Multiplier Rationale
- Income stability: {income_stability}
- Income CV observed: {income_cv_observed}
- Dependents: {dependents}
- Irreplaceable income: {irreplaceable_income}
- Multiplier chosen: {target_multiplier_months} (rationale: {target_multiplier_rationale})

## Strategy
- **Account placement:** {chosen_account_strategy} ({chosen_account_strategy_rationale})
- **Funding mechanism:** {chosen_funding_strategy} ({chosen_funding_strategy_rationale})

## Account Configuration
{account_configuration}

## Action Steps
{action_steps}

## Milestones
{milestones}

## Drawdown Rules (User-Defined)
- **What counts as an emergency:** {drawdown_rules_user_defined}
- **Replenishment plan if drawn down:** {replenishment_commitment}

## Supplementary Safety Net Facts (User-Stated)
{safety_net_facts}

## Cross-Skill Reference
- Debt-payoff artifact present: {debt_payoff_artifact_present}
- Debt-payoff artifact generated_at: {debt_payoff_artifact_generated_at}
- User Phase-4 decision: {user_decision}
- Phase-4 rationale (user-stated): {rationale_user_stated}

## Targets
- **Target-met date:** {target_met_date}
- **Monthly commitment:** {monthly_commitment}

## Monitoring
- **Cadence:** {monitoring_cadence}
- **Next check-in:** {next_check_in}

## Generated machine-readable footer (for the drawdown-no-replenishment intervention)
```yaml
{yaml_footer}```
"""


def _emergency_fund_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_emergency_fund"


def _normalize_emergency_fund_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(_EMERGENCY_FUND_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")
    payload = dict(plan_payload)
    generated_at = payload.get("generated_at")
    if not generated_at:
        generated_at = utc_now_iso()
        payload["generated_at"] = generated_at
    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _render_account_configuration(value: Any) -> str:
    if not value:
        return "- None recorded"
    if not isinstance(value, list):
        value = [value]
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            parts = []
            if "account_id" in item:
                parts.append(f"account_id={item.get('account_id')}")
            if "account_name" in item:
                parts.append(f"name={item.get('account_name')}")
            if "target_balance_cents" in item:
                parts.append(
                    f"target={_format_dollars_from_cents(item.get('target_balance_cents'))}"
                )
            if "role" in item:
                parts.append(f"role={item.get('role')}")
            text = " | ".join(parts) if parts else json.dumps(item, sort_keys=True)
        else:
            text = str(item)
        lines.append(f"- {text}")
    return "\n".join(lines)


def _render_safety_net_facts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    labels = {
        "ui_eligible": "UI eligibility",
        "cobra_option": "COBRA option",
        "fmla_eligible": "FMLA eligibility",
        "employer_disability": "Employer disability / workers' comp",
    }
    lines: list[str] = []
    for key, label in labels.items():
        if key not in value:
            continue
        raw = value.get(key)
        if raw is True:
            rendered = "yes"
        elif raw is False:
            rendered = "no"
        else:
            rendered = "unknown" if raw is None else str(raw)
        lines.append(f"- {label}: {rendered}")
    if not lines:
        return "- None recorded"
    return "\n".join(lines)


def _render_emergency_fund_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    smart_goal_value = payload.get("smart_goal")
    if isinstance(smart_goal_value, dict):
        smart_goal_text = smart_goal_value.get("text") or json.dumps(
            smart_goal_value, sort_keys=True
        )
    else:
        smart_goal_text = str(smart_goal_value) if smart_goal_value else "TBD"
    multiplier_rationale_value = payload.get("target_multiplier_rationale")
    if isinstance(multiplier_rationale_value, dict):
        income_stability = multiplier_rationale_value.get("income_stability") or "TBD"
        income_cv_observed = multiplier_rationale_value.get("income_cv_observed")
        dependents = multiplier_rationale_value.get("dependents")
        irreplaceable_income = multiplier_rationale_value.get("irreplaceable_income")
        multiplier_rationale = multiplier_rationale_value.get("rationale") or "TBD"
    else:
        income_stability = "TBD"
        income_cv_observed = None
        dependents = None
        irreplaceable_income = None
        multiplier_rationale = (
            str(multiplier_rationale_value) if multiplier_rationale_value else "TBD"
        )
    account_strategy = payload.get("chosen_account_strategy") or "TBD"
    account_strategy_rationale = (
        payload.get("chosen_account_strategy_rationale") or "TBD"
    )
    funding_strategy = payload.get("chosen_funding_strategy") or "TBD"
    funding_strategy_rationale = (
        payload.get("chosen_funding_strategy_rationale") or "TBD"
    )
    cross_skill_value = (
        payload.get("cross_skill_reference")
        if isinstance(payload.get("cross_skill_reference"), dict)
        else {}
    )
    debt_artifact_present = cross_skill_value.get("debt_payoff_artifact_present")
    debt_artifact_generated_at = (
        cross_skill_value.get("debt_payoff_artifact_generated_at") or "n/a"
    )
    user_decision = (
        cross_skill_value.get("user_decision") or payload.get("user_decision") or "TBD"
    )
    rationale_user_stated = (
        cross_skill_value.get("rationale_user_stated")
        or payload.get("rationale_user_stated")
        or "TBD"
    )
    if debt_artifact_present is True:
        debt_artifact_present_text = "yes"
    elif debt_artifact_present is False:
        debt_artifact_present_text = "no"
    else:
        debt_artifact_present_text = "unknown"
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _EMERGENCY_FUND_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        smart_goal=smart_goal_text,
        target_phase=payload.get("target_phase") or "TBD",
        essential_monthly_expenses=_format_dollars_from_cents(
            payload.get("essential_monthly_expenses_cents")
        ),
        target_multiplier_months=payload.get("target_multiplier_months") or "TBD",
        target_balance=_format_dollars_from_cents(payload.get("target_balance_cents")),
        current_liquid_balance=_format_dollars_from_cents(
            payload.get("current_liquid_balance_cents")
        ),
        gap=_format_dollars_from_cents(payload.get("gap_cents")),
        income_stability=income_stability,
        income_cv_observed=(
            "TBD" if income_cv_observed is None else income_cv_observed
        ),
        dependents=("TBD" if dependents is None else dependents),
        irreplaceable_income=(
            "yes"
            if irreplaceable_income is True
            else "no"
            if irreplaceable_income is False
            else "TBD"
        ),
        target_multiplier_rationale=multiplier_rationale,
        chosen_account_strategy=account_strategy,
        chosen_account_strategy_rationale=account_strategy_rationale,
        chosen_funding_strategy=funding_strategy,
        chosen_funding_strategy_rationale=funding_strategy_rationale,
        account_configuration=_render_account_configuration(
            payload.get("tier_balances_target")
        ),
        action_steps=_render_list_items(payload.get("action_steps"), numbered=True),
        milestones=_render_list_items(payload.get("milestones")),
        drawdown_rules_user_defined=str(
            payload.get("drawdown_rules_user_defined") or "TBD"
        ),
        replenishment_commitment=str(payload.get("replenishment_commitment") or "TBD"),
        safety_net_facts=_render_safety_net_facts(payload.get("safety_net_facts")),
        debt_payoff_artifact_present=debt_artifact_present_text,
        debt_payoff_artifact_generated_at=debt_artifact_generated_at,
        user_decision=user_decision,
        rationale_user_stated=rationale_user_stated,
        target_met_date=payload.get("target_met_date") or "TBD",
        monthly_commitment=_format_dollars_from_cents(
            payload.get("monthly_commitment_cents")
        ),
        monitoring_cadence=payload.get("monitoring_cadence") or "TBD",
        next_check_in=payload.get("next_check_in") or "TBD",
        yaml_footer=footer + "\n",
    )


def _parse_emergency_fund_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _emergency_fund_revisions_for_date(artifact_dir: Path, day: date) -> list[Path]:
    """Return all artifact files for a date, sorted oldest→newest (base first, then -r2, -r3, ...).

    Naming convention:
      <YYYYMMDD>.md          -> first save of the day
      <YYYYMMDD>-r2.md, -r3  -> subsequent same-day generations
    """
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_emergency_fund_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    """Resolve save path per the same-day revision rule.

    Returns (path, mode) where mode is one of:
      "create"            — path does not exist; new file
      "update_in_place"   — existing file's generated_at matches; overwrite
      "new_revision"      — same-date file exists with differing generated_at;
                            write a new -rN suffixed file
    """
    existing = _emergency_fund_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_emergency_fund_artifact(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_emergency_fund_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    """Resolve the artifact path for a read request. Returns (path, reason).

    Accepted ``date_query`` forms:
      - None                       -> most-recent artifact across all dates
      - "YYYY-MM-DD" / "YYYYMMDD"  -> latest revision for that day
      - "<date>-rN" (either form)  -> the specific -rN revision (N>=2)
    """
    if not artifact_dir.exists():
        return None, "no_directory"
    if date_query is None:
        latest = _latest_artifact_path(artifact_dir)
        if latest is None:
            return None, "no_artifacts"
        return latest, None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _emergency_fund_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_emergency_fund_auto_approved=True,
)
def coach_emergency_fund_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist an emergency-fund plan to the user's artifact directory.

    Persistence path: <data_dir>/artifacts/coach_emergency_fund/<YYYYMMDD>.md
    (directory created with parents=True, exist_ok=True on first use).

    Same-day revision rule:
      - If a file exists at the path AND the existing file's ``generated_at``
        matches the incoming payload's ``generated_at``, the existing file is
        updated in place. ``generated_at`` is preserved; ``last_modified_at``
        is bumped. This is the classification re-save path used by the Phase 9
        drawdown intervention flow.
      - If a file exists at the path AND ``generated_at`` differs (a new plan
        generated on the same day, e.g. after a major life event), the new
        file is written with a revision suffix: ``<YYYYMMDD>-r2.md``,
        ``-r3.md``, etc. All prior dated artifacts are preserved.

    Args:
        plan_payload: Dict with required keys smart_goal, target_phase,
          target_balance_cents, monthly_commitment_cents,
          essential_monthly_expenses_cents, target_multiplier_months,
          account_ids_in_fund, tier_balances_target, action_steps,
          drawdown_rules_user_defined, replenishment_commitment. Optional:
          milestones, obstacles, safety_net_facts, target_met_date,
          monitoring_cadence, next_check_in, target_multiplier_rationale,
          chosen_account_strategy(+_rationale), chosen_funding_strategy
          (+_rationale), cross_skill_reference, drawdown_events_classified,
          current_liquid_balance_cents, gap_cents. generated_at is filled
          server-side from utc_now_iso() if absent; last_modified_at
          defaults to generated_at on first save and is bumped on each save.
        dry_run: If True, validate the payload but don't write.

    Raises ValueError if a required key is missing.

    Returns:
        Dict with ``data: {artifact_path, generated_at, last_modified_at,
        save_mode, dry_run, plan_payload}, summary: {saved, valid,
        artifact_path, save_mode}``.

    Related tools: coach_emergency_fund_artifact_read.
    """
    payload = _normalize_emergency_fund_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _emergency_fund_artifact_dir()
    artifact_path, save_mode = _resolve_emergency_fund_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_emergency_fund_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_emergency_fund_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted emergency-fund plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).

    Returns:
        Dict. If artifact exists: ``{data: {plan_payload, artifact_path,
        generated_at, last_modified_at}, summary: {found: True, ...}}``. If
        artifact does not exist, returns ``{data: None, summary: {found:
        False, reason: <"no_directory" | "no_artifact_for_date" |
        "no_artifacts">}}``. Does NOT raise on missing.

    Related tools: coach_emergency_fund_artifact_save.
    """
    artifact_dir = _emergency_fund_artifact_dir()
    artifact_path, reason = _resolve_emergency_fund_read_path(artifact_dir, date)
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_emergency_fund_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


# ---------------------------------------------------------------------------
# Coach savings-goal artifacts
# ---------------------------------------------------------------------------

_SAVINGS_GOAL_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "goal_name",
        "smart_goal",
        "target_phase",
        "target_balance_cents",
        "monthly_commitment_cents",
        "goal_horizon_months",
        "target_met_date",
        "account_ids_in_goal",
        "action_steps",
        "milestones",
        "user_decision",
    }
)

_SAVINGS_GOAL_ARTIFACT_TEMPLATE = """# Savings Goal Plan
**Generated:** {generated_date}
**Goal name:** {goal_name}
**SMART goal:** {smart_goal}
**Target phase:** {target_phase}

## Target
- **Total target:** {target_balance}
- **Current balance toward goal:** {current_balance}
- **Gap:** {gap}
- **Goal horizon:** {goal_horizon_months} months from generated_at
- **Horizon warning:** {horizon_warning}

## Strategy
- **Account placement:** {chosen_account_strategy} ({chosen_account_strategy_rationale})
- **Funding mechanism:** {chosen_funding_strategy} ({chosen_funding_strategy_rationale})

## Account Configuration
{account_configuration}

## Action Steps
{action_steps}

## Milestones
{milestones}

## Cross-Skill Reference
- Debt-payoff artifact present: {debt_payoff_artifact_present}
- Debt-payoff artifact generated_at: {debt_payoff_artifact_generated_at}
- Emergency-fund artifact present: {efund_artifact_present}
- Emergency-fund artifact generated_at: {efund_artifact_generated_at}
- User Phase-4 decision: {user_decision}
- Phase-4 rationale (user-stated): {rationale_user_stated}

## Targets
- **Target-met date:** {target_met_date}
- **Monthly commitment:** {monthly_commitment}

## Monitoring
- **Cadence:** {monitoring_cadence}
- **Next check-in:** {next_check_in}

## Generated machine-readable footer (for milestone + stall interventions)
````yaml
{yaml_footer}````
"""


def _savings_goal_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_savings_goal"


_SAVINGS_GOAL_VALID_TARGET_PHASES = frozenset({"full", "starter_only"})
_SAVINGS_GOAL_VALID_UNLOCK_BLOCKERS = frozenset({"debt", "efund", "both"})
_SAVINGS_GOAL_ORIGINAL_FULL_FIELDS = (
    "original_full_target_balance_cents",
    "original_full_monthly_commitment_cents",
    "original_full_target_met_date",
    "original_full_goal_horizon_months",
)


def _normalize_savings_goal_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    """Validate + normalize a savings-goal artifact payload.

    Beyond the required-keys check, enforces the starter/full target-phase
    contract so a Phase 9 accepted-unlock can deterministically restore the
    full plan from artifact fields (plan §R5 fix):

      - ``target_phase`` must be ``"full"`` or ``"starter_only"``.
      - When ``target_phase == "starter_only"``: ``unlock_blocker`` must be one
        of {"debt","efund","both"}, and all four ``original_full_*`` fields
        must be present and non-null (positive ints for the cents/months
        fields, non-empty string for the date).
      - When ``target_phase == "full"``: ``unlock_blocker`` must be absent or
        null, and all four ``original_full_*`` fields must be absent or null
        (the artifact represents the active plan; original-full data only
        coexists during the starter phase).
      - Each ``milestones[*].threshold_cents`` must be a positive int and
        ``<= target_balance_cents`` so milestone evaluators measure against
        the artifact's own active target rather than a not-yet-unlocked
        full-target trajectory.

    These rules can't be bypassed at the LLM layer — the artifact tool is
    the contract boundary, and a malformed payload here would silently break
    the Phase 9 unlock-check + accepted-unlock write flow downstream.
    """
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(_SAVINGS_GOAL_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)

    target_phase = payload.get("target_phase")
    if target_phase not in _SAVINGS_GOAL_VALID_TARGET_PHASES:
        raise ValueError(
            f"target_phase must be one of {sorted(_SAVINGS_GOAL_VALID_TARGET_PHASES)}; "
            f"got {target_phase!r}"
        )

    def _coerce_positive_int(field_name: str, value: Any) -> int:
        """Coerce a positive-int field and write the canonical int back.

        Accept string forms for MCP ergonomics ("500000" -> 500000) but reject:
          - bool (``int(True)==1`` would otherwise sneak through)
          - floats with non-zero fractions (``500_000.5``)
          - any other numeric type (Decimal, Fraction) — ``int(Decimal("500000.5"))``
            silently truncates to 500000, so restrict accepted input types
            explicitly to ``int``/``float``/``str``
          - zero or negative values
        """
        if isinstance(value, bool):
            raise ValueError(f"{field_name} must be a positive int; got bool {value!r}")
        if isinstance(value, int):
            coerced = value
        elif isinstance(value, float):
            if not value.is_integer():
                raise ValueError(
                    f"{field_name} must be a positive int; got float {value!r}"
                )
            coerced = int(value)
        elif isinstance(value, str):
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{field_name} must be a positive int; got str {value!r} "
                    "(not parseable as int)"
                ) from None
        else:
            raise ValueError(
                f"{field_name} must be a positive int; got {type(value).__name__} "
                f"{value!r} (accepted types: int, integral float, str)"
            )
        if coerced <= 0:
            raise ValueError(f"{field_name} must be a positive int; got {coerced!r}")
        return coerced

    def _coerce_optional_nonnegative_int(field_name: str, value: Any) -> int | None:
        """Coerce optional render/evidence cents fields without rejecting zero."""
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(
                f"{field_name} must be a nonnegative int; got bool {value!r}"
            )
        if isinstance(value, int):
            coerced = value
        elif isinstance(value, float):
            if not value.is_integer():
                raise ValueError(
                    f"{field_name} must be a nonnegative int; got float {value!r}"
                )
            coerced = int(value)
        elif isinstance(value, str):
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{field_name} must be a nonnegative int; got str {value!r} "
                    "(not parseable as int)"
                ) from None
        else:
            raise ValueError(
                f"{field_name} must be a nonnegative int; got {type(value).__name__} "
                f"{value!r} (accepted types: int, integral float, str)"
            )
        if coerced < 0:
            raise ValueError(f"{field_name} must be a nonnegative int; got {coerced!r}")
        return coerced

    target_balance_int = _coerce_positive_int(
        "target_balance_cents", payload.get("target_balance_cents")
    )
    payload["target_balance_cents"] = target_balance_int

    # Required active numeric fields. Coerce + write back so the stall
    # evaluator's `int(payload.get("monthly_commitment_cents") or 0)` reads
    # a canonical int from the persisted YAML rather than truncating a
    # smuggled float (500_000.5) or string ("500000.5") at intervention
    # time. Same class as the target_balance_cents fix above.
    payload["monthly_commitment_cents"] = _coerce_positive_int(
        "monthly_commitment_cents", payload.get("monthly_commitment_cents")
    )
    payload["goal_horizon_months"] = _coerce_positive_int(
        "goal_horizon_months", payload.get("goal_horizon_months")
    )

    for field in ("current_balance_toward_goal_cents", "gap_cents"):
        if field in payload:
            payload[field] = _coerce_optional_nonnegative_int(field, payload.get(field))

    account_ids_in_goal = payload.get("account_ids_in_goal")
    if isinstance(account_ids_in_goal, list):
        for index, entry in enumerate(account_ids_in_goal):
            if not isinstance(entry, dict) or "target_balance_cents" not in entry:
                continue
            entry["target_balance_cents"] = _coerce_optional_nonnegative_int(
                f"account_ids_in_goal[{index}].target_balance_cents",
                entry.get("target_balance_cents"),
            )

    unlock_evidence = payload.get("unlock_evidence")
    if isinstance(unlock_evidence, dict):
        for field in (
            "debt_in_scope_sum_cents",
            "efund_balance_sum_cents",
            "efund_target_balance_cents",
        ):
            if field in unlock_evidence:
                unlock_evidence[field] = _coerce_optional_nonnegative_int(
                    f"unlock_evidence.{field}",
                    unlock_evidence.get(field),
                )

    unlock_blocker = payload.get("unlock_blocker")
    if target_phase == "starter_only":
        if unlock_blocker not in _SAVINGS_GOAL_VALID_UNLOCK_BLOCKERS:
            raise ValueError(
                "target_phase='starter_only' requires unlock_blocker to be one of "
                f"{sorted(_SAVINGS_GOAL_VALID_UNLOCK_BLOCKERS)}; got {unlock_blocker!r}"
            )
        missing_originals: list[str] = []
        for field in _SAVINGS_GOAL_ORIGINAL_FULL_FIELDS:
            value = payload.get(field)
            if value is None:
                missing_originals.append(field)
                continue
            if field.endswith("_cents") or field.endswith("_months"):
                payload[field] = _coerce_positive_int(field, value)
            else:
                # original_full_target_met_date — strictly YYYY-MM-DD.
                # `date.fromisoformat` in Python 3.11+ accepts compact forms
                # (`20271115`) and ISO week dates (`2027-W46-1`); we want the
                # narrow YYYY-MM-DD shape, so parse then round-trip and require
                # the canonical isoformat back.
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"{field} must be a non-empty YYYY-MM-DD ISO date string when "
                        f"target_phase='starter_only'; got {value!r}"
                    )
                stripped = value.strip()
                try:
                    parsed = date.fromisoformat(stripped)
                except ValueError:
                    raise ValueError(
                        f"{field} must be a YYYY-MM-DD ISO date string when "
                        f"target_phase='starter_only'; got {value!r} (not parseable)"
                    ) from None
                if parsed.isoformat() != stripped:
                    raise ValueError(
                        f"{field} must be canonical YYYY-MM-DD (not compact / week / "
                        f"ordinal form); got {value!r} (parses to {parsed.isoformat()!r})"
                    )
                payload[field] = stripped
        if missing_originals:
            raise ValueError(
                "target_phase='starter_only' requires all original_full_* fields to be "
                f"present and non-null; missing or null: {', '.join(sorted(missing_originals))}. "
                "These preserve the pre-starter plan so the Phase 9 accepted-unlock write "
                "flow can restore it without parsing prose."
            )
    else:  # target_phase == "full"
        if unlock_blocker is not None:
            raise ValueError(
                "target_phase='full' requires unlock_blocker to be None or absent; "
                f"got {unlock_blocker!r}"
            )
        populated = [
            field
            for field in _SAVINGS_GOAL_ORIGINAL_FULL_FIELDS
            if payload.get(field) is not None
        ]
        if populated:
            raise ValueError(
                "target_phase='full' requires all original_full_* fields to be None or "
                f"absent (artifact represents the active plan, no starter to restore); "
                f"populated: {', '.join(sorted(populated))}"
            )

    milestones = payload.get("milestones")
    if not isinstance(milestones, list):
        raise ValueError(f"milestones must be a list; got {type(milestones).__name__}")
    for index, entry in enumerate(milestones):
        if not isinstance(entry, dict):
            raise ValueError(
                f"milestones[{index}] must be a dict; got {type(entry).__name__}"
            )
        threshold_int = _coerce_positive_int(
            f"milestones[{index}].threshold_cents", entry.get("threshold_cents")
        )
        if threshold_int > target_balance_int:
            raise ValueError(
                f"milestones[{index}].threshold_cents ({threshold_int}) exceeds "
                f"target_balance_cents ({target_balance_int}). When target_phase="
                f"{target_phase!r}, milestones must be subdivisions of the artifact's "
                "active target. For starter_only plans, recompute the milestone schedule "
                "against the starter target — the full-target milestone schedule belongs "
                "in the Phase 9 accepted-unlock write flow's rebuild step, not here."
            )
        entry["threshold_cents"] = threshold_int

    generated_at = payload.get("generated_at")
    if not generated_at:
        generated_at = utc_now_iso()
        payload["generated_at"] = generated_at
    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _render_savings_goal_account_configuration(value: Any) -> str:
    if not value:
        return "- None recorded"
    if not isinstance(value, list):
        value = [value]
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            parts = []
            if "account_id" in item:
                parts.append(f"account_id={item.get('account_id')}")
            if "account_name" in item:
                parts.append(f"name={item.get('account_name')}")
            if "role" in item:
                parts.append(f"role={item.get('role')}")
            if "target_balance_cents" in item:
                parts.append(
                    f"target={_format_dollars_from_cents(item.get('target_balance_cents'))}"
                )
            text = " | ".join(parts) if parts else json.dumps(item, sort_keys=True)
        else:
            text = str(item)
        lines.append(f"- {text}")
    return "\n".join(lines)


def _render_savings_goal_milestones(value: Any) -> str:
    if not value:
        return "- None recorded"
    if not isinstance(value, list):
        value = [value]
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            pct = item.get("threshold_pct")
            cents = item.get("threshold_cents")
            target_date = item.get("target_date") or "TBD"
            hit_at = item.get("hit_at")
            box = "[x]" if hit_at else "[ ]"
            pct_text = f"{pct}%" if pct is not None else "??%"
            cents_text = _format_dollars_from_cents(cents)
            hit_text = hit_at if hit_at else "null"
            lines.append(
                f"- {box} {pct_text} — {cents_text} (target date {target_date}; hit {hit_text})"
            )
        else:
            lines.append(f"- {item}")
    return "\n".join(lines)


def _render_savings_goal_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    smart_goal_value = payload.get("smart_goal")
    if isinstance(smart_goal_value, dict):
        smart_goal_text = smart_goal_value.get("text") or json.dumps(
            smart_goal_value, sort_keys=True
        )
    else:
        smart_goal_text = str(smart_goal_value) if smart_goal_value else "TBD"
    account_strategy = payload.get("chosen_account_strategy") or "TBD"
    account_strategy_rationale = (
        payload.get("chosen_account_strategy_rationale") or "TBD"
    )
    funding_strategy = payload.get("chosen_funding_strategy") or "TBD"
    funding_strategy_rationale = (
        payload.get("chosen_funding_strategy_rationale") or "TBD"
    )
    cross_skill_value = (
        payload.get("cross_skill_reference")
        if isinstance(payload.get("cross_skill_reference"), dict)
        else {}
    )
    debt_artifact_present = cross_skill_value.get("debt_payoff_artifact_present")
    debt_artifact_generated_at = (
        cross_skill_value.get("debt_payoff_artifact_generated_at") or "n/a"
    )
    efund_artifact_present = cross_skill_value.get("efund_artifact_present")
    efund_artifact_generated_at = (
        cross_skill_value.get("efund_artifact_generated_at") or "n/a"
    )
    user_decision = payload.get("user_decision") or "TBD"
    rationale_user_stated = (
        cross_skill_value.get("rationale_user_stated")
        or payload.get("rationale_user_stated")
        or "TBD"
    )

    def _present(val: Any) -> str:
        if val is True:
            return "yes"
        if val is False:
            return "no"
        return "unknown"

    horizon_warning = "yes" if payload.get("horizon_warning_surfaced") is True else "no"
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _SAVINGS_GOAL_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        goal_name=payload.get("goal_name") or "TBD",
        smart_goal=smart_goal_text,
        target_phase=payload.get("target_phase") or "TBD",
        target_balance=_format_dollars_from_cents(payload.get("target_balance_cents")),
        current_balance=_format_dollars_from_cents(
            payload.get("current_balance_toward_goal_cents")
        ),
        gap=_format_dollars_from_cents(payload.get("gap_cents")),
        goal_horizon_months=payload.get("goal_horizon_months") or "TBD",
        horizon_warning=horizon_warning,
        chosen_account_strategy=account_strategy,
        chosen_account_strategy_rationale=account_strategy_rationale,
        chosen_funding_strategy=funding_strategy,
        chosen_funding_strategy_rationale=funding_strategy_rationale,
        account_configuration=_render_savings_goal_account_configuration(
            payload.get("account_ids_in_goal")
        ),
        action_steps=_render_list_items(payload.get("action_steps"), numbered=True),
        milestones=_render_savings_goal_milestones(payload.get("milestones")),
        debt_payoff_artifact_present=_present(debt_artifact_present),
        debt_payoff_artifact_generated_at=debt_artifact_generated_at,
        efund_artifact_present=_present(efund_artifact_present),
        efund_artifact_generated_at=efund_artifact_generated_at,
        user_decision=user_decision,
        rationale_user_stated=rationale_user_stated,
        target_met_date=payload.get("target_met_date") or "TBD",
        monthly_commitment=_format_dollars_from_cents(
            payload.get("monthly_commitment_cents")
        ),
        monitoring_cadence=payload.get("monitoring_cadence") or "TBD",
        next_check_in=payload.get("next_check_in") or "TBD",
        yaml_footer=footer + "\n",
    )


def _parse_savings_goal_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("````yaml", marker_index)
    fence_close_token = "````"
    if fence_start < 0:
        # Backwards compatibility with the 3-backtick artifact templates from
        # the debt-payoff / e-fund skills (in case a future template tweak
        # de-escalates the outer fence).
        fence_start = markdown.find("```yaml", marker_index)
        fence_close_token = "```"
        if fence_start < 0:
            return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find(fence_close_token, yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _savings_goal_revisions_for_date(artifact_dir: Path, day: date) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_savings_goal_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    """Resolve save path per the same-date revision rule (see e-fund precedent)."""
    existing = _savings_goal_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_savings_goal_artifact(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_savings_goal_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"
    if date_query is None:
        latest = _latest_artifact_path(artifact_dir)
        if latest is None:
            return None, "no_artifacts"
        return latest, None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _savings_goal_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_savings_goal_auto_approved=True,
)
def coach_savings_goal_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a savings-goal plan to the user's artifact directory.

    Persistence path: <data_dir>/artifacts/coach_savings_goal/<YYYYMMDD>.md
    (directory created with parents=True, exist_ok=True on first use).

    Same-day revision rule:
      - If a file exists at the path AND the existing file's ``generated_at``
        matches the incoming payload's ``generated_at``, the existing file is
        updated in place (``generated_at`` preserved; ``last_modified_at``
        bumped). This is the path used by Phase 9 milestone re-saves and the
        accepted-unlock in-place update flow.
      - If a file exists at the path AND ``generated_at`` differs (a new plan
        generated on the same day), the new file is written with a revision
        suffix: ``<YYYYMMDD>-r2.md``, etc. All prior artifacts are preserved.

    Args:
        plan_payload: Dict with required keys ``goal_name``, ``smart_goal``,
          ``target_phase`` (``"full"`` or ``"starter_only"``),
          ``target_balance_cents``, ``monthly_commitment_cents``,
          ``goal_horizon_months``, ``target_met_date``, ``account_ids_in_goal``,
          ``action_steps``, ``milestones``, ``user_decision``. Optional:
          ``goal_id``, ``unlock_blocker``, ``original_full_target_balance_cents``,
          ``original_full_monthly_commitment_cents``,
          ``original_full_target_met_date``, ``original_full_goal_horizon_months``,
          ``current_balance_toward_goal_cents``, ``gap_cents``,
          ``horizon_warning_surfaced``, ``obstacles``,
          ``chosen_account_strategy(+_rationale)``,
          ``chosen_funding_strategy(+_rationale)``, ``cross_skill_reference``,
          ``rationale_user_stated``, ``monitoring_cadence``, ``next_check_in``,
          ``unlock_prompted_at``, ``unlock_user_decision``, ``unlock_evidence``.
          ``generated_at`` is filled server-side from ``utc_now_iso()`` if
          absent; ``last_modified_at`` defaults to ``generated_at`` on first save
          and is bumped on each save.
        dry_run: If True, validate the payload but don't write.

    Raises ValueError if a required key is missing.

    Returns:
        Dict shaped ``{data: {artifact_path, generated_at, last_modified_at,
        save_mode, dry_run, plan_payload}, summary: {saved, valid, artifact_path,
        save_mode}}``.

    Related tools: coach_savings_goal_artifact_read, coach_savings_goal_check_unlock_conditions.
    """
    payload = _normalize_savings_goal_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _savings_goal_artifact_dir()
    artifact_path, save_mode = _resolve_savings_goal_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_savings_goal_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_savings_goal_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted savings-goal plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).

    Returns:
        Dict. If artifact exists: ``{data: {plan_payload, artifact_path,
        generated_at, last_modified_at}, summary: {found: True, ...}}``. If
        artifact does not exist, returns ``{data: None, summary: {found: False,
        reason: <"no_directory" | "no_artifact_for_date" | "no_artifacts">}}``.
        Does NOT raise on missing.

    Related tools: coach_savings_goal_artifact_save, coach_savings_goal_check_unlock_conditions.
    """
    artifact_dir = _savings_goal_artifact_dir()
    artifact_path, reason = _resolve_savings_goal_read_path(artifact_dir, date)
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_savings_goal_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


# ---------------------------------------------------------------------------
# Coach spending-plan artifacts + data-quality MCP wrapper
# ---------------------------------------------------------------------------

_SPENDING_PLAN_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "strategy",
        "expected_monthly_income_cents",
        "expected_monthly_expenses_cents",
        "allocations",
        "review_cadence",
    }
)

_SPENDING_PLAN_ARTIFACT_TEMPLATE = """# Spending Plan
**Generated:** {generated_date}
**Strategy:** {strategy}
**Review cadence:** {review_cadence}
**Next review:** {next_review}

## Cash Flow Baseline
- **Average monthly income:** {expected_income}
- **Average monthly essential expenses:** {essential_expenses}
- **Average monthly discretionary expenses:** {discretionary_expenses}
- **Average monthly net:** {net}

## Allocations (per category, monthly)
{allocations_table}

## Periodic Reservations (annual / 12)
{periodic_reservations}

## Line Items (savings + debt + giving)
- **Emergency fund contribution:** {efund_line}
- **Debt-paydown contribution:** {debt_line}

## Reconciliation Decisions
{reconciliation_decisions}

## Variance History
{variance_history}

## Mirror Status
- **State:** {mirror_state}
- **Failed categories:** {mirror_failed}
- **Recorded at:** {mirror_recorded_at}

## Generated machine-readable footer (for monthly_variance_review + cross_skill_commitment_drift)
````yaml
{yaml_footer}````
"""


def _spending_plan_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_spending_plan"


_TAX_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "tax_year",
        "profile",
        "preparation_route",
        "document_checklist",
        "next_actions",
    }
)

_TAX_READINESS_ARTIFACT_TEMPLATE = """# Tax Readiness Plan
**Generated:** {generated_date}
**Tax year:** {tax_year}
**Preparation route:** {route_label}

## Profile
{profile}

## Preparation Route
{preparation_route}

## Document Checklist
{document_checklist}

## Business Readiness
{business_readiness}

## Withholding / Estimated Tax
{tax_calibration}

## Risk Flags and Referrals
{risk_flags}

## Next Actions
{next_actions}

## Generated machine-readable footer (for tax-readiness re-entry)
```yaml
{yaml_footer}```
"""


def _tax_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_tax_readiness"


_FINANCIAL_PLAN_INTAKE_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "snapshot_status",
        "household_context",
        "goals",
        "assets_liabilities",
        "cash_flow",
        "domain_readiness",
        "sibling_artifacts",
        "planning_sequence",
        "professional_handoffs",
        "data_gaps",
        "monitoring",
    }
)

_FINANCIAL_PLAN_INTAKE_STATUSES = frozenset(
    {"complete", "data_needed", "limited", "refer"}
)

_FINANCIAL_PLAN_INTAKE_DOMAIN_STATUSES = frozenset(
    {"ready", "active_plan", "data_needed", "fix_first", "refer", "not_applicable"}
)

_FINANCIAL_PLAN_INTAKE_HANDOFF_TYPES = frozenset(
    {
        "none",
        "cfp",
        "ria",
        "cpa",
        "attorney",
        "insurance_agent",
        "plan_administrator",
        "benefits_team",
        "mental_health_professional",
        "other",
    }
)

_FINANCIAL_PLAN_INTAKE_PROHIBITED_PAYLOAD_KEYS = frozenset(
    {
        "allocation",
        "assetallocation",
        "buyorder",
        "beneficiarydecision",
        "claimrecommendation",
        "coverageamount",
        "coverageamountrecommendation",
        "etf",
        "etfs",
        "filingposition",
        "fundid",
        "fundname",
        "fundselection",
        "fundsymbol",
        "fundticker",
        "insuranceproductrecommendation",
        "insurancepolicyrecommendation",
        "insurancerecommendation",
        "legaladvice",
        "legalconclusion",
        "legaldocument",
        "legaldocumenttext",
        "modelportfolio",
        "policyreplacement",
        "portfolio",
        "portfolioallocation",
        "recommendedcoverage",
        "rebalancing",
        "rolloverrecommendation",
        "securities",
        "security",
        "securityselection",
        "selectedfund",
        "selectedinvestment",
        "selectedsecurity",
        "specificinvestment",
        "stockticker",
        "taxcrediteligibility",
        "taxeligibility",
        "taxfilingposition",
        "taxlossharvesting",
        "taxreturnposition",
        "ticker",
        "tickers",
        "trade",
        "trades",
        "underwritingrecommendation",
        "willtext",
    }
)

_FINANCIAL_PLAN_INTAKE_ARTIFACT_TEMPLATE = """# Financial Planning Snapshot
**Generated:** {generated_date}
**Snapshot status:** {snapshot_status}
**Next review:** {next_review}

## Household Context
{household_context}

## Goals
{goals}

## Assets and Liabilities
{assets_liabilities}

## Cash Flow
{cash_flow}

## Domain Readiness
{domain_readiness}

## Sibling Artifacts
{sibling_artifacts}

## Planning Sequence
{planning_sequence}

## Professional Handoffs
{professional_handoffs}

## Data Gaps
{data_gaps}

## Monitoring
{monitoring}

## Generated machine-readable footer (for financial-plan-intake re-entry)
```yaml
{yaml_footer}```
"""


def _financial_plan_intake_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_financial_plan_intake"


_RISK_INSURANCE_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "readiness_status",
        "household_context",
        "liquidity_context",
        "coverage_inventory",
        "risk_flags",
        "professional_handoffs",
        "planning_implications",
        "data_gaps",
        "next_actions",
    }
)

_RISK_INSURANCE_READINESS_STATUSES = frozenset(
    {
        "education_only",
        "data_needed",
        "review_recommended",
        "risk_gap",
        "ready",
        "refer",
    }
)

_RISK_INSURANCE_HANDOFF_TYPES = frozenset(
    {
        "none",
        "insurance_agent",
        "benefits_team",
        "attorney",
        "state_insurance_department",
        "fiduciary",
        "cpa",
        "ship_counselor",
        "other",
    }
)

_RISK_INSURANCE_FLAG_SEVERITIES = frozenset({"low", "medium", "high"})

_RISK_INSURANCE_PROHIBITED_PAYLOAD_KEYS = frozenset(
    {
        "benefitamountrecommendation",
        "cancelpolicy",
        "claimappealstrategy",
        "claimrecommendation",
        "claimstrategy",
        "coverageamount",
        "coverageamountrecommendation",
        "coveragechoice",
        "coveragelimit",
        "coveragelimitrecommendation",
        "insurerrecommendation",
        "insurancepolicyrecommendation",
        "insuranceproductrecommendation",
        "insurancerecommendation",
        "legaladvice",
        "legalconclusion",
        "policycancellation",
        "policychoice",
        "policyrecommendation",
        "policyreplacement",
        "premiumrecommendation",
        "recommendedcoverage",
        "recommendedinsurer",
        "recommendedpolicy",
        "riderrecommendation",
        "selectedinsurer",
        "selectedpolicy",
        "specificinsurer",
        "specificpolicy",
        "underwritingadvice",
        "underwritingrecommendation",
    }
)

_RISK_INSURANCE_PROHIBITED_TEXT_PATTERNS = (
    (
        "insurance purchase directive",
        re.compile(
            r"\bbuy\s+(?:[\w$,.]+\s+){0,8}"
            r"(?:policy|coverage|term life|whole life|life insurance|rider|insurance)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "insurer selection directive",
        re.compile(
            r"\b(?:choose|use|switch to)\s+"
            r"(?:state farm|geico|allstate|progressive|[a-z0-9& .'-]+ insurance)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "coverage amount directive",
        re.compile(
            r"\b(?:you\s+(?:need|should|get)|set\s+your\s+coverage\s+at)\s+"
            r"\$?\d[\d,.]*(?:\s?(?:k|m|million|thousand))?\b",
            re.IGNORECASE,
        ),
    ),
    (
        "claim or legal conclusion",
        re.compile(
            r"\b(?:appeal\s+the\s+claim\s+(?:by|this way)|"
            r"insurer\s+must\s+pay|policy\s+language\s+means)\b",
            re.IGNORECASE,
        ),
    ),
)

_RISK_INSURANCE_READINESS_ARTIFACT_TEMPLATE = """# Risk and Insurance Readiness Plan
**Generated:** {generated_date}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}

## Household Context
{household_context}

## Liquidity Context
{liquidity_context}

## Coverage Inventory
{coverage_inventory}

## Risk Flags
{risk_flags}

## Professional Handoffs
{professional_handoffs}

## Planning Implications
{planning_implications}

## Data Gaps
{data_gaps}

## Next Actions
{next_actions}

## Generated machine-readable footer (for risk-insurance-readiness re-entry)
```yaml
{yaml_footer}```
"""


def _risk_insurance_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_risk_insurance_readiness"


_ADVISOR_HANDOFF_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "handoff_status",
        "request_classification",
        "professional_type",
        "cashnerd_context",
        "handoff_questions",
        "documents_to_bring",
        "disclosures_to_surface",
        "boundary_response",
        "next_actions",
    }
)

_ADVISOR_HANDOFF_STATUSES = frozenset(
    {
        "education_only",
        "handoff_recommended",
        "handoff_ready",
        "compliance_review_needed",
    }
)

_ADVISOR_HANDOFF_RELEASE_MODES = frozenset(
    {
        "education",
        "planning_support",
        "referral_handoff",
        "partner_supervised",
        "registered_in_house",
    }
)

_ADVISOR_HANDOFF_PROFESSIONAL_TYPES = frozenset(
    {
        "cfp",
        "ria",
        "cpa",
        "attorney",
        "insurance_agent",
        "ship_counselor",
        "hud_counselor",
        "unknown",
    }
)

_ADVISOR_HANDOFF_DISCLOSURE_FLAGS = frozenset(
    {
        "referral_compensation",
        "scope_boundary",
        "conflict_of_interest",
        "none",
    }
)

_ADVISOR_HANDOFF_PROHIBITED_PAYLOAD_KEYS = frozenset(
    {
        "advisorrecommendation",
        "advisorselection",
        "adviserrecommendation",
        "adviserselection",
        "allocation",
        "assetallocation",
        "buyorder",
        "claimrecommendation",
        "coverageamount",
        "coverageamountrecommendation",
        "etf",
        "etfs",
        "filingposition",
        "fundselection",
        "fundsymbol",
        "fundticker",
        "insuranceproductrecommendation",
        "insurancepolicyrecommendation",
        "legaladvice",
        "legalconclusion",
        "legaldocumenttext",
        "modelportfolio",
        "policyrecommendation",
        "portfolio",
        "portfolioallocation",
        "recommendedadvisor",
        "recommendedadviser",
        "recommendedattorney",
        "recommendedcpa",
        "recommendedcoverage",
        "recommendedinsurer",
        "recommendedprofessional",
        "recommendedria",
        "rebalancing",
        "rolloverrecommendation",
        "selectedadvisor",
        "selectedadviser",
        "selectedattorney",
        "selectedcpa",
        "selectedinsurer",
        "selectedprofessional",
        "selectedsecurity",
        "securities",
        "security",
        "securityselection",
        "stockticker",
        "taxcrediteligibility",
        "taxfilingposition",
        "taxreturnposition",
        "ticker",
        "tickers",
        "trade",
        "trades",
        "underwritingrecommendation",
        "willtext",
    }
)

_ADVISOR_HANDOFF_MONETIZED_REFERRAL_KEYS = frozenset(
    {
        "affiliatecompensation",
        "affiliatefee",
        "economicbenefit",
        "paidplacement",
        "paidreferral",
        "promotercompensation",
        "referralcompensation",
        "referralfee",
        "solicitorfee",
    }
)

_ADVISOR_HANDOFF_PROHIBITED_TEXT_PATTERNS = (
    (
        "security or ticker directive",
        re.compile(r"\b(?:buy|purchase|sell|hold|rebalance)\s+[A-Z]{2,5}\b"),
    ),
    (
        "investment product directive",
        re.compile(
            r"\b(?:buy|purchase|sell|hold|rebalance)\s+(?:an?\s+)?"
            r"(?:etf|index fund|mutual fund|stock|bond|fund|security|shares?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "allocation directive",
        re.compile(
            r"\b(?:\d{1,3}\s*/\s*\d{1,3}\s+(?:allocation|portfolio)|"
            r"(?:set|use|choose|recommend)\s+(?:an?\s+)?"
            r"(?:\d{1,3}\s*/\s*\d{1,3}\s+)?allocation)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "insurance purchase directive",
        re.compile(
            r"\bbuy\s+(?:[\w$,.]+\s+){0,8}"
            r"(?:policy|coverage|term life|whole life|life insurance|rider|insurance)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "tax filing directive",
        re.compile(
            r"\b(?:claim|take|file|amend)\s+(?:the\s+)?"
            r"(?:credit|deduction|filing status|tax position|return|amended return)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "legal directive",
        re.compile(
            r"\b(?:sign|draft|file|sue|settle)\s+(?:the\s+)?"
            r"(?:will|trust|poa|lawsuit|claim|contract|complaint)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "named professional selection directive",
        re.compile(
            r"\b(?:hire|use|choose|select)\s+"
            r"[A-Z][\w .'-]{1,80}\s+(?:as\s+)?(?:your\s+)?"
            r"(?:advisor|adviser|cfp|ria|attorney|cpa|insurance agent)\b"
        ),
    ),
)

_ADVISOR_HANDOFF_ARTIFACT_TEMPLATE = """# Advisor Handoff Readiness Packet
**Generated:** {generated_date}
**Handoff status:** {handoff_status}
**Next check-in:** {next_check_in}

## Request Classification
{request_classification}

## Professional Type
{professional_type}

## CashNerd Context
{cashnerd_context}

## Handoff Questions
{handoff_questions}

## Documents to Bring
{documents_to_bring}

## Disclosures to Surface
{disclosures_to_surface}

## Boundary Response
{boundary_response}

## Next Actions
{next_actions}

## Generated machine-readable footer (for advisor-handoff-readiness re-entry)
```yaml
{yaml_footer}```
"""


def _advisor_handoff_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_advisor_handoff_readiness"


_HOMEBUYING_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "household_profile",
        "affordability_scenarios",
        "cash_to_close",
        "ratios",
        "credit_readiness",
        "readiness_status",
        "readiness_flags",
        "cross_skill_context",
        "preapproval_checklist",
        "next_actions",
        "referrals",
        "scope_notes",
    }
)

_HOMEBUYING_READINESS_STATUSES = frozenset(
    {"education_only", "not_ready", "fix_first", "preapproval_ready", "refer"}
)

_HOMEBUYING_RATIO_KEYS = frozenset(
    {
        "front_end_ratio_pct",
        "back_end_ratio_pct",
        "full_homeownership_cost_ratio_pct",
    }
)

_HOMEBUYING_READINESS_ARTIFACT_TEMPLATE = """# Homebuying Readiness Plan
**Generated:** {generated_date}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}

## Household Profile
{household_profile}

## Affordability Scenarios
{affordability_scenarios}

## Cash to Close
{cash_to_close}

## Ratios
{ratios}

## Credit Readiness
{credit_readiness}

## Readiness Flags
{readiness_flags}

## Cross-Skill Context
{cross_skill_context}

## Preapproval Checklist
{preapproval_checklist}

## Next Actions
{next_actions}

## Referrals
{referrals}

## Scope Notes
{scope_notes}

## Generated machine-readable footer (for homebuying-readiness re-entry)
```yaml
{yaml_footer}```
"""


_RETIREMENT_CONTRIBUTION_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "tax_year",
        "readiness_status",
        "household_profile",
        "cash_flow_context",
        "employer_plan_context",
        "hsa_context",
        "ira_context",
        "priority_result",
        "selected_commitment",
        "readiness_flags",
        "cross_skill_context",
        "next_actions",
        "referrals",
        "scope_notes",
    }
)

_RETIREMENT_CONTRIBUTION_READINESS_STATUSES = frozenset(
    {
        "education_only",
        "data_needed",
        "fix_first",
        "match_ready",
        "contribution_ready",
        "target_set",
        "refer",
    }
)

_RETIREMENT_CONTRIBUTION_WRITE_STATUSES = frozenset(
    {
        "not_requested",
        "dry_run_ok",
        "user_confirmed_written",
        "skipped",
    }
)

_RETIREMENT_CONTRIBUTION_TARGET_WRITE_TOOLS = frozenset(
    {
        "set_monthly_retirement_target",
        "setup_monthly_transfer_goal",
    }
)

_RETIREMENT_CONTRIBUTION_READINESS_ARTIFACT_TEMPLATE = """# Retirement Contribution Readiness Plan
**Generated:** {generated_date}
**Tax year:** {tax_year}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}

## Household Profile
{household_profile}

## Cash-Flow Context
{cash_flow_context}

## Employer Plan Context
{employer_plan_context}

## HSA Context
{hsa_context}

## IRA Context
{ira_context}

## Priority Result
{priority_result}

## Selected Commitment
{selected_commitment}

## Readiness Flags
{readiness_flags}

## Cross-Skill Context
{cross_skill_context}

## Next Actions
{next_actions}

## Referrals
{referrals}

## Scope Notes
{scope_notes}

## Generated machine-readable footer (for retirement-contribution-readiness re-entry)
```yaml
{yaml_footer}```
"""

_RETIREMENT_INCOME_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "readiness_status",
        "household_timeline",
        "income_sources",
        "health_and_risk_context",
        "cash_flow_context",
        "milestones",
        "rmd_context",
        "professional_handoffs",
        "boundary_response",
        "questions_to_ask",
        "documents_to_gather",
        "data_gaps",
        "next_actions",
        "scope_notes",
        "next_check_in",
    }
)

_RETIREMENT_INCOME_READINESS_STATUSES = frozenset(
    {
        "education_only",
        "data_needed",
        "inventory_ready",
        "timing_review_needed",
        "professional_review_needed",
        "transition_ready",
        "refer",
    }
)

_RETIREMENT_INCOME_SOURCE_STATUSES: dict[str, frozenset[str]] = {
    "social_security_estimate_status": frozenset(
        {"unknown", "missing", "user_provided", "sourced"}
    ),
    "pension_status": frozenset(
        {"unknown", "none", "user_provided", "needs_plan_document"}
    ),
    "retirement_account_status": frozenset({"unknown", "partial", "inventoried"}),
    "taxable_account_status": frozenset({"unknown", "partial", "inventoried"}),
    "annuity_status": frozenset(
        {"unknown", "none", "existing_contract", "considering_purchase"}
    ),
}

_RETIREMENT_INCOME_MEDICARE_TIMING_STATUSES = frozenset(
    {"not_relevant", "unknown", "review_needed", "handoff_needed"}
)

_RETIREMENT_INCOME_MILESTONE_STATUSES = frozenset(
    {"unknown", "future", "active", "past"}
)

_RETIREMENT_INCOME_RMD_RELEVANCE = frozenset(
    {"not_applicable", "future", "current", "unknown"}
)

_RETIREMENT_INCOME_HANDOFF_TYPES = frozenset(
    {
        "none",
        "fiduciary",
        "cpa",
        "ship_counselor",
        "insurance_agent",
        "benefits_administrator",
        "attorney",
    }
)

_RETIREMENT_INCOME_SOURCE_BACKED_MILESTONES = frozenset(
    {
        "medicare_enrollment_window",
        "rmd_beginning_date",
        "social_security_claiming_window",
    }
)

_RETIREMENT_INCOME_PROHIBITED_PAYLOAD_KEYS = frozenset(
    {
        "accountwrite",
        "annuityproduct",
        "annuityrecommendation",
        "benefitsaccountwrite",
        "brokerageorder",
        "brokeragetrade",
        "claimingage",
        "claimingrecommendation",
        "conversionamount",
        "conversionrecommendation",
        "filingposition",
        "goalwrite",
        "legaladvice",
        "medicareplan",
        "medicareplanrecommendation",
        "notification",
        "pensionelection",
        "pensionrecommendation",
        "portfolioallocation",
        "recommendedannuity",
        "recommendedclaimingage",
        "recommendedconversion",
        "recommendedmedicareplan",
        "recommendedwithdrawal",
        "reminder",
        "retirementgoalwrite",
        "rmdamount",
        "rmdcalculation",
        "rothconversion",
        "rothconversionrecommendation",
        "securityselection",
        "siblingartifactwrite",
        "taxfilingposition",
        "taxrecommendation",
        "transferschedule",
        "withdrawalamount",
        "withdrawalorder",
        "withdrawalrecommendation",
        "withdrawalsequence",
    }
)

_RETIREMENT_INCOME_PROHIBITED_TEXT_PATTERNS = (
    (
        "social security claiming directive",
        re.compile(
            r"\b(?:claim|file for|start|take)\s+"
            r"(?:social security|ss)\s+(?:at|when|now|early|late|\d{2})\b",
            re.IGNORECASE,
        ),
    ),
    (
        "withdrawal sequence directive",
        re.compile(
            r"\b(?:withdraw|draw|take distributions?)\s+"
            r"(?:from\s+)?(?:taxable|ira|roth|401k|401\(k\)|brokerage)"
            r"(?:\s+(?:first|before|next|now))?\b",
            re.IGNORECASE,
        ),
    ),
    (
        "roth conversion directive",
        re.compile(
            r"\b(?:convert|do)\s+(?:\$?\d[\d,.]*\s+)?"
            r"(?:to\s+)?(?:a\s+)?roth(?:\s+conversion)?\b",
            re.IGNORECASE,
        ),
    ),
    (
        "annuity purchase directive",
        re.compile(
            r"\b(?:buy|purchase|choose)\s+(?:an?\s+)?"
            r"(?:annuity|immediate annuity|deferred annuity)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "medicare plan selection directive",
        re.compile(
            r"\b(?:choose|enroll in|switch to)\s+"
            r"(?:medicare advantage|medigap|part d|medicare plan)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "tax filing directive",
        re.compile(
            r"\b(?:claim|take|file|amend)\s+(?:the\s+)?"
            r"(?:credit|deduction|filing status|tax position|return|amended return)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "pension election directive",
        re.compile(
            r"\b(?:choose|take|elect)\s+(?:the\s+)?"
            r"(?:lump sum|single life|joint and survivor|pension option)\b",
            re.IGNORECASE,
        ),
    ),
)

_RETIREMENT_INCOME_READINESS_ARTIFACT_TEMPLATE = """# Retirement Income Readiness Plan
**Generated:** {generated_date}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}

## Household Timeline
{household_timeline}

## Income Sources
{income_sources}

## Health and Risk Context
{health_and_risk_context}

## Cash-Flow Context
{cash_flow_context}

## Milestones
{milestones}

## RMD Context
{rmd_context}

## Professional Handoffs
{professional_handoffs}

## Boundary Response
{boundary_response}

## Questions to Ask
{questions_to_ask}

## Documents to Gather
{documents_to_gather}

## Data Gaps
{data_gaps}

## Next Actions
{next_actions}

## Scope Notes
{scope_notes}

## Generated machine-readable footer (for retirement-income-readiness re-entry)
```yaml
{yaml_footer}```
"""

_INVESTMENT_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "readiness_status",
        "user_goal",
        "cash_flow_context",
        "retirement_tax_context",
        "risk_context",
        "candidate_actions",
        "selected_action",
        "boundary",
        "data_gaps",
        "next_actions",
        "monitoring",
    }
)

_INVESTMENT_READINESS_STATUSES = frozenset(
    {
        "education_only",
        "data_needed",
        "fix_first",
        "cash_ready",
        "account_funding_ready",
        "draft_move_ready",
        "refer",
    }
)

_INVESTMENT_READINESS_WRITE_STATUSES = frozenset(
    {
        "not_requested",
        "unavailable",
        "manual_only",
        "dry_run_ok",
        "draft_intent_requested",
        "draft_intent_created",
        "skipped",
    }
)

_INVESTMENT_READINESS_PROHIBITED_PAYLOAD_KEYS = frozenset(
    {
        "allocation",
        "allocationpct",
        "assetallocation",
        "bondspct",
        "buyorder",
        "cashpct",
        "cryptoasset",
        "cryptoassets",
        "cryptopct",
        "cusip",
        "etf",
        "etfs",
        "fundid",
        "fundids",
        "fundname",
        "fundnames",
        "fundselection",
        "fundsymbol",
        "fundticker",
        "holding",
        "holdings",
        "isin",
        "modelportfolio",
        "mutualfund",
        "mutualfunds",
        "mutualfundticker",
        "position",
        "positions",
        "portfolio",
        "portfolioallocation",
        "portfolioid",
        "portfoliomodel",
        "rebalance",
        "rebalancing",
        "security",
        "securities",
        "securityid",
        "securityids",
        "securityname",
        "securityselection",
        "selectedfund",
        "selectedinvestment",
        "selectedsecurity",
        "sellorder",
        "specificinvestment",
        "stockticker",
        "stockspct",
        "symbol",
        "symbols",
        "targetallocation",
        "taxlossharvesting",
        "ticker",
        "tickers",
        "trade",
        "tradeorder",
        "trades",
    }
)

_INVESTMENT_READINESS_PROHIBITED_TEXT_PATTERNS = (
    (
        "security ticker directive",
        re.compile(r"\b(?:buy|purchase|sell|hold|rebalance)\s+[A-Z]{2,5}\b"),
    ),
    (
        "investment product directive",
        re.compile(
            r"\b(?:buy|purchase|sell|hold|rebalance)\s+(?:an?\s+)?"
            r"(?:etf|index fund|mutual fund|stock|bond|fund|security|shares?)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "allocation directive",
        re.compile(
            r"\b(?:\d{1,3}\s*/\s*\d{1,3}\s+(?:allocation|portfolio)|"
            r"(?:set|use|choose|recommend)\s+(?:an?\s+)?"
            r"(?:\d{1,3}\s*/\s*\d{1,3}\s+)?allocation)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "investment advice directive",
        re.compile(
            r"\b(?:you\s+(?:should|need|must)|i\s+recommend|recommend(?:ed)?)\s+"
            r"(?:buy|purchase|sell|hold|rebalance|allocate|invest in)\b",
            re.IGNORECASE,
        ),
    ),
)

_INVESTMENT_READINESS_LIVE_MONEY_MOVEMENT_KEYS = frozenset(
    {
        "achtransferid",
        "dwollatransferid",
        "completedtransfer",
        "executedtransfer",
        "externaltransferid",
        "paymentid",
        "settledtransferid",
        "settlementid",
        "submittedtransferid",
        "submissionresult",
        "transferid",
        "transferresult",
    }
)

_INVESTMENT_READINESS_LIVE_WRITE_STATUSES = frozenset(
    {
        "completed",
        "complete",
        "executed",
        "posted",
        "sent",
        "settled",
        "submitted",
        "succeeded",
        "success",
        "transfer_completed",
        "transfer_submitted",
        "user_confirmed_written",
    }
)

_INVESTMENT_READINESS_ARTIFACT_TEMPLATE = """# Investment Readiness Plan
**Generated:** {generated_date}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}

## User Goal
{user_goal}

## Cash-Flow Context
{cash_flow_context}

## Retirement / Tax Context
{retirement_tax_context}

## Risk Context
{risk_context}

## Candidate Actions
{candidate_actions}

## Selected Action
{selected_action}

## Boundary
{boundary}

## Data Gaps
{data_gaps}

## Next Actions
{next_actions}

## Monitoring
{monitoring}

## Generated machine-readable footer (for investment-readiness re-entry)
```yaml
{yaml_footer}```
"""

_ESTATE_DOCUMENT_READINESS_ARTIFACT_REQUIRED_KEYS = frozenset(
    {
        "generated_at",
        "readiness_status",
        "legal_boundary_acknowledged",
        "jurisdiction_context",
        "household_context",
        "document_inventory",
        "beneficiary_review",
        "referral_context",
        "next_actions",
        "next_check_in",
        "scope_notes",
    }
)

_ESTATE_DOCUMENT_READINESS_STATUSES = frozenset(
    {
        "education_only",
        "data_needed",
        "checklist_ready",
        "checklist_saved",
        "beneficiary_review_only",
        "life_event_review",
        "attorney_recommended",
    }
)

_ESTATE_DOCUMENT_STATUS_VALUES = frozenset(
    {
        "present",
        "missing",
        "unknown",
        "stale",
        "needs_attorney_review",
        "not_applicable",
    }
)

_ESTATE_DOCUMENT_INVENTORY_KEYS = frozenset(
    {
        "will",
        "financial_power_of_attorney",
        "healthcare_proxy_or_medical_poa",
        "advance_directive_or_living_will",
        "hipaa_release",
        "trust",
        "guardianship_nomination",
        "beneficiary_designations",
        "digital_assets_inventory",
        "emergency_contacts_and_storage",
    }
)

_ESTATE_DOCUMENT_NOTE_MAX_CHARS = 280

_ESTATE_DOCUMENT_CONTENT_KEYS = frozenset(
    {
        "attorneycommunication",
        "attorneyworkproduct",
        "credentials",
        "documentbody",
        "documentcontent",
        "documentfile",
        "documentimage",
        "documentscan",
        "documenttext",
        "documentupload",
        "extractedtext",
        "filecontents",
        "fulltext",
        "governmentid",
        "legalcontent",
        "legaldocumenttext",
        "legaltext",
        "password",
        "privateattorneycommunication",
        "rawdocument",
        "rawtext",
        "scannedtext",
        "signature",
        "ssn",
        "uploadeddocument",
    }
)

_ESTATE_DOCUMENT_CONTENT_SUBJECTS = (
    "advance",
    "beneficiary",
    "directive",
    "document",
    "form",
    "poa",
    "proxy",
    "release",
    "trust",
    "will",
)

_ESTATE_DOCUMENT_CONTENT_MARKERS = (
    "body",
    "content",
    "file",
    "image",
    "scan",
    "text",
    "upload",
)

_ESTATE_DOCUMENT_READINESS_ARTIFACT_TEMPLATE = """# Estate Document Readiness Checklist
**Generated:** {generated_date}
**Readiness status:** {readiness_status}
**Next check-in:** {next_check_in}
**Legal boundary acknowledged:** {legal_boundary_acknowledged}

## Jurisdiction Context
{jurisdiction_context}

## Household Context
{household_context}

## Document Inventory
{document_inventory}

## Beneficiary Review
{beneficiary_review}

## Referral Context
{referral_context}

## Next Actions
{next_actions}

## Scope Notes
{scope_notes}

## Generated machine-readable footer (for estate-document-readiness re-entry)
```yaml
{yaml_footer}```
"""


def _homebuying_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_homebuying_readiness"


def _retirement_contribution_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_retirement_contribution_readiness"


def _retirement_income_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_retirement_income_readiness"


def _investment_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_investment_readiness"


def _estate_document_readiness_artifact_dir() -> Path:
    data_dir = _get_data_dir() or _get_db_path().parent
    return Path(data_dir) / "artifacts" / "coach_estate_document_readiness"


def _financial_plan_intake_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _reject_financial_plan_intake_prohibited_fields(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _financial_plan_intake_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _FINANCIAL_PLAN_INTAKE_PROHIBITED_PAYLOAD_KEYS:
                raise ValueError(
                    f"{child_path} may not store securities, allocation, tax "
                    "filing, legal, or insurance product recommendations"
                )
            _reject_financial_plan_intake_prohibited_fields(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_financial_plan_intake_prohibited_fields(
                item,
                path=f"{path}[{index}]",
            )


def _require_financial_plan_intake_dict_list(
    payload: dict[str, Any],
    key: str,
) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a list")
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{key}[{index}] must be a dict")
    return value


def _normalize_financial_plan_intake_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_financial_plan_intake_prohibited_fields(plan_payload)

    missing = sorted(_FINANCIAL_PLAN_INTAKE_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("snapshot_status")
    if status not in _FINANCIAL_PLAN_INTAKE_STATUSES:
        allowed = ", ".join(sorted(_FINANCIAL_PLAN_INTAKE_STATUSES))
        raise ValueError(f"snapshot_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    for key in (
        "household_context",
        "assets_liabilities",
        "cash_flow",
        "domain_readiness",
        "monitoring",
    ):
        if not isinstance(payload.get(key), dict):
            raise ValueError(f"{key} must be a dict")

    for key in ("goals", "sibling_artifacts", "planning_sequence"):
        _require_financial_plan_intake_dict_list(payload, key)
    professional_handoffs = _require_financial_plan_intake_dict_list(
        payload,
        "professional_handoffs",
    )
    if not isinstance(payload.get("data_gaps"), list):
        raise ValueError("data_gaps must be a list")

    if status != "data_needed" and not payload.get("planning_sequence"):
        raise ValueError(
            "planning_sequence must include at least one item unless "
            "snapshot_status=data_needed"
        )

    domain_readiness = payload.get("domain_readiness")
    if isinstance(domain_readiness, dict):
        for key, value in domain_readiness.items():
            if value not in _FINANCIAL_PLAN_INTAKE_DOMAIN_STATUSES:
                allowed = ", ".join(sorted(_FINANCIAL_PLAN_INTAKE_DOMAIN_STATUSES))
                raise ValueError(
                    f"domain_readiness.{key} must be one of: {allowed}"
                )

    for index, handoff in enumerate(professional_handoffs):
        handoff_type = str(handoff.get("type") or "none").strip().lower()
        if handoff_type not in _FINANCIAL_PLAN_INTAKE_HANDOFF_TYPES:
            allowed = ", ".join(sorted(_FINANCIAL_PLAN_INTAKE_HANDOFF_TYPES))
            raise ValueError(
                f"professional_handoffs[{index}].type must be one of: {allowed}"
            )
        if handoff_type and handoff_type != "none" and not str(
            handoff.get("reason") or ""
        ).strip():
            raise ValueError(
                f"professional_handoffs[{index}].reason is required when type "
                "is not none"
            )

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _risk_insurance_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _reject_prohibited_text_values(
    value: Any,
    *,
    patterns: tuple[tuple[str, re.Pattern[str]], ...],
    message: str,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            _reject_prohibited_text_values(
                item,
                patterns=patterns,
                message=message,
                path=f"{path}.{raw_key}",
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_prohibited_text_values(
                item,
                patterns=patterns,
                message=message,
                path=f"{path}[{index}]",
            )
    elif isinstance(value, str):
        for label, pattern in patterns:
            if pattern.search(value):
                raise ValueError(f"{path} {message} ({label})")


def _reject_risk_insurance_prohibited_fields(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _risk_insurance_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _RISK_INSURANCE_PROHIBITED_PAYLOAD_KEYS:
                raise ValueError(
                    f"{child_path} may not store insurance policy, coverage "
                    "amount, insurer, claim, legal, underwriting, or product "
                    "recommendation fields"
                )
            _reject_risk_insurance_prohibited_fields(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_risk_insurance_prohibited_fields(
                item,
                path=f"{path}[{index}]",
            )


def _require_risk_insurance_dict_list(
    payload: dict[str, Any],
    key: str,
) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a list")
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{key}[{index}] must be a dict")
    return value


def _normalize_risk_insurance_readiness_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_risk_insurance_prohibited_fields(plan_payload)
    _reject_prohibited_text_values(
        plan_payload,
        patterns=_RISK_INSURANCE_PROHIBITED_TEXT_PATTERNS,
        message=(
            "may not store insurance advice text; use neutral inventory, "
            "readiness, data-gap, or professional-handoff wording"
        ),
    )

    missing = sorted(
        _RISK_INSURANCE_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload)
    )
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _RISK_INSURANCE_READINESS_STATUSES:
        allowed = ", ".join(sorted(_RISK_INSURANCE_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    for key in ("household_context", "liquidity_context", "coverage_inventory"):
        if not isinstance(payload.get(key), dict):
            raise ValueError(f"{key} must be a dict")

    risk_flags = _require_risk_insurance_dict_list(payload, "risk_flags")
    professional_handoffs = _require_risk_insurance_dict_list(
        payload,
        "professional_handoffs",
    )
    _require_risk_insurance_dict_list(payload, "next_actions")

    for key in ("planning_implications", "data_gaps"):
        if not isinstance(payload.get(key), list):
            raise ValueError(f"{key} must be a list")

    for index, flag in enumerate(risk_flags):
        severity = flag.get("severity")
        if severity not in (None, "") and severity not in _RISK_INSURANCE_FLAG_SEVERITIES:
            allowed = ", ".join(sorted(_RISK_INSURANCE_FLAG_SEVERITIES))
            raise ValueError(f"risk_flags[{index}].severity must be one of: {allowed}")
        if severity == "high" and not str(flag.get("rationale") or "").strip():
            raise ValueError(
                f"risk_flags[{index}].rationale is required for high severity"
            )

    if status == "risk_gap" and not risk_flags:
        raise ValueError("readiness_status=risk_gap requires at least one risk_flag")

    refer_reason_present = False
    for index, handoff in enumerate(professional_handoffs):
        handoff_type = str(handoff.get("type") or "none").strip().lower()
        if handoff_type not in _RISK_INSURANCE_HANDOFF_TYPES:
            allowed = ", ".join(sorted(_RISK_INSURANCE_HANDOFF_TYPES))
            raise ValueError(
                f"professional_handoffs[{index}].type must be one of: {allowed}"
            )
        reason = str(handoff.get("reason") or "").strip()
        if handoff_type and handoff_type != "none" and not reason:
            raise ValueError(
                f"professional_handoffs[{index}].reason is required when type "
                "is not none"
            )
        if reason:
            refer_reason_present = True

    if status == "refer" and not refer_reason_present:
        raise ValueError(
            "readiness_status=refer requires at least one professional_handoff "
            "with a reason"
        )

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _advisor_handoff_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _reject_advisor_handoff_prohibited_fields(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _advisor_handoff_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _ADVISOR_HANDOFF_PROHIBITED_PAYLOAD_KEYS:
                raise ValueError(
                    f"{child_path} may not store advisor selection, securities, "
                    "allocation, tax filing, legal, insurance product, coverage, "
                    "or claim recommendation fields"
                )
            _reject_advisor_handoff_prohibited_fields(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_advisor_handoff_prohibited_fields(
                item,
                path=f"{path}[{index}]",
            )


def _advisor_handoff_text_scan_allowed_path(path: str) -> bool:
    normalized = path.lower()
    return not any(
        marker in normalized
        for marker in (
            ".user_request",
            ".user_questions",
            ".handoff_questions",
            ".refused_topics",
        )
    )


def _reject_advisor_handoff_prohibited_text_values(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            _reject_advisor_handoff_prohibited_text_values(
                item,
                path=f"{path}.{raw_key}",
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_advisor_handoff_prohibited_text_values(
                item,
                path=f"{path}[{index}]",
            )
    elif isinstance(value, str) and _advisor_handoff_text_scan_allowed_path(path):
        for label, pattern in _ADVISOR_HANDOFF_PROHIBITED_TEXT_PATTERNS:
            if pattern.search(value):
                raise ValueError(
                    f"{path} may not store regulated advice text; use boundary, "
                    f"data-gap, due-diligence, or professional-handoff wording "
                    f"({label})"
                )


def _advisor_handoff_has_monetized_referral_metadata(value: Any) -> bool:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            normalized = _advisor_handoff_normalized_payload_key(raw_key)
            if normalized in _ADVISOR_HANDOFF_MONETIZED_REFERRAL_KEYS:
                if item not in (None, "", False, [], {}):
                    return True
            if _advisor_handoff_has_monetized_referral_metadata(item):
                return True
    elif isinstance(value, list):
        return any(_advisor_handoff_has_monetized_referral_metadata(item) for item in value)
    return False


def _require_advisor_handoff_dict(
    payload: dict[str, Any],
    key: str,
) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a dict")
    return value


def _require_advisor_handoff_list(
    payload: dict[str, Any],
    key: str,
) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a list")
    return value


def _normalize_advisor_handoff_readiness_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_advisor_handoff_prohibited_fields(plan_payload)
    _reject_advisor_handoff_prohibited_text_values(plan_payload)

    missing = sorted(
        _ADVISOR_HANDOFF_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload)
    )
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("handoff_status")
    if status not in _ADVISOR_HANDOFF_STATUSES:
        allowed = ", ".join(sorted(_ADVISOR_HANDOFF_STATUSES))
        raise ValueError(f"handoff_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    request_classification = _require_advisor_handoff_dict(
        payload,
        "request_classification",
    )
    professional_type = _require_advisor_handoff_dict(payload, "professional_type")
    cashnerd_context = _require_advisor_handoff_dict(payload, "cashnerd_context")
    boundary_response = _require_advisor_handoff_dict(payload, "boundary_response")
    handoff_questions = _require_advisor_handoff_list(payload, "handoff_questions")
    _require_advisor_handoff_list(payload, "documents_to_bring")
    disclosures_to_surface = _require_advisor_handoff_list(
        payload,
        "disclosures_to_surface",
    )
    _require_advisor_handoff_list(payload, "next_actions")

    release_mode = request_classification.get("release_mode")
    if release_mode not in _ADVISOR_HANDOFF_RELEASE_MODES:
        allowed = ", ".join(sorted(_ADVISOR_HANDOFF_RELEASE_MODES))
        raise ValueError(f"request_classification.release_mode must be one of: {allowed}")
    if not str(request_classification.get("user_request") or "").strip():
        raise ValueError("request_classification.user_request is required")
    prohibited_if_unsupervised = request_classification.get(
        "prohibited_response_if_unsupervised"
    )
    if not isinstance(prohibited_if_unsupervised, bool):
        raise ValueError(
            "request_classification.prohibited_response_if_unsupervised must be a bool"
        )

    primary = str(professional_type.get("primary") or "unknown").strip().lower()
    if primary not in _ADVISOR_HANDOFF_PROFESSIONAL_TYPES:
        allowed = ", ".join(sorted(_ADVISOR_HANDOFF_PROFESSIONAL_TYPES))
        raise ValueError(f"professional_type.primary must be one of: {allowed}")
    professional_type["primary"] = primary
    rationale = str(professional_type.get("rationale") or "").strip()
    handoff_like = status != "education_only" or release_mode != "education"
    if handoff_like and not rationale:
        raise ValueError("professional_type.rationale is required for handoff")
    if status in {"handoff_recommended", "handoff_ready"} and primary == "unknown":
        raise ValueError(
            "professional_type.primary cannot be unknown when handoff is recommended"
        )

    for key in ("relevant_artifacts", "key_facts", "user_questions"):
        if not isinstance(cashnerd_context.get(key), list):
            raise ValueError(f"cashnerd_context.{key} must be a list")

    for flag in disclosures_to_surface:
        if flag not in _ADVISOR_HANDOFF_DISCLOSURE_FLAGS:
            allowed = ", ".join(sorted(_ADVISOR_HANDOFF_DISCLOSURE_FLAGS))
            raise ValueError(f"disclosures_to_surface entries must be one of: {allowed}")
    if "none" in disclosures_to_surface and len(disclosures_to_surface) > 1:
        raise ValueError("disclosures_to_surface cannot combine none with other flags")
    if (
        _advisor_handoff_has_monetized_referral_metadata(plan_payload)
        and "referral_compensation" not in disclosures_to_surface
    ):
        raise ValueError(
            "monetized referral metadata requires disclosures_to_surface to include "
            "referral_compensation"
        )

    if not str(boundary_response.get("user_facing_summary") or "").strip():
        raise ValueError("boundary_response.user_facing_summary is required")
    refused_topics = boundary_response.get("refused_topics")
    allowed_help = boundary_response.get("allowed_help")
    if not isinstance(refused_topics, list):
        raise ValueError("boundary_response.refused_topics must be a list")
    if not isinstance(allowed_help, list):
        raise ValueError("boundary_response.allowed_help must be a list")
    if prohibited_if_unsupervised and not refused_topics:
        raise ValueError(
            "prohibited_response_if_unsupervised=True requires refused_topics"
        )
    if prohibited_if_unsupervised and not allowed_help:
        raise ValueError(
            "prohibited_response_if_unsupervised=True requires allowed_help"
        )
    if handoff_like and not handoff_questions:
        raise ValueError("handoff_questions must include at least one handoff question")

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _normalize_homebuying_readiness_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(_HOMEBUYING_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _HOMEBUYING_READINESS_STATUSES:
        allowed = ", ".join(sorted(_HOMEBUYING_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    scenarios = payload.get("affordability_scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("affordability_scenarios must contain at least one scenario")
    if any(not isinstance(item, dict) for item in scenarios):
        raise ValueError("affordability_scenarios entries must be dicts")

    household = payload.get("household_profile")
    if not isinstance(household, dict):
        raise ValueError("household_profile must be a dict")
    ratios = payload.get("ratios")
    if not isinstance(ratios, dict):
        raise ValueError("ratios must be a dict")

    missing_ratio_keys = sorted(
        key for key in _HOMEBUYING_RATIO_KEYS if ratios.get(key) in (None, "")
    )
    if missing_ratio_keys:
        gross_income = household.get("gross_monthly_income_cents")
        gross_income_unknown = gross_income in (None, "", "unknown")
        ratio_notes = ratios.get("ratio_notes")
        ratio_note_present = (
            isinstance(ratio_notes, list)
            and any(str(item).strip() for item in ratio_notes)
        ) or (isinstance(ratio_notes, str) and bool(ratio_notes.strip()))
        if not gross_income_unknown or not ratio_note_present:
            raise ValueError(
                "ratio fields may be omitted only when gross income is unknown "
                "and ratio_notes explains the missing input"
            )

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")
    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _retirement_target_write_evidence_present(
    selected_commitment: dict[str, Any],
) -> bool:
    evidence = (
        selected_commitment.get("write_result")
        or selected_commitment.get("approval_evidence")
        or selected_commitment.get("target_write_result")
    )
    if not isinstance(evidence, dict):
        return False
    tool_name = (
        evidence.get("tool_name")
        or evidence.get("tool")
        or selected_commitment.get("write_tool")
    )
    if tool_name not in _RETIREMENT_CONTRIBUTION_TARGET_WRITE_TOOLS:
        return False
    status = str(
        evidence.get("status")
        or evidence.get("result_status")
        or evidence.get("summary_status")
        or ""
    ).lower()
    success = evidence.get("success") is True
    status_success = status in {"ok", "success", "succeeded", "written", "completed"}
    return success or status_success


def _normalize_retirement_contribution_readiness_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(
        _RETIREMENT_CONTRIBUTION_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload)
    )
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _RETIREMENT_CONTRIBUTION_READINESS_STATUSES:
        allowed = ", ".join(sorted(_RETIREMENT_CONTRIBUTION_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    raw_tax_year = payload.get("tax_year")
    if isinstance(raw_tax_year, bool):
        raise ValueError("tax_year must be an integer")
    try:
        tax_year = int(raw_tax_year)
    except (TypeError, ValueError) as exc:
        raise ValueError("tax_year must be an integer") from exc
    payload["tax_year"] = tax_year

    for key in (
        "household_profile",
        "cash_flow_context",
        "employer_plan_context",
        "hsa_context",
        "ira_context",
        "cross_skill_context",
    ):
        if not isinstance(payload.get(key), dict):
            raise ValueError(f"{key} must be a dict")
    for key in ("readiness_flags", "next_actions", "referrals", "scope_notes"):
        if not isinstance(payload.get(key), list):
            raise ValueError(f"{key} must be a list")

    priority = payload.get("priority_result")
    if not isinstance(priority, dict):
        raise ValueError("priority_result must be a dict")
    if priority.get("helper") != "advisory_contribution_priority":
        raise ValueError("priority_result.helper must be advisory_contribution_priority")
    source_tax_year = priority.get("source_tax_year")
    try:
        source_tax_year_int = int(source_tax_year)
    except (TypeError, ValueError) as exc:
        raise ValueError("priority_result.source_tax_year must be an integer") from exc
    if source_tax_year_int != tax_year:
        raise ValueError("priority_result.source_tax_year must match tax_year")
    if not isinstance(priority.get("steps"), list):
        raise ValueError("priority_result.steps must be a list")

    unsupported_year = priority.get("unsupported_year")
    data_needed = priority.get("data_needed")
    if tax_year not in SUPPORTED_LIMIT_YEARS:
        if status != "data_needed":
            raise ValueError(
                "unsupported tax_year artifacts must use readiness_status=data_needed"
            )
        if unsupported_year is not True or not (
            isinstance(data_needed, list) and data_needed
        ):
            raise ValueError(
                "unsupported tax_year artifacts must include "
                "priority_result.unsupported_year=True and data_needed"
            )
    elif unsupported_year is True:
        raise ValueError(
            "supported tax_year artifacts cannot mark priority_result.unsupported_year=True"
        )

    selected_commitment = payload.get("selected_commitment")
    if not isinstance(selected_commitment, dict):
        raise ValueError("selected_commitment must be a dict")
    write_status = selected_commitment.get("write_status")
    if write_status not in _RETIREMENT_CONTRIBUTION_WRITE_STATUSES:
        allowed = ", ".join(sorted(_RETIREMENT_CONTRIBUTION_WRITE_STATUSES))
        raise ValueError(f"selected_commitment.write_status must be one of: {allowed}")
    write_tool = selected_commitment.get("write_tool")
    if write_tool not in (None, "") and write_tool not in (
        _RETIREMENT_CONTRIBUTION_TARGET_WRITE_TOOLS
    ):
        allowed = ", ".join(sorted(_RETIREMENT_CONTRIBUTION_TARGET_WRITE_TOOLS))
        raise ValueError(f"selected_commitment.write_tool must be one of: {allowed}")
    if write_status == "user_confirmed_written" and not (
        _retirement_target_write_evidence_present(selected_commitment)
    ):
        raise ValueError(
            "selected_commitment.write_status=user_confirmed_written requires "
            "write_result evidence for an approval-required retirement target tool"
        )

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _retirement_income_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _reject_retirement_income_prohibited_fields(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _retirement_income_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _RETIREMENT_INCOME_PROHIBITED_PAYLOAD_KEYS:
                raise ValueError(
                    f"{child_path} may not store retirement-income claiming, "
                    "withdrawal, conversion, annuity, Medicare-plan, pension, "
                    "tax, legal, portfolio, account-write, reminder, transfer, "
                    "notification, or sibling-artifact write fields"
                )
            _reject_retirement_income_prohibited_fields(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_retirement_income_prohibited_fields(
                item,
                path=f"{path}[{index}]",
            )


def _retirement_income_text_scan_allowed_path(path: str) -> bool:
    normalized = path.lower()
    return not any(
        marker in normalized
        for marker in (
            ".question_to_ask",
            ".questions_to_ask",
            ".user_request_preserved_for_professional",
        )
    )


def _reject_retirement_income_prohibited_text_values(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            _reject_retirement_income_prohibited_text_values(
                item,
                path=f"{path}.{raw_key}",
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_retirement_income_prohibited_text_values(
                item,
                path=f"{path}[{index}]",
            )
    elif isinstance(value, str) and _retirement_income_text_scan_allowed_path(path):
        for label, pattern in _RETIREMENT_INCOME_PROHIBITED_TEXT_PATTERNS:
            if pattern.search(value):
                raise ValueError(
                    f"{path} may not store retirement-income implementation "
                    "advice text; use education, inventory, data-gap, "
                    f"boundary, or professional-handoff wording ({label})"
                )


def _require_retirement_income_dict(
    payload: dict[str, Any],
    key: str,
) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a dict")
    return value


def _require_retirement_income_list(
    payload: dict[str, Any],
    key: str,
) -> list[Any]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a list")
    return value


def _require_retirement_income_dict_list(
    payload: dict[str, Any],
    key: str,
) -> list[dict[str, Any]]:
    items = _require_retirement_income_list(payload, key)
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"{key}[{index}] must be a dict")
    return items


def _retirement_income_source_metadata_complete(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    source_url = str(value.get("source_url") or value.get("url") or "").strip()
    source_year = value.get("source_year") or value.get("year")
    if isinstance(source_year, bool):
        return False
    try:
        source_year_int = int(source_year)
    except (TypeError, ValueError):
        return False
    return bool(source_url) and source_year_int >= 1900


def _normalize_retirement_income_readiness_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_retirement_income_prohibited_fields(plan_payload)
    _reject_retirement_income_prohibited_text_values(plan_payload)

    missing = sorted(
        _RETIREMENT_INCOME_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload)
    )
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _RETIREMENT_INCOME_READINESS_STATUSES:
        allowed = ", ".join(sorted(_RETIREMENT_INCOME_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    _require_retirement_income_dict(payload, "household_timeline")
    income_sources = _require_retirement_income_dict(payload, "income_sources")
    health_context = _require_retirement_income_dict(
        payload,
        "health_and_risk_context",
    )
    _require_retirement_income_dict(payload, "cash_flow_context")
    rmd_context = _require_retirement_income_dict(payload, "rmd_context")
    boundary_response = _require_retirement_income_dict(
        payload,
        "boundary_response",
    )
    milestones = _require_retirement_income_dict_list(payload, "milestones")
    handoffs = _require_retirement_income_dict_list(
        payload,
        "professional_handoffs",
    )
    for key in (
        "questions_to_ask",
        "documents_to_gather",
        "data_gaps",
        "next_actions",
        "scope_notes",
    ):
        _require_retirement_income_list(payload, key)

    for key, allowed_values in _RETIREMENT_INCOME_SOURCE_STATUSES.items():
        value = income_sources.get(key)
        if value not in allowed_values:
            allowed = ", ".join(sorted(allowed_values))
            raise ValueError(f"income_sources.{key} must be one of: {allowed}")

    medicare_status = health_context.get("medicare_timing_status")
    if medicare_status not in _RETIREMENT_INCOME_MEDICARE_TIMING_STATUSES:
        allowed = ", ".join(sorted(_RETIREMENT_INCOME_MEDICARE_TIMING_STATUSES))
        raise ValueError(
            f"health_and_risk_context.medicare_timing_status must be one of: {allowed}"
        )

    for index, milestone in enumerate(milestones):
        milestone_status = milestone.get("status")
        if milestone_status not in (None, "") and (
            milestone_status not in _RETIREMENT_INCOME_MILESTONE_STATUSES
        ):
            allowed = ", ".join(sorted(_RETIREMENT_INCOME_MILESTONE_STATUSES))
            raise ValueError(f"milestones[{index}].status must be one of: {allowed}")
        milestone_name = str(milestone.get("name") or "").strip()
        if (
            milestone_name in _RETIREMENT_INCOME_SOURCE_BACKED_MILESTONES
            and milestone_status in {"future", "active"}
        ):
            source_url = str(milestone.get("source_url") or "").strip()
            source_metadata = milestone.get("source_metadata")
            if not source_url and not _retirement_income_source_metadata_complete(
                source_metadata
            ):
                raise ValueError(
                    f"milestones[{index}] requires source_url or source_metadata "
                    "for source-backed annual/regulatory timing"
                )

    rmd_relevance = rmd_context.get("relevance")
    if rmd_relevance not in _RETIREMENT_INCOME_RMD_RELEVANCE:
        allowed = ", ".join(sorted(_RETIREMENT_INCOME_RMD_RELEVANCE))
        raise ValueError(f"rmd_context.relevance must be one of: {allowed}")
    if rmd_relevance in {"future", "current"} and not (
        _retirement_income_source_metadata_complete(
            rmd_context.get("source_metadata")
        )
    ):
        raise ValueError(
            "rmd_context.source_metadata with source_year and source_url is "
            "required when rmd_context.relevance is future or current"
        )

    handoff_present = False
    for index, handoff in enumerate(handoffs):
        handoff_type = str(handoff.get("type") or "none").strip().lower()
        if handoff_type not in _RETIREMENT_INCOME_HANDOFF_TYPES:
            allowed = ", ".join(sorted(_RETIREMENT_INCOME_HANDOFF_TYPES))
            raise ValueError(
                f"professional_handoffs[{index}].type must be one of: {allowed}"
            )
        handoff["type"] = handoff_type
        if handoff_type != "none":
            handoff_present = True
            if not str(handoff.get("trigger") or "").strip():
                raise ValueError(
                    f"professional_handoffs[{index}].trigger is required when "
                    "type is not none"
                )
            if not str(handoff.get("question_to_ask") or "").strip():
                raise ValueError(
                    f"professional_handoffs[{index}].question_to_ask is "
                    "required when type is not none"
                )

    prohibited_detected = boundary_response.get("prohibited_request_detected")
    if not isinstance(prohibited_detected, bool):
        raise ValueError("boundary_response.prohibited_request_detected must be a bool")
    if prohibited_detected:
        if not str(
            boundary_response.get("user_request_preserved_for_professional") or ""
        ).strip():
            raise ValueError(
                "boundary_response.user_request_preserved_for_professional is "
                "required when prohibited_request_detected=True"
            )
        if not handoff_present:
            raise ValueError(
                "prohibited_request_detected=True requires at least one "
                "professional_handoff"
            )

    if status in {"timing_review_needed", "professional_review_needed", "refer"}:
        if not handoff_present:
            raise ValueError(
                f"readiness_status={status} requires at least one professional_handoff"
            )

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _investment_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _reject_investment_prohibited_fields(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _investment_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _INVESTMENT_READINESS_PROHIBITED_PAYLOAD_KEYS:
                raise ValueError(
                    f"{child_path} may not store securities, fund, allocation, "
                    "portfolio, rebalancing, tax-loss harvesting, or trade fields"
                )
            _reject_investment_prohibited_fields(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_investment_prohibited_fields(item, path=f"{path}[{index}]")


def _investment_evidence_is_draft_only(evidence: dict[str, Any]) -> bool:
    if evidence.get("draft_only") is True or evidence.get("draft") is True:
        return True
    intent_type = str(evidence.get("intent_type") or "").lower()
    if intent_type == "draft":
        return True
    tool_name = str(evidence.get("tool_name") or evidence.get("tool") or "").lower()
    return "draft" in tool_name and "intent" in tool_name


def _investment_evidence_indicates_success(evidence: dict[str, Any]) -> bool:
    if evidence.get("success") is True:
        return True
    raw_status = (
        evidence.get("status")
        or evidence.get("result_status")
        or evidence.get("summary_status")
        or evidence.get("write_status")
    )
    status = str(raw_status or "").lower()
    return status in _INVESTMENT_READINESS_LIVE_WRITE_STATUSES


def _reject_investment_live_money_movement(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _investment_normalized_payload_key(key)
            child_path = f"{path}.{key}"
            if normalized in _INVESTMENT_READINESS_LIVE_MONEY_MOVEMENT_KEYS:
                raise ValueError(
                    f"{child_path} may not store submitted or completed money "
                    "movement evidence; investment readiness supports draft "
                    "intents only"
                )
            if normalized in {"writestatus", "moneymovementstatus", "transferstatus"}:
                status = str(item or "").lower()
                if status in _INVESTMENT_READINESS_LIVE_WRITE_STATUSES:
                    raise ValueError(
                        f"{child_path} may not be {status!r}; investment "
                        "readiness supports draft intents only"
                    )
            if normalized in {
                "approvalevidence",
                "moneymovementresult",
                "transferresult",
                "writeresult",
            }:
                if isinstance(item, dict) and _investment_evidence_indicates_success(
                    item
                ):
                    if not _investment_evidence_is_draft_only(item):
                        raise ValueError(
                            f"{child_path} successful evidence must reference a "
                            "draft money-movement intent only"
                        )
            _reject_investment_live_money_movement(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_investment_live_money_movement(item, path=f"{path}[{index}]")


def _investment_action_is_account_funding(action: dict[str, Any]) -> bool:
    action_id = _investment_normalized_payload_key(action.get("action_id") or "")
    if action_id in {
        "fundinvestmentaccount",
        "fundbrokerageaccount",
        "fundira",
        "fundhsa",
        "fundworkplaceplan",
    }:
        return True
    if "fund" in action_id and (
        "invest" in action_id
        or "brokerage" in action_id
        or "ira" in action_id
        or "hsa" in action_id
    ):
        return True
    destination = _investment_normalized_payload_key(
        action.get("destination_account_label") or action.get("destination") or ""
    )
    return any(
        marker in destination
        for marker in ("brokerage", "investment", "ira", "hsa", "workplaceplan")
    )


def _validate_investment_funding_action(
    action: dict[str, Any],
    *,
    path: str,
) -> None:
    if _investment_action_is_account_funding(action):
        if action.get("scope_label") != "cash_movement_only":
            raise ValueError(
                f"{path}.scope_label must be cash_movement_only for investment "
                "account funding actions"
            )


def _normalize_investment_readiness_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_investment_prohibited_fields(plan_payload)
    _reject_prohibited_text_values(
        plan_payload,
        patterns=_INVESTMENT_READINESS_PROHIBITED_TEXT_PATTERNS,
        message=(
            "may not store investment advice text; use cash-movement readiness, "
            "data-gap, boundary, or RIA-handoff wording"
        ),
    )
    _reject_investment_live_money_movement(plan_payload)

    missing = sorted(_INVESTMENT_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _INVESTMENT_READINESS_STATUSES:
        allowed = ", ".join(sorted(_INVESTMENT_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")

    for key in (
        "user_goal",
        "cash_flow_context",
        "retirement_tax_context",
        "risk_context",
        "boundary",
        "monitoring",
    ):
        if not isinstance(payload.get(key), dict):
            raise ValueError(f"{key} must be a dict")
    for key in ("candidate_actions", "data_gaps", "next_actions"):
        if not isinstance(payload.get(key), list):
            raise ValueError(f"{key} must be a list")

    selected_action = payload.get("selected_action")
    if not isinstance(selected_action, dict):
        raise ValueError("selected_action must be a dict")
    write_status = selected_action.get("write_status")
    if write_status not in _INVESTMENT_READINESS_WRITE_STATUSES:
        allowed = ", ".join(sorted(_INVESTMENT_READINESS_WRITE_STATUSES))
        raise ValueError(f"selected_action.write_status must be one of: {allowed}")
    _validate_investment_funding_action(
        selected_action,
        path="selected_action",
    )

    for index, action in enumerate(payload.get("candidate_actions") or []):
        if not isinstance(action, dict):
            raise ValueError("candidate_actions entries must be dicts")
        _validate_investment_funding_action(
            action,
            path=f"candidate_actions[{index}]",
        )

    if status == "draft_move_ready":
        if write_status != "draft_intent_created":
            raise ValueError(
                "readiness_status=draft_move_ready requires "
                "selected_action.write_status=draft_intent_created"
            )
        if not selected_action.get("money_movement_intent_id"):
            raise ValueError(
                "readiness_status=draft_move_ready requires "
                "selected_action.money_movement_intent_id"
            )

    boundary = payload.get("boundary")
    if not isinstance(boundary.get("prohibited_topics_surfaced", []), list):
        raise ValueError("boundary.prohibited_topics_surfaced must be a list")
    referral_recommended = boundary.get("referral_recommended")
    if referral_recommended is not None and not isinstance(referral_recommended, bool):
        raise ValueError("boundary.referral_recommended must be a bool when present")

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _estate_normalized_payload_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _estate_key_is_legal_content_attempt(key: Any) -> bool:
    normalized = _estate_normalized_payload_key(key)
    if normalized in _ESTATE_DOCUMENT_CONTENT_KEYS:
        return True
    has_subject = any(
        subject in normalized for subject in _ESTATE_DOCUMENT_CONTENT_SUBJECTS
    )
    has_marker = any(
        marker in normalized for marker in _ESTATE_DOCUMENT_CONTENT_MARKERS
    )
    return has_subject and has_marker


def _reject_estate_document_content_attempts(
    value: Any,
    *,
    path: str = "plan_payload",
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            child_path = f"{path}.{key}"
            if _estate_key_is_legal_content_attempt(key):
                raise ValueError(
                    f"{child_path} may not store legal document content, uploads, "
                    "signatures, credentials, or private attorney communications"
                )
            _reject_estate_document_content_attempts(item, path=child_path)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_estate_document_content_attempts(item, path=f"{path}[{index}]")


def _validate_estate_short_notes(
    value: Any,
    *,
    path: str = "plan_payload",
    in_notes_field: bool = False,
) -> None:
    if isinstance(value, dict):
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized = _estate_normalized_payload_key(key)
            is_notes = normalized == "note" or normalized.endswith("notes")
            _validate_estate_short_notes(
                item,
                path=f"{path}.{key}",
                in_notes_field=is_notes,
            )
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _validate_estate_short_notes(
                item,
                path=f"{path}[{index}]",
                in_notes_field=in_notes_field,
            )
    elif in_notes_field and isinstance(value, str):
        if len(value) > _ESTATE_DOCUMENT_NOTE_MAX_CHARS:
            raise ValueError(
                f"{path} must be short metadata only "
                f"({_ESTATE_DOCUMENT_NOTE_MAX_CHARS} chars max)"
            )


def _normalize_estate_document_readiness_payload(
    plan_payload: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    _reject_estate_document_content_attempts(plan_payload)
    _validate_estate_short_notes(plan_payload)

    missing = sorted(
        _ESTATE_DOCUMENT_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload)
    )
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")

    payload = dict(plan_payload)
    status = payload.get("readiness_status")
    if status not in _ESTATE_DOCUMENT_READINESS_STATUSES:
        allowed = ", ".join(sorted(_ESTATE_DOCUMENT_READINESS_STATUSES))
        raise ValueError(f"readiness_status must be one of: {allowed}")

    generated_at = payload.get("generated_at")
    if not generated_at:
        raise ValueError("generated_at is required")
    if payload.get("legal_boundary_acknowledged") is not True:
        raise ValueError("legal_boundary_acknowledged must be true before saving")

    jurisdiction = payload.get("jurisdiction_context")
    if not isinstance(jurisdiction, dict):
        raise ValueError("jurisdiction_context must be a dict")
    if jurisdiction.get("state_specific_law_not_interpreted") is not True:
        raise ValueError(
            "jurisdiction_context.state_specific_law_not_interpreted must be true"
        )

    if not isinstance(payload.get("household_context"), dict):
        raise ValueError("household_context must be a dict")

    document_inventory = payload.get("document_inventory")
    if not isinstance(document_inventory, dict):
        raise ValueError("document_inventory must be a dict")
    missing_documents = sorted(
        _ESTATE_DOCUMENT_INVENTORY_KEYS - set(document_inventory)
    )
    if missing_documents:
        raise ValueError(
            "Missing required document_inventory keys: "
            + ", ".join(missing_documents)
        )
    for key in sorted(document_inventory):
        item = document_inventory.get(key)
        if not isinstance(item, dict):
            raise ValueError(f"document_inventory.{key} must be a dict")
        item_status = item.get("status")
        if item_status not in _ESTATE_DOCUMENT_STATUS_VALUES:
            allowed = ", ".join(sorted(_ESTATE_DOCUMENT_STATUS_VALUES))
            raise ValueError(
                f"document_inventory.{key}.status must be one of: {allowed}"
            )

    beneficiary_review = payload.get("beneficiary_review")
    if not isinstance(beneficiary_review, dict):
        raise ValueError("beneficiary_review must be a dict")
    for key in ("accounts_to_review", "mismatch_flags", "user_tasks"):
        if not isinstance(beneficiary_review.get(key), list):
            raise ValueError(f"beneficiary_review.{key} must be a list")

    referral_context = payload.get("referral_context")
    if not isinstance(referral_context, dict):
        raise ValueError("referral_context must be a dict")
    attorney_recommended = referral_context.get("attorney_recommended")
    reasons = referral_context.get("reasons")
    specialist_resources = referral_context.get("specialist_resources")
    if not isinstance(attorney_recommended, bool):
        raise ValueError("referral_context.attorney_recommended must be a bool")
    if not isinstance(reasons, list):
        raise ValueError("referral_context.reasons must be a list")
    if not isinstance(specialist_resources, list) or "attorney" not in [
        str(item) for item in specialist_resources
    ]:
        raise ValueError("referral_context.specialist_resources must include attorney")
    if status == "attorney_recommended" and not attorney_recommended:
        raise ValueError(
            "readiness_status=attorney_recommended requires "
            "referral_context.attorney_recommended=true"
        )
    if attorney_recommended and not any(str(reason).strip() for reason in reasons):
        raise ValueError(
            "referral_context.reasons must include at least one reason when "
            "attorney_recommended is true"
        )

    for key in ("next_actions", "scope_notes"):
        if not isinstance(payload.get(key), list):
            raise ValueError(f"{key} must be a list")

    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _normalize_tax_readiness_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(_TAX_READINESS_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")
    payload = dict(plan_payload)
    generated_at = payload.get("generated_at")
    if not generated_at:
        generated_at = utc_now_iso()
        payload["generated_at"] = generated_at
    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _normalize_spending_plan_payload(plan_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan_payload, dict):
        raise ValueError("plan_payload must be a dict")
    missing = sorted(_SPENDING_PLAN_ARTIFACT_REQUIRED_KEYS - set(plan_payload))
    if missing:
        raise ValueError(f"Missing required plan_payload keys: {', '.join(missing)}")
    payload = dict(plan_payload)
    generated_at = payload.get("generated_at")
    if not generated_at:
        generated_at = utc_now_iso()
        payload["generated_at"] = generated_at
    if not payload.get("last_modified_at"):
        payload["last_modified_at"] = generated_at
    return payload


def _render_spending_plan_allocations(value: Any) -> str:
    if not isinstance(value, dict):
        return "- None recorded"
    by_category = value.get("by_category")
    if not isinstance(by_category, list) or not by_category:
        return "- None recorded"
    lines = [
        "| Category | Type | Plan ($) | Anchor (3-mo avg) | Notes |",
        "|---|---|---|---|---|",
    ]
    for entry in by_category:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("category_name") or entry.get("category_id") or "?")
        cat_type = str(entry.get("type") or "?")
        plan_text = _format_dollars_from_cents(entry.get("monthly_cents"))
        anchor_text = _format_dollars_from_cents(entry.get("anchor_3mo_avg_cents"))
        notes = str(entry.get("notes") or "")
        lines.append(f"| {name} | {cat_type} | {plan_text} | {anchor_text} | {notes} |")
    if len(lines) == 2:
        return "- None recorded"
    return "\n".join(lines)


def _render_spending_plan_periodic_reservations(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Item | Annual ($) | Monthly reserve ($) | Next hit |",
        "|---|---|---|---|",
    ]
    for entry in value:
        if not isinstance(entry, dict):
            continue
        item_name = str(entry.get("item_name") or "?")
        annual_text = _format_dollars_from_cents(entry.get("annual_cents"))
        monthly_text = _format_dollars_from_cents(entry.get("monthly_reserve_cents"))
        next_hit = str(entry.get("next_hit_estimated") or "TBD")
        lines.append(f"| {item_name} | {annual_text} | {monthly_text} | {next_hit} |")
    if len(lines) == 2:
        return "- None recorded"
    return "\n".join(lines)


def _render_spending_plan_line_item(value: Any, source_key: str) -> str:
    if not isinstance(value, dict):
        return "TBD"
    monthly_text = _format_dollars_from_cents(value.get("monthly_cents"))
    source = value.get("sourced_from") or "user_stated"
    return f"{monthly_text}/mo (sourced from {source})"


def _render_spending_plan_reconciliation_decisions(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines: list[str] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        recorded_at = str(entry.get("recorded_at") or "?")
        kind = str(entry.get("type") or "?")
        sibling_text = _format_dollars_from_cents(entry.get("sibling_value_cents"))
        this_text = _format_dollars_from_cents(entry.get("this_plan_value_cents"))
        choice = str(entry.get("user_choice") or "?")
        rationale = str(entry.get("rationale") or "")
        lines.append(
            f"- **{recorded_at}** ({kind}): sibling={sibling_text}, this={this_text} → {choice}. {rationale}".rstrip()
        )
    return "\n".join(lines) if lines else "- None recorded"


def _render_spending_plan_variance_history(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines: list[str] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        month = str(entry.get("month") or "?")
        overall = entry.get("overall") if isinstance(entry.get("overall"), dict) else {}
        plan_text = _format_dollars_from_cents(overall.get("plan_total_cents"))
        actual_text = _format_dollars_from_cents(overall.get("actual_total_cents"))
        pct_value = overall.get("variance_pct")
        pct_text = "?"
        if isinstance(pct_value, (int, float)):
            pct_text = f"{pct_value:+.1f}%"
        per_category = entry.get("per_category")
        cat_summary = ""
        if isinstance(per_category, list):
            signal_count = sum(
                1
                for item in per_category
                if isinstance(item, dict)
                and item.get("classification") in ("signal", "directional")
            )
            cat_summary = f" — {signal_count} signal/directional"
        lines.append(
            f"- **{month}**: plan {plan_text} vs actual {actual_text} ({pct_text}){cat_summary}"
        )
    return "\n".join(lines) if lines else "- None recorded"


def _render_spending_plan_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    allocations = payload.get("allocations")
    allocations_dict = allocations if isinstance(allocations, dict) else {}
    expected_income_cents = int(payload.get("expected_monthly_income_cents") or 0)
    expected_expenses_cents = int(payload.get("expected_monthly_expenses_cents") or 0)
    net_cents = expected_income_cents - expected_expenses_cents
    essential_text = _format_dollars_from_cents(
        payload.get("expected_essential_monthly_cents")
    )
    discretionary_text = _format_dollars_from_cents(
        payload.get("expected_discretionary_monthly_cents")
    )
    mirror_status = payload.get("mirror_status")
    if not isinstance(mirror_status, dict):
        mirror_status = {}
    failed = mirror_status.get("failed_categories")
    if isinstance(failed, list) and failed:
        failed_text = ", ".join(str(item) for item in failed)
    else:
        failed_text = "none"
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _SPENDING_PLAN_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        strategy=str(payload.get("strategy") or "TBD"),
        review_cadence=str(payload.get("review_cadence") or "monthly"),
        next_review=str(payload.get("next_review_at") or "TBD"),
        expected_income=_format_dollars_from_cents(expected_income_cents),
        essential_expenses=essential_text,
        discretionary_expenses=discretionary_text,
        net=_format_dollars_from_cents(net_cents),
        allocations_table=_render_spending_plan_allocations(allocations_dict),
        periodic_reservations=_render_spending_plan_periodic_reservations(
            payload.get("periodic_reservations")
        ),
        efund_line=_render_spending_plan_line_item(
            allocations_dict.get("emergency_fund"), "coach_emergency_fund"
        ),
        debt_line=_render_spending_plan_line_item(
            allocations_dict.get("debt_paydown"), "coach_debt_payoff"
        ),
        reconciliation_decisions=_render_spending_plan_reconciliation_decisions(
            payload.get("reconciliation_decisions")
        ),
        variance_history=_render_spending_plan_variance_history(
            payload.get("variance_history")
        ),
        mirror_state=str(mirror_status.get("state") or "ok"),
        mirror_failed=failed_text,
        mirror_recorded_at=str(mirror_status.get("recorded_at") or "TBD"),
        yaml_footer=footer + "\n",
    )


def _render_tax_readiness_profile(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- Not recorded"
    labels = {
        "filing_status_assumption": "Filing-status assumption",
        "income_types": "Income types",
        "has_business_activity": "Business activity",
        "has_contractor_payments": "Contractor payments",
        "has_prior_year_issues": "Prior-year issues",
        "has_irs_or_state_notice": "IRS/state notice",
        "state_filing_notes": "State/local filing notes",
    }
    lines: list[str] = []
    for key, label in labels.items():
        if key not in value:
            continue
        item = value.get(key)
        if isinstance(item, list):
            text = ", ".join(str(part) for part in item) or "none"
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    extra_keys = sorted(set(value) - set(labels))
    for key in extra_keys:
        lines.append(f"- **{key}:** {value[key]}")
    return "\n".join(lines) if lines else "- Not recorded"


def _render_tax_readiness_route(value: Any) -> tuple[str, str]:
    if not isinstance(value, dict):
        return "TBD", "- Not recorded"
    route = str(value.get("route") or value.get("route_label") or "TBD")
    rationale = str(value.get("rationale") or "TBD")
    referrals = value.get("referrals")
    referral_text = (
        ", ".join(str(item) for item in referrals)
        if isinstance(referrals, list)
        else ""
    )
    lines = [
        f"- **Route:** {route}",
        f"- **Rationale:** {rationale}",
    ]
    if referral_text:
        lines.append(f"- **Referral records:** {referral_text}")
    if value.get("scope_note"):
        lines.append(f"- **Scope note:** {value['scope_note']}")
    return route, "\n".join(lines)


def _render_tax_readiness_checklist(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = ["| Item | Status | Owner | Notes |", "|---|---|---|---|"]
    for entry in value:
        if not isinstance(entry, dict):
            lines.append(f"| {entry} | needed | user | |")
            continue
        item = str(entry.get("item") or entry.get("name") or "?")
        status = str(entry.get("status") or "needed")
        owner = str(entry.get("owner") or "user")
        notes = str(entry.get("notes") or "")
        lines.append(f"| {item} | {status} | {owner} | {notes} |")
    return "\n".join(lines)


def _render_tax_readiness_mapping(value: Any) -> str:
    if not value:
        return "- None recorded"
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                rendered = yaml.safe_dump(
                    item, sort_keys=False, allow_unicode=False
                ).strip()
                lines.append(f"- **{key}:** `{rendered}`")
            else:
                lines.append(f"- **{key}:** {item}")
        return "\n".join(lines) if lines else "- None recorded"
    return _render_list_items(value)


def _render_tax_readiness_referrals(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines: list[str] = []
    for entry in value:
        if isinstance(entry, dict):
            referral_id = str(entry.get("referral_id") or entry.get("id") or "?")
            reason = str(entry.get("reason") or "")
            lines.append(f"- **{referral_id}:** {reason}".rstrip())
        else:
            lines.append(f"- {entry}")
    return "\n".join(lines)


def _render_tax_readiness_next_actions(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines: list[str] = []
    for index, entry in enumerate(value, start=1):
        if isinstance(entry, dict):
            action = str(entry.get("action") or entry.get("step") or "TBD")
            owner = str(entry.get("owner") or "user")
            due = str(entry.get("due") or entry.get("due_date") or "TBD")
            status = str(entry.get("status") or "open")
            lines.append(
                f"{index}. {action} (owner: {owner}; due: {due}; status: {status})"
            )
        else:
            lines.append(f"{index}. {entry}")
    return "\n".join(lines)


def _render_tax_readiness_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    route_label, route_text = _render_tax_readiness_route(
        payload.get("preparation_route")
    )
    calibration = {
        "withholding_plan": payload.get("withholding_plan"),
        "estimated_tax_plan": payload.get("estimated_tax_plan"),
    }
    risk_flags = payload.get("risk_flags")
    risk_text = _render_tax_readiness_mapping(risk_flags)
    referrals = _render_tax_readiness_referrals(payload.get("referrals"))
    if referrals != "- None recorded":
        risk_text = f"{risk_text}\n\n### Referrals\n{referrals}"
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _TAX_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        tax_year=str(payload.get("tax_year") or "TBD"),
        route_label=route_label,
        profile=_render_tax_readiness_profile(payload.get("profile")),
        preparation_route=route_text,
        document_checklist=_render_tax_readiness_checklist(
            payload.get("document_checklist")
        ),
        business_readiness=_render_tax_readiness_mapping(
            payload.get("business_readiness")
        ),
        tax_calibration=_render_tax_readiness_mapping(calibration),
        risk_flags=risk_text,
        next_actions=_render_tax_readiness_next_actions(payload.get("next_actions")),
        yaml_footer=footer + "\n",
    )


def _format_optional_pct(value: Any) -> str:
    if value in (None, ""):
        return "TBD"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _render_financial_plan_intake_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _financial_plan_intake_table_cell(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("\n", " ").replace("|", "\\|")


def _render_financial_plan_intake_goals(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Goal | Horizon | Priority | Source | Notes |",
        "|---|---|---|---|---|",
    ]
    for goal in value:
        if not isinstance(goal, dict):
            continue
        lines.append(
            "| {name} | {horizon} | {priority} | {source} | {notes} |".format(
                name=_financial_plan_intake_table_cell(
                    goal.get("name") or goal.get("goal_id")
                ),
                horizon=_financial_plan_intake_table_cell(
                    goal.get("time_horizon")
                ),
                priority=_financial_plan_intake_table_cell(goal.get("priority")),
                source=_financial_plan_intake_table_cell(goal.get("source")),
                notes=_financial_plan_intake_table_cell(goal.get("notes")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_financial_plan_intake_sequence(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Order | Next skill | Rationale | Status |",
        "|---|---|---|---|",
    ]
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            continue
        lines.append(
            "| {order} | {next_skill} | {rationale} | {status} |".format(
                order=index,
                next_skill=_financial_plan_intake_table_cell(
                    item.get("next_skill") or item.get("handoff_type")
                ),
                rationale=_financial_plan_intake_table_cell(item.get("rationale")),
                status=_financial_plan_intake_table_cell(
                    item.get("status") or item.get("readiness_status")
                ),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_financial_plan_intake_handoffs(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Type | Reason | Status |",
        "|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| {type} | {reason} | {status} |".format(
                type=_financial_plan_intake_table_cell(item.get("type")),
                reason=_financial_plan_intake_table_cell(item.get("reason")),
                status=_financial_plan_intake_table_cell(item.get("status")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_financial_plan_intake_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    monitoring = payload.get("monitoring")
    next_review = (
        monitoring.get("next_review_date")
        if isinstance(monitoring, dict)
        else None
    )
    return _FINANCIAL_PLAN_INTAKE_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        snapshot_status=str(payload.get("snapshot_status") or "TBD"),
        next_review=str(next_review or payload.get("next_review_date") or "TBD"),
        household_context=_render_financial_plan_intake_mapping(
            payload.get("household_context")
        ),
        goals=_render_financial_plan_intake_goals(payload.get("goals")),
        assets_liabilities=_render_financial_plan_intake_mapping(
            payload.get("assets_liabilities")
        ),
        cash_flow=_render_financial_plan_intake_mapping(payload.get("cash_flow")),
        domain_readiness=_render_financial_plan_intake_mapping(
            payload.get("domain_readiness")
        ),
        sibling_artifacts=_render_list_items(payload.get("sibling_artifacts")),
        planning_sequence=_render_financial_plan_intake_sequence(
            payload.get("planning_sequence")
        ),
        professional_handoffs=_render_financial_plan_intake_handoffs(
            payload.get("professional_handoffs")
        ),
        data_gaps=_render_list_items(payload.get("data_gaps")),
        monitoring=_render_financial_plan_intake_mapping(payload.get("monitoring")),
        yaml_footer=footer + "\n",
    )


def _render_risk_insurance_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _risk_insurance_table_cell(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("\n", " ").replace("|", "\\|")


def _render_risk_insurance_coverage_inventory(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines = [
        "| Coverage | Known | Details |",
        "|---|---|---|",
    ]
    for coverage_type in sorted(value):
        item = value.get(coverage_type)
        if isinstance(item, dict):
            known = item.get("known")
            details = {
                key: val
                for key, val in item.items()
                if key != "known" and val not in (None, "")
            }
            detail_text = (
                yaml.safe_dump(details, sort_keys=True, allow_unicode=False).strip()
                if details
                else ""
            )
        else:
            known = ""
            detail_text = str(item)
        lines.append(
            "| {coverage} | {known} | {details} |".format(
                coverage=_risk_insurance_table_cell(
                    coverage_type.replace("_", " ")
                ),
                known=_risk_insurance_table_cell(known),
                details=_risk_insurance_table_cell(detail_text),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_risk_insurance_flags(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Flag | Severity | Rationale |",
        "|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| {flag} | {severity} | {rationale} |".format(
                flag=_risk_insurance_table_cell(
                    item.get("flag_id") or item.get("flag")
                ),
                severity=_risk_insurance_table_cell(item.get("severity")),
                rationale=_risk_insurance_table_cell(item.get("rationale")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_risk_insurance_handoffs(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Type | Reason | Status |",
        "|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| {type} | {reason} | {status} |".format(
                type=_risk_insurance_table_cell(item.get("type")),
                reason=_risk_insurance_table_cell(item.get("reason")),
                status=_risk_insurance_table_cell(item.get("status")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_risk_insurance_readiness_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    monitoring = payload.get("monitoring")
    next_check_in = (
        monitoring.get("next_check_in")
        if isinstance(monitoring, dict)
        else None
    )
    return _RISK_INSURANCE_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(next_check_in or payload.get("next_check_in") or "TBD"),
        household_context=_render_risk_insurance_mapping(
            payload.get("household_context")
        ),
        liquidity_context=_render_risk_insurance_mapping(
            payload.get("liquidity_context")
        ),
        coverage_inventory=_render_risk_insurance_coverage_inventory(
            payload.get("coverage_inventory")
        ),
        risk_flags=_render_risk_insurance_flags(payload.get("risk_flags")),
        professional_handoffs=_render_risk_insurance_handoffs(
            payload.get("professional_handoffs")
        ),
        planning_implications=_render_list_items(
            payload.get("planning_implications")
        ),
        data_gaps=_render_list_items(payload.get("data_gaps")),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        yaml_footer=footer + "\n",
    )


def _render_advisor_handoff_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _render_advisor_handoff_context(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for label, key in (
        ("Relevant artifacts", "relevant_artifacts"),
        ("Key facts", "key_facts"),
        ("User questions", "user_questions"),
    ):
        lines.append(f"**{label}**")
        lines.append(_render_list_items(value.get(key)))
        lines.append("")
    extras = {
        key: item
        for key, item in value.items()
        if key not in {"relevant_artifacts", "key_facts", "user_questions"}
    }
    if extras:
        lines.append("**Additional context**")
        lines.append(_render_advisor_handoff_mapping(extras))
    return "\n".join(line for line in lines).strip()


def _render_advisor_handoff_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    monitoring = payload.get("monitoring")
    next_check_in = (
        monitoring.get("next_check_in")
        if isinstance(monitoring, dict)
        else None
    )
    return _ADVISOR_HANDOFF_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        handoff_status=str(payload.get("handoff_status") or "TBD"),
        next_check_in=str(next_check_in or payload.get("next_check_in") or "TBD"),
        request_classification=_render_advisor_handoff_mapping(
            payload.get("request_classification")
        ),
        professional_type=_render_advisor_handoff_mapping(
            payload.get("professional_type")
        ),
        cashnerd_context=_render_advisor_handoff_context(
            payload.get("cashnerd_context")
        ),
        handoff_questions=_render_list_items(payload.get("handoff_questions")),
        documents_to_bring=_render_list_items(payload.get("documents_to_bring")),
        disclosures_to_surface=_render_list_items(
            payload.get("disclosures_to_surface")
        ),
        boundary_response=_render_advisor_handoff_mapping(
            payload.get("boundary_response")
        ),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        yaml_footer=footer + "\n",
    )


def _render_homebuying_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _render_homebuying_scenarios(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Scenario | Home price | Down payment | Loan amount | Rate | Term | Housing payment | Full ownership cost |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for entry in value:
        if not isinstance(entry, dict):
            continue
        rate = entry.get("rate_assumption")
        rate_pct = rate.get("value_pct") if isinstance(rate, dict) else None
        rate_source = rate.get("source") if isinstance(rate, dict) else ""
        rate_text = _format_optional_pct(rate_pct)
        if rate_source:
            rate_text = f"{rate_text} ({rate_source})"
        lines.append(
            "| {scenario} | {home_price} | {down_payment} | {loan_amount} | "
            "{rate} | {term} | {payment} | {full_cost} |".format(
                scenario=str(entry.get("scenario_id") or "scenario"),
                home_price=_format_dollars_from_cents(entry.get("home_price_cents")),
                down_payment=_format_dollars_from_cents(
                    entry.get("down_payment_cents")
                ),
                loan_amount=_format_dollars_from_cents(entry.get("loan_amount_cents")),
                rate=rate_text,
                term=str(entry.get("term_years") or "TBD"),
                payment=_format_dollars_from_cents(
                    entry.get("monthly_housing_payment_cents")
                ),
                full_cost=_format_dollars_from_cents(
                    entry.get("monthly_homeownership_cost_cents")
                ),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_homebuying_ratios(value: Any) -> str:
    if not isinstance(value, dict):
        return "- None recorded"
    lines = [
        f"- **Front-end ratio:** {_format_optional_pct(value.get('front_end_ratio_pct'))}",
        f"- **Back-end ratio:** {_format_optional_pct(value.get('back_end_ratio_pct'))}",
        "- **Full homeownership cost ratio:** "
        f"{_format_optional_pct(value.get('full_homeownership_cost_ratio_pct'))}",
        "- **Other monthly debt payments:** "
        f"{_format_dollars_from_cents(value.get('other_monthly_debt_payments_cents'))}",
    ]
    notes = value.get("ratio_notes")
    if notes:
        lines.append("### Ratio Notes")
        lines.append(_render_list_items(notes))
    return "\n".join(lines)


def _render_homebuying_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _HOMEBUYING_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(payload.get("next_check_in") or "TBD"),
        household_profile=_render_homebuying_mapping(
            payload.get("household_profile")
        ),
        affordability_scenarios=_render_homebuying_scenarios(
            payload.get("affordability_scenarios")
        ),
        cash_to_close=_render_homebuying_mapping(payload.get("cash_to_close")),
        ratios=_render_homebuying_ratios(payload.get("ratios")),
        credit_readiness=_render_homebuying_mapping(payload.get("credit_readiness")),
        readiness_flags=_render_list_items(payload.get("readiness_flags")),
        cross_skill_context=_render_homebuying_mapping(
            payload.get("cross_skill_context")
        ),
        preapproval_checklist=_render_list_items(
            payload.get("preapproval_checklist")
        ),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        referrals=_render_list_items(payload.get("referrals")),
        scope_notes=_render_list_items(payload.get("scope_notes")),
        yaml_footer=footer + "\n",
    )


def _render_retirement_contribution_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _render_retirement_priority_result(value: Any) -> str:
    if not isinstance(value, dict):
        return "- None recorded"
    lines = [
        f"- **Helper:** {value.get('helper') or 'TBD'}",
        f"- **Source tax year:** {value.get('source_tax_year') or 'TBD'}",
    ]
    limits_source = value.get("limits_source")
    if isinstance(limits_source, dict) and limits_source:
        lines.append("- **Limits source:**")
        for key in sorted(limits_source):
            lines.append(f"  - {key}: {limits_source[key]}")
    data_needed = value.get("data_needed")
    if isinstance(data_needed, list) and data_needed:
        lines.append("- **Data needed:**")
        for item in data_needed:
            lines.append(f"  - {item}")

    steps = value.get("steps")
    if isinstance(steps, list) and steps:
        lines.extend(
            [
                "",
                "| Order | Account | Action | Annual | Monthly | Rank | Reason |",
                "|---|---|---|---|---|---|---|",
            ]
        )
        for step in steps:
            if not isinstance(step, dict):
                continue
            lines.append(
                "| {order} | {account} | {action} | {annual} | {monthly} | {rank} | {reason} |".format(
                    order=str(step.get("order") or ""),
                    account=str(step.get("account") or ""),
                    action=str(step.get("action") or ""),
                    annual=_format_dollars_from_cents(
                        step.get("annual_amount_cents")
                    ),
                    monthly=_format_dollars_from_cents(
                        step.get("monthly_equivalent_cents")
                    ),
                    rank=str(step.get("priority_rank") or ""),
                    reason=str(step.get("reason") or ""),
                )
            )
    else:
        lines.append("- **Steps:** none recorded")
    return "\n".join(lines)


def _render_retirement_contribution_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _RETIREMENT_CONTRIBUTION_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        tax_year=str(payload.get("tax_year") or "TBD"),
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(payload.get("next_check_in") or "TBD"),
        household_profile=_render_retirement_contribution_mapping(
            payload.get("household_profile")
        ),
        cash_flow_context=_render_retirement_contribution_mapping(
            payload.get("cash_flow_context")
        ),
        employer_plan_context=_render_retirement_contribution_mapping(
            payload.get("employer_plan_context")
        ),
        hsa_context=_render_retirement_contribution_mapping(payload.get("hsa_context")),
        ira_context=_render_retirement_contribution_mapping(payload.get("ira_context")),
        priority_result=_render_retirement_priority_result(
            payload.get("priority_result")
        ),
        selected_commitment=_render_retirement_contribution_mapping(
            payload.get("selected_commitment")
        ),
        readiness_flags=_render_list_items(payload.get("readiness_flags")),
        cross_skill_context=_render_retirement_contribution_mapping(
            payload.get("cross_skill_context")
        ),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        referrals=_render_list_items(payload.get("referrals")),
        scope_notes=_render_list_items(payload.get("scope_notes")),
        yaml_footer=footer + "\n",
    )


def _render_retirement_income_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _retirement_income_table_cell(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("\n", " ").replace("|", "\\|")


def _render_retirement_income_milestones(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Milestone | Status | Source |",
        "|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            continue
        source = item.get("source_url")
        if not source and isinstance(item.get("source_metadata"), dict):
            source = item["source_metadata"].get("source_url")
        lines.append(
            "| {name} | {status} | {source} |".format(
                name=_retirement_income_table_cell(item.get("name")),
                status=_retirement_income_table_cell(item.get("status")),
                source=_retirement_income_table_cell(source),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_retirement_income_handoffs(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Type | Trigger | Question to ask |",
        "|---|---|---|",
    ]
    for item in value:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| {type} | {trigger} | {question} |".format(
                type=_retirement_income_table_cell(item.get("type")),
                trigger=_retirement_income_table_cell(item.get("trigger")),
                question=_retirement_income_table_cell(item.get("question_to_ask")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_retirement_income_readiness_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _RETIREMENT_INCOME_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(payload.get("next_check_in") or "TBD"),
        household_timeline=_render_retirement_income_mapping(
            payload.get("household_timeline")
        ),
        income_sources=_render_retirement_income_mapping(
            payload.get("income_sources")
        ),
        health_and_risk_context=_render_retirement_income_mapping(
            payload.get("health_and_risk_context")
        ),
        cash_flow_context=_render_retirement_income_mapping(
            payload.get("cash_flow_context")
        ),
        milestones=_render_retirement_income_milestones(payload.get("milestones")),
        rmd_context=_render_retirement_income_mapping(payload.get("rmd_context")),
        professional_handoffs=_render_retirement_income_handoffs(
            payload.get("professional_handoffs")
        ),
        boundary_response=_render_retirement_income_mapping(
            payload.get("boundary_response")
        ),
        questions_to_ask=_render_list_items(payload.get("questions_to_ask")),
        documents_to_gather=_render_list_items(payload.get("documents_to_gather")),
        data_gaps=_render_list_items(payload.get("data_gaps")),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        scope_notes=_render_list_items(payload.get("scope_notes")),
        yaml_footer=footer + "\n",
    )


def _render_investment_readiness_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if key.endswith("_cents"):
            text = _format_dollars_from_cents(item)
        elif key.endswith("_pct"):
            text = _format_optional_pct(item)
        elif isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _investment_table_cell(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("\n", " ").replace("|", "\\|")


def _render_investment_candidate_actions(value: Any) -> str:
    if not isinstance(value, list) or not value:
        return "- None recorded"
    lines = [
        "| Action | Amount | Cadence | Source | Destination | Scope | Write status | Rationale |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for action in value:
        if not isinstance(action, dict):
            continue
        lines.append(
            "| {action_id} | {amount} | {cadence} | {source} | {destination} | {scope} | {write_status} | {rationale} |".format(
                action_id=_investment_table_cell(action.get("action_id")),
                amount=_format_dollars_from_cents(action.get("amount_cents")),
                cadence=_investment_table_cell(action.get("cadence")),
                source=_investment_table_cell(action.get("source_account_label")),
                destination=_investment_table_cell(
                    action.get("destination_account_label")
                ),
                scope=_investment_table_cell(action.get("scope_label")),
                write_status=_investment_table_cell(action.get("write_status")),
                rationale=_investment_table_cell(action.get("rationale")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_investment_readiness_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    monitoring = payload.get("monitoring")
    next_check_in = (
        monitoring.get("next_check_in")
        if isinstance(monitoring, dict)
        else None
    )
    return _INVESTMENT_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(next_check_in or payload.get("next_check_in") or "TBD"),
        user_goal=_render_investment_readiness_mapping(payload.get("user_goal")),
        cash_flow_context=_render_investment_readiness_mapping(
            payload.get("cash_flow_context")
        ),
        retirement_tax_context=_render_investment_readiness_mapping(
            payload.get("retirement_tax_context")
        ),
        risk_context=_render_investment_readiness_mapping(payload.get("risk_context")),
        candidate_actions=_render_investment_candidate_actions(
            payload.get("candidate_actions")
        ),
        selected_action=_render_investment_readiness_mapping(
            payload.get("selected_action")
        ),
        boundary=_render_investment_readiness_mapping(payload.get("boundary")),
        data_gaps=_render_list_items(payload.get("data_gaps")),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        monitoring=_render_investment_readiness_mapping(payload.get("monitoring")),
        yaml_footer=footer + "\n",
    )


def _render_estate_mapping(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines: list[str] = []
    for key in sorted(value):
        item = value.get(key)
        label = key.replace("_", " ")
        if isinstance(item, list):
            text = ", ".join(str(part) for part in item) if item else "none"
        elif isinstance(item, dict):
            text = yaml.safe_dump(item, sort_keys=False, allow_unicode=False).strip()
        else:
            text = str(item)
        lines.append(f"- **{label}:** {text}")
    return "\n".join(lines) if lines else "- None recorded"


def _estate_table_cell(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).replace("\n", " ").replace("|", "\\|")


def _render_estate_document_inventory(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "- None recorded"
    lines = [
        "| Document | Status | Last reviewed | Notes |",
        "|---|---|---|---|",
    ]
    ordered_keys = [
        *[key for key in sorted(_ESTATE_DOCUMENT_INVENTORY_KEYS) if key in value],
        *[key for key in sorted(set(value) - _ESTATE_DOCUMENT_INVENTORY_KEYS)],
    ]
    for key in ordered_keys:
        item = value.get(key)
        if not isinstance(item, dict):
            continue
        label = key.replace("_", " ")
        lines.append(
            "| {label} | {status} | {last_reviewed} | {notes} |".format(
                label=_estate_table_cell(label),
                status=_estate_table_cell(item.get("status")),
                last_reviewed=_estate_table_cell(item.get("last_reviewed")),
                notes=_estate_table_cell(item.get("notes")),
            )
        )
    return "\n".join(lines) if len(lines) > 2 else "- None recorded"


def _render_estate_beneficiary_review(value: Any) -> str:
    if not isinstance(value, dict):
        return "- None recorded"
    lines = [
        "### Accounts to Review",
        _render_list_items(value.get("accounts_to_review")),
        "",
        "### Mismatch Flags",
        _render_list_items(value.get("mismatch_flags")),
        "",
        "### User Tasks",
        _render_list_items(value.get("user_tasks"), numbered=True),
    ]
    return "\n".join(lines)


def _render_estate_referral_context(value: Any) -> str:
    if not isinstance(value, dict):
        return "- None recorded"
    attorney_recommended = value.get("attorney_recommended")
    specialist_resources = value.get("specialist_resources")
    resources_text = (
        ", ".join(str(item) for item in specialist_resources)
        if isinstance(specialist_resources, list)
        else "attorney"
    )
    return "\n".join(
        [
            f"- **Attorney recommended:** {attorney_recommended}",
            "- **Reasons:**",
            _render_list_items(value.get("reasons")),
            f"- **Specialist resources:** {resources_text}",
        ]
    )


def _render_estate_document_readiness_artifact(payload: dict[str, Any]) -> str:
    generated_date = _generated_at_date(payload.get("generated_at")).isoformat()
    footer = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    return _ESTATE_DOCUMENT_READINESS_ARTIFACT_TEMPLATE.format(
        generated_date=generated_date,
        readiness_status=str(payload.get("readiness_status") or "TBD"),
        next_check_in=str(payload.get("next_check_in") or "TBD"),
        legal_boundary_acknowledged=str(
            payload.get("legal_boundary_acknowledged") is True
        ),
        jurisdiction_context=_render_estate_mapping(
            payload.get("jurisdiction_context")
        ),
        household_context=_render_estate_mapping(payload.get("household_context")),
        document_inventory=_render_estate_document_inventory(
            payload.get("document_inventory")
        ),
        beneficiary_review=_render_estate_beneficiary_review(
            payload.get("beneficiary_review")
        ),
        referral_context=_render_estate_referral_context(
            payload.get("referral_context")
        ),
        next_actions=_render_list_items(payload.get("next_actions"), numbered=True),
        scope_notes=_render_list_items(payload.get("scope_notes")),
        yaml_footer=footer + "\n",
    )


def _parse_spending_plan_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("````yaml", marker_index)
    fence_close_token = "````"
    if fence_start < 0:
        fence_start = markdown.find("```yaml", marker_index)
        fence_close_token = "```"
        if fence_start < 0:
            return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find(fence_close_token, yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_tax_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_financial_plan_intake_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_risk_insurance_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_advisor_handoff_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_homebuying_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_retirement_contribution_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_retirement_income_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_investment_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_estate_document_readiness_artifact(markdown: str) -> dict[str, Any]:
    marker = "## Generated machine-readable footer"
    marker_index = markdown.find(marker)
    if marker_index < 0:
        return {}
    fence_start = markdown.find("```yaml", marker_index)
    if fence_start < 0:
        return {}
    yaml_start = markdown.find("\n", fence_start)
    fence_end = markdown.find("```", yaml_start + 1)
    if yaml_start < 0 or fence_end < 0:
        return {}
    parsed = yaml.safe_load(markdown[yaml_start + 1 : fence_end].strip()) or {}
    return parsed if isinstance(parsed, dict) else {}


def _spending_plan_revisions_for_date(artifact_dir: Path, day: date) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_spending_plan_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    """Same-date revision rule: matching generated_at -> update_in_place;
    differing generated_at on the same day -> new -rN file."""
    existing = _spending_plan_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_spending_plan_artifact(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_spending_plan_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    """Resolve a read request to a single artifact path or a reason for absence.

    Accepts ``None`` (latest across all dates), ``"YYYY-MM-DD"`` /
    ``"YYYYMMDD"`` (latest revision for that day), or ``"<date>-rN"`` for an
    explicit revision (N >= 2).
    """
    if not artifact_dir.exists():
        return None, "no_directory"

    # Sort by (date_stem, revision_int) so 20260607-r2 sorts AFTER 20260607.md
    # (lexicographic sort would put -r2 before the base file).
    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _spending_plan_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _tax_readiness_revisions_for_date(artifact_dir: Path, day: date) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_tax_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _tax_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_tax_readiness_artifact(path.read_text(encoding="utf-8"))
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_tax_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _tax_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _financial_plan_intake_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_financial_plan_intake_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _financial_plan_intake_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_financial_plan_intake_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_financial_plan_intake_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _financial_plan_intake_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _risk_insurance_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_risk_insurance_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _risk_insurance_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_risk_insurance_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_risk_insurance_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _risk_insurance_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _advisor_handoff_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_advisor_handoff_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _advisor_handoff_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_advisor_handoff_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_advisor_handoff_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _advisor_handoff_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _homebuying_readiness_revisions_for_date(artifact_dir: Path, day: date) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_homebuying_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _homebuying_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_homebuying_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_homebuying_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _homebuying_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _retirement_contribution_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_retirement_contribution_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _retirement_contribution_readiness_revisions_for_date(
        artifact_dir,
        day,
    )
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_retirement_contribution_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_retirement_contribution_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _retirement_contribution_readiness_revisions_for_date(
        artifact_dir,
        day,
    )
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _retirement_income_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_retirement_income_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _retirement_income_readiness_revisions_for_date(
        artifact_dir,
        day,
    )
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_retirement_income_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_retirement_income_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _retirement_income_readiness_revisions_for_date(
        artifact_dir,
        day,
    )
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _investment_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_investment_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _investment_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_investment_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_investment_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _investment_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


def _estate_document_readiness_revisions_for_date(
    artifact_dir: Path,
    day: date,
) -> list[Path]:
    stem = day.strftime("%Y%m%d")
    base = artifact_dir / f"{stem}.md"
    candidates: list[Path] = [base] if base.exists() else []
    rev = 2
    while True:
        candidate = artifact_dir / f"{stem}-r{rev}.md"
        if not candidate.exists():
            break
        candidates.append(candidate)
        rev += 1
    return candidates


def _resolve_estate_document_readiness_save_path(
    artifact_dir: Path,
    payload_generated_at: str,
    day: date,
) -> tuple[Path, str]:
    existing = _estate_document_readiness_revisions_for_date(artifact_dir, day)
    if not existing:
        return artifact_dir / f"{day.strftime('%Y%m%d')}.md", "create"
    for path in existing:
        try:
            parsed = _parse_estate_document_readiness_artifact(
                path.read_text(encoding="utf-8")
            )
        except OSError:
            continue
        if parsed.get("generated_at") == payload_generated_at:
            return path, "update_in_place"
    next_rev = len(existing) + 1
    return artifact_dir / f"{day.strftime('%Y%m%d')}-r{next_rev}.md", "new_revision"


def _resolve_estate_document_readiness_read_path(
    artifact_dir: Path,
    date_query: Optional[str],
) -> tuple[Optional[Path], Optional[str]]:
    if not artifact_dir.exists():
        return None, "no_directory"

    def _sort_key(path: Path) -> tuple[str, int]:
        stem = path.stem
        if "-r" in stem:
            base, rev = stem.rsplit("-r", 1)
            try:
                return base, int(rev)
            except ValueError:
                return stem, 0
        return stem, 1

    all_files = sorted(artifact_dir.glob("*.md"), key=_sort_key)
    if not all_files:
        return None, "no_artifacts"
    if date_query is None:
        return all_files[-1], None
    raw = str(date_query).strip()
    if not raw:
        return None, "no_artifact_for_date"
    revision_suffix = ""
    date_portion = raw
    if "-r" in raw:
        date_portion, revision_suffix = raw.rsplit("-r", 1)
    stem = date_portion.replace("-", "")
    if len(stem) != 8 or not stem.isdigit():
        return None, "no_artifact_for_date"
    try:
        day = datetime.strptime(stem, "%Y%m%d").date()
    except ValueError:
        return None, "no_artifact_for_date"
    if revision_suffix:
        if not revision_suffix.isdigit():
            return None, "no_artifact_for_date"
        candidate = artifact_dir / f"{stem}-r{revision_suffix}.md"
        if not candidate.exists():
            return None, "no_artifact_for_date"
        return candidate, None
    revisions = _estate_document_readiness_revisions_for_date(artifact_dir, day)
    if not revisions:
        return None, "no_artifact_for_date"
    return revisions[-1], None


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_spending_plan_auto_approved=True,
)
def coach_spending_plan_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a spending plan to the user's artifact directory.

    Persistence path: <data_dir>/artifacts/coach_spending_plan/<YYYYMMDD>.md
    (directory created with parents=True, exist_ok=True on first use).

    Same-date revision rule (mirrors emergency-fund + savings-goal):
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix; all prior dated artifacts preserved.

    Args:
        plan_payload: Dict with required keys ``strategy``,
          ``expected_monthly_income_cents``, ``expected_monthly_expenses_cents``,
          ``allocations``, ``review_cadence``. Optional:
          ``expected_essential_monthly_cents``,
          ``expected_discretionary_monthly_cents``,
          ``periodic_reservations``, ``next_review_at``,
          ``last_review_recorded_at``, ``last_directional_flag_at``,
          ``last_drift_classified``, ``variance_history``,
          ``reconciliation_decisions``, ``mirror_status``,
          ``cross_skill_reference``. ``generated_at`` is filled server-side
          from ``utc_now_iso()`` if absent; ``last_modified_at`` defaults
          to ``generated_at`` on first save and is bumped on each save.
        dry_run: If True, validate the payload but don't write.

    Raises ValueError if a required key is missing.

    Returns:
        Dict with ``data: {artifact_path, generated_at, last_modified_at,
        save_mode, dry_run, plan_payload}, summary: {saved, valid,
        artifact_path, save_mode}``.
    """
    payload = _normalize_spending_plan_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _spending_plan_artifact_dir()
    artifact_path, save_mode = _resolve_spending_plan_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_spending_plan_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_spending_plan_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted spending plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).

    Returns:
        Dict. If artifact exists: ``{data: {plan_payload, artifact_path,
        generated_at, last_modified_at}, summary: {found: True, ...}}``. If
        artifact does not exist, returns ``{data: None, summary: {found:
        False, reason: <"no_directory" | "no_artifact_for_date" |
        "no_artifacts">}}``. Does NOT raise on missing.
    """
    artifact_dir = _spending_plan_artifact_dir()
    artifact_path, reason = _resolve_spending_plan_read_path(artifact_dir, date)
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_spending_plan_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_tax_readiness_auto_approved=True,
)
def coach_tax_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a tax-readiness plan to the user's artifact directory.

    Persistence path: <data_dir>/artifacts/coach_tax_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict with required keys ``tax_year``, ``profile``,
          ``preparation_route``, ``document_checklist``, and ``next_actions``.
          Optional keys include ``business_readiness``, ``withholding_plan``,
          ``estimated_tax_plan``, ``risk_flags``, ``referrals``,
          ``next_check_in``, and ``generated_at``.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_tax_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _tax_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_tax_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_tax_readiness_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_tax_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted tax-readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _tax_readiness_artifact_dir()
    artifact_path, reason = _resolve_tax_readiness_read_path(artifact_dir, date)
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_tax_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_financial_plan_intake_auto_approved=True,
)
def coach_financial_plan_intake_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a financial-planning snapshot.

    Persistence path:
    <data_dir>/artifacts/coach_financial_plan_intake/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the financial-plan-intake artifact
          contract in ``PLAN_SKILL_COACH_FINANCIAL_PLAN_INTAKE.md``.
          The payload may store intake, triage, and handoff metadata only;
          securities, tax filing, legal, and insurance product recommendation
          fields are rejected.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_financial_plan_intake_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _financial_plan_intake_artifact_dir()
    artifact_path, save_mode = _resolve_financial_plan_intake_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_financial_plan_intake_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_financial_plan_intake_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted financial-planning snapshot.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _financial_plan_intake_artifact_dir()
    artifact_path, reason = _resolve_financial_plan_intake_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_financial_plan_intake_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_risk_insurance_readiness_auto_approved=True,
)
def coach_risk_insurance_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a risk-and-insurance readiness plan.

    Persistence path:
    <data_dir>/artifacts/coach_risk_insurance_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the risk-insurance readiness artifact
          contract in ``PLAN_SKILL_COACH_RISK_INSURANCE_READINESS.md``.
          The payload may store inventory, gap, and handoff metadata only;
          product, insurer, coverage amount, claim, legal, and underwriting
          recommendation fields are rejected.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_risk_insurance_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _risk_insurance_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_risk_insurance_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_risk_insurance_readiness_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_risk_insurance_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted risk-and-insurance readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _risk_insurance_readiness_artifact_dir()
    artifact_path, reason = _resolve_risk_insurance_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_risk_insurance_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_advisor_handoff_readiness_auto_approved=True,
)
def coach_advisor_handoff_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist an advisor handoff readiness packet.

    Persistence path:
    <data_dir>/artifacts/coach_advisor_handoff_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the advisor-handoff readiness artifact
          contract in ``PLAN_SKILL_COACH_ADVISOR_HANDOFF_READINESS.md``.
          The payload may store classification, facts, questions, disclosures,
          boundary response, and handoff metadata only; it rejects regulated
          answers, named-professional selection, and undisclosed monetized
          referral metadata.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_advisor_handoff_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _advisor_handoff_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_advisor_handoff_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_advisor_handoff_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_advisor_handoff_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted advisor handoff readiness packet.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _advisor_handoff_readiness_artifact_dir()
    artifact_path, reason = _resolve_advisor_handoff_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_advisor_handoff_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_homebuying_readiness_auto_approved=True,
)
def coach_homebuying_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a homebuying-readiness plan to the user's artifact directory.

    Persistence path:
    <data_dir>/artifacts/coach_homebuying_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the homebuying-readiness artifact
          contract in ``PLAN_SKILL_COACH_HOMEBUYING_READINESS.md``.
          ``generated_at`` is required; ``last_modified_at`` is maintained
          server-side.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_homebuying_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _homebuying_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_homebuying_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_homebuying_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_homebuying_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted homebuying-readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _homebuying_readiness_artifact_dir()
    artifact_path, reason = _resolve_homebuying_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_homebuying_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_retirement_contribution_readiness_auto_approved=True,
)
def coach_retirement_contribution_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a retirement contribution-readiness plan.

    Persistence path:
    <data_dir>/artifacts/coach_retirement_contribution_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the retirement contribution-readiness
          artifact contract in
          ``PLAN_SKILL_COACH_RETIREMENT_CONTRIBUTION_READINESS.md``.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_retirement_contribution_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _retirement_contribution_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_retirement_contribution_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_retirement_contribution_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_retirement_contribution_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously-persisted retirement contribution-readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _retirement_contribution_readiness_artifact_dir()
    artifact_path, reason = _resolve_retirement_contribution_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_retirement_contribution_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_retirement_income_readiness_auto_approved=True,
)
def coach_retirement_income_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist a retirement income-readiness plan.

    Persistence path:
    <data_dir>/artifacts/coach_retirement_income_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the retirement income-readiness artifact
          contract in ``PLAN_SKILL_COACH_RETIREMENT_INCOME_READINESS.md``.
          The payload may store education, inventory, data gaps, source-backed
          timing context, boundary response, and professional-handoff metadata
          only; it rejects claiming, withdrawal, conversion, annuity,
          Medicare-plan, pension-election, tax, legal, portfolio, reminder,
          transfer, notification, account-write, and sibling-artifact write
          fields.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_retirement_income_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _retirement_income_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_retirement_income_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_retirement_income_readiness_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_retirement_income_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted retirement income-readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _retirement_income_readiness_artifact_dir()
    artifact_path, reason = _resolve_retirement_income_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_retirement_income_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_investment_readiness_auto_approved=True,
)
def coach_investment_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist an investment-readiness plan.

    Persistence path:
    <data_dir>/artifacts/coach_investment_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the investment-readiness artifact
          contract in ``PLAN_SKILL_COACH_INVESTMENT_READINESS.md``.
          The payload may store readiness and account-funding metadata only;
          securities, allocation, portfolio, trade, and live transfer fields
          are rejected.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_investment_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _investment_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_investment_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_investment_readiness_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_investment_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted investment-readiness plan.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _investment_readiness_artifact_dir()
    artifact_path, reason = _resolve_investment_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_investment_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(
    sync_behavior="no_sync",
    approval_required=True,
    coach_estate_document_readiness_auto_approved=True,
)
def coach_estate_document_readiness_artifact_save(
    plan_payload: dict,
    dry_run: bool = False,
) -> dict:
    """Persist an estate document-readiness checklist.

    Persistence path:
    <data_dir>/artifacts/coach_estate_document_readiness/<YYYYMMDD>.md.

    Same-date revision rule:
      - Matching ``generated_at`` -> existing file updated in place;
        ``last_modified_at`` is bumped to real time.
      - Differing ``generated_at`` on the same day -> new file with
        ``-r2.md``, ``-r3.md``, ... suffix.

    Args:
        plan_payload: Dict matching the estate document-readiness artifact
          contract in ``PLAN_SKILL_COACH_ESTATE_DOCUMENT_READINESS.md``.
        dry_run: If True, validate the payload but don't write.
    """
    payload = _normalize_estate_document_readiness_payload(plan_payload)
    generated_date = _generated_at_date(payload["generated_at"])
    artifact_dir = _estate_document_readiness_artifact_dir()
    artifact_path, save_mode = _resolve_estate_document_readiness_save_path(
        artifact_dir,
        payload["generated_at"],
        generated_date,
    )
    if save_mode == "update_in_place":
        payload["last_modified_at"] = utc_now_iso()
    rendered = _render_estate_document_readiness_artifact(payload)

    if not dry_run:
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(rendered, encoding="utf-8")

    return {
        "data": {
            "artifact_path": str(artifact_path),
            "generated_at": payload["generated_at"],
            "last_modified_at": payload["last_modified_at"],
            "save_mode": save_mode,
            "dry_run": dry_run,
            "plan_payload": payload,
        },
        "summary": {
            "saved": not dry_run,
            "valid": True,
            "artifact_path": str(artifact_path),
            "save_mode": save_mode,
            "generated_at": payload["generated_at"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_estate_document_readiness_artifact_read(
    date: Optional[str] = None,
) -> dict:
    """Read a previously persisted estate document-readiness checklist.

    Args:
        date: ISO date YYYY-MM-DD (returns latest revision for that day),
          or "<YYYYMMDD>-rN" / "YYYY-MM-DD-rN" (returns the specific revision),
          or None (returns the most-recent artifact across all dates).
    """
    artifact_dir = _estate_document_readiness_artifact_dir()
    artifact_path, reason = _resolve_estate_document_readiness_read_path(
        artifact_dir,
        date,
    )
    if artifact_path is None:
        summary: dict[str, Any] = {"found": False, "reason": reason}
        if date is not None and reason == "no_artifact_for_date":
            summary["date"] = str(date)
        return {"data": None, "summary": summary}

    markdown = artifact_path.read_text(encoding="utf-8")
    payload = _parse_estate_document_readiness_artifact(markdown)
    generated_at = payload.get("generated_at")
    last_modified_at = payload.get("last_modified_at") or generated_at
    return {
        "data": {
            "plan_payload": payload,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
        "summary": {
            "found": True,
            "artifact_path": str(artifact_path),
            "generated_at": generated_at,
            "last_modified_at": last_modified_at,
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def data_quality_gap_ratio(
    view: str = "personal",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> dict:
    """Categorization-gap ratio: union(uncategorized, unreviewed) / total active txns.

    Single semantic path for both the in-skill check (Phase 0 of
    ``coach_spending_plan``) AND the intervention-engine evaluators, which
    import :func:`finance_cli.interventions.helpers.data_quality_gap_ratio`
    directly so the MCP wrapper and the server-side helper never disagree
    on the same DB.

    Args:
        view: ``"personal"`` (default — matches CLI view semantics: rows
          tagged Personal OR NULL), ``"business"``, or ``"all"``.
        date_from: ISO date inclusive lower bound. Default = today - 90d.
        date_to: ISO date inclusive upper bound. Default = today.

    Returns:
        ``{
          data: {gap_ratio: float, uncat_or_unreviewed_count: int,
          total_count: int, window_days: int, date_from, date_to, view},
          summary: {gap_ratio, total_count, view}
        }``. Counting semantics: union — a transaction is counted in the
        numerator if ``category_id IS NULL`` OR ``is_reviewed = 0``; the two
        counts overlap and must not be added.
    """
    from finance_cli.interventions.helpers import data_quality_gap_ratio as _impl

    with _get_conn() as conn:
        result = _impl(conn, view=view, date_from=date_from, date_to=date_to)
    return {
        "data": result,
        "summary": {
            "gap_ratio": result["gap_ratio"],
            "total_count": result["total_count"],
            "view": result["view"],
        },
    }


def _scope_ids_from_debt_payoff(payload: dict[str, Any]) -> tuple[set[str], set[str]]:
    """Extract liability/account scope IDs and manual_loan scope IDs from a
    debt-payoff artifact's ``debts_in_scope`` field.

    Manual loans are distinguished by ``source == "manual_loan"`` if present in
    the entry, OR by id format (manual_loans use hex UUIDs). Otherwise the id
    is treated as a liability/account id and matched dual-IN in the SQL.
    """
    scope_field = payload.get("debts_in_scope") or []
    if not isinstance(scope_field, list):
        return set(), set()
    liability_or_account_ids: set[str] = set()
    manual_loan_ids: set[str] = set()
    for item in scope_field:
        if isinstance(item, dict):
            scope_id = item.get("id") or item.get("card_id") or item.get("liability_id")
            source = (item.get("source") or "").lower()
            if scope_id is None:
                continue
            if source == "manual_loan":
                manual_loan_ids.add(str(scope_id))
            else:
                liability_or_account_ids.add(str(scope_id))
                # If we can't tell, include in manual-loan check too (manual_loans
                # query will simply return no rows for non-matching ids).
                if source == "":
                    manual_loan_ids.add(str(scope_id))
        elif item is not None:
            liability_or_account_ids.add(str(item))
            manual_loan_ids.add(str(item))
    return liability_or_account_ids, manual_loan_ids


def _evaluate_debt_cleared(conn: Any, debt_payoff_payload: dict[str, Any]) -> int:
    """Sum live in-scope debt balances in cents (always >= 0)."""
    liability_ids, manual_loan_ids = _scope_ids_from_debt_payoff(debt_payoff_payload)
    total = 0
    if liability_ids:
        placeholders = ",".join("?" for _ in liability_ids)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(ABS(COALESCE(a.balance_current_cents, 0))), 0) AS balance_cents
              FROM liabilities l
              LEFT JOIN accounts a ON a.id = l.account_id
             WHERE l.id IN ({placeholders})
                OR l.account_id IN ({placeholders})
            """,
            tuple(liability_ids) + tuple(liability_ids),
        ).fetchone()
        total += int(row["balance_cents"] or 0) if row is not None else 0
    if manual_loan_ids:
        placeholders = ",".join("?" for _ in manual_loan_ids)
        row = conn.execute(
            f"""
            SELECT COALESCE(SUM(current_balance_cents), 0) AS balance_cents
              FROM manual_loans
             WHERE id IN ({placeholders})
            """,
            tuple(manual_loan_ids),
        ).fetchone()
        total += int(row["balance_cents"] or 0) if row is not None else 0
    return total


def _evaluate_efund_balance(conn: Any, efund_payload: dict[str, Any]) -> int:
    """Sum the latest snapshot balance across ``account_ids_in_fund`` in cents.

    Uses the standard MAX(snapshot_date <= today) pattern per snapshot per
    account (mirrors coach_debt_payoff.py's precedent).
    """
    accounts_field = efund_payload.get("account_ids_in_fund") or []
    if not isinstance(accounts_field, list):
        return 0
    account_ids: list[str] = []
    for item in accounts_field:
        if isinstance(item, dict):
            acct_id = item.get("account_id")
            if acct_id is None:
                continue
            account_ids.append(str(acct_id))
        elif item is not None:
            account_ids.append(str(item))
    if not account_ids:
        return 0
    today = datetime.now(timezone.utc).date().isoformat()
    total = 0
    for acct_id in account_ids:
        row = conn.execute(
            """
            SELECT balance_current_cents
              FROM balance_snapshots
             WHERE account_id = ?
               AND snapshot_date <= ?
             ORDER BY snapshot_date DESC
             LIMIT 1
            """,
            (acct_id, today),
        ).fetchone()
        if row is not None and row["balance_current_cents"] is not None:
            total += int(row["balance_current_cents"])
    return total


_DEBT_CLEARED_TOLERANCE_CENTS = 5000  # $50 residual-interest tolerance


@mcp.tool(sync_behavior="no_sync", read_only=True)
def coach_savings_goal_check_unlock_conditions(
    savings_goal_artifact_path: Optional[str] = None,
) -> dict:
    """Live-data check of the cross-skill unlock conditions for a savings-goal artifact.

    Reads the savings-goal artifact (latest by default; specific path date or
    "YYYYMMDD-rN" if provided), reads its ``unlock_blocker`` field, then runs
    live-data SQL to evaluate whether the prior-skill blocker(s) have resolved:
      - debt-cleared: sum of in-scope debt balances (liabilities + manual_loans
        scoped via the debt-payoff artifact's ``debts_in_scope``) <= $50.
      - e-fund-target-met: sum of latest snapshot balances across the e-fund
        artifact's ``account_ids_in_fund`` >= the artifact's
        ``target_balance_cents``.

    Gates ``unlock_eligible`` on ``unlock_blocker``:
      - "debt"  -> debt_cleared (efund_target_met informational)
      - "efund" -> efund_target_met (debt_cleared informational)
      - "both"  -> debt_cleared AND efund_target_met
      - null / target_phase=full -> ``unlock_eligible=False`` with
        ``summary.reason="not_starter_only"``.

    Missing prerequisite artifacts when their blocker is active force
    ``unlock_eligible=False`` and ``evidence.missing_prerequisite_artifacts``
    records which were missing.

    Args:
        savings_goal_artifact_path: Optional. None reads the latest artifact.
          Accepts "YYYY-MM-DD", "YYYYMMDD", or "<date>-rN" forms.

    Returns:
        Dict shaped ``{data: {unlock_blocker, evidence: {debt_cleared,
        efund_target_met, debt_in_scope_sum_cents, efund_balance_sum_cents,
        efund_target_balance_cents, missing_prerequisite_artifacts, observed_at}},
        summary: {unlock_eligible, blocker_resolved, reason}}``.

    Related tools: coach_savings_goal_artifact_read, coach_savings_goal_artifact_save.
    """
    sg_read = coach_savings_goal_artifact_read(date=savings_goal_artifact_path)
    if not sg_read.get("data"):
        return {
            "data": {
                "unlock_blocker": None,
                "evidence": {
                    "debt_cleared": None,
                    "efund_target_met": None,
                    "debt_in_scope_sum_cents": None,
                    "efund_balance_sum_cents": None,
                    "efund_target_balance_cents": None,
                    "missing_prerequisite_artifacts": [],
                    "observed_at": None,
                },
            },
            "summary": {
                "unlock_eligible": False,
                "blocker_resolved": "none",
                "reason": "no_savings_goal_artifact",
            },
        }
    sg_payload = sg_read["data"]["plan_payload"]
    blocker = sg_payload.get("unlock_blocker")
    target_phase = sg_payload.get("target_phase")
    observed_at = datetime.now(timezone.utc).date().isoformat()

    if blocker is None or target_phase != "starter_only":
        return {
            "data": {
                "unlock_blocker": blocker,
                "evidence": {
                    "debt_cleared": None,
                    "efund_target_met": None,
                    "debt_in_scope_sum_cents": None,
                    "efund_balance_sum_cents": None,
                    "efund_target_balance_cents": None,
                    "missing_prerequisite_artifacts": [],
                    "observed_at": observed_at,
                },
            },
            "summary": {
                "unlock_eligible": False,
                "blocker_resolved": "none",
                "reason": "not_starter_only",
            },
        }

    needs_debt = blocker in {"debt", "both"}
    needs_efund = blocker in {"efund", "both"}

    debt_cleared: Optional[bool] = None
    efund_target_met: Optional[bool] = None
    debt_in_scope_sum_cents: Optional[int] = None
    efund_balance_sum_cents: Optional[int] = None
    efund_target_balance_cents: Optional[int] = None
    missing_prerequisite_artifacts: list[str] = []

    debt_payload: Optional[dict[str, Any]] = None
    efund_payload: Optional[dict[str, Any]] = None

    if needs_debt:
        debt_read = coach_debt_payoff_artifact_read(date=None)
        if debt_read.get("data") and isinstance(debt_read["data"], dict):
            debt_payload = debt_read["data"].get("action_plan_payload")
        if not isinstance(debt_payload, dict):
            missing_prerequisite_artifacts.append("debt_payoff")
            debt_cleared = None
        else:
            with _get_conn() as conn:
                debt_in_scope_sum_cents = _evaluate_debt_cleared(conn, debt_payload)
            debt_cleared = debt_in_scope_sum_cents <= _DEBT_CLEARED_TOLERANCE_CENTS

    if needs_efund:
        efund_read = coach_emergency_fund_artifact_read(date=None)
        if efund_read.get("data") and isinstance(efund_read["data"], dict):
            efund_payload = efund_read["data"].get("plan_payload")
        if not isinstance(efund_payload, dict):
            missing_prerequisite_artifacts.append("emergency_fund")
            efund_target_met = None
        else:
            efund_target_balance_cents = int(
                efund_payload.get("target_balance_cents") or 0
            )
            with _get_conn() as conn:
                efund_balance_sum_cents = _evaluate_efund_balance(conn, efund_payload)
            efund_target_met = (
                efund_target_balance_cents > 0
                and efund_balance_sum_cents >= efund_target_balance_cents
            )

    if missing_prerequisite_artifacts:
        return {
            "data": {
                "unlock_blocker": blocker,
                "evidence": {
                    "debt_cleared": debt_cleared,
                    "efund_target_met": efund_target_met,
                    "debt_in_scope_sum_cents": debt_in_scope_sum_cents,
                    "efund_balance_sum_cents": efund_balance_sum_cents,
                    "efund_target_balance_cents": efund_target_balance_cents,
                    "missing_prerequisite_artifacts": missing_prerequisite_artifacts,
                    "observed_at": observed_at,
                },
            },
            "summary": {
                "unlock_eligible": False,
                "blocker_resolved": "none",
                "reason": "missing_prerequisite_artifact",
            },
        }

    if blocker == "debt":
        unlock_eligible = bool(debt_cleared)
        resolved = "debt" if unlock_eligible else "none"
    elif blocker == "efund":
        unlock_eligible = bool(efund_target_met)
        resolved = "efund" if unlock_eligible else "none"
    else:  # "both"
        unlock_eligible = bool(debt_cleared) and bool(efund_target_met)
        resolved = "both" if unlock_eligible else "none"

    return {
        "data": {
            "unlock_blocker": blocker,
            "evidence": {
                "debt_cleared": debt_cleared,
                "efund_target_met": efund_target_met,
                "debt_in_scope_sum_cents": debt_in_scope_sum_cents,
                "efund_balance_sum_cents": efund_balance_sum_cents,
                "efund_target_balance_cents": efund_target_balance_cents,
                "missing_prerequisite_artifacts": [],
                "observed_at": observed_at,
            },
        },
        "summary": {
            "unlock_eligible": unlock_eligible,
            "blocker_resolved": resolved,
            "reason": None,
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def agent_session_search(query: str, days: int = 30) -> dict:
    """Search past session notes by keyword. Use to recall what was discussed
    in previous conversations.

    Args:
        query: Search term.
        days: How many days back to search (default 30).

    Related tools: agent_session_read, agent_session_write.
    """
    with _get_conn() as conn:
        result = memory_cmd.handle_session_search(
            _ns(query=query, days=days),
            conn,
            data_dir=_get_data_dir(),
        )
    return _result_envelope(result)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def agent_session_read(date: str = "") -> dict:
    """Read a specific day's session notes.

    Args:
        date: Date in YYYY-MM-DD format. Defaults to today.

    Related tools: agent_session_search, agent_session_write.
    """
    with _get_conn() as conn:
        result = memory_cmd.handle_session_read(
            _ns(date=date), conn, data_dir=_get_data_dir()
        )
    return _result_envelope(result)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def session_recap(session_id: str = "") -> dict:
    """Recap a previous Telegram bot conversation session.

    Returns the session transcript, including compacted messages flagged with a
    boolean. If session_id is empty, uses the most recently closed session with
    messages. Content is truncated to 2000 chars per message and capped at 200
    messages.

    Args:
        session_id: Specific session ID to recap. Defaults to the last closed session.
    """
    with _get_conn() as conn:
        if not session_id:
            row = conn.execute(
                """
                SELECT *
                FROM bot_sessions
                WHERE ended_at IS NOT NULL AND message_count > 0
                ORDER BY ended_at DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return {"data": {}, "summary": "No completed sessions found."}
            session_id = str(row["session_id"])
            session = dict(row)
        else:
            row = conn.execute(
                "SELECT * FROM bot_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return {"data": {}, "summary": f"Session {session_id} not found."}
            session = dict(row)

        if session.get("ended_at") is None:
            live = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM bot_chat_messages WHERE bot_session_id = ?) AS message_count,
                    (SELECT COUNT(*) FROM bot_requests WHERE bot_session_id = ?) AS request_count,
                    (SELECT COALESCE(SUM(estimated_cost), 0.0)
                     FROM bot_requests WHERE bot_session_id = ?) AS total_cost
                """,
                (session_id, session_id, session_id),
            ).fetchone()
            if live is not None:
                session["message_count"] = live["message_count"]
                session["request_count"] = live["request_count"]
                session["total_cost"] = live["total_cost"]

        messages = conn.execute(
            """
            SELECT m.role, m.content, m.created_at, m.compacted_at
            FROM bot_chat_messages AS m
            WHERE m.bot_session_id = ?
              AND (
                  m.request_id = ?
                  OR (
                      m.request_id IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM bot_chat_messages AS p
                          WHERE p.request_id = m.request_id AND p.role = 'assistant'
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM bot_chat_messages AS p
                          WHERE p.request_id = m.request_id AND p.role = 'user'
                      )
                      AND EXISTS (
                          SELECT 1
                          FROM bot_requests AS r
                          WHERE r.request_id = m.request_id AND r.error IS NULL
                      )
                  )
              )
            ORDER BY m.id ASC
            LIMIT 201
            """,
            (session_id, "[COMPACTION]"),
        ).fetchall()

        tools = conn.execute(
            """
            SELECT tool_name, COUNT(*) AS call_count
            FROM bot_tool_calls
            WHERE request_id IN (
                SELECT request_id
                FROM bot_requests
                WHERE bot_session_id = ? AND error IS NULL
            )
            GROUP BY tool_name
            ORDER BY call_count DESC
            LIMIT 10
            """,
            (session_id,),
        ).fetchall()

    truncated = len(messages) > 200
    messages = messages[:200]
    compacted_count = sum(1 for row in messages if row["compacted_at"] is not None)
    message_list = [
        {
            "role": row["role"],
            "content": str(row["content"])[:2000],
            "time": row["created_at"],
            "compacted": row["compacted_at"] is not None,
        }
        for row in messages
    ]
    tool_list = [
        {"tool": row["tool_name"], "count": row["call_count"]} for row in tools
    ]

    total_cost = float(session.get("total_cost", 0.0) or 0.0)
    summary = f"Session {session.get('started_at', '?')} - {len(message_list)} messages"
    if truncated:
        summary += " (oldest 200 shown, session may have more)"
    if compacted_count:
        summary += f" ({compacted_count} compacted)"
    summary += f", ${total_cost:.4f}"

    return {
        "data": {"session": session, "messages": message_list, "tools": tool_list},
        "summary": summary,
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def session_list(days: int = 7, limit: int = 10) -> dict:
    """List recent Telegram bot conversation sessions.

    Args:
        days: How many days back to look.
        limit: Maximum sessions to return.
    """
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                s.session_id,
                s.started_at,
                s.ended_at,
                s.end_reason,
                s.last_activity_at,
                CASE
                    WHEN s.ended_at IS NOT NULL THEN s.message_count
                    ELSE (
                        SELECT COUNT(*)
                        FROM bot_chat_messages
                        WHERE bot_session_id = s.session_id
                    )
                END AS message_count,
                CASE
                    WHEN s.ended_at IS NOT NULL THEN s.request_count
                    ELSE (
                        SELECT COUNT(*)
                        FROM bot_requests
                        WHERE bot_session_id = s.session_id
                    )
                END AS request_count,
                CASE
                    WHEN s.ended_at IS NOT NULL THEN s.total_cost
                    ELSE (
                        SELECT COALESCE(SUM(estimated_cost), 0.0)
                        FROM bot_requests
                        WHERE bot_session_id = s.session_id
                    )
                END AS total_cost
            FROM bot_sessions AS s
            WHERE date(s.started_at) >= date('now', ? || ' days')
            ORDER BY s.started_at DESC
            LIMIT ?
            """,
            (f"-{days}", limit),
        ).fetchall()

    sessions = [dict(row) for row in rows]
    return {
        "data": {"sessions": sessions},
        "summary": f"{len(sessions)} sessions in last {days} days",
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def db_status() -> dict:
    """Database overview: transaction counts, date range, accounts, uncategorized count, top categories.

    Returns:
        Dict with transaction_counts, date_range, active_account_count,
        uncategorized_count, category_source_distribution, top_categories,
        and last_import_at.

    Examples:
        db_status()
    """
    result = _call(db_cmd.handle_status, {})
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE is_active = 1 AND is_reviewed = 0"
        ).fetchone()
    data = dict(result.get("data", {}))
    data["unreviewed_count"] = int(row["cnt"] or 0)
    return {"data": data, "summary": result.get("summary", {})}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def setup_check() -> dict:
    """Verify financial foundation: environment, connections, and configuration.

    Returns:
        Dict with ready (bool), checks list, counts, and next_steps.

    Examples:
        setup_check()
    """
    db_path = current_db_path()
    with _get_conn() as conn:
        result = setup_cmd.handle_check(
            _ns(),
            conn,
            db_path=Path(db_path).expanduser().resolve() if db_path else None,
            rules_path=_get_rules_path(),
        )
    return _result_envelope(result)


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def setup_status() -> dict:
    """Financial foundation dashboard: connections, coverage, data health, next steps.

    Returns:
        Dict with environment, database, plaid, category_coverage, and next_steps.

    Examples:
        setup_status()

    Setup redaction fields filtered by this tool include access_token_ref and sync_cursor.
    """
    db_path = current_db_path()
    with _get_conn() as conn:
        result = setup_cmd.handle_status(
            _ns(),
            conn,
            db_path=Path(db_path).expanduser().resolve() if db_path else None,
            rules_path=_get_rules_path(),
        )
    return {"data": result.get("data", {}), "summary": result.get("summary", {})}


# ===================================================================
# 2. Account Management (6 tools, read+write)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def account_list(
    status: str = "active",
    account_type: Optional[str] = None,
    institution: Optional[str] = None,
    source: Optional[str] = None,
    is_business: Optional[bool] = None,
) -> dict:
    """List connected financial accounts with optional filters.

    Args:
        status: Account status filter: 'active', 'inactive', or 'all'. Defaults to 'active'.
        account_type: Optional account type filter.
        institution: Optional free-text institution filter.
        source: Optional free-text source filter (plaid/csv_import/pdf_import/manual/schwab/etc.).
        is_business: Optional business-account filter.

    Returns:
        Dict with accounts list and filter echo.

    Examples:
        account_list()
        account_list(status="all", institution="Bank of America")
        account_list(source="schwab")
        account_list(is_business=True)
    """
    return _call(
        account_cmd.handle_list,
        {
            "status": status,
            "account_type": account_type,
            "institution": institution,
            "source": source,
            "is_business": is_business,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def account_show(id: str) -> dict:
    """Show full details for a connected financial account.

    Args:
        id: Account ID.

    Returns:
        Dict with account details and transaction stats.

    Examples:
        account_show(id="abc123")

    Discovery: use account_list or account_show to choose id before account updates.
    """
    return _call(account_cmd.handle_show, {"id": id})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def account_set_type(id: str, account_type: str, dry_run: bool = False) -> dict:
    """Set account type classification for a connected account.

    Args:
        id: Account ID.
        account_type: New type ('checking', 'savings', 'credit_card', 'investment', 'loan').

    Returns:
        Dict with previous/new type values.

    Examples:
        account_set_type(id="abc123", account_type="investment")

    Discovery: use account_list or account_show to choose id before account updates.
    Related tools: account_set_business.
    """
    return _call(
        account_cmd.handle_set_type,
        {"id": id, "account_type": account_type, "dry_run": dry_run},
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def account_set_business(
    id: str, is_business: bool, backfill: bool = False, dry_run: bool = False
) -> dict:
    """Classify an account as business or personal and optionally update its history.

    Args:
        id: Account ID.
        is_business: True to mark business, False to mark personal.
        backfill: If True, update active transaction use_type for this account:
            - True mode: NULL -> 'Business'
            - False mode: 'Business' -> NULL

    Returns:
        Dict with prior/new business flag values and backfill counts.

    Examples:
        account_set_business(id="abc123", is_business=True)
        account_set_business(id="abc123", is_business=False, backfill=True)

    Discovery: use account_list or account_show to choose id before account updates.
    Related tools: account_set_type.
    """
    return _call(
        account_cmd.handle_set_business,
        {
            "id": id,
            "business": bool(is_business),
            "personal": not bool(is_business),
            "backfill": backfill,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def bank_account_deactivate(
    id: str, cascade: bool = False, force: bool = False, dry_run: bool = False
) -> dict:
    """Deactivate a connected bank account, optionally cascading to its history.

    Args:
        id: Account ID.
        cascade: If True, deactivate linked transactions and auto-detected subscriptions.
        force: If True, allow deactivation even when the account is an alias canonical target.

    Returns:
        Dict with deactivation and cascade results.

    Examples:
        bank_account_deactivate(id="abc123")
        bank_account_deactivate(id="abc123", cascade=True, force=True)

    Discovery: use account_list or account_show to choose id before account updates.
    Related tools: bank_account_activate.
    """
    return _call(
        account_cmd.handle_deactivate,
        {"id": id, "cascade": cascade, "force": force, "dry_run": dry_run},
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def bank_account_activate(id: str, dry_run: bool = False) -> dict:
    """Reactivate a connected bank account to resume tracking its history.

    Args:
        id: Account ID.

    Returns:
        Dict with activation results.

    Examples:
        bank_account_activate(id="abc123")

    Discovery: use account_list or account_show to choose id before account updates.
    Related tools: bank_account_deactivate.
    """
    return _call(account_cmd.handle_activate, {"id": id, "dry_run": dry_run})


# ===================================================================
# 3. Financial Reports (16 tools, read-only)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def interventions_get(surface: str = "agent_prompt") -> dict:
    """Return the top-ranked interventions for the requested surface.

    Pure read - does NOT log fires to intervention_log. Use this when you
    need to know what CashNerd would coach the user on right now.

    Surfaces:
      - "dashboard"     -> top 1 (single highest-value intervention)
      - "agent_prompt"  -> top 3 (default; matches the agent system-prompt block)
      - "action_queue"  -> top 5 (everything ranked above the noise floor)
    """
    return _call(
        intervention_cmd.handle_get,
        {"surface": surface},
        pass_rules=True,
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def interventions_act(log_id: int) -> dict:
    """Mark an intervention as acted upon. Takes the log_id from interventions_get or the agent prompt.

    Discovery: use interventions_get to choose log_id before acting on or dismissing an intervention.
    """
    return _call(intervention_cmd.handle_act, {"log_id": log_id})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def interventions_dismiss(log_id: int) -> dict:
    """Dismiss an intervention (suppresses the pattern for 30 days).

    Discovery: use interventions_get to choose log_id before acting on or dismissing an intervention.
    """
    return _call(intervention_cmd.handle_dismiss, {"log_id": log_id})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def interventions_mute(pattern_id: str, reason: str = "") -> dict:
    """Permanently mute a pattern. It will never fire again until unmuted.

    Discovery: use interventions_get to choose pattern_id before muting or unmuting an intervention pattern.
    """
    return _call(
        intervention_cmd.handle_mute, {"pattern_id": pattern_id, "reason": reason}
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def interventions_unmute(pattern_id: str) -> dict:
    """Unmute a previously muted pattern.

    Discovery: use interventions_get to choose pattern_id before muting or unmuting an intervention pattern.
    """
    return _call(intervention_cmd.handle_unmute, {"pattern_id": pattern_id})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def daily_summary(date: Optional[str] = None, view: str = "all") -> dict:
    """Show transactions and spending for a specific date.

    Args:
        date: ISO date string (YYYY-MM-DD). Defaults to today.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with date, transactions list, unreviewed_count, and data_range.

    Examples:
        daily_summary()
        daily_summary(date="2026-02-15")
    """
    full_result = _call(
        daily.handle_daily, {"date": date, "pending": False, "view": view}
    )
    _write_cache("daily_summary", full_result)

    data = dict(full_result.get("data", {}))
    transactions = data.get("transactions", [])
    data["transactions"] = [
        _strip_txn_fields(txn_data) if isinstance(txn_data, dict) else txn_data
        for txn_data in transactions
    ]
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def weekly_summary(
    week: Optional[str] = None, compare: bool = False, view: str = "all"
) -> dict:
    """Weekly spending by category, optionally compared to the prior week.

    Args:
        week: ISO week string like '2026-W07'. Defaults to current week.
        compare: If True, include prior week comparison with deltas.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with week_start, week_end, categories, and optional comparison data.

    Examples:
        weekly_summary()
        weekly_summary(week="2026-W07", compare=True)
    """
    return _call(weekly.handle_weekly, {"week": week, "compare": compare, "view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def balance_net_worth(exclude_investments: bool = False, view: str = "all") -> dict:
    """Compute net worth from current account balances.

    Args:
        exclude_investments: Exclude investment accounts from the calculation.
        view: Use-type view filter: 'personal', 'business', or 'all' (default).

    Returns:
        Dict with assets, liabilities, net_worth (dollars and cents), and breakdown by account type.

    Examples:
        balance_net_worth()
        balance_net_worth(exclude_investments=True)
    """
    return _call(
        balance_cmd.handle_net_worth,
        {"exclude_investments": exclude_investments, "view": view},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def balance_show(
    account_type: Optional[str] = None,
    show_all: bool = False,
    view: str = "all",
) -> dict:
    """Show current balances with optional account type filtering."""
    return _call(
        balance_cmd.handle_show,
        {"type": account_type, "show_all": show_all, "view": view},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def balance_history(account: str, days: int = 90) -> dict:
    """Show daily balance history for one account."""
    return _call(
        balance_cmd.handle_history, {"account": account, "days": days, "view": "all"}
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def balance_update(
    account: str,
    current: Optional[float] = None,
    available: Optional[float] = None,
    balance_limit: Optional[float] = None,
    snapshot_date: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Record a manual balance update and daily snapshot.

    Use this for accounts whose balances cannot be refreshed through Plaid or
    another provider. Discover the canonical account ID with balance_show().

    Args:
        account: Account ID to update.
        current: Current balance in dollars.
        available: Available balance in dollars.
        balance_limit: Credit limit in dollars.
        snapshot_date: Optional YYYY-MM-DD snapshot date. Defaults to today.
        dry_run: Preview without writing.

    Returns:
        Dict with the updated account fields and manual snapshot metadata.
    """
    return _call(
        balance_cmd.handle_update,
        {
            "account": account,
            "current": current,
            "available": available,
            "balance_limit": balance_limit,
            "snapshot_date": snapshot_date,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def liquidity(view: str = "all", include_investments: bool = True) -> dict:
    """Liquidity snapshot: liquid balance, credit owed, 90-day income/expense, subscription burn, projected net.

    Returns:
        Dict with liquid_balance, credit_owed, income/expense_90d, subscription burn, projected_net.

    Examples:
        liquidity()
    """
    return _call(
        liquidity_cmd.handle_liquidity,
        {
            "forecast": 90,
            "include_investments": include_investments,
            "view": view,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def debt_dashboard(include_zero_balance: bool = False, sort: str = "balance") -> dict:
    """Per-card debt breakdown: balances, APRs, minimums, monthly interest, and totals.

    Args:
        include_zero_balance: Include zero-balance credit cards.
        sort: Sort key for cards: 'balance', 'apr', or 'interest'.

    Returns:
        Dict with debt dashboard data and summary counts.

    Examples:
        debt_dashboard()
        debt_dashboard(sort="apr")
    """
    return _call(
        debt_cmd.handle_dashboard,
        {
            "include_zero_balance": include_zero_balance,
            "sort": sort,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def debt_interest(months: int = 12, summary_only: bool = True) -> dict:
    """Project minimum-payment interest over N months.

    Args:
        months: Number of months to project (>= 1).
        summary_only: If True (default), omit per-card monthly rows to reduce payload size.

    Returns:
        Dict with projection schedule and aggregate interest totals.

    Examples:
        debt_interest()
        debt_interest(months=24)
    """
    return _call(
        debt_cmd.handle_interest, {"months": months, "summary_only": summary_only}
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def debt_simulate(
    extra_dollars: float = 500,
    strategy: str = "compare",
    summary_only: bool = True,
    lump_sum: float = 0,
    lump_sum_month: int = 1,
) -> dict:
    """Simulate debt paydown using avalanche, snowball, or side-by-side comparison.

    Args:
        extra_dollars: Extra monthly payment in dollars.
        strategy: 'avalanche', 'snowball', or 'compare'.
        summary_only: If True (default), omit per-card monthly rows to reduce payload size.
        lump_sum: One-time lump sum payment in dollars (e.g. tax refund, bonus).
        lump_sum_month: Month number (1-based) when the lump sum is applied (default 1).

    Returns:
        Dict with simulation outputs and strategy summary.

    Examples:
        debt_simulate()
        debt_simulate(extra_dollars=750, strategy="avalanche")
        debt_simulate(extra_dollars=500, lump_sum=5000, lump_sum_month=3)
    """
    return _call(
        debt_cmd.handle_simulate,
        {
            "extra": extra_dollars,
            "strategy": strategy,
            "summary_only": summary_only,
            "lump_sum": lump_sum,
            "lump_month": lump_sum_month,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def debt_set_apr(account_id: str, apr_pct: float, dry_run: bool = False) -> dict:
    """Manually set or override purchase APR for a credit-card account.

    Discovery: use debt_dashboard(sort="apr") or balance_show(account_type="credit_card")
    to choose account_id. This updates liabilities.apr_purchase for that card and
    creates the credit liability row if the account has no liability metadata yet.
    """
    return _call(
        debt_cmd.handle_set_apr,
        {
            "account": account_id,
            "apr": apr_pct,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def debt_balance_portion_add(
    account_id: str,
    label: str,
    principal_dollars: float,
    apr_pct: float,
    monthly_payment_dollars: Optional[float] = None,
    portion_type: str = "installment",
    promo_end_date: Optional[str] = None,
    notes: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Add an APR-specific portion of a parent credit-card balance.

    Discovery: use debt_dashboard() or balance_show(account_type="credit_card")
    to choose the parent account_id. The parent card balance remains the source
    of truth; this portion only changes debt dashboard/simulation math.
    """
    return _call(
        debt_cmd.handle_portion_add,
        {
            "account": account_id,
            "label": label,
            "principal": principal_dollars,
            "apr": apr_pct,
            "monthly_payment": monthly_payment_dollars,
            "portion_type": portion_type,
            "promo_end_date": promo_end_date,
            "expected_payoff_date": None,
            "notes": notes,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def debt_balance_portion_list(
    account_id: Optional[str] = None,
    active_only: bool = True,
) -> dict:
    """List APR-specific portions of parent credit-card balances."""
    return _call(
        debt_cmd.handle_portion_list,
        {
            "account": account_id,
            "include_inactive": not active_only,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def debt_balance_portion_update(
    portion_id: str,
    label: Optional[str] = None,
    principal_dollars: Optional[float] = None,
    apr_pct: Optional[float] = None,
    monthly_payment_dollars: Optional[float] = None,
    clear_monthly_payment: bool = False,
    portion_type: Optional[str] = None,
    promo_end_date: Optional[str] = None,
    clear_promo_end_date: bool = False,
    notes: Optional[str] = None,
    clear_notes: bool = False,
    dry_run: bool = False,
) -> dict:
    """Update an APR-specific credit-card balance portion."""
    return _call(
        debt_cmd.handle_portion_update,
        {
            "portion_id": portion_id,
            "label": label,
            "principal": principal_dollars,
            "apr": apr_pct,
            "monthly_payment": monthly_payment_dollars,
            "clear_monthly_payment": clear_monthly_payment,
            "portion_type": portion_type,
            "promo_end_date": promo_end_date,
            "expected_payoff_date": None,
            "clear_promo_end_date": clear_promo_end_date,
            "notes": notes,
            "clear_notes": clear_notes,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def debt_balance_portion_deactivate(portion_id: str, dry_run: bool = False) -> dict:
    """Deactivate an APR-specific credit-card balance portion without deleting history."""
    return _call(
        debt_cmd.handle_portion_deactivate,
        {
            "portion_id": portion_id,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def flag_card_for_paydown(
    account_id: str,
    suggested_payment_cents: int = 0,
    cash_source_account_id: str = "",
    interest_saved_annual_cents: Optional[int] = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict:
    """Flag a credit card as the next paydown target.

    Discovery: use debt_dashboard(sort="apr") or liability_show(type="credit")
    to choose account_id. Optional cash_source_account_id should be a checking
    or savings account from account_list/balance_show.
    """
    with _get_conn() as conn:
        return card_paydown_flags.flag_card_for_paydown(
            conn,
            account_id=account_id,
            suggested_payment_cents=suggested_payment_cents,
            cash_source_account_id=cash_source_account_id or None,
            interest_saved_annual_cents=interest_saved_annual_cents,
            reason=reason,
            source=source,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def card_paydown_flags_list(status: str = "active", limit: int = 100) -> dict:
    """List cards flagged for paydown."""
    with _get_conn() as conn:
        return card_paydown_flags.list_card_paydown_flags(
            conn, status=status, limit=limit
        )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def clear_card_paydown_flag(
    flag_id: str,
    status: str = "resolved",
    dry_run: bool = False,
) -> dict:
    """Resolve or cancel a credit-card paydown flag."""
    with _get_conn() as conn:
        return card_paydown_flags.clear_card_paydown_flag(
            conn,
            flag_id=flag_id,
            status=status,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def subs_list(show_all: bool = False, limit: int = 100, offset: int = 0) -> dict:
    """List tracked subscriptions, sorted by monthly cost.

    Args:
        show_all: Include cancelled/inactive subscriptions.
        limit: Maximum number of subscriptions to return.
        offset: Number of subscriptions to skip before returning results.

    Returns:
        Dict with subscriptions list and summary counts.

    Examples:
        subs_list()
        subs_list(show_all=True)
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")

    full_result = _call(subs.handle_list, {"show_all": show_all})
    _write_cache("subs_list", full_result)

    data = dict(full_result.get("data", {}))
    all_subscriptions_raw = data.get("subscriptions", [])
    all_subscriptions: list[Any] = []
    for item in all_subscriptions_raw:
        if not isinstance(item, dict):
            all_subscriptions.append(item)
            continue
        sub = dict(item)
        monthly_amount = sub.get("monthly_amount")
        if monthly_amount is not None:
            sub["monthly_amount"] = round(float(monthly_amount), 2)
        vendor = str(sub.get("vendor_name") or "")
        sub["short_name"] = vendor[:30]
        all_subscriptions.append(sub)
    active_subscriptions = [
        item
        for item in all_subscriptions
        if isinstance(item, dict) and bool(int(item.get("is_active") or 0))
    ]
    inactive_subscriptions = [
        item
        for item in all_subscriptions
        if isinstance(item, dict) and not bool(int(item.get("is_active") or 0))
    ]

    data["subscriptions"] = all_subscriptions if show_all else active_subscriptions
    summary = dict(full_result.get("summary", {}))
    summary.update(
        {
            "active_subscriptions": len(active_subscriptions),
            "inactive_subscriptions": len(inactive_subscriptions),
            "total_subscriptions": len(all_subscriptions),
        }
    )
    data["subscriptions"] = data["subscriptions"][offset : offset + limit]
    data["total_count"] = len(all_subscriptions)
    data["limit"] = limit
    data["offset"] = offset
    return {"data": data, "summary": summary}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def subs_total() -> dict:
    """Total monthly subscription burn rate.

    Returns:
        Dict with monthly_burn, yearly_burn, and active_subscriptions count.

    Examples:
        subs_total()
    """
    return _call(subs.handle_total, {})


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def subs_detect(dry_run: bool = False) -> dict:
    """Detect recurring subscriptions from transactions."""
    return _call(subs.handle_detect, {"dry_run": dry_run})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def subs_recurring(summary_only: bool = True) -> dict:
    """List detected recurring spending patterns (pre-subscription candidates).

    Args:
        summary_only: If True (default), return CLI report instead of full pattern list to reduce payload size.
    """
    with _get_conn() as conn:
        result = subs.handle_recurring(_ns(), conn)
    if summary_only:
        cache_id = _write_cache_safe("subs_recurring", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def subs_add(
    vendor: str,
    amount: float,
    frequency: str,
    category: Optional[str] = None,
    use_type: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Add a recurring subscription record.

    Discovery: run subs_recurring or subs_list first to verify the vendor and avoid duplicates.
    Safety: pass dry_run=True to preview validation without writing the subscription.
    """
    return _call(
        subs.handle_add,
        {
            "vendor": vendor,
            "amount": amount,
            "frequency": frequency,
            "category": category,
            "use_type": use_type,
            "idempotency_key": idempotency_key,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def subs_update(
    id: str,
    vendor: Optional[str] = None,
    amount: Optional[float] = None,
    frequency: Optional[str] = None,
    category: Optional[str] = None,
    clear_category: bool = False,
    use_type: Optional[str] = None,
    clear_use_type: bool = False,
    dry_run: bool = False,
) -> dict:
    """Update an existing subscription record in place.

    Discovery: use subs_list to choose id before changing amount, frequency, category, or use_type.
    Safety: pass dry_run=True to preview validation without writing the subscription.
    """
    return _call(
        subs.handle_update,
        {
            "id": id,
            "vendor": vendor,
            "amount": amount,
            "frequency": frequency,
            "category": category,
            "clear_category": clear_category,
            "use_type": use_type,
            "clear_use_type": clear_use_type,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def subs_cancel(id: str, dry_run: bool = False) -> dict:
    """Cancel a subscription.

    Discovery: use subs_list to choose id before canceling a subscription.
    """
    return _call(subs.handle_cancel, {"id": id, "dry_run": dry_run})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def subs_audit() -> dict:
    """Audit subscriptions vs debt: classify essential/discretionary,
    model debt payoff impact of cutting discretionary subs."""
    return _attach_simulation_cap_caveat(_call(subs.handle_audit, {}, pass_rules=True))


@mcp.tool(sync_behavior="no_sync", read_only=True)
def debt_impact(months: int = 3, cut_pct: int = 50) -> dict:
    """Model discretionary spending cuts -> debt payoff impact.

    Computes N-month average spending per category, classifies as
    essential/discretionary, then models how cutting discretionary
    spending would accelerate debt payoff via avalanche simulation.

    Args:
        months: Lookback months for average spending (default: 3).
        cut_pct: Discretionary cut percentage 1-100 (default: 50).
    """
    return _attach_simulation_cap_caveat(
        _call(
            debt_cmd.handle_impact,
            {"months": months, "cut_pct": cut_pct},
            pass_rules=True,
        )
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_pl(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    compare: bool = False,
) -> dict:
    """Business income statement (P&L) for a period."""
    return _call(
        biz_cmd.handle_pl,
        {
            "month": month,
            "quarter": quarter,
            "year": year,
            "compare": compare,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_cashflow(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
) -> dict:
    """Business cash flow statement for a period."""
    return _call(
        biz_cmd.handle_cashflow,
        {
            "month": month,
            "quarter": quarter,
            "year": year,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_tax(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    detail: Optional[str] = None,
    salary: Optional[float] = None,
) -> dict:
    """Schedule C tax report for a period.

    Related tools: biz_tax_detail, biz_tax_package, biz_tax_setup.
    """
    return _call(
        biz_cmd.handle_tax,
        {
            "month": month,
            "quarter": quarter,
            "year": year,
            "detail": detail,
            "salary": salary,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_tax_detail(
    detail: str,
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
    salary: Optional[float] = None,
) -> dict:
    """Schedule C tax report detail section.

    Related tools: biz_tax, biz_tax_package, biz_tax_setup.
    """
    return _call(
        biz_cmd.handle_tax,
        {
            "month": month,
            "quarter": quarter,
            "year": year,
            "detail": detail,
            "salary": salary,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def biz_tax_setup(
    year: str,
    method: Optional[str] = None,
    sqft: Optional[int] = None,
    total_sqft: Optional[int] = None,
    filing_status: Optional[str] = None,
    state: Optional[str] = None,
    health_insurance_monthly: Optional[float] = None,
    w2_wages: Optional[float] = None,
    mileage_method: Optional[str] = None,
) -> dict:
    """Configure tax assumptions for a tax year.

    Related tools: biz_tax, biz_tax_detail, biz_tax_package.
    """
    return _call(
        biz_cmd.handle_tax_setup,
        {
            "year": year,
            "method": method,
            "sqft": sqft,
            "total_sqft": total_sqft,
            "filing_status": filing_status,
            "state": state,
            "health_insurance_monthly": health_insurance_monthly,
            "w2_wages": w2_wages,
            "mileage_method": mileage_method,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def setup_home_office_tracking(
    year: str,
    sqft: int,
    method: str = "simplified",
    total_sqft: Optional[int] = None,
    dry_run: bool = False,
) -> dict:
    """Configure simplified home-office tax tracking for a tax year.

    Discovery
    ---------
    - Ask the user for the dedicated home-office square footage and tax year.
    - Use rules_show before calling if rent/utilities split rules may already exist.

    When to use
    -----------
    - Use after the user confirms they have a dedicated home-office space.
    - NOT for actual-expense Form 8829 tracking; this tool supports the simplified method only.

    Behavior
    --------
    - Writes existing synced tax_config keys used by biz_tax and biz_tax_package.
    - Estimates the simplified deduction at $5/sqft, capped at 300 sqft.
    - Rejects existing Rent/Utilities split-rule conflicts to avoid double-counting.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        home_office_tracking.handle_setup,
        {
            "year": year,
            "sqft": sqft,
            "method": method,
            "total_sqft": total_sqft,
            "dry_run": dry_run,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def set_monthly_retirement_target(
    tax_year: str,
    account_type: str,
    monthly_target_cents: int,
    start_month: str,
    end_month: str,
    room_remaining_cents: Optional[int] = None,
    annual_limit_cents: Optional[int] = None,
    contributed_ytd_cents: Optional[int] = None,
    estimated_tax_savings_cents: Optional[int] = None,
    deadline: Optional[str] = None,
    reason: str = "",
    update_monthly_plans: bool = True,
    dry_run: bool = False,
) -> dict:
    """Set an explicit monthly retirement contribution target.

    Discovery
    ---------
    - Use current-year income/contribution context to calculate the monthly target first.
    - Ask the user which retirement account type the target is for before writing.

    When to use
    -----------
    - Use after the user accepts a Q4/year-end retirement contribution target.
    - NOT for generic savings goals or brokerage investing; use the goal/monthly-plan tools instead.

    Behavior
    --------
    - Requires explicit tax year, account type, month range, and monthly target cents.
    - Upserts one synced target per tax year/account/month range.
    - Optionally mirrors the target into monthly_plans.investment_target_cents.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        retirement_targets.handle_set,
        {
            "tax_year": tax_year,
            "account_type": account_type,
            "monthly_target_cents": monthly_target_cents,
            "start_month": start_month,
            "end_month": end_month,
            "room_remaining_cents": room_remaining_cents,
            "annual_limit_cents": annual_limit_cents,
            "contributed_ytd_cents": contributed_ytd_cents,
            "estimated_tax_savings_cents": estimated_tax_savings_cents,
            "deadline": deadline,
            "reason": reason,
            "update_monthly_plans": update_monthly_plans,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def setup_monthly_transfer_goal(
    tax_year: str,
    monthly_transfer_cents: int,
    room_remaining_cents: int,
    start_month: str,
    end_month: str,
    account_type: str = "roth_ira",
    annual_limit_cents: Optional[int] = None,
    contributed_ytd_cents: Optional[int] = None,
    estimated_tax_savings_cents: Optional[int] = None,
    reason: str = "",
    update_monthly_plans: bool = True,
    dry_run: bool = False,
) -> dict:
    """Set a monthly retirement transfer goal from remaining contribution room.

    Discovery
    ---------
    - Use Roth/retirement contribution-room math before calling; pass room_remaining_cents.
    - Ask the user to confirm the monthly transfer amount and month range.

    When to use
    -----------
    - Use after the user accepts a Roth/monthly contribution prompt.
    - NOT for emergency-fund or taxable savings automation; use those goal tools instead.

    Behavior
    --------
    - Reuses the synced retirement_contribution_targets table.
    - Requires the planned month range total to fit inside room_remaining_cents.
    - Optionally mirrors the transfer amount into monthly_plans.investment_target_cents.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        retirement_targets.handle_transfer_goal,
        {
            "tax_year": tax_year,
            "monthly_transfer_cents": monthly_transfer_cents,
            "room_remaining_cents": room_remaining_cents,
            "start_month": start_month,
            "end_month": end_month,
            "account_type": account_type,
            "annual_limit_cents": annual_limit_cents,
            "contributed_ytd_cents": contributed_ytd_cents,
            "estimated_tax_savings_cents": estimated_tax_savings_cents,
            "reason": reason,
            "update_monthly_plans": update_monthly_plans,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="no_sync", excluded_from_agent=True)
def biz_tax_package(
    year: str,
    output: Optional[str] = None,
    salary: Optional[float] = None,
    summary_only: bool = True,
) -> dict:
    """Generate full tax package output for a tax year.

    Args:
        year: Tax year (YYYY).
        output: Optional output file path for full package export.
        salary: Optional salary override for SE tax calculations.
        summary_only: If True (default), omit transaction_groups to reduce payload size.

    Related tools: biz_tax, biz_tax_detail, biz_tax_setup.
    """
    extra: dict[str, Any] = {}
    rules_path = _get_rules_path()
    if rules_path is not None:
        extra["rules_path"] = rules_path
    with _get_conn() as conn:
        result = biz_cmd.handle_tax_package(
            _ns(year=year, output=output, salary=salary), conn, **extra
        )
    full_result = _result_envelope(result)
    if summary_only:
        cache_id = _write_cache_safe("biz_tax_package", full_result)
        data = dict(full_result.get("data", {}))
        data.pop("transaction_groups", None)
        data["cache_id"] = cache_id
        return {"data": data, "summary": full_result.get("summary", {})}
    return full_result


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_estimated_tax(
    est_quarter: Optional[str] = None,
    year: Optional[int] = None,
    rate: Optional[float] = None,
    include_se: bool = True,
    salary: Optional[float] = None,
) -> dict:
    """Quarterly estimated tax calculation."""
    return _call(
        biz_cmd.handle_estimated_tax,
        {
            "est_quarter": est_quarter,
            "year": year,
            "rate": rate,
            "include_se": include_se,
            "salary": salary,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def biz_mileage_add(
    date: str,
    miles: float,
    destination: str,
    purpose: str,
    vehicle: str = "primary",
    round_trip: bool = False,
    notes: Optional[str] = None,
) -> dict:
    """Add a mileage log trip for Schedule C Line 9 standard mileage tracking.

    Related tools: biz_mileage_list, biz_mileage_summary.
    """
    return _call(
        biz_cmd.handle_mileage_add,
        {
            "date": date,
            "miles": miles,
            "destination": destination,
            "purpose": purpose,
            "vehicle": vehicle,
            "round_trip": round_trip,
            "notes": notes,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_mileage_list(
    year: Optional[str] = None,
    vehicle: Optional[str] = None,
    limit: int = 50,
) -> dict:
    """List mileage log entries for a year (and optional vehicle filter).

    Related tools: biz_mileage_add, biz_mileage_summary.
    """
    return _call(
        biz_cmd.handle_mileage_list,
        {"year": year, "vehicle": vehicle, "limit": limit},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_mileage_summary(year: Optional[str] = None) -> dict:
    """Mileage deduction summary: total miles, rate, deduction vs transaction-based Line 9.

    Related tools: biz_mileage_add, biz_mileage_list.
    """
    return _call(biz_cmd.handle_mileage_summary, {"year": year})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def biz_contractor_add(
    name: str,
    tin_last4: Optional[str] = None,
    entity_type: str = "individual",
    notes: Optional[str] = None,
) -> dict:
    """Add a contractor for 1099-NEC tracking.

    Discovery: use biz_contractor_list to avoid duplicate contractor names before adding a contractor.
    Related tools: biz_contractor_link, biz_contractor_list.
    """
    return _call(
        biz_cmd.handle_contractor_add,
        {
            "name": name,
            "tin_last4": tin_last4,
            "entity_type": entity_type,
            "notes": notes,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_contractor_list(
    year: Optional[int] = None, include_inactive: bool = False
) -> dict:
    """List contractors with payment totals for 1099-NEC tracking.

    Related tools: biz_contractor_add, biz_contractor_link.
    """
    return _call(
        biz_cmd.handle_contractor_list,
        {"year": year, "include_inactive": include_inactive},
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def biz_contractor_link(
    contractor_id: str,
    transaction_id: str,
    paid_via_card: bool = False,
) -> dict:
    """Link a business transaction to a contractor payment record.

    Discovery: use txn_list, txn_show, or the categorization preview output to choose transaction IDs.
    Related tools: biz_contractor_add, biz_contractor_list.
    """
    return _call(
        biz_cmd.handle_contractor_link,
        {
            "contractor_id": contractor_id,
            "transaction_id": transaction_id,
            "paid_via_card": paid_via_card,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_1099_report(year: str) -> dict:
    """Build a contractor 1099-NEC threshold report for a tax year."""
    return _call(biz_cmd.handle_1099_report, {"year": year})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def flag_contractor_january_prep(
    contractor_id: str,
    tax_year: Optional[int] = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict:
    """Flag a contractor for January 1099-NEC prep.

    Discovery: call biz_contractor_list(year=YYYY) or biz_1099_report(year=YYYY)
    first, then pass the contractor_id. This tool persists a year-scoped prep
    flag and snapshots current non-card/card payment totals.
    """
    with _get_conn() as conn:
        return contractor_tax_prep.flag_contractor_january_prep(
            conn,
            contractor_id=contractor_id,
            tax_year=tax_year,
            reason=reason,
            source=source,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def contractor_january_prep_flags_list(
    tax_year: Optional[int] = None,
    status: str = "active",
    limit: int = 100,
) -> dict:
    """List contractors flagged for January 1099-NEC prep."""
    with _get_conn() as conn:
        return contractor_tax_prep.list_contractor_january_prep_flags(
            conn,
            tax_year=tax_year,
            status=status,
            limit=limit,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_forecast(months: int = 6, streams: bool = False) -> dict:
    """Revenue projections by stream with trend analysis."""
    return _call(
        biz_cmd.handle_forecast, {"months": months, "streams": streams}, pass_rules=True
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_runway(months: int = 3) -> dict:
    """Business burn rate and cash runway estimate."""
    result = _call(biz_cmd.handle_runway, {"months": months})
    data = dict(result.get("data", {}))
    if int(data.get("monthly_net_burn_cents", 0)) < 0:
        data["is_profitable"] = True
        data["note"] = (
            "Monthly net burn is negative; business is profitable and runway is effectively uncapped."
        )
    return {"data": data, "summary": result.get("summary", {})}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_seasonal() -> dict:
    """Month-of-year seasonal revenue averages with confidence levels."""
    return _call(biz_cmd.handle_seasonal, {})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def biz_budget_set(
    section: str,
    amount: float,
    period: str = "monthly",
    effective_from: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Set a business budget for an expense P&L section.

    Discovery: run `biz_contractor_list` first to obtain `section` values.

    Valid section values: cogs | opex_marketing | opex_technology | opex_professional | opex_facilities | opex_people | opex_other
    Valid period values: monthly | quarterly | yearly

    Related tools: biz_budget_status.
    """
    return _call(
        biz_cmd.handle_biz_budget_set,
        {
            "section": section,
            "amount": amount,
            "period": period,
            "effective_from": effective_from,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def biz_budget_status(month: Optional[str] = None) -> dict:
    """Show business budget vs actual spend per P&L section.

    Related tools: biz_budget_set.
    """
    return _call(biz_cmd.handle_biz_budget_status, {"month": month})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cat_tree() -> dict:
    """Category hierarchy tree with transaction counts per category.

    Returns:
        Dict with tree (nested parent/child categories with txn_count) and total_categories.

    Examples:
        cat_tree()
    """
    return _call(cat.handle_tree, {})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cat_list() -> dict:
    """List all categories with hierarchy and transaction counts."""
    return _call(cat.handle_list, {})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_add(name: str, parent: Optional[str] = None, dry_run: bool = False) -> dict:
    """Add a new category, optionally under a parent.

    Discovery: use cat_list to avoid duplicate category names before adding a category.
    """
    return _call(cat.handle_add, {"name": name, "parent": parent, "dry_run": dry_run})


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def cat_normalize(dry_run: bool = True) -> dict:
    """Normalize category names to maintain consistent financial history."""
    return _call(cat.handle_normalize, {"dry_run": dry_run}, pass_rules=True)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def budget_set(
    category: str,
    amount: float,
    period: str = "monthly",
    view: str = "personal",
    dry_run: bool = False,
) -> dict:
    """Set a budget for a category.

    Valid period values: monthly | weekly | yearly
    Valid view values: personal | business
    """
    return _call(
        budget.handle_set,
        {
            "category": category,
            "amount": amount,
            "period": period,
            "view": view,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def budget_update(
    category: str,
    amount: float,
    period: str = "monthly",
    view: str = "personal",
    dry_run: bool = False,
) -> dict:
    """Update an existing active budget amount for a category.

    Valid period values: monthly | weekly | yearly
    Valid view values: personal | business
    """
    return _call(
        budget.handle_update,
        {
            "category": category,
            "amount": amount,
            "period": period,
            "view": view,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def budget_reallocate(
    from_category: str,
    to_category: str,
    amount: float,
    period: str = "monthly",
    view: str = "personal",
    dry_run: bool = False,
) -> dict:
    """Move budget room from one active category budget to another.

    Valid period values: monthly | weekly | yearly
    Valid view values: personal | business
    """
    return _call(
        budget.handle_reallocate,
        {
            "from_category": from_category,
            "to_category": to_category,
            "amount": amount,
            "period": period,
            "view": view,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def budget_delete(
    category: str,
    period: str = "monthly",
    view: str = "personal",
    dry_run: bool = False,
) -> dict:
    """Delete an existing active budget for a category.

    Valid period values: monthly | weekly | yearly
    Valid view values: personal | business
    """
    return _call(
        budget.handle_delete,
        {"category": category, "period": period, "view": view, "dry_run": dry_run},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def budget_list(view: str = "all") -> dict:
    """List configured budgets."""
    return _call(budget.handle_list, {"view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def budget_status(month: Optional[str] = None, view: str = "all") -> dict:
    """Show monthly budget vs actual status."""
    return _call(budget.handle_status, {"month": month, "view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def budget_forecast(month: Optional[str] = None, view: str = "all") -> dict:
    """Forecast month-end spending vs budget."""
    return _call(budget.handle_forecast, {"month": month, "view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def budget_alerts(month: Optional[str] = None, view: str = "all") -> dict:
    """Check which budgets are at risk based on current spending run rate."""
    return _call(budget.handle_alerts, {"month": month, "view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def budget_suggest(
    goal: str = "savings", target: float = 500, view: str = "all"
) -> dict:
    """Suggest budget cuts to hit a target."""
    return _call(budget.handle_suggest, {"goal": goal, "target": target, "view": view})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def add_late_month_buffer_budget(
    amount_cents: int,
    category_name: str = "Late-Month Buffer",
    parent_category_name: str = "",
    effective_from: str = "",
    dry_run: bool = False,
) -> dict:
    """Create or update a personal monthly late-month buffer budget.

    Discovery: use cat_tree to choose an optional parent_category_name. The
    tool creates the category when missing and upserts the monthly personal
    budget for that category.
    """
    with _get_conn() as conn:
        return late_month_buffer.add_late_month_buffer_budget(
            conn,
            amount_cents=amount_cents,
            category_name=category_name,
            parent_category_name=parent_category_name,
            effective_from=effective_from,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def set_spending_freeze_flag(
    scope: str = "discretionary",
    hold_until: str = "",
    reason: str = "",
    account_id: str = "",
    category_id: str = "",
    bill_name: str = "",
    bill_amount_cents: Optional[int] = None,
    due_date: str = "",
    target_balance_after_cents: Optional[int] = None,
    source: str = "agent",
    dry_run: bool = False,
) -> dict:
    """Create or update a temporary spending freeze flag.

    Discovery: use account_list for account_id and cat_list for category_id.
    Use this after the user confirms a short-term hold on discretionary or
    nonessential spending around an upcoming bill or balance crunch.
    """
    with _get_conn() as conn:
        return spending_freeze.set_spending_freeze_flag(
            conn,
            scope=scope,
            hold_until=hold_until or None,
            reason=reason,
            account_id=account_id or None,
            category_id=category_id or None,
            bill_name=bill_name,
            bill_amount_cents=bill_amount_cents,
            due_date=due_date or None,
            target_balance_after_cents=target_balance_after_cents,
            source=source,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def spending_freeze_flags_list(status: str = "active", limit: int = 100) -> dict:
    """List spending freeze flags."""
    with _get_conn() as conn:
        return spending_freeze.list_spending_freeze_flags(
            conn, status=status, limit=limit
        )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def clear_spending_freeze_flag(
    flag_id: str,
    status: str = "resolved",
    dry_run: bool = False,
) -> dict:
    """Resolve or cancel a spending freeze flag."""
    with _get_conn() as conn:
        return spending_freeze.clear_spending_freeze_flag(
            conn,
            flag_id=flag_id,
            status=status,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def notify_budget_alerts(
    channel: str = "telegram", view: str = "all", dry_run: bool = False
) -> dict:
    """Send or preview budget alert notifications."""
    return _call(
        notify_cmd.handle_budget_alerts,
        {"channel": channel, "view": view, "month": None, "dry_run": dry_run},
        pass_data_dir=True,
    )


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def notify_test(channel: str = "telegram", dry_run: bool = False) -> dict:
    """Send or preview a test notification."""
    return _call(
        notify_cmd.handle_test,
        {"channel": channel, "dry_run": dry_run},
        pass_data_dir=True,
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def notify_channel_set(channel: str, config: str, label: str = "") -> dict:
    """Create or update a notification channel config.

    Related tools: notify_channel_list, notify_channel_remove.
    """
    return _call(
        notify_cmd.handle_channel_set,
        {"channel": channel, "config": config, "label": label},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def notify_channel_list() -> dict:
    """List configured notification channels.

    Related tools: notify_channel_remove, notify_channel_set.
    """
    return _call(notify_cmd.handle_channel_list, {})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def notify_channel_remove(channel: str, dry_run: bool = False) -> dict:
    """Remove a notification channel config.

    Discovery: run notify_channel_list first to choose an existing channel.
    Sibling tools: use notify_channel_set to create or update channels.
    Safety: pass dry_run=True to preview the removal without deleting the config.
    """
    return _call(
        notify_cmd.handle_channel_remove, {"channel": channel, "dry_run": dry_run}
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def card_rotation_reminder_set(
    zero_apr_account_id: str,
    paydown_account_id: str,
    intro_apr_end_date: str,
    avg_monthly_spend_cents: int = 0,
    estimated_interest_saved_cents: int = 0,
    channel: str = "telegram",
    days_before: int = 7,
    dry_run: bool = False,
) -> dict:
    """Schedule a reminder before a 0% APR card promotion expires.

    Discovery: use account_list or account_show to choose account IDs for paydown and zero-APR accounts.
    """
    return _call(
        reminder_cmd.handle_card_rotation_set,
        {
            "zero_apr_account_id": zero_apr_account_id,
            "paydown_account_id": paydown_account_id,
            "intro_apr_end_date": intro_apr_end_date,
            "avg_monthly_spend_cents": avg_monthly_spend_cents,
            "estimated_interest_saved_cents": estimated_interest_saved_cents,
            "channel": channel,
            "days_before": days_before,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def set_balance_transfer_reminder(
    account_id: str,
    remind_on: str,
    balance_transfer_fee_percent: float = 3.0,
    channel: str = "telegram",
    note: str = "",
    dry_run: bool = False,
) -> dict:
    """Schedule a balance-transfer opportunity reminder for a high-APR card.

    Discovery
    ---------
    - Use account_list/account_show to find a canonical credit-card account_id.
    - Use debt_show or account_show to confirm the card has an active credit liability
      with apr_purchase; refresh liabilities or import a statement first if APR is absent.
    - Choose remind_on explicitly with the user, usually mid-cycle or just after a statement.

    When to use
    -----------
    - Use after the user accepts a balance-transfer follow-up for a card carrying a balance.
    - NOT for routine 0% APR spend rotation; use card_rotation_reminder_set for that.

    Behavior
    --------
    - Rejects alias/business/non-card accounts and cards below the D-6 balance/APR/net-savings
      trigger, then stores a durable synced reminder and resets any matching cancelled/failed
      reminder to pending.
    - Computes the fee and 12-month interest snapshot from the current balance/APR.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        reminder_cmd.handle_balance_transfer_set,
        {
            "account_id": account_id,
            "remind_on": remind_on,
            "balance_transfer_fee_percent": balance_transfer_fee_percent,
            "channel": channel,
            "note": note,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def flag_account_for_hysa_transfer(
    account_id: str,
    suggested_transfer_cents: int,
    hysa_apy_bps: int,
    current_apy_bps: int = 0,
    retained_buffer_cents: int = 0,
    minimum_balance_cents: int = 200_000,
    lookback_days: int = 90,
    as_of: Optional[str] = None,
    reason: str = "",
    source: str = "agent",
    dry_run: bool = False,
) -> dict:
    """Flag surplus checking cash for transfer to a high-yield savings option.

    Discovery
    ---------
    - Use account_list/balance_show to choose the canonical personal checking account ID.
    - Verify balance_snapshots cover the lookback window before writing.
    - Ask the user for the transfer amount and keep-buffer amount explicitly.

    When to use
    -----------
    - Use after the user accepts a surplus-cash-drag recommendation.
    - NOT for recommending a named bank or executing a transfer; this only records the action flag.

    Behavior
    --------
    - Requires explicit suggested_transfer_cents and hysa_apy_bps.
    - Rejects alias/business/non-checking accounts, insufficient balance, stale/missing balance
      history, unstable transfer amounts, and APYs where the HYSA option does not beat the current
      account.
    - Stores a synced flag with the balance/rate/evidence snapshot.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        hysa_transfer_flags.handle_flag,
        {
            "account_id": account_id,
            "suggested_transfer_cents": suggested_transfer_cents,
            "hysa_apy_bps": hysa_apy_bps,
            "current_apy_bps": current_apy_bps,
            "retained_buffer_cents": retained_buffer_cents,
            "minimum_balance_cents": minimum_balance_cents,
            "lookback_days": lookback_days,
            "as_of": as_of,
            "reason": reason,
            "source": source,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def setup_savings_automation(
    goal_id: str,
    amount_cents: int,
    start_date: str,
    cadence: str = "monthly",
    funding_method: str = "auto_transfer",
    day_of_month: Optional[int] = None,
    source_account_id: Optional[str] = None,
    destination_account_id: Optional[str] = None,
    target_amount_cents: Optional[int] = None,
    projected_end_balance_cents: Optional[int] = None,
    goal_date: Optional[str] = None,
    reason: str = "",
    dry_run: bool = False,
) -> dict:
    """Record an accepted savings automation plan for a goal.

    Discovery
    ---------
    - Use goal_find/goal_status to identify the active goal_id.
    - Use account_list to choose optional checking source and savings/investment destination accounts.
    - Ask the user to confirm amount, cadence, start date, and funding method.

    When to use
    -----------
    - Use after the user accepts an automatic savings/paycheck-split plan for a goal.
    - NOT for executing bank transfers; this records the plan for monitoring.

    Behavior
    --------
    - Stores one synced active automation per goal and updates it idempotently.
    - Rejects inactive goals, non-checking source accounts, and non-savings/investment destinations.
    - Does not write recurring_flows, because savings transfers are not expenses.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        savings_automations.handle_setup,
        {
            "goal_id": goal_id,
            "amount_cents": amount_cents,
            "start_date": start_date,
            "cadence": cadence,
            "funding_method": funding_method,
            "day_of_month": day_of_month,
            "source_account_id": source_account_id,
            "destination_account_id": destination_account_id,
            "target_amount_cents": target_amount_cents,
            "projected_end_balance_cents": projected_end_balance_cents,
            "goal_date": goal_date,
            "reason": reason,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def set_low_balance_alert(
    account_id: str,
    threshold_cents: int,
    channel: str = "telegram",
    cooldown_hours: int = 24,
    label: str = "",
    dry_run: bool = False,
) -> dict:
    """Create or update a persistent low-balance alert for a checking/savings account.

    Discovery: use account_list or balance_show to choose the account ID.
    """
    with _get_conn() as conn:
        return account_alerts.set_low_balance_alert(
            conn,
            account_id=account_id,
            threshold_cents=threshold_cents,
            channel=channel,
            cooldown_hours=cooldown_hours,
            label=label,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def low_balance_alerts_list(status: str = "active", limit: int = 100) -> dict:
    """List configured low-balance alert rules."""
    with _get_conn() as conn:
        return account_alerts.list_account_alert_rules(conn, status=status, limit=limit)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def low_balance_alerts_check(
    dry_run: bool = True,
    limit: int = 50,
    channel: str = "",
    now: str = "",
) -> dict:
    """Evaluate active low-balance alert rules and send notifications for triggered rules."""
    with _get_conn() as conn:
        return account_alerts.evaluate_account_alert_rules(
            conn,
            now=now or None,
            limit=limit,
            dry_run=dry_run,
            channel_override=channel or None,
        )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def liability_show(
    liability_type: Optional[str] = None,
    include_inactive: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """Show liabilities with optional type filtering."""
    return _call(
        liability_cmd.handle_show,
        {
            "type": liability_type,
            "include_inactive": include_inactive,
            "limit": limit,
            "offset": offset,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def liability_upcoming(days: int = 30, liability_type: Optional[str] = None) -> dict:
    """Show upcoming liability payments."""
    return _call(liability_cmd.handle_upcoming, {"days": days, "type": liability_type})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def plan_create(month: Optional[str] = None, dry_run: bool = False) -> dict:
    """Create or refresh a monthly plan."""
    return _call(
        plan.handle_create,
        {"month": month or datetime.now().strftime("%Y-%m"), "dry_run": dry_run},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def plan_show(month: Optional[str] = None) -> dict:
    """Show the monthly cash plan for a requested month.

    Discovery: omit month for the current month or pass YYYY-MM after plan_create.
    """
    return _call(plan.handle_show, {"month": month})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def plan_review() -> dict:
    """Review the current month's plan against actuals."""
    return _call(plan.handle_review, {})


# ===================================================================
# 4. Transaction Tools (5 tools, read-only)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def txn_list(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
    account_id: Optional[str] = None,
    use_type: Optional[str] = None,
    uncategorized: bool = False,
    unreviewed: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """List transactions with optional filters and pagination.

    Args:
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
        category: Filter by category name (exact match).
        uncategorized: Only show uncategorized transactions.
        unreviewed: Only show unreviewed transactions.
        limit: Max rows to return (default 50).
        offset: Skip this many rows for pagination (default 0).

    Returns:
        Dict with transactions list, total_count, and pagination info.

    Examples:
        txn_list()
        txn_list(date_from="2026-02-01", limit=20)
        txn_list(uncategorized=True)
        txn_list(category="Groceries", limit=10)
    """
    return _call(
        txn.handle_list,
        {
            "date_from": date_from,
            "date_to": date_to,
            "category": category,
            "account_id": account_id,
            "use_type": use_type,
            "uncategorized": uncategorized,
            "unreviewed": unreviewed,
            "limit": limit,
            "offset": offset,
            "project": None,
            "verbose": False,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def txn_search(query: str, category: str = "", limit: int = 20) -> dict:
    """Full-text search across transaction descriptions (FTS5, falls back to LIKE).

    Args:
        query: Search term or FTS5 query string.
        category: Optional category name filter.

    Returns:
        Dict with matching transactions and query echo.

    Examples:
        txn_search(query="STARBUCKS")
        txn_search(query="amazon prime")
    """
    params = {"query": query}
    if category:
        params["category"] = category

    full_result = _call(txn.handle_search, params)
    _write_cache("txn_search", full_result)

    data = dict(full_result.get("data", {}))
    transactions = data.get("transactions", [])
    data["transactions"] = [
        _strip_txn_fields(txn_data) if isinstance(txn_data, dict) else txn_data
        for txn_data in transactions
    ][:limit]
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def txn_show(id: str) -> dict:
    """Full details for a single transaction.

    Args:
        id: Transaction ID (hex UUID).

    Returns:
        Dict with all transaction fields including category, account, and notes.

    Examples:
        txn_show(id="a1b2c3d4...")

    Discovery: use txn_list or txn_show to choose id before transaction reads or edits.
    """
    full_result = _call(txn.handle_show, {"id": id})
    _write_cache("txn_show", full_result)

    data = dict(full_result.get("data", {}))
    transaction = data.get("transaction")
    if isinstance(transaction, dict):
        data["transaction"] = _strip_txn_fields(transaction)
    return {"data": data, "summary": full_result.get("summary", {})}


@mcp.tool(sync_behavior="no_sync", read_only=True)
def txn_explain(id: str) -> dict:
    """Explain how a transaction was categorized: source, rule, reasoning.

    Args:
        id: Transaction ID (hex UUID).

    Returns:
        Dict with categorization source, matched rule, and confidence.

    Examples:
        txn_explain(id="a1b2c3d4...")

    Discovery: use txn_list or txn_show to choose id before transaction reads or edits.
    """
    return _call(txn.handle_explain, {"id": id}, pass_rules=True)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def txn_coverage(date_from: Optional[str] = None) -> dict:
    """Date coverage per account with gap detection.

    Args:
        date_from: Reference start date for gap detection (defaults to earliest transaction).

    Returns:
        Dict with per-account date ranges, transaction counts, and detected gaps.

    Examples:
        txn_coverage()
        txn_coverage(date_from="2026-01-01")
    """
    full_result = _call(txn.handle_coverage, {"date_from": date_from, "date_to": None})
    _write_cache("txn_coverage", full_result)
    return full_result


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_add(
    amount: float,
    date: str,
    description: str,
    account_id: Optional[str] = None,
    category: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Add a manual transaction."""
    return _call(
        txn.handle_add,
        {
            "amount": amount,
            "date": date,
            "description": description,
            "account_id": account_id,
            "category": category,
            "idempotency_key": idempotency_key,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_edit(
    id: str,
    amount: Optional[float] = None,
    date: Optional[str] = None,
    description: Optional[str] = None,
    notes: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Edit one transaction.

    Use this for: changing amount, date, description, or notes on one transaction.
    NOT for: categorizing, project-tagging, or listing transactions. → see `txn_categorize`, `txn_tag`, `txn_list`.

    Discovery: use txn_list or txn_show to choose id before transaction reads or edits.
    """
    return _call(
        txn.handle_edit,
        {
            "id": id,
            "amount": amount,
            "date": date,
            "description": description,
            "notes": notes,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_deactivate(id: str, dry_run: bool = False) -> dict:
    """Soft-deactivate one transaction.

    Use this for: hiding a specific confirmed duplicate or erroneous transaction
    while preserving the row for audit/history.
    NOT for: bulk duplicate cleanup or reviewed dedup candidates. → see
    `dedup_cross_format` and `dedup_same_source_apply`.

    Discovery: use txn_list or txn_show to choose id before deactivating.
    """
    return _call(txn.handle_deactivate, {"id": id, "dry_run": dry_run})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_tag(id: str, project: str, dry_run: bool = False) -> dict:
    """Tag one transaction with a project.

    Use this for: assigning a project tag to one transaction.
    NOT for: editing transaction fields or assigning financial categories. → see `txn_edit`, `txn_categorize`.

    Discovery: use txn_list or txn_show to choose id before transaction reads or edits.
    """
    return _call(txn.handle_tag, {"id": id, "project": project, "dry_run": dry_run})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_bulk_tag(items: list[dict[str, Any]], dry_run: bool = False) -> dict:
    """Tag multiple transactions with project labels.

    Each item must contain id and project. Returns per-item success/error results.

    Related tools: txn_bulk_categorize.
    """
    results: list[dict[str, Any]] = []
    for raw_item in items or []:
        item = _bulk_dict_item(raw_item)
        payload = {
            "id": item.get("id"),
            "project": item.get("project"),
            "dry_run": dry_run,
        }
        try:
            result = _call_full(txn.handle_tag, payload, _caller_name="txn_bulk_tag")
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("txn_bulk_tag", results)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_dispute_workflow(
    transaction_id: str,
    dispute_reason: str = "duplicate_charge",
    duplicate_transaction_id: Optional[str] = None,
    note: str = "",
    dry_run: bool = False,
) -> dict:
    """Prepare and track a transaction dispute workflow.

    Discovery
    ---------
    - Use txn_explain/txn_show to review the suspect charge before calling.
    - For duplicate_charge, pass both transaction IDs after the user confirms the duplicate.

    When to use
    -----------
    - Use after the user accepts a duplicate-charge or suspicious-charge dispute workflow.
    - NOT for filing the dispute with a bank/card issuer; this only records the workflow.

    Behavior
    --------
    - Rejects non-expense or inactive transactions.
    - For duplicate_charge, requires same account, same amount, and dates within 7 days.
    - Stores a synced dispute-prep record with transaction snapshots and next-step prompts.
    - Pass dry_run=True to preview without writing.
    """
    return _call(
        transaction_disputes.handle_workflow,
        {
            "transaction_id": transaction_id,
            "dispute_reason": dispute_reason,
            "duplicate_transaction_id": duplicate_transaction_id,
            "note": note,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def bulk_tag_billable_expenses(
    ids: list[str],
    project: str,
    overwrite_existing_project: bool = False,
    dry_run: bool = False,
) -> dict:
    """Tag selected Business expense transactions with a client/project.

    Discovery
    ---------
    - Use txn_list(use_type="Business") or intervention output to collect transaction ids.
    - Pick a project/client label that should appear in txn_list --project filters.

    When to use
    -----------
    - Use after the user agrees that selected Business expenses may be billable.
    - NOT for personal expenses; run bulk_reclassify_business first only when the user confirms.

    Behavior
    --------
    - Requires explicit transaction ids and rejects non-expense or non-Business rows.
    - Creates/reuses the project label and writes transactions.project_id.
    - Refuses to overwrite existing different project tags unless explicitly requested.
    - Pass dry_run=True to preview without writing.
    """
    with _get_conn() as conn:
        return business_bulk_actions.bulk_tag_billable_expenses(
            conn,
            ids=ids,
            project=project,
            overwrite_existing_project=overwrite_existing_project,
            dry_run=dry_run,
        )


# ===================================================================
# 5. Rules & Categorization (9 tools, read+write)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def rules_test(
    description: str, category: Optional[str] = None, source: str = "plaid"
) -> dict:
    """Test how accumulated rules would categorize a transaction description.

    Args:
        description: Transaction description to test against rules.
        category: Optional category name to test overrides against.
        source: Category source context (default 'plaid').

    Returns:
        Dict with keyword match, split rule, and category override results.

    Examples:
        rules_test(description="VENMO PAYMENT")
        rules_test(description="STARBUCKS", category="Coffee")
    """
    return _call(
        rules.handle_test,
        {
            "description": description,
            "category": category,
            "source": source,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def rules_show() -> dict:
    """Show loaded rules.yaml contents."""
    return _call(rules.handle_show, {}, pass_rules=True)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def rules_validate() -> dict:
    """Validate rules.yaml against known categories."""
    return _call(rules.handle_validate, {}, pass_rules=True)


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def rules_add_keyword(
    keyword: str,
    category: str,
    use_type: Optional[str] = None,
    priority: int = 0,
) -> dict:
    """Add a keyword rule the system will use for future categorization.

    Valid use_type values: Business | Personal

    Related tools: rules_add_keywords, rules_add_split.
    """
    return _call(
        rules.handle_add_keyword,
        {
            "keyword": keyword,
            "category": category,
            "use_type": use_type,
            "priority": priority,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def rules_add_keywords(items: list[dict[str, Any]]) -> dict:
    """Add multiple keyword categorization rules with per-item error reporting.

    Each item accepts keyword, category, optional use_type, and optional priority.

    Related tools: rules_add_keyword, rules_add_split.
    """
    results: list[dict[str, Any]] = []
    for raw_item in items or []:
        item = _bulk_dict_item(raw_item)
        payload = {
            "keyword": item.get("keyword"),
            "category": item.get("category"),
            "use_type": item.get("use_type"),
            "priority": item.get("priority") if item.get("priority") is not None else 0,
        }
        try:
            result = _call_full(
                rules.handle_add_keyword,
                payload,
                pass_rules=True,
                _caller_name="rules_add_keywords",
            )
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("rules_add_keywords", results)


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def rules_add_split(
    business_pct: float,
    business_category: str,
    personal_category: str,
    match_category: Optional[str] = None,
    match_keywords: Optional[list[str]] = None,
    note: Optional[str] = None,
) -> dict:
    """Add a split rule for mixed business/personal expenses. Use split
    rules when a category or vendor should always be split between
    business and personal use by a fixed percentage.

    Args:
        business_pct: Business share as a float greater than 0 and less than 100.
        business_category: Category to assign to the business-side split.
        personal_category: Category to assign to the personal-side split.
        match_category: Optional category name that triggers the split rule.
        match_keywords: Optional list of description keywords that trigger the split rule.
        note: Optional note saved with the rule for future reference.

    Returns:
        Dict MCP envelope with:
        - data.rule: The saved split rule dict with match, business_pct,
          business_category, personal_category, and optional note.
        - data.split_rule_count: Total number of split rules after the add.
        - summary.updated: Count of updated records.
        - summary.split_rule_count: Total number of split rules after the add.

    Examples:
        rules_add_split(
            business_pct=80,
            business_category="Professional Fees",
            personal_category="Software & Subscriptions",
            match_category="Software & Subscriptions",
            note="Shared SaaS tools",
        )
        rules_add_split(
            business_pct=90,
            business_category="Office Expense",
            personal_category="Rent",
            match_keywords=["COWORKING"],
            note="Office membership",
        )

    Related tools: rules_add_keyword, rules_add_keywords.
    """
    return _call(
        rules.handle_add_split,
        {
            "business_pct": business_pct,
            "business_category": business_category,
            "personal_category": personal_category,
            "match_category": match_category,
            "match_keywords": match_keywords,
            "note": note,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def rules_remove_keyword(keyword: str, dry_run: bool = False) -> dict:
    """Remove a keyword from the categorization knowledge base.

    Discovery: run `rules_list` first to choose an existing keyword.
    Valid keyword values: existing keyword from `rules_list`
    Safety: pass dry_run=True to preview the changed rule without writing rules.yaml.
    """
    return _call(
        rules.handle_remove_keyword,
        {"keyword": keyword, "dry_run": dry_run},
        pass_rules=True,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def rules_list(limit: int = 200, offset: int = 0) -> dict:
    """List accumulated categorization rules.

    Returns:
        Dict with list of rules, each containing rule_index, category,
        keywords, use_type, and priority.

    Examples:
        rules_list()
    """
    return _call(rules.handle_list, {"limit": limit, "offset": offset}, pass_rules=True)


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def rules_update_priority(
    rule_index: int,
    priority: int,
) -> dict:
    """Change the priority on a keyword rule. Use rules_list() first to
    find the rule_index. Higher priority wins ties among equal-length
    keyword matches.

    Args:
        rule_index: Index of the rule to update (from rules_list output).
        priority: New priority value.

    Returns:
        Dict with category, old_priority, new_priority, and rule_index.

    Examples:
        rules_update_priority(rule_index=0, priority=5)
        rules_update_priority(rule_index=3, priority=10)
    """
    return _call(
        rules.handle_update_priority,
        {
            "rule_index": rule_index,
            "priority": priority,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cat_memory_list(
    unconfirmed: bool = False,
    limit: int = 50,
    search: Optional[str] = None,
) -> dict:
    """List learned vendor categorization patterns.

    Related tools: cat_memory_add, cat_memory_confirm, cat_memory_delete.
    """
    return _call(
        cat.handle_memory_list,
        {"unconfirmed": unconfirmed, "limit": limit, "search": search},
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_add(
    pattern: str, category: str, use_type: str = "Any", dry_run: bool = False
) -> dict:
    """Teach the system a vendor categorization pattern it will remember.

    Related tools: cat_memory_confirm, cat_memory_delete, cat_memory_delete_bulk.
    """
    return _call(
        cat.handle_memory_add,
        {
            "pattern": pattern,
            "category": category,
            "use_type": use_type,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_add_bulk(
    rules: list[dict[str, Any]], dry_run: bool = False
) -> dict:
    """Teach multiple vendor categorization patterns with per-item reporting.

    Related tools: cat_memory_add, cat_memory_confirm, cat_memory_delete_bulk.
    """
    results: list[dict[str, Any]] = []
    for index, rule in enumerate(rules or []):
        item: dict[str, Any] = {"index": index}
        try:
            if not isinstance(rule, dict):
                raise ValueError("rule must be an object")
            pattern = str(rule.get("pattern") or "").strip()
            category = str(rule.get("category") or "").strip()
            use_type = str(rule.get("use_type") or "Any").strip() or "Any"
            item = {
                "index": index,
                "pattern": pattern,
                "category": category,
                "use_type": use_type,
            }
            if not pattern:
                raise ValueError("pattern is required")
            if not category:
                raise ValueError("category is required")
            result = _call_full(
                cat.handle_memory_add,
                {
                    "pattern": pattern,
                    "category": category,
                    "use_type": use_type,
                    "dry_run": dry_run,
                },
                _caller_name="cat_memory_add_bulk",
            )
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("cat_memory_add_bulk", results)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_review_new_merchants(
    items: list[dict[str, Any]], dry_run: bool = False
) -> dict:
    """Review newly auto-categorized merchants with confirm/fix/skip decisions.

    Each item accepts:
    - decision: confirm | fix | skip
    - pattern, category, use_type for confirm/fix memory
    - txn_ids or transaction_ids for fix decisions

    Confirm saves vendor memory. Fix recategorizes the provided transactions and
    saves vendor memory. Skip records a no-op result.
    """
    results: list[dict[str, Any]] = []
    for index, raw_item in enumerate(items or []):
        item: dict[str, Any] = {"index": index}
        try:
            if not isinstance(raw_item, dict):
                raise ValueError("review item must be an object")
            decision = str(raw_item.get("decision") or "confirm").strip().lower()
            pattern = str(raw_item.get("pattern") or "").strip()
            category = str(raw_item.get("category") or "").strip()
            use_type = str(raw_item.get("use_type") or "Any").strip() or "Any"
            raw_txn_ids = raw_item.get("txn_ids", raw_item.get("transaction_ids", []))
            if isinstance(raw_txn_ids, str):
                txn_ids = [
                    value.strip() for value in raw_txn_ids.split(",") if value.strip()
                ]
            elif isinstance(raw_txn_ids, list):
                txn_ids = [str(value).strip() for value in raw_txn_ids if str(value).strip()]
            else:
                txn_ids = []
            item = {
                "index": index,
                "decision": decision,
                "pattern": pattern,
                "category": category,
                "use_type": use_type,
                "txn_ids": txn_ids,
            }

            if decision == "skip":
                results.append(
                    {
                        "status": "success",
                        "item": item,
                        "result": _result_envelope(
                            {
                                "data": {
                                    "skipped": True,
                                    **({"dry_run": True} if dry_run else {}),
                                },
                                "summary": {"skipped": 1},
                                "cli_report": "Skipped merchant review item",
                            }
                        ),
                    }
                )
                continue

            if decision not in {"confirm", "fix"}:
                raise ValueError("decision must be confirm, fix, or skip")
            if not category:
                raise ValueError("category is required")

            if decision == "confirm":
                if not pattern:
                    raise ValueError("pattern is required")
                result = _call_full(
                    cat.handle_memory_add,
                    {
                        "pattern": pattern,
                        "category": category,
                        "use_type": use_type,
                        "dry_run": dry_run,
                    },
                    _caller_name="cat_review_new_merchants",
                )
            else:
                if not txn_ids:
                    raise ValueError("txn_ids are required for fix decisions")
                result = _call_full(
                    txn.handle_categorize,
                    {
                        "category": category,
                        "bulk": True,
                        "query": None,
                        "date_from": None,
                        "date_to": None,
                        "remember": True,
                        "txn_id": None,
                        "ids": ",".join(txn_ids),
                        "dry_run": dry_run,
                    },
                    _caller_name="cat_review_new_merchants",
                )
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("cat_review_new_merchants", results)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_disable(id: str, dry_run: bool = False) -> dict:
    """Disable a vendor-memory rule.

    Use this for: deactivating a vendor-memory rule without deleting it.
    NOT for: undoing transaction memory, permanent deletion, confirmation, adding, or listing. → see `cat_memory_undo`, `cat_memory_delete`, `cat_memory_confirm`, `cat_memory_add`, `cat_memory_list`.

    Discovery: use cat_memory_list to choose id before confirming, disabling, or deleting memory rows.
    """
    return _call(cat.handle_memory_disable, {"id": id, "dry_run": dry_run})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_disable_bulk(ids: list[str], dry_run: bool = False) -> dict:
    """Disable multiple vendor-memory rules by id with per-item error reporting.

    Related tools: cat_memory_add, cat_memory_confirm, cat_memory_delete.
    """
    results: list[dict[str, Any]] = []
    for memory_id in ids or []:
        item = {"id": memory_id}
        try:
            result = _call_full(
                cat.handle_memory_disable,
                {"id": memory_id, "dry_run": dry_run},
                _caller_name="cat_memory_disable_bulk",
            )
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("cat_memory_disable_bulk", results)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_confirm(id: str, dry_run: bool = False) -> dict:
    """Confirm a learned vendor pattern to strengthen future categorization.

    Use this for: confirming an existing learned vendor pattern.
    NOT for: undoing transaction memory, deleting, disabling, adding, or listing memory rules. → see `cat_memory_undo`, `cat_memory_delete`, `cat_memory_disable`, `cat_memory_add`, `cat_memory_list`.

    Discovery: use cat_memory_list to choose id before confirming, disabling, or deleting memory rows.
    """
    return _call(cat.handle_memory_confirm, {"id": id, "dry_run": dry_run})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_delete(id: str, dry_run: bool = False) -> dict:
    """Soft-delete a vendor memory rule and return a restore token.

    Use this for: removing a vendor-memory rule by id while preserving a `cat_memory_restore` recovery path.
    NOT for: undoing transaction memory, temporary disablement, confirmation, adding, or listing. → see `cat_memory_undo`, `cat_memory_disable`, `cat_memory_confirm`, `cat_memory_add`, `cat_memory_list`.

    Discovery: use cat_memory_list to choose id before confirming, disabling, or deleting memory rows.
    """
    return _call(cat.handle_memory_delete, {"id": id, "dry_run": dry_run})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_delete_bulk(ids: list[str], dry_run: bool = False) -> dict:
    """Soft-delete multiple vendor-memory rules by id with per-item restore tokens.

    Related tools: cat_memory_add, cat_memory_confirm, cat_memory_delete.
    """
    results: list[dict[str, Any]] = []
    for memory_id in ids or []:
        item = {"id": memory_id}
        try:
            result = _call_full(
                cat.handle_memory_delete,
                {"id": memory_id, "dry_run": dry_run},
                _caller_name="cat_memory_delete_bulk",
            )
            results.append(
                {"status": "success", "item": item, "result": _result_envelope(result)}
            )
        except Exception as exc:
            results.append(_bulk_error(item, exc))
    return _bulk_envelope("cat_memory_delete_bulk", results)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_restore(restore_token: str, dry_run: bool = False) -> dict:
    """Restore a vendor memory rule using the restore_token returned by `cat_memory_delete`."""
    return _call(
        cat.handle_memory_restore, {"restore_token": restore_token, "dry_run": dry_run}
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_memory_undo(txn_id: str, dry_run: bool = False) -> dict:
    """Undo vendor memory for a transaction (revert to uncategorized).

    Use this for: reversing vendor memory learned from a specific transaction.
    NOT for: deleting, disabling, confirming, adding, or listing vendor-memory rules. → see `cat_memory_delete`, `cat_memory_disable`, `cat_memory_confirm`, `cat_memory_add`, `cat_memory_list`.

    Discovery: use txn_list, txn_show, or the categorization preview output to choose transaction IDs.
    """
    return _call(cat.handle_memory_undo, {"txn_id": txn_id, "dry_run": dry_run})


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def cat_auto_categorize(dry_run: bool = True, ai: bool = False) -> dict:
    """Apply accumulated categorization knowledge to uncategorized transactions.

    Args:
        dry_run: If True (default), preview matches without saving. Set False to commit.
        ai: If True, also run AI categorization pass on remaining unmatched.

    Returns:
        Dict with updated count, by_source breakdown, and ambiguous count.

    Examples:
        cat_auto_categorize()
        cat_auto_categorize(dry_run=False)
        cat_auto_categorize(dry_run=False, ai=True)
    """
    # Cannot use _call helper: needs manual commit when dry_run=False.
    with _get_conn() as conn:
        result = cat.handle_auto_categorize(
            _ns(dry_run=dry_run, ai=ai, provider=None, batch_size=None),
            conn,
            rules_path=_get_rules_path(),
        )
        if not dry_run:
            conn.commit()
    return _result_envelope(result)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_apply_splits(
    commit: bool = False, backfill: bool = False, summary_only: bool = True
) -> dict:
    """Apply business/personal split rules to allocate mixed expenses.

    Args:
        commit: If True, create split children and deactivate parent rows.
        backfill: If True, scan all active unsplit transactions (not just unreviewed).
        summary_only: If True (default), return CLI report instead of full match list to reduce payload size.

    Examples:
        cat_apply_splits()
        cat_apply_splits(commit=True)
        cat_apply_splits(commit=True, backfill=True)
    """
    extra: dict[str, Any] = {}
    rules_path = _get_rules_path()
    if rules_path is not None:
        extra["rules_path"] = rules_path
    with _get_conn() as conn:
        result = cat.handle_apply_splits(
            _ns(commit=commit, backfill=backfill), conn, **extra
        )
    if summary_only:
        cache_id = _write_cache_safe("cat_apply_splits", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cat_classify_use_type(commit: bool = False) -> dict:
    """Classify personal vs business transactions using accumulated rules.

    Args:
        commit: If True, persist updates. Default False runs as dry-run.

    Returns:
        Dict with scanned count, candidate updates, applied updates, and reason breakdown.

    Examples:
        cat_classify_use_type()
        cat_classify_use_type(commit=True)
    """
    return _call(cat.handle_classify_use_type, {"commit": commit}, pass_rules=True)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_categorize(
    txn_id: str, category: str, remember: bool = False, dry_run: bool = False
) -> dict:
    """Categorize a single transaction (and optionally save as vendor memory rule).

    Args:
        txn_id: Transaction ID to categorize.
        category: Category name to assign (must exist in DB).
        remember: If True, save a vendor memory rule for future auto-matching.

    Returns:
        Dict with transaction_id, category, previous/updated state, and remembered flag.

    Examples:
        txn_categorize(txn_id="abc123", category="Dining")
        txn_categorize(txn_id="abc123", category="Coffee", remember=True)

    Use this for: assigning a category to one transaction.
    NOT for: editing fields, project-tagging, bulk categorization, or automatic categorization. → see `txn_edit`, `txn_tag`, `txn_bulk_categorize`, `cat_auto_categorize`.

    Discovery: use txn_list, txn_show, or the categorization preview output to choose transaction IDs.
    """
    return _call(
        txn.handle_categorize,
        {
            "txn_id": txn_id,
            "category": category,
            "remember": remember,
            "bulk": False,
            "ids": None,
            "date_from": None,
            "date_to": None,
            "query": None,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_bulk_categorize(
    category: str,
    query: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    ids: Optional[list[str]] = None,
    remember: bool = False,
    dry_run: bool = False,
) -> dict:
    """Categorize multiple transactions matching filters or explicit ids.

    Related tools: txn_bulk_tag.
    """
    return _call(
        txn.handle_categorize,
        {
            "category": category,
            "bulk": True,
            "query": query,
            "date_from": date_from,
            "date_to": date_to,
            "remember": remember,
            "txn_id": None,
            "ids": ",".join(str(value) for value in ids) if ids else None,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def bulk_reclassify_business(
    ids: list[str],
    category: Optional[str] = None,
    remember: bool = False,
    dry_run: bool = False,
) -> dict:
    """Reclassify selected expense transactions as Business deductions.

    Discovery
    ---------
    - Use txn_list, txn_search, or intervention output to collect exact transaction ids.
    - Use category_list or existing transaction categories to choose an optional category.

    When to use
    -----------
    - Use after the user confirms selected personal/unknown expenses are business deductions.
    - NOT for income or broad account backfills; use account_set_business for account-level history.

    Behavior
    --------
    - Requires explicit transaction ids and rejects non-expense rows.
    - Sets transactions.use_type='Business'.
    - If category is provided, also assigns that category and can remember Business vendor patterns.
    - Pass dry_run=True to preview without writing.
    """
    with _get_conn() as conn:
        return business_bulk_actions.bulk_reclassify_business(
            conn,
            ids=ids,
            category=category,
            remember=remember,
            dry_run=dry_run,
        )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def txn_review(
    txn_id: Optional[str] = None,
    all_today: bool = False,
    before: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Mark transactions as reviewed.

    Args:
        txn_id: Single transaction ID to mark reviewed.
        all_today: If True, mark all of today's transactions reviewed.
        before: Mark all transactions before this date (YYYY-MM-DD) reviewed.

    Returns:
        Dict with updated count or transaction_id confirmation.

    Examples:
        txn_review(txn_id="abc123")
        txn_review(all_today=True)
        txn_review(before="2026-02-01")
    """
    return _call(
        txn.handle_review,
        {
            "txn_id": txn_id,
            "all_today": all_today,
            "before": before,
            "dry_run": dry_run,
        },
    )


# ===================================================================
# 6. Setup & Import (7 tools, write)
# ===================================================================


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def setup_init(dry_run: bool = True) -> dict:
    """Establish financial foundation: seed categories, create config templates.

    In CLI/Telegram mode, also creates .env template and bootstraps rules.yaml.
    In web gateway mode, .env and rules.yaml are server-managed (skipped).

    Args:
        dry_run: If True (default), preview changes without applying.

    Returns:
        Dict with env_template, categories, and rules_file status.

    Examples:
        setup_init()
        setup_init(dry_run=False)
    """
    gateway = current_db_path() is not None
    return _call(setup_cmd.handle_init, {"dry_run": dry_run, "gateway": gateway})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def setup_connect(
    user_id: Optional[str] = None,
    include_liabilities: bool = False,
    timeout: int = 300,
    skip_sync: bool = False,
    open_browser: bool = False,
) -> dict:
    """Connect a bank to start building financial history via Plaid.

    Args:
        user_id: Client user ID for Plaid (default 'default').
        include_liabilities: Request liabilities product during link.
        timeout: Seconds to wait for user to complete link flow (default 300).
        skip_sync: If True, skip initial transaction sync after linking.
        open_browser: If True, automatically open the hosted link URL.

    Returns:
        Dict with linked item details, sync results, and hosted link URL.

    Examples:
        setup_connect(open_browser=True)
        setup_connect(include_liabilities=True, timeout=600)
    """
    return _call(
        setup_cmd.handle_connect,
        {
            "user_id": user_id or "default",
            "include_liabilities": include_liabilities,
            "timeout": timeout,
            "skip_sync": skip_sync,
            "open_browser": open_browser,
        },
    )


@mcp.tool(
    sync_behavior="server_proxied",
    approval_required=True,
    onboarding_auto_approved=True,
)
def plaid_sync(
    days: Optional[int] = None,
    item: Optional[str] = None,
    force: bool = False,
    backfill: bool = False,
) -> dict:
    """Pull new transactions from Plaid to keep your financial history current.

    Args:
        days: Limit sync to last N days.
        item: Specific Plaid item ID to sync.
        force: Force refresh even if recently synced.
        backfill: Ignore the stored cursor and request historical transactions.

    Returns:
        Dict with items_synced, added, modified, removed counts.

    Examples:
        plaid_sync()
        plaid_sync(days=7, force=True)
        plaid_sync(backfill=True)
    """
    return _call(
        plaid_cmd.handle_sync,
        {"days": days, "item": item, "force": force, "backfill": backfill},
        pass_rules=True,
    )


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def plaid_status() -> dict:
    """Plaid configuration status and linked item registry.

    Returns:
        Dict with configured flag, items list, active/error counts.

    Examples:
        plaid_status()
    """
    return _call(plaid_cmd.handle_status, {})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def plaid_usage(day: bool = False, month: bool = False) -> dict:
    """Plaid API usage by endpoint for the current user."""
    return _call(plaid_cmd.handle_usage, {"day": day, "month": month})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def plaid_link(
    user_id: Optional[str] = None,
    wait: bool = False,
    timeout: int = 300,
    open_browser: bool = False,
    update: bool = False,
    item: Optional[str] = None,
    product: Optional[str] = None,
    include_balance: bool = False,
    include_liabilities: bool = False,
    allow_duplicate: bool = False,
) -> dict:
    """Connect a bank account via Plaid to start building financial history.

    Generates a hosted link URL for the user to complete bank authentication.
    Returns the URL immediately by default — pass wait=True to block until
    the user finishes and exchange the token.

    Args:
        user_id: Client user ID for Plaid (default 'finance-cli-user').
        wait: If True, poll until user completes link flow (default False).
        timeout: Seconds to wait for completion when wait=True (default 300).
        open_browser: Open the hosted link URL in the user's browser (default False).
        update: Use update mode for an existing item (requires item param).
        item: Plaid item ID for update-mode re-authentication.
        product: Comma-separated Plaid products to request (e.g. 'transactions,investments').
        include_balance: Request balance product (default False).
        include_liabilities: Request liabilities product (default False).
        allow_duplicate: Allow linking same institution twice (default False).

    Returns:
        Dict with hosted_link_url, session details, and linked_item (if wait=True).

    Examples:
        plaid_link()
        plaid_link(wait=True, open_browser=True, include_liabilities=True)
        plaid_link(update=True, item="item_abc123")
    """
    products = [p.strip() for p in product.split(",")] if product else None
    return _call(
        plaid_cmd.handle_link,
        {
            "user_id": user_id or "finance-cli-user",
            "wait": wait,
            "timeout": timeout,
            "poll_seconds": 10,
            "open_browser": open_browser,
            "update": update,
            "item": item,
            "product": products,
            "include_balance": include_balance,
            "include_liabilities": include_liabilities,
            "allow_duplicate": allow_duplicate,
        },
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def plaid_exchange(
    link_token: str,
    requested_products: Optional[list[str]] = None,
    timeout: int = 300,
    allow_duplicate: bool = False,
) -> dict:
    """Complete a previously created Plaid Hosted Link session.

    Exchanges the completed hosted link flow into a persisted Plaid item.

    Args:
        link_token: Hosted Link session token returned by plaid_link().
        requested_products: Products requested when the link session was created.
        timeout: Seconds to wait for Plaid Hosted Link completion.
        allow_duplicate: Allow linking the same institution twice.

    Returns:
        Dict with the persisted Plaid item details.

    Examples:
        plaid_exchange(link_token="link-token-123")
        plaid_exchange(link_token="link-token-123", requested_products=["transactions", "liabilities"])
    """
    return _call(
        plaid_cmd.handle_plaid_exchange,
        {
            "link_token": link_token,
            "requested_products": requested_products,
            "timeout": timeout,
            "poll_seconds": 10,
            "allow_duplicate_institution": allow_duplicate,
        },
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def plaid_unlink(item: str) -> dict:
    """Disconnect a Plaid item (bank connection).

    Deactivates the local item, its accounts, and all linked transactions.
    Creates a database backup before unlinking.

    Args:
        item: Plaid item ID to disconnect (from plaid_status output).

    Returns:
        Dict with item_id, status, and backup_path.

    Examples:
        plaid_unlink(item="item_abc123")
    """
    return _call(plaid_cmd.handle_unlink, {"item": item})


@mcp.tool(
    sync_behavior="server_proxied",
    approval_required=True,
    onboarding_auto_approved=True,
)
def plaid_balance_refresh(item: Optional[str] = None, force: bool = False) -> dict:
    """Refresh account balances to update net worth and liquidity calculations.

    Args:
        item: Specific Plaid item ID to refresh.
        force: Force refresh even if recently updated.

    Returns:
        Dict with items_refreshed, accounts_updated, snapshots_updated.

    Examples:
        plaid_balance_refresh()
        plaid_balance_refresh(force=True)
    """
    return _call(plaid_cmd.handle_balance_refresh, {"item": item, "force": force})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def stripe_link() -> dict:
    """Connect Stripe to start tracking business revenue history.

    Returns:
        Dict with linked Stripe account metadata.

    Examples:
        stripe_link()
    """
    return _call(stripe_cmd.handle_link, {})


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def stripe_sync(
    days: Optional[int] = None, force: bool = False, backfill: bool = False
) -> dict:
    """Sync Stripe payouts to keep business revenue history current.

    Args:
        days: Optional lookback window (overrides stored cursor).
        force: If True, bypass sync cooldown.
        backfill: If True, ignore stored cursor and pull full history.

    Returns:
        Dict with sync counters and payout dedup outcomes.

    Examples:
        stripe_sync()
        stripe_sync(days=30, force=True)
        stripe_sync(backfill=True)
    """
    return _call(
        stripe_cmd.handle_sync, {"days": days, "force": force, "backfill": backfill}
    )


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def stripe_status() -> dict:
    """Stripe configuration and connection status.

    Returns:
        Dict with readiness flags, connection metadata, and Stripe transaction counts.

    Examples:
        stripe_status()
    """
    return _call(stripe_cmd.handle_status, {})


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def stripe_revenue(
    month: Optional[str] = None,
    quarter: Optional[str] = None,
    year: Optional[str] = None,
) -> dict:
    """Stripe revenue summary grouped by month.

    Args:
        month: Optional month filter (YYYY-MM).
        quarter: Optional quarter filter (YYYY-QN).
        year: Optional year filter (YYYY).

    Returns:
        Dict with monthly gross, fees, refunds, and net totals.

    Examples:
        stripe_revenue()
        stripe_revenue(month="2026-02")
        stripe_revenue(year="2026")
    """
    return _call(
        stripe_cmd.handle_revenue, {"month": month, "quarter": quarter, "year": year}
    )


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def stripe_unlink() -> dict:
    """Disconnect Stripe by marking the local connection as disconnected.

    Returns:
        Dict with unlink status.

    Examples:
        stripe_unlink()
    """
    return _call(stripe_cmd.handle_unlink, {})


@mcp.tool(sync_behavior="db_write", excluded_from_agent=True)
def ingest_csv(file: str, institution: str, commit: bool = False) -> dict:
    """Add CSV bank transactions to your financial history.

    Args:
        file: Path to the CSV file.
        institution: Institution name (e.g. 'chase', 'amex', 'schwab') or 'auto'.
        commit: If True, save imported transactions. Default is dry-run.

    Returns:
        Dict with file info, row counts, inserted/skipped/error counts.

    Examples:
        ingest_csv(file="/path/to/chase.csv", institution="chase")
        ingest_csv(file="/path/to/chase.csv", institution="chase", commit=True)
    """
    path = _validate_upload_path(file)
    if not institution or institution.lower() == "auto":
        detected = detect_csv_institution(path)
        if not detected:
            raise ValueError(
                "Could not auto-detect CSV format. Please specify the institution."
            )
        institution = detected
    return _call(
        ingest.handle_ingest_csv,
        {
            "file": str(path),
            "institution": institution,
            "commit": commit,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="db_write", excluded_from_agent=True)
def ingest_statement(
    file: str,
    commit: bool = False,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    institution: Optional[str] = None,
    card_ending: Optional[str] = None,
    account_id: Optional[str] = None,
    replace: bool = False,
    allow_partial: bool = False,
) -> dict:
    """Add PDF statement transactions to your financial history via AI parser.

    Args:
        file: Path to the PDF statement file.
        commit: If True, save to DB. Default is dry-run preview.
        provider: AI provider ('claude' or 'openai'). Uses rules.yaml default if omitted.
        model: Model name override.
        institution: Institution hint (e.g. 'chase', 'amex').
        card_ending: Card ending hint (e.g. '1234').
        account_id: Existing account ID to tag transactions with.
        replace: Replace previously imported data for same file hash.
        allow_partial: Import unblocked rows when some are confidence-blocked.

    Returns:
        Dict with transaction_count, inserted, skipped_duplicates, reconcile_status.

    Examples:
        ingest_statement(file="/path/to/statement.pdf")
        ingest_statement(file="/path/to/statement.pdf", commit=True, institution="amex")
    """
    path = _validate_upload_path(file)
    return _call(
        ingest.handle_ingest_statement,
        {
            "file": str(path),
            "dir": None,
            "commit": commit,
            "backend": None,
            "provider": provider,
            "model": model,
            "max_tokens": None,
            "institution": institution,
            "card_ending": card_ending,
            "account_id": account_id,
            "replace": replace,
            "allow_partial": allow_partial,
            "require_reconciled": False,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="db_write", excluded_from_agent=True)
def ingest_batch(
    dir: str,
    commit: bool = False,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    institution: Optional[str] = None,
    card_ending: Optional[str] = None,
    allow_partial: bool = False,
) -> dict:
    """Build transaction history in bulk from a directory of statements.

    Args:
        dir: Directory containing PDF/CSV statement files.
        commit: If True, save to DB. Default is dry-run preview.
        provider: AI provider for PDF parsing ('claude' or 'openai').
        model: Model name override for PDF parsing.
        institution: Institution hint for account matching.
        card_ending: Card ending hint for account matching.
        allow_partial: For PDFs, import unblocked rows when confidence-blocked.

    Returns:
        Dict with per-file reports, total inserted/skipped/error counts.

    Examples:
        ingest_batch(dir="/path/to/statements/")
        ingest_batch(dir="/path/to/statements/", commit=True)
    """
    return _call(
        ingest.handle_ingest_batch,
        {
            "dir": dir,
            "commit": commit,
            "backend": None,
            "provider": provider,
            "model": model,
            "max_tokens": None,
            "institution": institution,
            "card_ending": card_ending,
            "allow_partial": allow_partial,
        },
        pass_rules=True,
    )


@mcp.tool(sync_behavior="no_sync", read_only=True, normalizer=True)
def statement_normalizer_sample_csv(file: str, lines: int = 20) -> dict:
    """Preview a bank statement CSV to build a normalizer for a new institution.

    Related tools: statement_normalizer_activate, statement_normalizer_list, statement_normalizer_stage.
    """
    path = _validate_upload_path(file)
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        sampled_lines = [fh.readline() for _ in range(max(0, lines))]
    text = "".join(line for line in sampled_lines if line)
    return {
        "data": {
            "file": str(path),
            "line_count": len([line for line in sampled_lines if line]),
            "lines": sampled_lines,
            "text": text,
        },
        "summary": {
            "file": str(path),
            "line_count": len([line for line in sampled_lines if line]),
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True, normalizer=True)
def statement_normalizer_list() -> dict:
    """List active bank statement CSV normalizers that expand import capabilities.

    Related tools: statement_normalizer_activate, statement_normalizer_sample_csv, statement_normalizer_stage.
    """
    normalizers = [
        {
            "aliases": list(entry.aliases),
            "primary_key": entry.primary_key,
            "source_name": entry.source_name,
            "tier": entry.tier,
        }
        for entry in get_normalizer_loader().list_entries()
    ]
    normalizers.sort(key=lambda item: item["primary_key"])
    return {
        "data": {"normalizers": normalizers},
        "summary": {"count": len(normalizers)},
    }


@mcp.tool(sync_behavior="no_sync", read_only=True, normalizer=True)
def normalizer_validate(file: str, institution: str) -> dict:
    """Validate normalizer output to ensure import accuracy."""
    path = _validate_upload_path(file)
    preview = _run_normalizer_preview(institution, path)
    validation = preview["validation"]
    return {
        "data": {
            "file": str(path),
            "institution": normalize_registry_key(institution),
            "source_name": preview["source_name"],
            "tier": preview["tier"],
            "validation": validation,
        },
        "summary": {
            "institution": normalize_registry_key(institution),
            "tier": preview["tier"],
            "valid": validation["valid"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True, normalizer=True)
def statement_normalizer_test(file: str, institution: str) -> dict:
    """Test a bank statement normalizer against real data before activating it.

    Related tools: statement_normalizer_activate, statement_normalizer_list, statement_normalizer_sample_csv.
    """
    path = _validate_upload_path(file)
    preview = _run_normalizer_preview(institution, path)
    result = preview["result"]
    validation = preview["validation"]
    return {
        "data": {
            "content_hash": preview["content_hash"],
            "file": str(path),
            "institution": normalize_registry_key(institution),
            "module_path": str(preview["module_path"]),
            "sample_rows": result.rows[:10],
            "skipped_row_count": result.skipped_row_count,
            "source_name": preview["source_name"],
            "tier": preview["tier"],
            "validation": validation,
            "warnings": result.warnings,
            "warning_count": len(result.warnings),
            "raw_row_count": result.raw_row_count,
            "row_count": len(result.rows),
        },
        "summary": {
            "institution": normalize_registry_key(institution),
            "row_count": len(result.rows),
            "tier": preview["tier"],
            "valid": validation["valid"],
        },
    }


@mcp.tool(sync_behavior="no_sync", read_only=True, normalizer=True)
def normalizer_detect(file: str) -> dict:
    """Auto-detect a CSV format to enable importing from a new institution."""
    path = _validate_upload_path(file)
    institution = detect_csv_institution(path)
    entry = get_normalizer_loader().get_entry(institution) if institution else None
    return {
        "data": {
            "file": str(path),
            "institution": institution,
            "source_name": entry.source_name if entry else None,
            "tier": entry.tier if entry else None,
        },
        "summary": {"detected": institution is not None, "institution": institution},
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True, normalizer=True)
def statement_normalizer_stage(key: str, source: str) -> dict:
    """Stage a new bank statement normalizer to expand import capabilities.

    Discovery: use statement_normalizer_list to choose key before staging, activating, or updating a normalizer.
    Related tools: statement_normalizer_activate, statement_normalizer_list, statement_normalizer_sample_csv.
    """
    normalized_key = normalize_registry_key(key)
    loader = get_normalizer_loader()
    existing = loader.get_entry(normalized_key)
    if existing and existing.tier == BUILT_IN_TIER:
        raise ValueError(
            f"normalizer '{normalized_key}' is built-in and cannot be staged"
        )
    if normalizer_file_exists(_active_user_normalizer_path(normalized_key)):
        raise ValueError(
            f"user normalizer '{normalized_key}' already exists; use normalizer_update instead"
        )

    metadata = _validate_normalizer_source_for_key(normalized_key, source)
    _assert_normalizer_key_conflicts(normalized_key, metadata)
    staged_path = _staged_user_normalizer_path(normalized_key)
    write_normalizer_text(staged_path, _normalize_source_blob(source))
    _clear_normalizer_test_state(normalized_key)
    reset_normalizer_loader_cache()
    return {
        "data": {
            "aliases": metadata.aliases,
            "primary_key": metadata.primary_key,
            "source_name": metadata.source_name,
            "staged_path": str(staged_path),
        },
        "summary": {"key": normalized_key, "staged": True},
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True, normalizer=True)
def statement_normalizer_activate(key: str) -> dict:
    """Activate a tested bank statement normalizer to permanently expand import capabilities.

    Discovery: use statement_normalizer_list to choose key before staging, activating, or updating a normalizer.
    Related tools: statement_normalizer_list, statement_normalizer_sample_csv, statement_normalizer_stage.
    """
    normalized_key = normalize_registry_key(key)
    staged_path = _staged_user_normalizer_path(normalized_key)
    if not normalizer_file_exists(staged_path):
        raise FileNotFoundError(f"staged normalizer not found for '{normalized_key}'")

    loader = get_normalizer_loader()
    existing = loader.get_entry(normalized_key)
    if existing and existing.tier == BUILT_IN_TIER:
        raise ValueError(
            f"normalizer '{normalized_key}' is built-in and cannot be activated"
        )

    metadata = load_user_module_metadata(staged_path)
    content_hash = normalizer_file_content_hash(staged_path)
    state = _load_normalizer_test_state().get(normalized_key)
    if not state or state.get("content_hash") != content_hash:
        raise ValueError(
            f"staged normalizer '{normalized_key}' must pass statement_normalizer_test before activation"
        )

    active_path = _active_user_normalizer_path(normalized_key)
    replace_normalizer_file(staged_path, active_path)
    _clear_normalizer_test_state(normalized_key, best_effort=True)
    reset_normalizer_loader_cache()
    return {
        "data": {
            "active_path": str(active_path),
            "aliases": metadata.aliases,
            "content_hash": content_hash,
            "primary_key": metadata.primary_key,
            "source_name": metadata.source_name,
        },
        "summary": {"activated": True, "key": normalized_key},
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True, normalizer=True)
def normalizer_update(key: str, source: str) -> dict:
    """Update an existing normalizer to improve import accuracy.

    Discovery: use statement_normalizer_list to choose key before staging, activating, or updating a normalizer.
    """
    normalized_key = normalize_registry_key(key)
    loader = get_normalizer_loader()
    existing = loader.get_entry(normalized_key)
    if existing and existing.tier == BUILT_IN_TIER:
        raise ValueError(
            f"normalizer '{normalized_key}' is built-in and cannot be updated"
        )

    active_exists = normalizer_file_exists(_active_user_normalizer_path(normalized_key))
    staged_exists = normalizer_file_exists(_staged_user_normalizer_path(normalized_key))
    if not active_exists and not staged_exists:
        raise ValueError(f"user normalizer '{normalized_key}' does not exist")

    metadata = _validate_normalizer_source_for_key(normalized_key, source)
    _assert_normalizer_key_conflicts(normalized_key, metadata)
    staged_path = _staged_user_normalizer_path(normalized_key)
    write_normalizer_text(staged_path, _normalize_source_blob(source))
    _clear_normalizer_test_state(normalized_key)
    reset_normalizer_loader_cache()
    return {
        "data": {
            "aliases": metadata.aliases,
            "primary_key": metadata.primary_key,
            "source_name": metadata.source_name,
            "staged_path": str(staged_path),
        },
        "summary": {"key": normalized_key, "updated": True},
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True, normalizer=True)
def normalizer_register_institution(
    canonical_name: str, aliases: Optional[list[str]] = None
) -> dict:
    """Register a new institution to expand importable sources.

    Discovery: use statement_normalizer_list and known institution names before registering canonical_name.
    """
    result = register_user_institution(canonical_name, aliases)
    reset_normalizer_loader_cache()
    return {
        "data": result,
        "summary": {
            "canonical_name": result["canonical_name"],
            "changed": result["changed"],
        },
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def export_sheets(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    year: Optional[str] = None,
    new: bool = False,
    spreadsheet_id: Optional[str] = None,
) -> dict:
    """Export financial records and reports to Google Sheets.

    Args:
        date_from: Optional start date (YYYY-MM-DD) for transactions/spending tabs.
        date_to: Optional end date (YYYY-MM-DD) for transactions/spending tabs.
        year: Optional business year (YYYY).
        new: If True, create a new spreadsheet and save it as default.
        spreadsheet_id: Optional existing spreadsheet id to update.

    Returns:
        Dict with spreadsheet metadata and per-tab export stats.

    Examples:
        export_sheets()
        export_sheets(year="2025")
        export_sheets(date_from="2025-01-01", date_to="2025-03-31", spreadsheet_id="abc123")
    """
    return _call(
        export_cmd.handle_sheets,
        {
            "date_from": date_from,
            "date_to": date_to,
            "year": year,
            "auth": False,
            "new": new,
            "spreadsheet_id": spreadsheet_id,
            "interactive": False,
        },
    )


@mcp.tool(sync_behavior="no_sync", excluded_from_agent=True)
def export_csv(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    """Export financial records to CSV for external use."""
    return _call(
        export_cmd.handle_csv,
        {
            "date_from": date_from,
            "date_to": date_to,
            "category": category,
            "output": _export_output_path("transactions"),
        },
    )


@mcp.tool(sync_behavior="no_sync", excluded_from_agent=True)
def export_summary(month: Optional[str] = None) -> dict:
    """Export a monthly financial summary for record-keeping."""
    resolved_month = month or datetime.now().strftime("%Y-%m")
    return _call(
        export_cmd.handle_summary,
        {
            "month": resolved_month,
            "output": _export_output_path(f"summary_{resolved_month.replace('-', '')}"),
        },
    )


@mcp.tool(sync_behavior="no_sync", excluded_from_agent=True)
def export_wave(month: str, output: str = "exports") -> dict:
    """Export financial records as Wave accounting CSVs for a given month."""
    return _call(export_cmd.handle_wave, {"month": month, "output": output})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def dedup_review_key_only(
    account_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    summary_only: bool = True,
) -> dict:
    """Review borderline dedup matches to protect transaction history accuracy.

    Args:
        account_id: Optional account filter.
        date_from: Optional start date filter.
        date_to: Optional end date filter.
        summary_only: If True (default), return CLI report instead of full match list to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_review_key_only(
            _ns(account_id=account_id, date_from=date_from, date_to=date_to),
            conn,
        )
    if summary_only:
        cache_id = _write_cache_safe("dedup_review_key_only", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def dedup_backfill_aliases(commit: bool = False) -> dict:
    """Link import-created accounts to canonical Plaid accounts for unified history."""
    return _call(dedup_cmd.handle_backfill_aliases, {"commit": commit})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def dedup_create_alias(from_id: str, to_id: str, commit: bool = False) -> dict:
    """Create an account alias to unify transaction history across sources.

    Discovery: use dedup_preview or account/transaction lists to choose from_id and to_id before creating an alias.
    """
    return _call(
        dedup_cmd.handle_create_alias,
        {"from_id": from_id, "to_id": to_id, "commit": commit},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def dedup_suggest_aliases(summary_only: bool = True) -> dict:
    """Suggest account aliases to unify transaction history across sources.

    Args:
        summary_only: If True (default), return CLI report instead of full suggestion list to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_suggest_aliases(_ns(), conn)
    if summary_only:
        cache_id = _write_cache_safe("dedup_suggest_aliases", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def dedup_detect_equivalences(min_overlap: int = 3, summary_only: bool = True) -> dict:
    """Detect institution name variations across sources for consistent history.

    Args:
        min_overlap: Minimum overlapping card endings to consider (default 3).
        summary_only: If True (default), return CLI report instead of full candidate list to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_detect_equivalences(
            _ns(min_overlap=min_overlap), conn
        )
    if summary_only:
        cache_id = _write_cache_safe(
            "dedup_detect_equivalences", _result_envelope(result)
        )
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(
    sync_behavior="db_write", approval_required=True, onboarding_auto_approved=True
)
def dedup_cross_format(
    dry_run: bool = True,
    account_id: Optional[str] = None,
    include_key_only: bool = False,
    summary_only: bool = True,
) -> dict:
    """Find and remove duplicate transactions across sources to maintain accurate history.
    Matches by account + amount + date (±2 days) + description similarity.
    Use dry_run=True to preview, dry_run=False to deactivate duplicates.
    Key-only matches are skipped unless include_key_only=True is set after review.

    Args:
        dry_run: If True (default), preview without deactivating.
        account_id: Optional account filter.
        include_key_only: If True with dry_run=False, also deactivate reviewed key-only matches.
        summary_only: If True (default), return CLI report instead of full match list to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_cross_format(
            _ns(
                account_id=account_id,
                date_from=None,
                date_to=None,
                commit=not dry_run,
                include_key_only=include_key_only,
            ),
            conn,
        )
    if summary_only:
        cache_id = _write_cache_safe("dedup_cross_format", _result_envelope(result))
        return _summarize_result(
            result,
            {
                "dry_run": dry_run,
                "include_key_only": include_key_only,
                "cache_id": cache_id,
            },
        )
    return _result_envelope(result)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def dedup_same_source(
    account_id: Optional[str] = None,
    min_amount_cents: int = 0,
    summary_only: bool = True,
) -> dict:
    """Detect same-source duplicates to maintain accurate transaction history.
    Groups by (account, date, amount, description) where count > 1.
    Returns detection only; use dedup_same_source_apply to deactivate.

    Args:
        account_id: Optional account filter.
        min_amount_cents: Minimum absolute amount in cents.
        summary_only: If True (default), return CLI report instead of full group JSON to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_same_source(
            _ns(
                account_id=account_id,
                min_amount=min_amount_cents,
                commit=False,
                ids=None,
            ),
            conn,
        )
    if summary_only:
        cache_id = _write_cache_safe("dedup_same_source", _result_envelope(result))
        return {
            "data": {
                "cli_report": result.get("cli_report", ""),
                "cache_id": cache_id,
                "total_groups": result.get("summary", {}).get("total_groups", 0),
                "total_excess_rows": result.get("summary", {}).get(
                    "total_excess_rows", 0
                ),
            },
            "summary": result.get("summary", {}),
        }
    return _result_envelope(result)


@mcp.tool(sync_behavior="db_write", approval_required=True)
def dedup_same_source_apply(
    ids: str, account_id: Optional[str] = None, min_amount_cents: int = 0
) -> dict:
    """Remove confirmed duplicates to maintain accurate history.
    Run dedup_same_source first to identify candidates, then pass comma-separated
    transaction IDs to deactivate. Creates a backup before deactivating."""
    return _call(
        dedup_cmd.handle_same_source,
        {
            "account_id": account_id,
            "min_amount": min_amount_cents,
            "commit": True,
            "ids": ids,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def dedup_audit_names(summary_only: bool = True) -> dict:
    """Audit institution names and aliasing gaps that affect history accuracy.

    Args:
        summary_only: If True (default), return CLI report instead of full issues list to reduce payload size.
    """
    with _get_conn() as conn:
        result = dedup_cmd.handle_audit_names(_ns(), conn)
    if summary_only:
        cache_id = _write_cache_safe("dedup_audit_names", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


@mcp.tool(sync_behavior="no_sync", read_only=True)
def provider_status() -> dict:
    """Show institution provider routing status."""
    return _call(provider_cmd.handle_status, {})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def provider_switch(institution: str, provider: str, dry_run: bool = False) -> dict:
    """Switch institution routing provider."""
    with _get_conn() as conn:
        result = provider_cmd.handle_switch(
            _ns(institution=institution, provider=provider, dry_run=dry_run), conn
        )
        if dry_run:
            conn.rollback()
            result.setdefault("data", {})["dry_run"] = True
        else:
            conn.commit()
    return _result_envelope(result)


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def schwab_sync() -> dict:
    """Sync Schwab balances to keep investment tracking current."""
    return _call(schwab_cmd.handle_sync, {})


@mcp.tool(sync_behavior="server_proxied", read_only=True)
def schwab_status() -> dict:
    """Show Schwab integration status."""
    return _call(schwab_cmd.handle_status, {})


# ===================================================================
# 7. Pipeline (1 tool)
# ===================================================================


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def monthly_run(
    month: Optional[str] = None,
    sync: bool = False,
    ai: bool = False,
    dry_run: bool = True,
    skip: Optional[list[str]] = None,
    summary_only: bool = True,
) -> dict:
    """Monthly learning cycle: sync data, clean duplicates, apply categorization knowledge, detect patterns.

    Args:
        month: Target month (YYYY-MM). Defaults to current month.
        sync: If True, run Plaid sync + balance refresh first.
        ai: If True, include AI categorization pass.
        dry_run: If True (default), preview without committing changes.
        skip: List of steps to skip. Valid: 'dedup', 'categorize', 'detect'.
        summary_only: If True (default), strip nested step result data to reduce payload size.

    Examples:
        monthly_run()
        monthly_run(month="2026-02", dry_run=False)
        monthly_run(sync=True, ai=True, dry_run=False)
        monthly_run(skip=["dedup", "detect"], dry_run=True)
    """
    # monthly_run manages its own commits/rollbacks internally per step.
    with _get_conn() as conn:
        result = monthly_cmd.handle_run(
            _ns(
                month=month or datetime.now().strftime("%Y-%m"),
                sync=sync,
                ai=ai,
                dry_run=dry_run,
                skip=skip or [],
                export_dir=None,
            ),
            conn,
            rules_path=_get_rules_path(),
        )
    full_result = _result_envelope(result)
    if summary_only:
        cache_id = _write_cache_safe("monthly_run", full_result)
        data = dict(full_result.get("data", {}))
        # Strip nested step results to status + summary only
        steps = data.get("steps", {})
        if isinstance(steps, dict):
            compact_steps: dict[str, Any] = {}
            for step_name, step_data in steps.items():
                if isinstance(step_data, dict):
                    compact_steps[step_name] = {
                        "status": step_data.get("status"),
                        "summary": step_data.get("result", {}).get("summary")
                        if isinstance(step_data.get("result"), dict)
                        else None,
                        "error": step_data.get("error"),
                    }
                else:
                    compact_steps[step_name] = step_data
            data["steps"] = compact_steps
        data["cache_id"] = cache_id
        return {"data": data, "summary": full_result.get("summary", {})}
    return full_result


# ===================================================================
# 8. Database (7 tools)
# ===================================================================


def _active_backup_user_id() -> str:
    user_context = get_user_context()
    if user_context is not None and str(user_context.expected_user_id or "").strip():
        return str(user_context.expected_user_id).strip()

    from finance_cli import config as config_module

    return str(config_module.default_user_id)


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def db_backup(offhost: bool = False) -> dict:
    """Back up your financial records, rules, and configuration.

    Returns:
        Dict with bundle path, checksums, and file count.

    Examples:
        db_backup()

    Related tools: db_backup_list, db_backup_prune, db_backup_verify.
    """
    from finance_cli.backup import create_backup

    with _get_conn() as conn:
        result = create_backup(
            conn,
            include_offhost=offhost,
            data_dir=_get_data_dir(),
            rules_path=_get_rules_path(),
            user_id=_active_backup_user_id(),
        )
    envelope = _result_envelope(
        {
            "data": {
                "backup_path": str(result.bundle_path),
                "bundle_path": str(result.bundle_path),
                "bundle_sha256": result.bundle_sha256,
                "size_bytes": result.bundle_size,
                "db_sha256": result.db_sha256,
                "migration_version": result.migration_ver,
                "duration_ms": result.duration_ms,
                "file_count": len(result.files),
            },
            "summary": {
                "size_bytes": result.bundle_size,
                "file_count": len(result.files),
            },
            "cli_report": f"Backup created: {result.bundle_path}",
        }
    )
    _record_mcp_sensitive_audit_event(
        event_type="db.backup.created",
        target_type="backup_bundle",
        target_id=result.bundle_sha256 or result.bundle_path,
        details={
            "offhost": offhost,
            "bundle_path": str(result.bundle_path),
            "bundle_sha256": result.bundle_sha256,
            "bundle_size": result.bundle_size,
            "file_count": len(result.files),
        },
    )
    return envelope


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def db_export_preferences() -> dict:
    """Export learned preferences and rules to a portable bundle.

    Returns:
        Bundle path, size, per-table row counts, and referenced categories.
    """
    from finance_cli.preferences import export_preferences

    with _get_conn() as conn:
        result = export_preferences(
            conn,
            data_dir=_get_data_dir(),
            rules_path=_get_rules_path(),
        )

    total_rows = int(sum(result.table_counts.values()))
    envelope = _result_envelope(
        {
            "data": {
                "bundle_path": str(result.bundle_path),
                "bundle_size": result.bundle_size,
                "table_counts": result.table_counts,
                "file_count": result.file_count,
                "categories_referenced": result.categories_referenced,
            },
            "summary": {
                "bundle_size": result.bundle_size,
                "total_rows": total_rows,
            },
            "cli_report": (
                f"Preferences exported to {result.bundle_path} "
                f"({result.bundle_size:,} bytes, "
                f"{total_rows} rows across {len(result.table_counts)} tables)"
            ),
        }
    )
    _record_mcp_sensitive_audit_event(
        event_type="data_export.preferences",
        target_type="preferences_bundle",
        target_id=result.bundle_path,
        details={
            "bundle_path": str(result.bundle_path),
            "bundle_size": result.bundle_size,
            "total_rows": total_rows,
            "file_count": result.file_count,
        },
    )
    return envelope


@mcp.tool(sync_behavior="server_proxied", approval_required=True)
def db_import_preferences(
    bundle_path: str,
    mode: str = "merge",
    create_missing_categories: bool = False,
    dry_run: bool = True,
) -> dict:
    """Import learned preferences and rules from a portable bundle.

    Args:
        bundle_path: Path to the .tar.gz preferences bundle.
        mode: 'merge' or 'overwrite'.
        create_missing_categories: Auto-create unknown categories under Other.
        dry_run: Preview changes without applying them.

    Discovery: use db_backup_list or the export command response to choose bundle_path before verifying, importing, or restoring.
    """
    from finance_cli.preferences import import_preferences

    if mode == "overwrite" and dry_run:
        raise ValueError(
            "overwrite mode requires dry_run=False (explicit opt-in for safety)"
        )

    with _get_conn() as conn:
        result = import_preferences(
            Path(bundle_path),
            conn,
            mode=mode,
            create_missing_categories=create_missing_categories,
            dry_run=dry_run,
            data_dir=_get_data_dir(),
            rules_path=_get_rules_path(),
        )

    total_imported = int(sum(result.tables_imported.values()))
    total_skipped = int(sum(result.tables_skipped.values()))
    envelope = _result_envelope(
        {
            "data": {
                "dry_run": result.dry_run,
                "mode": result.mode,
                "tables_imported": result.tables_imported,
                "tables_skipped": result.tables_skipped,
                "categories_missing": result.categories_missing,
                "categories_created": result.categories_created,
                "accounts_resolved": result.accounts_resolved,
                "accounts_unresolved": result.accounts_unresolved,
                "files_copied": result.files_copied,
                "warnings": result.warnings,
            },
            "summary": {
                "total_imported": total_imported,
                "total_skipped": total_skipped,
            },
            "cli_report": (
                f"{'DRY RUN: ' if result.dry_run else ''}"
                f"Imported {total_imported} rows, skipped {total_skipped} conflicts "
                f"({len(result.categories_missing)} missing categories, "
                f"{result.accounts_unresolved} unresolved accounts)"
            ),
        }
    )
    _record_mcp_sensitive_audit_event(
        event_type="data_import.preferences",
        target_type="preferences_bundle",
        target_id=bundle_path,
        details={
            "bundle_path": bundle_path,
            "mode": result.mode,
            "dry_run": result.dry_run,
            "total_imported": total_imported,
            "total_skipped": total_skipped,
            "warnings_count": len(result.warnings),
        },
    )
    return envelope


@mcp.tool(sync_behavior="no_sync", read_only=True)
def db_backup_list(backup_type: Optional[str] = None, limit: int = 20) -> dict:
    """List recent backups of your financial records.

    Args:
        backup_type: Filter by type: 'local', 'offhost', or None for all.
        limit: Maximum number of backups to return.

    Related tools: db_backup, db_backup_prune, db_backup_verify.
    """
    from finance_cli.backup import list_backups

    with _get_conn() as conn:
        entries = list_backups(
            conn,
            backup_type=backup_type,
            limit=limit,
            data_dir=_get_data_dir(),
        )
    return _result_envelope(
        {
            "data": entries,
            "summary": {
                "count": len(entries),
                "backup_type": backup_type or "all",
                "limit": int(limit),
            },
            "cli_report": f"{len(entries)} backups found",
        }
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def db_backup_verify(bundle_path: str) -> dict:
    """Verify a financial records backup's integrity.

    Args:
        bundle_path: Path to the .tar.gz backup bundle.

    Discovery: use db_backup_list or the export command response to choose bundle_path before verifying, importing, or restoring.
    Related tools: db_backup, db_backup_list, db_backup_prune.
    """
    from finance_cli.backup import verify_backup

    with _get_conn() as conn:
        result = verify_backup(
            Path(bundle_path),
            conn=conn,
            user_id=_active_backup_user_id(),
        )
    envelope = _result_envelope(
        {
            "data": {
                "valid": result.valid,
                "manifest": result.manifest,
                "errors": result.errors,
                "warnings": result.warnings,
            },
            "summary": {
                "valid": result.valid,
                "error_count": len(result.errors),
                "warning_count": len(result.warnings),
            },
            "cli_report": (
                f"{'VALID' if result.valid else 'INVALID'}: "
                f"{len(result.errors)} errors, {len(result.warnings)} warnings"
            ),
        }
    )
    _record_mcp_sensitive_audit_event(
        event_type="db.backup.verified",
        target_type="backup_bundle",
        target_id=result.manifest.get("bundle_sha256") or bundle_path,
        outcome="succeeded" if result.valid else "failed",
        details={
            "bundle_path": bundle_path,
            "valid": result.valid,
            "error_count": len(result.errors),
            "warning_count": len(result.warnings),
        },
    )
    return envelope


@mcp.tool(sync_behavior="server_proxied", excluded_from_agent=True)
def db_restore(bundle_path: str, dry_run: bool = True) -> dict:
    """Restore financial records and configuration from a backup.

    Args:
        bundle_path: Path to the .tar.gz backup bundle.
        dry_run: If True, report what would change without applying it.

    Discovery: use db_backup_list or the export command response to choose bundle_path before verifying, importing, or restoring.
    """
    from finance_cli.backup import restore_backup

    conn = _get_conn()
    user_context = get_user_context()
    expected_user_id = (
        user_context.expected_user_id
        if user_context is not None and user_context.expected_user_id is not None
        else _active_backup_user_id()
    )
    try:
        result = restore_backup(
            Path(bundle_path),
            conn=conn,
            dry_run=dry_run,
            data_dir=_get_data_dir(),
            rules_path=_get_rules_path(),
            expected_user_id=expected_user_id,
            user_id=_active_backup_user_id(),
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    envelope = _result_envelope(
        {
            "data": {
                "restored": result.restored,
                "dry_run": result.dry_run,
                "bundle_path": str(result.bundle_path),
                "warnings": result.warnings,
            },
            "summary": {
                "restored": result.restored,
                "dry_run": result.dry_run,
                "warning_count": len(result.warnings),
            },
            "cli_report": (
                f"{'DRY RUN: would restore' if result.dry_run else 'Restored'} from {result.bundle_path}"
            ),
        }
    )
    _record_mcp_sensitive_audit_event(
        event_type="db.restore.previewed" if result.dry_run else "db.restore.completed",
        target_type="backup_bundle",
        target_id=result.bundle_path,
        details={
            "bundle_path": str(result.bundle_path),
            "dry_run": result.dry_run,
            "restored": result.restored,
            "warning_count": len(result.warnings),
        },
    )
    return envelope


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def db_backup_prune(dry_run: bool = True) -> dict:
    """Apply retention policy to old financial record backups.

    Args:
        dry_run: If True, report what would be deleted without deleting.

    Related tools: db_backup, db_backup_list, db_backup_verify.
    """
    from finance_cli.backup import prune_backups

    with _get_conn() as conn:
        result = prune_backups(
            conn,
            dry_run=dry_run,
            data_dir=_get_data_dir(),
            user_id=_active_backup_user_id(),
        )
    return _result_envelope(
        {
            "data": {
                "dry_run": result.dry_run,
                "kept": result.kept,
                "deleted": result.deleted,
                "deleted_paths": result.deleted_paths,
                "freed_bytes": result.freed_bytes,
            },
            "summary": {
                "dry_run": result.dry_run,
                "kept": result.kept,
                "deleted": result.deleted,
                "freed_bytes": result.freed_bytes,
            },
            "cli_report": (
                f"{'DRY RUN: ' if result.dry_run else ''}"
                f"Kept {result.kept}, deleted {result.deleted}"
            ),
        }
    )


# ===================================================================
# 9. Reporting (3 tools, read-only)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def financial_summary(view: str = "all") -> dict:
    """Financial health dashboard: net worth, cash flow, savings rate, obligations, data health.

    Args:
        view: Filter transactions by use_type ('personal', 'business', 'all').

    Returns:
        Dict with balances, cash flow, risk metrics, obligations, and data health.

    Examples:
        financial_summary()
        financial_summary(view="personal")
    """
    return _call(summary_cmd.handle_summary, {"view": view})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def income_mix(months: int = 3) -> dict:
    """Income source mix over complete calendar months.

    Args:
        months: Number of complete calendar months to include.

    Returns:
        Dict with per-source totals, monthly source totals, and top-source share.

    Examples:
        income_mix()
        income_mix(months=6)
    """
    months = int(months)
    if months < 1:
        raise ValueError("months must be >= 1")

    as_of = date.today()
    month_labels = _complete_month_labels(as_of, months)
    with _get_conn() as conn:
        rows = income_by_stream(conn, months=months, as_of=as_of)

    per_source: dict[str, dict[str, int]] = {}
    total_income_cents = 0
    for row in rows:
        month = str(row["month"])
        if month not in month_labels:
            continue
        source = str(row["stream"])
        cents = int(row["total_cents"])
        source_months = per_source.setdefault(source, {label: 0 for label in month_labels})
        source_months[month] += cents
        total_income_cents += cents

    sources: list[dict[str, Any]] = []
    for source, source_months in per_source.items():
        total_cents = sum(int(source_months[label]) for label in month_labels)
        share_pct = bounded_whole_percent(total_cents, total_income_cents)
        sources.append(
            {
                "name": source,
                "total_cents": total_cents,
                "total": total_cents / 100,
                "monthly_avg_cents": int(
                    (Decimal(total_cents) / Decimal(months)).quantize(
                        Decimal("1"), rounding=ROUND_HALF_UP
                    )
                ),
                "share_pct": share_pct,
                "monthly_totals": [
                    {"month": label, "cents": int(source_months[label])}
                    for label in month_labels
                ],
            }
        )
    sources.sort(key=lambda source: (-int(source["total_cents"]), str(source["name"]).casefold()))

    top_source = sources[0] if sources else None
    return {
        "data": {
            "months": months,
            "complete_months": month_labels,
            "total_income_cents": total_income_cents,
            "total_income": total_income_cents / 100,
            "sources": sources,
            "top_source": top_source,
        },
        "summary": {
            "months": months,
            "total_income_cents": total_income_cents,
            "source_count": len(sources),
            "top_source": top_source["name"] if top_source else None,
            "top_share_pct": int(top_source["share_pct"]) if top_source else 0,
        },
        "cli_report": _income_mix_report(
            months=months,
            month_labels=month_labels,
            sources=sources,
            total_income_cents=total_income_cents,
        ),
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def spending_trends(months: int = 6, view: str = "all", categories: list[str] | None = None) -> dict:
    """Monthly spending trends by category with trend indicators.

    Args:
        months: Number of months to include (default 6).
        view: Filter transactions by use_type ('personal', 'business', 'all').
        categories: Optional category names to preload/filter the trends view.

    Returns:
        Dict with per-category monthly spending pivot and trend arrows.

    Examples:
        spending_trends()
        spending_trends(months=3, view="business")
        spending_trends(months=6, view="personal", categories=["Dining", "Travel"])
    """
    return _call(spending_cmd.handle_trends, {"months": months, "view": view, "categories": categories or []})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def liability_obligations(summary_only: bool = True) -> dict:
    """Consolidated view of all fixed monthly obligations: recurring flows, debt minimums, subscriptions.

    Args:
        summary_only: If True (default), return CLI report instead of full obligation lists to reduce payload size.

    Examples:
        liability_obligations()
    """
    with _get_conn() as conn:
        result = liability_cmd.handle_obligations(_ns(), conn)
    if summary_only:
        cache_id = _write_cache_safe("liability_obligations", _result_envelope(result))
        return _summarize_result(result, {"cache_id": cache_id})
    return _result_envelope(result)


# ===================================================================
# 9A. Manual Loans (8 tools)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def loan_list(
    include_inactive: bool = False, limit: int = 100, offset: int = 0
) -> dict:
    """List manual loans with balances and payment status.

    Args:
        include_inactive: Include closed or fully paid manual loans when True.
        limit: Maximum number of loans to return.
        offset: Number of loans to skip before returning results.

    Returns:
        Dict with manual loan rows and summary totals.

    Examples:
        loan_list()
        loan_list(include_inactive=True)
    """
    return _call(
        loan_cmd.handle_list,
        {"include_inactive": include_inactive, "limit": limit, "offset": offset},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def loan_show(loan_id: str) -> dict:
    """Show full details for one manual loan: terms, disbursements, payments, and events.

    Args:
        loan_id: Manual loan ID.

    Returns:
        Dict with the loan record plus disbursement history, payment history, and event log.

    Examples:
        loan_show(loan_id="abc123")

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(loan_cmd.handle_show, {"loan_id": loan_id})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def loan_schedule(loan_id: str, months: int = 0, summary_only: bool = True) -> dict:
    """Project the repayment schedule for a manual loan.

    Args:
        loan_id: Manual loan ID.
        months: Projection horizon in months; 0 means auto-project until payoff or the internal cap.
        summary_only: If True (default), return a compact schedule for long projections.

    Returns:
        Dict with projected payment rows, payoff summary, and warnings.

    Examples:
        loan_schedule(loan_id="abc123")
        loan_schedule(loan_id="abc123", months=24, summary_only=False)

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(
        loan_cmd.handle_schedule,
        {
            "loan_id": loan_id,
            "months": months,
            "summary_only": summary_only,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def loan_add(
    creditor: str,
    amount: float,
    start_date: str,
    rate: float = 0.0,
    interest_type: Optional[str] = None,
    monthly_payment: Optional[float] = None,
    due_day: Optional[int] = None,
    expected_payoff: Optional[str] = None,
    use_type: str = "Personal",
    description: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Create a new manual loan or informal liability.

    Args:
        creditor: Creditor name.
        amount: Initial disbursement amount in dollars.
        start_date: Loan start date (YYYY-MM-DD).
        rate: Annual interest rate percentage.
        interest_type: Interest model: 'none', 'simple', or 'compound'.
        monthly_payment: Optional agreed monthly payment in dollars.
        due_day: Optional payment due day of month (1-31).
        expected_payoff: Optional target payoff date (YYYY-MM-DD).
        use_type: Loan scope: 'Personal' or 'Business'.
        description: Optional free-text notes.

    Returns:
        Dict with the created loan and initial disbursement details.

    Examples:
        loan_add(creditor="Mom", amount=5000, start_date="2026-01-01")
        loan_add(
            creditor="Partner",
            amount=12000,
            start_date="2026-01-01",
            monthly_payment=500,
            due_day=15,
        )
    """
    return _call(
        loan_cmd.handle_add,
        {
            "creditor": creditor,
            "amount": amount,
            "start_date": start_date,
            "rate": rate,
            "interest_type": interest_type,
            "monthly_payment": monthly_payment,
            "due_day": due_day,
            "expected_payoff": expected_payoff,
            "use_type": use_type,
            "description": description,
            "idempotency_key": idempotency_key,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def loan_payment(
    loan_id: str,
    amount: float,
    date: Optional[str] = None,
    transaction_id: Optional[str] = None,
    notes: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Record a repayment on a manual loan.

    Args:
        loan_id: Manual loan ID.
        amount: Payment amount in dollars.
        date: Optional payment date (YYYY-MM-DD). Defaults to today or the linked transaction date.
        transaction_id: Optional transaction ID to link as the repayment source.
        notes: Optional payment notes.

    Returns:
        Dict with the recorded payment and updated loan summary.

    Examples:
        loan_payment(loan_id="abc123", amount=250)
        loan_payment(loan_id="abc123", amount=250, transaction_id="txn456")

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(
        loan_cmd.handle_payment,
        {
            "loan_id": loan_id,
            "amount": amount,
            "date": date,
            "transaction_id": transaction_id,
            "notes": notes,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def loan_disburse(
    loan_id: str,
    amount: float,
    date: Optional[str] = None,
    notes: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Record an additional disbursement on a manual loan.

    Args:
        loan_id: Manual loan ID.
        amount: Additional borrowed amount in dollars.
        date: Optional disbursement date (YYYY-MM-DD). Defaults to today.
        notes: Optional disbursement notes.

    Returns:
        Dict with the new disbursement and updated loan balance summary.

    Examples:
        loan_disburse(loan_id="abc123", amount=500)
        loan_disburse(loan_id="abc123", amount=500, date="2026-02-01")

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(
        loan_cmd.handle_disburse,
        {
            "loan_id": loan_id,
            "amount": amount,
            "date": date,
            "notes": notes,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def loan_adjust(
    loan_id: str,
    rate: Optional[float] = None,
    interest_type: Optional[str] = None,
    monthly_payment: Optional[float] = None,
    due_day: Optional[int] = None,
    expected_payoff: Optional[str] = None,
    balance: Optional[float] = None,
    description: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Update terms on a manual loan, including rate, payment amount, and balance corrections.

    Args:
        loan_id: Manual loan ID.
        rate: Optional updated annual interest rate percentage.
        interest_type: Optional updated interest model: 'none', 'simple', or 'compound'.
        monthly_payment: Optional updated monthly payment in dollars.
        due_day: Optional updated payment due day of month (1-31).
        expected_payoff: Optional updated target payoff date (YYYY-MM-DD).
        balance: Optional balance correction in dollars.
        description: Optional updated description.

    Returns:
        Dict with the updated loan snapshot and adjustment metadata.

    Examples:
        loan_adjust(loan_id="abc123", monthly_payment=400)
        loan_adjust(loan_id="abc123", rate=4.5, interest_type="simple")

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(
        loan_cmd.handle_adjust,
        {
            "loan_id": loan_id,
            "rate": rate,
            "interest_type": interest_type,
            "monthly_payment": monthly_payment,
            "due_day": due_day,
            "expected_payoff": expected_payoff,
            "balance": balance,
            "description": description,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="db_write", approval_required=True)
def loan_close(loan_id: str, forgiven: bool = False, dry_run: bool = False) -> dict:
    """Close a manual loan, optionally forgiving any remaining balance.

    Args:
        loan_id: Manual loan ID.
        forgiven: When True, forgive the remaining balance before closing.

    Returns:
        Dict with closure status and forgiveness metadata.

    Examples:
        loan_close(loan_id="abc123")
        loan_close(loan_id="abc123", forgiven=True)

    Discovery: use loan_list to choose loan_id before reading schedules or recording loan changes.
    """
    return _call(
        loan_cmd.handle_close,
        {"loan_id": loan_id, "forgiven": forgiven, "dry_run": dry_run},
    )


# ===================================================================
# 10. Planning & Goals (4 tools)
# ===================================================================


@mcp.tool(sync_behavior="no_sync", read_only=True)
def net_worth_projection(months: int = 12) -> dict:
    """Project net worth forward using current trends: income, expenses, debt paydown, investment growth.

    Args:
        months: Projection horizon in months (default 12).

    Returns:
        Dict with current balances, monthly averages, and milestone projections.

    Examples:
        net_worth_projection()
        net_worth_projection(months=24)
    """
    return _call(projection_cmd.handle_projection, {"months": months})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def goal_set(
    name: str,
    target: float,
    metric: str = "net_worth",
    direction: str = "up",
    deadline: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """Set or update a financial goal.

    Args:
        name: Goal name (unique; re-using a name updates the existing goal).
        target: Target value in dollars (or percentage for savings_rate).
        metric: One of 'net_worth', 'liquid_cash', 'total_debt', 'investments', 'savings_rate'.
        direction: 'up' (target above current) or 'down' (target below current, e.g. debt).
        deadline: Optional ISO date deadline (YYYY-MM-DD).

    Returns:
        Dict with created/updated goal details.

    Examples:
        goal_set(name="Emergency Fund", target=25000, metric="liquid_cash")
        goal_set(name="Debt Free", target=0, metric="total_debt", direction="down")

    Discovery: use goal_list or goal_find to choose or confirm the goal name before updating goals.
    """
    return _call(
        goal_cmd.handle_set,
        {
            "name": name,
            "target": target,
            "metric": metric,
            "direction": direction,
            "deadline": deadline,
            "dry_run": dry_run,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def goal_list() -> dict:
    """List all active financial goals.

    Returns:
        Dict with list of active goals and their current values.

    Examples:
        goal_list()
    """
    return _call(goal_cmd.handle_list, {})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def goal_find(name: str, include_inactive: bool = False) -> dict:
    """Find a goal row by exact name. Returns the row whether active or
    inactive when include_inactive=True; otherwise filters to is_active=1.

    Existence of an inactive row matters for collision handling: ``goal_set``
    uses INSERT OR REPLACE keyed on ``name`` and silently reactivates an
    inactive row. Callers that need to detect that case (e.g., the savings-goal
    skill's Phase 7 collision check) must pass ``include_inactive=True``.

    Also serves as the post-write recovery path for ``goal_set``: since
    ``goal_set`` does not return ``id`` in its response shape, callers call
    ``goal_find(name)`` after a successful set to recover the ``id`` and the
    fresh ``updated_at`` for race-safety verification.

    Args:
        name: Exact goal name to match.
        include_inactive: When True, also return rows where ``is_active = 0``.

    Returns:
        Dict shaped ``{data: {goal: {id, name, metric, target_cents, deadline,
        is_active, created_at, updated_at, ...} | null}, summary: {found: bool,
        name: str, is_active: bool | null}}``. Missing match returns ``data.goal
        = None`` and ``summary.found = False`` (does NOT raise).

    Examples:
        goal_find(name="Emergency Fund")
        goal_find(name="down-payment-2027", include_inactive=True)

    Discovery: use goal_list or goal_find to choose or confirm the goal name before updating goals.
    """
    return _call(
        goal_cmd.handle_find,
        {"name": name, "include_inactive": include_inactive},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def goal_status() -> dict:
    """Show progress on all active goals with progress bars and time estimates.

    Returns:
        Dict with per-goal progress percentage, current value, and estimated months to target.

    Examples:
        goal_status()
    """
    return _call(goal_cmd.handle_status, {})


# === Observability Tools ===

_ONBOARDING_STEPS: tuple[tuple[str, str], ...] = (
    ("onboarding.wizard", "wizard"),
    ("onboarding.plaid_link", "plaid_link"),
    ("onboarding.csv_import", "csv_import"),
    ("onboarding.first_categorization", "first_categorization"),
    ("onboarding.profile_captured", "profile_captured"),
    ("onboarding.focus_selected", "focus_selected"),
    ("onboarding.setup_acknowledged", "setup_acknowledged"),
    ("onboarding.complete", "complete"),
)
_VALID_SEVERITIES = {"bug", "warning", "suggestion"}
_VALID_TRIAGE_STATUSES = {"open", "investigating", "resolved", "wontfix"}
_VALID_ERROR_SEVERITIES = {"critical", "error", "warning"}
_VALID_COST_PROVIDERS = {"claude", "openai", "plaid", "all"}
_VALID_COST_PERIODS = {"daily", "monthly"}
_VALID_COST_ACTIONS = {"warn", "block"}
_ERROR_TIMELINE_LIMIT = 100
_MICRODOLLARS_PER_DOLLAR = 1_000_000


def _coerce_int(value: Any, *, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(parsed, minimum)


def _days_modifier(days: Any, *, default: int) -> str:
    return f"-{_coerce_int(days, default=default, minimum=1)} days"


def _months_modifier(months: Any, *, default: int) -> str:
    normalized = _coerce_int(months, default=default, minimum=1)
    return f"-{max(normalized - 1, 0)} months"


def _pct(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def _round_or_none(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _usd_from_usd6(value: Any) -> float:
    return round(_coerce_int(value, default=0, minimum=0) / _MICRODOLLARS_PER_DOLLAR, 6)


def _safe_json_load(raw_value: Any) -> Any:
    if raw_value in (None, ""):
        return None
    try:
        return json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return raw_value


def _enable_query_only(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA query_only = ON")
    except sqlite3.DatabaseError:
        pass


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _query_perf_percentiles(
    conn: sqlite3.Connection,
    *,
    days: int,
    group_sql: str,
    where_clauses: tuple[str, ...] = (),
    params: tuple[Any, ...] = (),
    order_by: str = "p95_ms DESC, avg_ms DESC, group_key",
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where_sql = " AND ".join(("created_at >= datetime('now', ?)",) + where_clauses)
    limit_sql = f"LIMIT {max(int(limit), 1)}" if limit is not None else ""
    sql = f"""
        WITH filtered AS (
            SELECT
                {group_sql} AS group_key,
                value_ms,
                is_error
            FROM perf_samples
            WHERE {where_sql}
        ),
        ranked AS (
            SELECT
                group_key,
                value_ms,
                is_error,
                ROW_NUMBER() OVER (PARTITION BY group_key ORDER BY value_ms) AS rn,
                COUNT(*) OVER (PARTITION BY group_key) AS cnt
            FROM filtered
            WHERE group_key IS NOT NULL
              AND TRIM(group_key) != ''
        ),
        scored AS (
            SELECT
                group_key,
                value_ms,
                is_error,
                rn,
                cnt,
                CAST((((cnt * 50) + 99) / 100) AS INTEGER) AS p50_rank,
                CAST((((cnt * 95) + 99) / 100) AS INTEGER) AS p95_rank,
                CAST((((cnt * 99) + 99) / 100) AS INTEGER) AS p99_rank
            FROM ranked
        )
        SELECT
            group_key,
            MAX(cnt) AS sample_count,
            ROUND(AVG(value_ms), 2) AS avg_ms,
            MIN(CASE WHEN rn >= p50_rank THEN value_ms END) AS p50_ms,
            MIN(CASE WHEN rn >= p95_rank THEN value_ms END) AS p95_ms,
            MIN(CASE WHEN rn >= p99_rank THEN value_ms END) AS p99_ms,
            MAX(value_ms) AS max_ms,
            ROUND(AVG(CASE WHEN is_error = 1 THEN 1.0 ELSE 0.0 END) * 100.0, 2) AS error_rate_pct
        FROM scored
        GROUP BY group_key
        ORDER BY {order_by}
        {limit_sql}
    """
    rows = conn.execute(sql, (_days_modifier(days, default=7), *params)).fetchall()
    return [dict(row) for row in rows]


def _current_cost_spend_usd6(
    conn: sqlite3.Connection, provider: str, period: str
) -> int:
    params: list[Any] = []
    where_parts = []
    if provider != "all":
        where_parts.append("provider = ?")
        params.append(provider)
    if period == "daily":
        where_parts.append("created_at >= datetime('now', 'start of day')")
    else:
        where_parts.append("strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')")
    where_sql = " AND ".join(where_parts)
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM(cost_usd6), 0) AS spent_usd6
        FROM cost_ledger
        WHERE {where_sql}
        """,
        tuple(params),
    ).fetchone()
    return _coerce_int(
        row["spent_usd6"] if row is not None else 0, default=0, minimum=0
    )


def _cost_limit_payload(
    conn: sqlite3.Connection, row: sqlite3.Row | dict[str, Any]
) -> dict[str, Any]:
    provider = str(row["provider"])
    period = str(row["period"])
    row_keys = row.keys()
    raw_limit_usd6 = row["limit_usd6"]
    raw_system_limit_usd6 = (
        row["system_limit_usd6"] if "system_limit_usd6" in row_keys else None
    )
    limit_usd6 = (
        None
        if raw_limit_usd6 is None
        else _coerce_int(raw_limit_usd6, default=0, minimum=0)
    )
    system_limit_usd6 = (
        None
        if raw_system_limit_usd6 is None
        else _coerce_int(raw_system_limit_usd6, default=0, minimum=0)
    )
    effective_candidates = [
        value for value in (limit_usd6, system_limit_usd6) if value is not None
    ]
    effective_usd6 = min(effective_candidates) if effective_candidates else None
    spent_usd6 = _current_cost_spend_usd6(conn, provider, period)
    pct_used = _pct(spent_usd6, effective_usd6) if effective_usd6 is not None else None
    remaining_usd6 = (
        max(effective_usd6 - spent_usd6, 0) if effective_usd6 is not None else None
    )
    return {
        "provider": provider,
        "period": period,
        "limit_usd6": limit_usd6,
        "system_limit_usd6": system_limit_usd6,
        "effective_usd6": effective_usd6,
        "limit_usd": _usd_from_usd6(effective_usd6)
        if effective_usd6 is not None
        else None,
        "system_limit_usd": _usd_from_usd6(system_limit_usd6)
        if system_limit_usd6 is not None
        else None,
        "effective_usd": _usd_from_usd6(effective_usd6)
        if effective_usd6 is not None
        else None,
        "action": str(row["action"]),
        "is_active": int(row["is_active"]) if "is_active" in row.keys() else 1,
        "spent_usd6": spent_usd6,
        "spent_usd": _usd_from_usd6(spent_usd6),
        "remaining_usd6": remaining_usd6,
        "remaining_usd": _usd_from_usd6(remaining_usd6)
        if remaining_usd6 is not None
        else None,
        "pct_used": pct_used,
    }


def _parse_price_points(raw_value: str) -> list[int]:
    points: list[int] = []
    for piece in str(raw_value or "").split(","):
        part = piece.strip()
        if not part:
            continue
        try:
            point = int(part)
        except ValueError as exc:
            raise ValueError(
                f"Invalid price point '{part}'. Use comma-separated integers."
            ) from exc
        if point <= 0:
            raise ValueError("Price points must be positive integers.")
        if point not in points:
            points.append(point)
    if not points:
        raise ValueError("At least one valid price point is required.")
    return points


def _cost_economics_data_root() -> Path:
    raw_root = os.getenv("FINANCE_WEB_DATA_ROOT") or os.getenv(
        "FINANCE_GATEWAY_DATA_ROOT"
    )
    if raw_root:
        return Path(raw_root).expanduser().resolve()
    return (
        Path(__file__).resolve().parents[1] / "finance-web" / "data" / "users"
    ).resolve()


def _plan_configs() -> dict[str, Any]:
    from finance_cli.billing import PLAN_CONFIGS

    return PLAN_CONFIGS


def _ops_postgres_connect(database_url: str):
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError("psycopg2 is required for cost_unit_economics") from exc
    return psycopg2.connect(database_url, cursor_factory=psycopg2.extras.RealDictCursor)


def _cost_period_bounds(period_days: int) -> tuple[date, date]:
    end_exclusive = datetime.now(timezone.utc).date() + timedelta(days=1)
    start = end_exclusive - timedelta(days=period_days)
    return start, end_exclusive


def _fetch_ops_cost_rollup_rows(
    *,
    database_url: str,
    start_date: date,
    end_exclusive: date,
) -> list[dict[str, Any]]:
    with _ops_postgres_connect(database_url) as pg_conn:
        cursor = pg_conn.cursor()
        cursor.execute(
            """
            SELECT
                date::text AS date,
                user_hash,
                provider,
                tier,
                plan_code,
                COALESCE(SUM(total_usd6), 0) AS total_usd6,
                COALESCE(SUM(request_count), 0) AS request_count
              FROM ops_cost_rollups
             WHERE date >= %s
               AND date < %s
               AND provider IN ('claude', 'openai')
             GROUP BY date, user_hash, provider, tier, plan_code
             ORDER BY date, plan_code, provider, user_hash
            """,
            (start_date, end_exclusive),
        )
        return [dict(row) for row in cursor.fetchall()]


@dataclass(frozen=True)
class _RollupPlanClassification:
    plan_code: str | None
    source: str


@dataclass(frozen=True)
class _RollupUserRef:
    user_id: str
    storage_mode: str


def _classify_rollup_plan(row: dict[str, Any]) -> _RollupPlanClassification:
    plan_configs = _plan_configs()
    plan_code = str(row.get("plan_code") or "").strip().lower()
    if plan_code in plan_configs:
        return _RollupPlanClassification(plan_code=plan_code, source="plan_code")
    if plan_code:
        return _RollupPlanClassification(plan_code=None, source="invalid_plan_code")

    tier = str(row.get("tier") or "").strip().lower()
    if tier == "lifetime":
        return _RollupPlanClassification(
            plan_code="lifetime",
            source="tier_lifetime_fallback",
        )
    if tier == "trial":
        return _RollupPlanClassification(
            plan_code="standard",
            source="tier_trial_fallback",
        )
    return _RollupPlanClassification(plan_code=None, source="missing_plan_code")


def _nearest_rank(values: list[int], percentile: int) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, ((len(ordered) * percentile) + 99) // 100)
    return ordered[min(rank - 1, len(ordered) - 1)]


def _cost_percentiles(values: list[int]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for percentile in (50, 85, 95, 99):
        usd6 = _nearest_rank(values, percentile)
        key = f"p{percentile}"
        payload[key] = _usd_from_usd6(usd6)
        payload[f"{key}_usd6"] = usd6
    return payload


def _period_sqlite_ts(value: date) -> str:
    return datetime.combine(value, datetime_time.min, tzinfo=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _default_cost_economics_session_manager():
    if not str(os.getenv("DATABASE_URL") or "").strip():
        return None
    try:
        from app_platform.db.session import SessionManager
    except Exception:
        return None
    try:
        return SessionManager._get_default_manager()
    except Exception:
        return None


def _fetch_rollup_user_refs(
    *,
    database_url: str,
    known_user_hashes: set[str],
) -> dict[str, _RollupUserRef]:
    if not database_url or not known_user_hashes:
        return {}
    refs: dict[str, _RollupUserRef] = {}
    try:
        with _ops_postgres_connect(database_url) as pg_conn:
            cursor = pg_conn.cursor()
            cursor.execute(
                """
                SELECT id, storage_mode
                  FROM users
                 WHERE deleted_at IS NULL
                """
            )
            rows = cursor.fetchall()
    except Exception:
        return {}

    for row in rows:
        user_id = str(row.get("id") if isinstance(row, dict) else row["id"])
        user_hash_value = hashlib.sha256(user_id.encode("utf-8")).hexdigest()
        if user_hash_value not in known_user_hashes:
            continue
        storage_mode = str(
            row.get("storage_mode") if isinstance(row, dict) else row["storage_mode"]
        )
        refs[user_hash_value] = _RollupUserRef(
            user_id=user_id,
            storage_mode=storage_mode.strip().lower() or "local",
        )
    return refs


def _user_has_credit_topup(
    *,
    data_root: Path,
    user_id: str,
    start_ts: str,
    end_ts: str,
    storage_mode: str | None = None,
    storage_session_manager=None,
) -> bool:
    from finance_cli.user_provisioning import user_db_path

    db_path = user_db_path(data_root, user_id)
    normalized_mode = str(storage_mode or "local").strip().lower() or "local"
    lease_metadata = {"source": "cost_unit_economics"}

    def has_topup(*, session_manager=None) -> bool:
        with connect(
            db_path=db_path,
            expected_user_id=user_id,
            storage_session_manager=session_manager,
            busy_timeout=5000,
        ) as user_conn:
            _enable_query_only(user_conn)
            if not _table_exists(user_conn, "credit_ledger"):
                return False
            row = user_conn.execute(
                """
                SELECT 1
                  FROM credit_ledger
                 WHERE source = 'topup'
                   AND datetime(created_at) >= datetime(?)
                   AND datetime(created_at) < datetime(?)
                 LIMIT 1
                """,
                (start_ts, end_ts),
            ).fetchone()
            return row is not None

    if normalized_mode == "local" and not db_path.exists():
        return False
    if normalized_mode in {"remote", "migrating", "replaying"}:
        manager = storage_session_manager
        if manager is None:
            return False
        try:
            with LeaseScope.acquire(
                user_id,
                session_manager=manager,
                operation="cost_unit_economics_credit_scan",
                metadata=lease_metadata,
            ) as scope:
                if isinstance(scope, Queued):
                    return False
                return has_topup(session_manager=manager)
        except LeaseUnavailableError:
            return False
        except Exception:
            return False

    try:
        with LeaseScope(
            user_id=user_id,
            lease=LocalLease("cost_unit_economics_credit_scan_local"),
            session_manager=None,
            owns_lease=False,
        ):
            return has_topup(session_manager=storage_session_manager)
    except Exception:
        return False


def _scan_credit_topup_user_hashes(
    *,
    data_root: Path,
    start_date: date,
    end_exclusive: date,
    known_user_hashes: set[str],
    database_url: str = "",
) -> set[str]:
    if not known_user_hashes:
        return set()

    start_ts = _period_sqlite_ts(start_date)
    end_ts = _period_sqlite_ts(end_exclusive)
    topup_hashes: set[str] = set()
    checked_hashes: set[str] = set()
    if data_root.exists():
        for child in sorted(data_root.iterdir()):
            if child.name.startswith(".") or not child.is_dir():
                continue
            current_hash = hashlib.sha256(child.name.encode("utf-8")).hexdigest()
            if current_hash not in known_user_hashes:
                continue
            checked_hashes.add(current_hash)
            if _user_has_credit_topup(
                data_root=data_root,
                user_id=child.name,
                start_ts=start_ts,
                end_ts=end_ts,
            ):
                topup_hashes.add(current_hash)

    remaining_hashes = known_user_hashes - checked_hashes
    if remaining_hashes and database_url:
        user_refs = _fetch_rollup_user_refs(
            database_url=database_url,
            known_user_hashes=remaining_hashes,
        )
        storage_session_manager = _default_cost_economics_session_manager()
        for user_hash_value, user_ref in sorted(user_refs.items()):
            if _user_has_credit_topup(
                data_root=data_root,
                user_id=user_ref.user_id,
                start_ts=start_ts,
                end_ts=end_ts,
                storage_mode=user_ref.storage_mode,
                storage_session_manager=storage_session_manager,
            ):
                topup_hashes.add(user_hash_value)
    return topup_hashes


def _build_cost_unit_economics_payload(
    *,
    rows: list[dict[str, Any]],
    period_days: int,
    price_points: list[int],
    data_root: Path,
    start_date: date,
    end_exclusive: date,
    database_url: str = "",
) -> dict[str, Any]:
    plan_configs = _plan_configs()
    user_totals: dict[str, dict[str, int]] = {code: {} for code in plan_configs}
    provider_user_totals: dict[str, dict[str, dict[str, int]]] = {
        code: {} for code in plan_configs
    }
    monthly_totals: dict[tuple[str, str, str], int] = {}
    request_counts: dict[str, int] = {code: 0 for code in plan_configs}
    known_user_hashes: set[str] = set()
    attribution_sources: dict[str, int] = {}
    excluded_reasons: dict[str, int] = {}
    included_row_count = 0
    excluded_row_count = 0
    excluded_total_usd6 = 0
    excluded_request_count = 0

    for row in rows:
        provider = str(row.get("provider") or "").strip().lower()
        if provider not in {"claude", "openai"}:
            continue
        user_hash_value = str(row.get("user_hash") or "").strip()
        if not user_hash_value:
            continue
        classification = _classify_rollup_plan(row)
        total_usd6 = _coerce_int(row.get("total_usd6"), default=0, minimum=0)
        request_count = _coerce_int(row.get("request_count"), default=0, minimum=0)
        attribution_sources[classification.source] = (
            attribution_sources.get(classification.source, 0) + 1
        )
        if classification.plan_code is None:
            excluded_row_count += 1
            excluded_total_usd6 += total_usd6
            excluded_request_count += request_count
            excluded_reasons[classification.source] = (
                excluded_reasons.get(classification.source, 0) + 1
            )
            continue

        plan_code = classification.plan_code
        day = str(row.get("date") or "")[:10]
        month = day[:7] if len(day) >= 7 else ""

        included_row_count += 1
        known_user_hashes.add(user_hash_value)
        user_totals.setdefault(plan_code, {})
        user_totals[plan_code][user_hash_value] = (
            user_totals[plan_code].get(user_hash_value, 0) + total_usd6
        )
        provider_totals = provider_user_totals.setdefault(plan_code, {}).setdefault(
            provider, {}
        )
        provider_totals[user_hash_value] = (
            provider_totals.get(user_hash_value, 0) + total_usd6
        )
        request_counts[plan_code] = request_counts.get(plan_code, 0) + request_count
        if month:
            key = (plan_code, user_hash_value, month)
            monthly_totals[key] = monthly_totals.get(key, 0) + total_usd6

    credit_topup_hashes = _scan_credit_topup_user_hashes(
        data_root=data_root,
        start_date=start_date,
        end_exclusive=end_exclusive,
        known_user_hashes=known_user_hashes,
        database_url=database_url,
    )

    cap_hits: dict[str, set[str]] = {code: set() for code in plan_configs}
    for (plan_code, user_hash_value, _month), total_usd6 in monthly_totals.items():
        plan = plan_configs.get(plan_code)
        if plan is not None and total_usd6 > plan.monthly_cap_usd6:
            cap_hits.setdefault(plan_code, set()).add(user_hash_value)

    plans: dict[str, dict[str, Any]] = {}
    for plan_code in plan_configs:
        totals_by_user = user_totals.get(plan_code, {})
        values = list(totals_by_user.values())
        total_users = len(totals_by_user)
        credit_users = len(set(totals_by_user) & credit_topup_hashes)
        total_usd6 = sum(values)
        provider_payload: dict[str, dict[str, Any]] = {}
        for provider, provider_totals in sorted(
            provider_user_totals.get(plan_code, {}).items()
        ):
            provider_values = list(provider_totals.values())
            provider_total_usd6 = sum(provider_values)
            provider_payload[provider] = {
                "total_users": len(provider_totals),
                "total_usd6": provider_total_usd6,
                "total_usd": _usd_from_usd6(provider_total_usd6),
                **_cost_percentiles(provider_values),
            }
        plans[plan_code] = {
            "total_users": total_users,
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
            "request_count": request_counts.get(plan_code, 0),
            "cap_hit_count": len(cap_hits.get(plan_code, set())),
            "credit_purchase_user_count": credit_users,
            "credit_purchase_rate_pct": _pct(credit_users, total_users) or 0.0,
            "by_provider": provider_payload,
            **_cost_percentiles(values),
        }

    available = included_row_count > 0
    return {
        "available": available,
        "reason": None if available else "no_attributed_plan_data",
        "period_days": period_days,
        "start_date": start_date.isoformat(),
        "end_exclusive": end_exclusive.isoformat(),
        "price_points": price_points,
        "plans": plans,
        "credit_purchase_design": "per_user_sqlite_scan",
        "rollup_plan_quality": {
            "included_row_count": included_row_count,
            "excluded_row_count": excluded_row_count,
            "excluded_total_usd6": excluded_total_usd6,
            "excluded_total_usd": _usd_from_usd6(excluded_total_usd6),
            "excluded_request_count": excluded_request_count,
            "attribution_sources": dict(sorted(attribution_sources.items())),
            "excluded_reasons": dict(sorted(excluded_reasons.items())),
        },
    }


def _empty_rollup_plan_quality() -> dict[str, Any]:
    return {
        "included_row_count": 0,
        "excluded_row_count": 0,
        "excluded_total_usd6": 0,
        "excluded_total_usd": _usd_from_usd6(0),
        "excluded_request_count": 0,
        "attribution_sources": {},
        "excluded_reasons": {},
    }


def _insert_issue(
    conn: sqlite3.Connection, title: str, description: str, severity: str
) -> dict:
    """Insert an issue report row into the user database."""
    severity = severity.strip().lower()
    if severity not in _VALID_SEVERITIES:
        return {
            "error": (
                f"Invalid severity '{severity}'. Must be one of: "
                f"{', '.join(sorted(_VALID_SEVERITIES))}"
            )
        }

    title = title.strip()
    if not title:
        return {"error": "Title must not be empty."}

    description = description.strip()
    if not description:
        return {"error": "Description must not be empty."}

    issue_id = uuid.uuid4().hex[:12]
    severity_label = severity.capitalize()
    with conn:
        conn.execute(
            """
            INSERT INTO issue_reports (
                id,
                title,
                description,
                severity,
                status
            )
            VALUES (?, ?, ?, ?, 'open')
            """,
            (issue_id, title, description, severity),
        )
        row = conn.execute(
            "SELECT created_at FROM issue_reports WHERE id = ?",
            (issue_id,),
        ).fetchone()

    logged = datetime.now().strftime("%Y-%m-%d")
    if row is not None and row["created_at"]:
        logged = str(row["created_at"]).split(" ", 1)[0]
    return {
        "id": issue_id,
        "title": title,
        "severity": severity_label,
        "status": "open",
        "logged": logged,
    }


def _handle_analytics_funnel(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=30, minimum=1)
    _enable_query_only(conn)
    rows = conn.execute(
        """
        SELECT
            event,
            SUM(CASE WHEN outcome = 'started' THEN 1 ELSE 0 END) AS started,
            SUM(CASE WHEN outcome = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) AS failed
        FROM analytics_events
        WHERE event LIKE 'onboarding.%'
          AND created_at >= datetime('now', ?)
        GROUP BY event
        """,
        (_days_modifier(days, default=30),),
    ).fetchall()
    by_event = {str(row["event"]): row for row in rows}

    steps: list[dict[str, Any]] = []
    for event_name, step_name in _ONBOARDING_STEPS:
        row = by_event.get(event_name)
        started = _coerce_int(
            row["started"] if row is not None else 0, default=0, minimum=0
        )
        succeeded = _coerce_int(
            row["succeeded"] if row is not None else 0, default=0, minimum=0
        )
        failed = _coerce_int(
            row["failed"] if row is not None else 0, default=0, minimum=0
        )
        abandoned = max(started - succeeded - failed, 0)
        steps.append(
            {
                "step": step_name,
                "event": event_name,
                "started": started,
                "succeeded": succeeded,
                "failed": failed,
                "abandoned": abandoned,
                "success_pct": _pct(succeeded, started),
            }
        )

    started_count = steps[0]["started"] if steps else 0
    completed_count = next(
        (step["succeeded"] for step in steps if step["event"] == "onboarding.complete"),
        0,
    )
    completion_pct = _pct(completed_count, started_count)
    cli_report = (
        f"Onboarding completion: {completed_count}/{started_count} "
        f"({completion_pct or 0:.2f}%) over {days}d"
        if started_count > 0
        else f"No onboarding starts recorded in the last {days}d"
    )
    return {
        "data": {
            "days": days,
            "steps": steps,
            "started": started_count,
            "completed": completed_count,
            "completion_pct": completion_pct,
        },
        "summary": {
            "started": started_count,
            "completed": completed_count,
            "completion_pct": completion_pct,
            "step_count": len(steps),
        },
        "cli_report": cli_report,
    }


def _handle_analytics_usage(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=30, minimum=1)
    _enable_query_only(conn)
    params = (_days_modifier(days, default=30),)

    domain_daily_rows = conn.execute(
        """
        SELECT
            date(created_at) AS day,
            domain,
            COUNT(*) AS event_count
        FROM analytics_events
        WHERE created_at >= datetime('now', ?)
        GROUP BY day, domain
        ORDER BY day DESC, domain
        """,
        params,
    ).fetchall()
    domain_totals_rows = conn.execute(
        """
        SELECT
            domain,
            COUNT(*) AS event_count
        FROM analytics_events
        WHERE created_at >= datetime('now', ?)
        GROUP BY domain
        ORDER BY event_count DESC, domain
        """,
        params,
    ).fetchall()
    active_days_row = conn.execute(
        """
        SELECT COUNT(DISTINCT date(created_at)) AS active_days
        FROM analytics_events
        WHERE created_at >= datetime('now', ?)
        """,
        params,
    ).fetchone()
    feature_rows = conn.execute(
        """
        SELECT
            event,
            COUNT(*) AS event_count,
            COUNT(DISTINCT date(created_at)) AS days_used
        FROM analytics_events
        WHERE event LIKE 'feature.%'
          AND created_at >= datetime('now', ?)
        GROUP BY event
        ORDER BY event_count DESC, event
        """,
        params,
    ).fetchall()

    active_days = _coerce_int(
        active_days_row["active_days"] if active_days_row is not None else 0,
        default=0,
        minimum=0,
    )
    total_events = sum(
        _coerce_int(row["event_count"], default=0, minimum=0)
        for row in domain_totals_rows
    )
    feature_adoption = [
        {
            "event": str(row["event"]),
            "event_count": _coerce_int(row["event_count"], default=0, minimum=0),
            "days_used": _coerce_int(row["days_used"], default=0, minimum=0),
            "active_days": active_days,
            "adoption_pct": _pct(
                _coerce_int(row["days_used"], default=0, minimum=0),
                active_days,
            ),
        }
        for row in feature_rows
    ]
    cli_report = f"{total_events} analytics events across {active_days} active day(s) in the last {days}d"
    return {
        "data": {
            "days": days,
            "active_days": active_days,
            "total_events": total_events,
            "domain_totals": [dict(row) for row in domain_totals_rows],
            "daily_domain_counts": [dict(row) for row in domain_daily_rows],
            "feature_adoption": feature_adoption,
        },
        "summary": {
            "total_events": total_events,
            "active_days": active_days,
            "feature_count": len(feature_adoption),
            "domains": len(domain_totals_rows),
        },
        "cli_report": cli_report,
    }


def _handle_analytics_session_stats(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=30, minimum=1)
    _enable_query_only(conn)
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS session_count,
            ROUND(AVG(COALESCE(CAST(json_extract(properties, '$.duration_min') AS REAL), 0)), 2) AS avg_duration_min,
            ROUND(AVG(COALESCE(CAST(json_extract(properties, '$.message_count') AS REAL), 0)), 2) AS avg_messages_per_session,
            ROUND(AVG(COALESCE(CAST(json_extract(properties, '$.tool_call_count') AS REAL), 0)), 2) AS avg_tool_calls_per_session,
            COALESCE(SUM(CAST(json_extract(properties, '$.message_count') AS INTEGER)), 0) AS total_messages,
            COALESCE(SUM(CAST(json_extract(properties, '$.tool_call_count') AS INTEGER)), 0) AS total_tool_calls
        FROM analytics_events
        WHERE event = 'chat.session'
          AND outcome = 'succeeded'
          AND created_at >= datetime('now', ?)
        """,
        (_days_modifier(days, default=30),),
    ).fetchone()
    session_count = _coerce_int(
        row["session_count"] if row is not None else 0, default=0, minimum=0
    )
    avg_duration = _round_or_none(row["avg_duration_min"] if row is not None else None)
    avg_messages = _round_or_none(
        row["avg_messages_per_session"] if row is not None else None
    )
    avg_tool_calls = _round_or_none(
        row["avg_tool_calls_per_session"] if row is not None else None
    )
    total_messages = _coerce_int(
        row["total_messages"] if row is not None else 0, default=0, minimum=0
    )
    total_tool_calls = _coerce_int(
        row["total_tool_calls"] if row is not None else 0, default=0, minimum=0
    )
    cli_report = (
        f"{session_count} sessions over {days}d; avg {avg_messages or 0:.2f} messages, "
        f"{avg_tool_calls or 0:.2f} tool calls, {avg_duration or 0:.2f} min"
    )
    return {
        "data": {
            "days": days,
            "session_count": session_count,
            "avg_duration_min": avg_duration,
            "avg_messages_per_session": avg_messages,
            "avg_tool_calls_per_session": avg_tool_calls,
            "total_messages": total_messages,
            "total_tool_calls": total_tool_calls,
        },
        "summary": {
            "session_count": session_count,
            "avg_duration_min": avg_duration,
            "avg_messages_per_session": avg_messages,
            "avg_tool_calls_per_session": avg_tool_calls,
        },
        "cli_report": cli_report,
    }


def _handle_perf_summary(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=7, minimum=1)
    _enable_query_only(conn)
    metric_rows = _query_perf_percentiles(
        conn,
        days=days,
        group_sql="metric",
        order_by="p95_ms DESC, avg_ms DESC, group_key",
    )
    tool_rows = _query_perf_percentiles(
        conn,
        days=days,
        group_sql="metric",
        where_clauses=("source = 'tool'", "metric LIKE 'tool.%'"),
        order_by="p95_ms DESC, avg_ms DESC, group_key",
        limit=10,
    )
    query_rows = conn.execute(
        """
        SELECT
            COALESCE(json_extract(tags, '$.sql_fingerprint'), metric) AS sql_fingerprint,
            COUNT(*) AS sample_count,
            ROUND(AVG(value_ms), 2) AS avg_ms,
            MAX(value_ms) AS max_ms
        FROM perf_samples
        WHERE source = 'query'
          AND created_at >= datetime('now', ?)
        GROUP BY sql_fingerprint
        HAVING sql_fingerprint IS NOT NULL
           AND TRIM(sql_fingerprint) != ''
        ORDER BY avg_ms DESC, max_ms DESC, sql_fingerprint
        LIMIT 5
        """,
        (_days_modifier(days, default=7),),
    ).fetchall()
    total_samples_row = conn.execute(
        """
        SELECT COUNT(*) AS sample_count
        FROM perf_samples
        WHERE created_at >= datetime('now', ?)
        """,
        (_days_modifier(days, default=7),),
    ).fetchone()
    total_samples = _coerce_int(
        total_samples_row["sample_count"] if total_samples_row is not None else 0,
        default=0,
        minimum=0,
    )
    slowest_tool = (
        tool_rows[0]["group_key"].removeprefix("tool.") if tool_rows else None
    )
    cli_report = f"{total_samples} perf samples across {len(metric_rows)} metrics in the last {days}d"
    return {
        "data": {
            "days": days,
            "total_samples": total_samples,
            "metric_percentiles": metric_rows,
            "top_slowest_tools": [
                {
                    **row,
                    "tool_name": str(row["group_key"]).removeprefix("tool."),
                }
                for row in tool_rows
            ],
            "top_slowest_query_fingerprints": [dict(row) for row in query_rows],
        },
        "summary": {
            "total_samples": total_samples,
            "metric_count": len(metric_rows),
            "slowest_tool": slowest_tool,
            "query_fingerprint_count": len(query_rows),
        },
        "cli_report": cli_report,
    }


def _handle_perf_slow_queries(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=7, minimum=1)
    _enable_query_only(conn)
    rows = conn.execute(
        """
        SELECT
            COALESCE(json_extract(tags, '$.sql_fingerprint'), metric) AS sql_fingerprint,
            COUNT(*) AS sample_count,
            ROUND(AVG(value_ms), 2) AS avg_ms,
            MAX(value_ms) AS max_ms
        FROM perf_samples
        WHERE source = 'query'
          AND created_at >= datetime('now', ?)
        GROUP BY sql_fingerprint
        HAVING sql_fingerprint IS NOT NULL
           AND TRIM(sql_fingerprint) != ''
        ORDER BY avg_ms DESC, max_ms DESC, sql_fingerprint
        """,
        (_days_modifier(days, default=7),),
    ).fetchall()
    sample_count = sum(
        _coerce_int(row["sample_count"], default=0, minimum=0) for row in rows
    )
    cli_report = f"{len(rows)} slow query fingerprint(s) across {sample_count} sample(s) in the last {days}d"
    return {
        "data": {
            "days": days,
            "slow_queries": [dict(row) for row in rows],
        },
        "summary": {
            "fingerprint_count": len(rows),
            "sample_count": sample_count,
        },
        "cli_report": cli_report,
    }


def _handle_perf_tool_stats(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=7, minimum=1)
    _enable_query_only(conn)
    rows = _query_perf_percentiles(
        conn,
        days=days,
        group_sql="metric",
        where_clauses=("source = 'tool'", "metric LIKE 'tool.%'"),
        order_by="p95_ms DESC, avg_ms DESC, group_key",
    )
    tool_rows = [
        {
            **row,
            "tool_name": str(row["group_key"]).removeprefix("tool."),
            "call_count": _coerce_int(row["sample_count"], default=0, minimum=0),
        }
        for row in rows
    ]
    total_calls = sum(row["call_count"] for row in tool_rows)
    cli_report = f"{len(tool_rows)} tool(s), {total_calls} call(s) in the last {days}d"
    return {
        "data": {
            "days": days,
            "tools": tool_rows,
        },
        "summary": {
            "tool_count": len(tool_rows),
            "call_count": total_calls,
        },
        "cli_report": cli_report,
    }


def _handle_error_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=7, minimum=1)
    status = str(args.status or "open").strip().lower()
    severity = str(args.severity).strip().lower() if args.severity else None
    source = str(args.source).strip().lower() if args.source else None
    if status != "all" and status not in _VALID_TRIAGE_STATUSES:
        raise ValidationError(f"Invalid status '{status}'.")
    if severity is not None and severity not in _VALID_ERROR_SEVERITIES:
        raise ValidationError(f"Invalid severity '{severity}'.")

    _enable_query_only(conn)
    where_parts = ["last_seen >= datetime('now', ?)"]
    params: list[Any] = [_days_modifier(days, default=7)]
    if status != "all":
        where_parts.append("status = ?")
        params.append(status)
    if severity is not None:
        where_parts.append("severity = ?")
        params.append(severity)
    if source is not None:
        where_parts.append("source = ?")
        params.append(source)
    where_sql = " AND ".join(where_parts)

    total_row = conn.execute(
        f"SELECT COUNT(*) AS total_errors FROM errors WHERE {where_sql}",
        tuple(params),
    ).fetchone()
    rows = conn.execute(
        f"""
        SELECT
            id,
            fingerprint,
            severity,
            source,
            endpoint,
            error_type,
            message,
            status,
            occurrence_count,
            first_seen,
            last_seen,
            resolved_at,
            resolution
        FROM errors
        WHERE {where_sql}
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 0
                WHEN 'error' THEN 1
                ELSE 2
            END,
            last_seen DESC,
            occurrence_count DESC
        LIMIT 200
        """,
        tuple(params),
    ).fetchall()
    total_errors = _coerce_int(
        total_row["total_errors"] if total_row is not None else 0,
        default=0,
        minimum=0,
    )
    cli_report = f"{total_errors} error(s) matched over the last {days}d"
    return {
        "data": {
            "days": days,
            "filters": {"status": status, "severity": severity, "source": source},
            "errors": [dict(row) for row in rows],
            "truncated": total_errors > len(rows),
        },
        "summary": {
            "total_errors": total_errors,
            "displayed_errors": len(rows),
            "status": status,
        },
        "cli_report": cli_report,
    }


def _handle_error_show(args, conn: sqlite3.Connection) -> dict[str, Any]:
    error_id = str(args.error_id or "").strip()
    if not error_id:
        raise ValidationError("error_id is required")

    _enable_query_only(conn)
    row = conn.execute(
        """
        SELECT *
        FROM errors
        WHERE id = ?
        """,
        (error_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"Error '{error_id}' not found")

    occurrences = conn.execute(
        """
        SELECT
            created_at,
            request_id,
            user_id,
            context
        FROM error_occurrences
        WHERE error_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (error_id, _ERROR_TIMELINE_LIMIT),
    ).fetchall()
    error_payload = dict(row)
    error_payload["context"] = _safe_json_load(error_payload.get("context"))
    timeline = []
    for occurrence in occurrences:
        timeline.append(
            {
                "created_at": occurrence["created_at"],
                "request_id": occurrence["request_id"],
                "user_id": occurrence["user_id"],
                "context": _safe_json_load(occurrence["context"]),
            }
        )

    occurrence_count = _coerce_int(row["occurrence_count"], default=0, minimum=0)
    cli_report = (
        f"{row['severity']} {row['error_type']} in {row['source']}/{row['endpoint'] or 'unknown'} "
        f"({occurrence_count} occurrence(s))"
    )
    return {
        "data": {
            "error": error_payload,
            "occurrence_timeline": timeline,
            "timeline_truncated": occurrence_count > len(timeline),
        },
        "summary": {
            "error_id": error_id,
            "status": row["status"],
            "occurrence_count": occurrence_count,
            "last_seen": row["last_seen"],
        },
        "cli_report": cli_report,
    }


def _handle_error_update(args, conn: sqlite3.Connection) -> dict[str, Any]:
    error_id = str(args.error_id or "").strip()
    status = str(args.status or "").strip().lower()
    resolution = str(args.resolution).strip() if args.resolution is not None else None
    if not error_id:
        raise ValidationError("error_id is required")
    if status not in _VALID_TRIAGE_STATUSES:
        raise ValidationError(f"Invalid status '{status}'.")

    row = conn.execute(
        """
        SELECT id, resolution
        FROM errors
        WHERE id = ?
        """,
        (error_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"Error '{error_id}' not found")

    if status in {"resolved", "wontfix"}:
        resolved_at = "datetime('now')"
        effective_resolution = resolution if resolution else row["resolution"]
    else:
        resolved_at = "NULL"
        effective_resolution = resolution

    conn.execute(
        f"""
        UPDATE errors
        SET status = ?,
            resolution = ?,
            resolved_at = {resolved_at}
        WHERE id = ?
        """,
        (status, effective_resolution, error_id),
    )
    updated = conn.execute(
        """
        SELECT id, status, resolution, resolved_at, last_seen
        FROM errors
        WHERE id = ?
        """,
        (error_id,),
    ).fetchone()
    if updated is None:
        raise NotFoundError(f"Error '{error_id}' not found after update")
    return {
        "data": dict(updated),
        "summary": {
            "error_id": error_id,
            "status": updated["status"],
        },
        "cli_report": f"Updated error {error_id} -> {status}",
    }


def _handle_error_stats(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=30, minimum=1)
    _enable_query_only(conn)
    params = (_days_modifier(days, default=30),)
    by_source = conn.execute(
        """
        SELECT source, COUNT(*) AS error_count
        FROM errors
        WHERE last_seen >= datetime('now', ?)
        GROUP BY source
        ORDER BY error_count DESC, source
        """,
        params,
    ).fetchall()
    by_severity = conn.execute(
        """
        SELECT severity, COUNT(*) AS error_count
        FROM errors
        WHERE last_seen >= datetime('now', ?)
        GROUP BY severity
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 0
                WHEN 'error' THEN 1
                ELSE 2
            END
        """,
        params,
    ).fetchall()
    top_fingerprints = conn.execute(
        """
        SELECT
            id,
            fingerprint,
            source,
            endpoint,
            error_type,
            occurrence_count,
            first_seen,
            last_seen,
            ROUND(
                occurrence_count /
                CASE
                    WHEN julianday('now') - julianday(first_seen) < 1 THEN 1
                    ELSE julianday('now') - julianday(first_seen)
                END,
                2
            ) AS occurrences_per_day
        FROM errors
        WHERE last_seen >= datetime('now', ?)
        ORDER BY occurrence_count DESC, last_seen DESC
        LIMIT 10
        """,
        params,
    ).fetchall()
    total_errors = sum(
        _coerce_int(row["error_count"], default=0, minimum=0) for row in by_source
    )
    open_errors_row = conn.execute(
        """
        SELECT COUNT(*) AS open_errors
        FROM errors
        WHERE status IN ('open', 'investigating')
          AND last_seen >= datetime('now', ?)
        """,
        params,
    ).fetchone()
    open_errors = _coerce_int(
        open_errors_row["open_errors"] if open_errors_row is not None else 0,
        default=0,
        minimum=0,
    )
    return {
        "data": {
            "days": days,
            "by_source": [dict(row) for row in by_source],
            "by_severity": [dict(row) for row in by_severity],
            "top_recurring_fingerprints": [dict(row) for row in top_fingerprints],
        },
        "summary": {
            "total_errors": total_errors,
            "open_errors": open_errors,
            "fingerprint_count": len(top_fingerprints),
        },
        "cli_report": f"{total_errors} error(s), {open_errors} still open/investigating in the last {days}d",
    }


def _handle_issue_list(args, conn: sqlite3.Connection) -> dict[str, Any]:
    status = str(args.status or "open").strip().lower()
    if status != "all" and status not in _VALID_TRIAGE_STATUSES:
        raise ValidationError(f"Invalid status '{status}'.")

    _enable_query_only(conn)
    params: tuple[Any, ...] = ()
    where_sql = ""
    if status != "all":
        where_sql = "WHERE status = ?"
        params = (status,)
    total_row = conn.execute(
        f"SELECT COUNT(*) AS total_issues FROM issue_reports {where_sql}",
        params,
    ).fetchone()
    rows = conn.execute(
        f"""
        SELECT
            id,
            title,
            description,
            severity,
            status,
            resolved_at,
            resolution,
            created_at
        FROM issue_reports
        {where_sql}
        ORDER BY
            CASE severity
                WHEN 'bug' THEN 0
                WHEN 'warning' THEN 1
                ELSE 2
            END,
            created_at DESC
        LIMIT 200
        """,
        params,
    ).fetchall()
    total_issues = _coerce_int(
        total_row["total_issues"] if total_row is not None else 0,
        default=0,
        minimum=0,
    )
    return {
        "data": {
            "issues": [dict(row) for row in rows],
            "status": status,
            "truncated": total_issues > len(rows),
        },
        "summary": {
            "status": status,
            "total_issues": total_issues,
            "displayed_issues": len(rows),
        },
        "cli_report": f"{total_issues} issue report(s) matched",
    }


def _handle_issue_update(args, conn: sqlite3.Connection) -> dict[str, Any]:
    issue_id = str(args.issue_id or "").strip()
    status = str(args.status or "").strip().lower()
    resolution = str(args.resolution).strip() if args.resolution is not None else None
    if not issue_id:
        raise ValidationError("issue_id is required")
    if status not in _VALID_TRIAGE_STATUSES:
        raise ValidationError(f"Invalid status '{status}'.")

    row = conn.execute(
        """
        SELECT id, resolution
        FROM issue_reports
        WHERE id = ?
        """,
        (issue_id,),
    ).fetchone()
    if row is None:
        raise NotFoundError(f"Issue '{issue_id}' not found")

    if status in {"resolved", "wontfix"}:
        resolved_at = "datetime('now')"
        effective_resolution = resolution if resolution else row["resolution"]
    else:
        resolved_at = "NULL"
        effective_resolution = resolution

    conn.execute(
        f"""
        UPDATE issue_reports
        SET status = ?,
            resolution = ?,
            resolved_at = {resolved_at}
        WHERE id = ?
        """,
        (status, effective_resolution, issue_id),
    )
    updated = conn.execute(
        """
        SELECT id, status, resolution, resolved_at, created_at
        FROM issue_reports
        WHERE id = ?
        """,
        (issue_id,),
    ).fetchone()
    if updated is None:
        raise NotFoundError(f"Issue '{issue_id}' not found after update")
    return {
        "data": dict(updated),
        "summary": {
            "issue_id": issue_id,
            "status": updated["status"],
        },
        "cli_report": f"Updated issue {issue_id} -> {status}",
    }


def _handle_cost_summary(args, conn: sqlite3.Connection) -> dict[str, Any]:
    months = _coerce_int(args.months, default=1, minimum=1)
    _enable_query_only(conn)
    month_modifier = _months_modifier(months, default=1)
    params = (month_modifier,)
    provider_rows = conn.execute(
        """
        SELECT
            provider,
            SUM(cost_usd6) AS total_usd6,
            COUNT(*) AS entry_count
        FROM cost_ledger
        WHERE created_at >= datetime('now', 'start of month', ?)
        GROUP BY provider
        ORDER BY total_usd6 DESC, provider
        """,
        params,
    ).fetchall()
    operation_rows = conn.execute(
        """
        SELECT
            provider,
            operation,
            SUM(cost_usd6) AS total_usd6,
            COUNT(*) AS entry_count,
            SUM(COALESCE(input_tokens, 0)) AS input_tokens,
            SUM(COALESCE(output_tokens, 0)) AS output_tokens
        FROM cost_ledger
        WHERE created_at >= datetime('now', 'start of month', ?)
        GROUP BY provider, operation
        ORDER BY total_usd6 DESC, provider, operation
        """,
        params,
    ).fetchall()
    total_usd6 = sum(
        _coerce_int(row["total_usd6"], default=0, minimum=0) for row in provider_rows
    )
    return {
        "data": {
            "months": months,
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
            "by_provider": [
                {
                    **dict(row),
                    "total_usd": _usd_from_usd6(row["total_usd6"]),
                }
                for row in provider_rows
            ],
            "by_provider_operation": [
                {
                    **dict(row),
                    "total_usd": _usd_from_usd6(row["total_usd6"]),
                }
                for row in operation_rows
            ],
        },
        "summary": {
            "months": months,
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
            "provider_count": len(provider_rows),
            "operation_count": len(operation_rows),
        },
        "cli_report": f"Cost summary: ${_usd_from_usd6(total_usd6):.2f} over the last {months} month(s)",
    }


def _handle_cost_daily(args, conn: sqlite3.Connection) -> dict[str, Any]:
    days = _coerce_int(args.days, default=30, minimum=1)
    _enable_query_only(conn)
    rows = conn.execute(
        """
        SELECT
            date(created_at) AS day,
            provider,
            SUM(cost_usd6) AS total_usd6,
            COUNT(*) AS entry_count
        FROM cost_ledger
        WHERE created_at >= datetime('now', ?)
        GROUP BY day, provider
        ORDER BY day DESC, provider
        """,
        (_days_modifier(days, default=30),),
    ).fetchall()
    by_day: dict[str, dict[str, Any]] = {}
    for row in rows:
        day = str(row["day"])
        payload = by_day.setdefault(
            day,
            {"day": day, "total_usd6": 0, "total_usd": 0.0, "providers": {}},
        )
        provider = str(row["provider"])
        provider_usd6 = _coerce_int(row["total_usd6"], default=0, minimum=0)
        payload["providers"][provider] = {
            "total_usd6": provider_usd6,
            "total_usd": _usd_from_usd6(provider_usd6),
            "entry_count": _coerce_int(row["entry_count"], default=0, minimum=0),
        }
        payload["total_usd6"] += provider_usd6
        payload["total_usd"] = _usd_from_usd6(payload["total_usd6"])

    daily_breakdown = [by_day[day] for day in sorted(by_day.keys(), reverse=True)]
    total_usd6 = sum(day["total_usd6"] for day in daily_breakdown)
    return {
        "data": {
            "days": days,
            "daily_breakdown": daily_breakdown,
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
        },
        "summary": {
            "days": days,
            "days_with_cost": len(daily_breakdown),
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
        },
        "cli_report": f"{len(daily_breakdown)} cost day(s), ${_usd_from_usd6(total_usd6):.2f} total over {days}d",
    }


def _handle_cost_limits_show(_args, conn: sqlite3.Connection) -> dict[str, Any]:
    _enable_query_only(conn)
    rows = conn.execute(
        """
        SELECT provider, period, limit_usd6, system_limit_usd6, action, is_active
        FROM cost_limits
        WHERE is_active = 1
        ORDER BY provider, period
        """
    ).fetchall()
    limits = [_cost_limit_payload(conn, row) for row in rows]
    max_pct_used = (
        max((limit["pct_used"] or 0.0) for limit in limits) if limits else 0.0
    )
    return {
        "data": {"limits": limits},
        "summary": {
            "limit_count": len(limits),
            "max_pct_used": round(max_pct_used, 2),
        },
        "cli_report": f"{len(limits)} active cost limit(s), max usage {max_pct_used:.2f}%",
    }


def _handle_cost_limits_set(args, conn: sqlite3.Connection) -> dict[str, Any]:
    provider = str(args.provider or "").strip().lower()
    period = str(args.period or "").strip().lower()
    action = str(args.action or "").strip().lower()
    try:
        limit_usd6 = int(args.limit_usd6)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit_usd6 must be an integer.") from exc
    if provider not in _VALID_COST_PROVIDERS:
        raise ValueError(f"Invalid provider '{provider}'.")
    if period not in _VALID_COST_PERIODS:
        raise ValueError(f"Invalid period '{period}'.")
    if action not in _VALID_COST_ACTIONS:
        raise ValueError(f"Invalid action '{action}'.")
    if limit_usd6 < 0:
        raise ValueError("limit_usd6 must be >= 0.")

    conn.execute(
        """
        INSERT INTO cost_limits (provider, period, limit_usd6, action, is_active)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(provider, period) DO UPDATE SET
            limit_usd6 = excluded.limit_usd6,
            action = excluded.action,
            is_active = 1
        """,
        (provider, period, limit_usd6, action),
    )
    row = conn.execute(
        """
        SELECT provider, period, limit_usd6, system_limit_usd6, action, is_active
        FROM cost_limits
        WHERE provider = ?
          AND period = ?
        """,
        (provider, period),
    ).fetchone()
    if row is None:
        raise ValueError("Failed to load updated cost limit.")
    payload = _cost_limit_payload(conn, row)
    return {
        "data": payload,
        "summary": {
            "provider": provider,
            "period": period,
            "action": action,
        },
        "cli_report": (
            f"Set {provider} {period} limit to ${payload['limit_usd']:.2f} ({action})"
        ),
    }


def _handle_cost_unit_economics(
    args,
    conn: sqlite3.Connection | None,
) -> dict[str, Any]:
    months = _coerce_int(args.months, default=3, minimum=1)
    price_points = _parse_price_points(args.price_points or "5,10,15,20")
    period_days = months * 30
    if conn is not None:
        _enable_query_only(conn)

    database_url = str(os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        reason = "database_url_missing"
        return {
            "data": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
                "price_points": price_points,
            },
            "summary": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
            },
            "cli_report": "Unit economics unavailable: DATABASE_URL is not configured.",
        }

    start_date, end_exclusive = _cost_period_bounds(period_days)
    try:
        rows = _fetch_ops_cost_rollup_rows(
            database_url=database_url,
            start_date=start_date,
            end_exclusive=end_exclusive,
        )
    except Exception as exc:
        reason = "postgres_error"
        return {
            "data": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
                "price_points": price_points,
                "error_type": type(exc).__name__,
            },
            "summary": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
            },
            "cli_report": f"Unit economics unavailable: {type(exc).__name__}.",
        }

    if not rows:
        reason = "no_data"
        return {
            "data": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
                "price_points": price_points,
                "rollup_plan_quality": _empty_rollup_plan_quality(),
            },
            "summary": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
                "excluded_rollup_row_count": 0,
            },
            "cli_report": f"No AI cost rollup data found for the trailing {period_days} days.",
        }

    payload = _build_cost_unit_economics_payload(
        rows=rows,
        period_days=period_days,
        price_points=price_points,
        data_root=_cost_economics_data_root(),
        start_date=start_date,
        end_exclusive=end_exclusive,
        database_url=database_url,
    )
    quality = payload.get("rollup_plan_quality") or {}
    excluded_row_count = _coerce_int(
        quality.get("excluded_row_count"), default=0, minimum=0
    )
    if not payload.get("available"):
        reason = str(payload.get("reason") or "no_attributed_plan_data")
        return {
            "data": payload,
            "summary": {
                "available": False,
                "reason": reason,
                "period_days": period_days,
                "excluded_rollup_row_count": excluded_row_count,
            },
            "cli_report": (
                f"No attributed AI cost rollup data found for the trailing {period_days} days; "
                f"excluded {excluded_row_count} row(s) without a usable plan_code."
            ),
        }

    total_users = sum(plan["total_users"] for plan in payload["plans"].values())
    total_usd6 = sum(plan["total_usd6"] for plan in payload["plans"].values())
    quality_note = (
        f"; excluded {excluded_row_count} unattributed row(s)"
        if excluded_row_count
        else ""
    )
    return {
        "data": payload,
        "summary": {
            "available": True,
            "period_days": period_days,
            "plan_count": len(payload["plans"]),
            "total_users": total_users,
            "total_usd6": total_usd6,
            "total_usd": _usd_from_usd6(total_usd6),
            "excluded_rollup_row_count": excluded_row_count,
        },
        "cli_report": (
            f"Unit economics: {total_users} user-plan sample(s), "
            f"${_usd_from_usd6(total_usd6):.2f} AI cost over {period_days}d"
            f"{quality_note}"
        ),
    }


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def finance_log_issue(title: str, description: str, severity: str = "bug") -> dict:
    """Log a bug, warning, or suggestion to the issue_reports table."""
    with _get_conn() as conn:
        result = _insert_issue(conn, title, description, severity)
    if "error" in result:
        return {"data": {}, "summary": result["error"]}
    return {
        "data": result,
        "summary": f"Logged {result['severity'].lower()}: {result['title']}",
    }


@mcp.tool(sync_behavior="no_sync", read_only=True)
def analytics_funnel(days: int = 30) -> dict:
    """Onboarding funnel counts for onboarding.* analytics events."""
    return _call(_handle_analytics_funnel, {"days": days})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def analytics_usage(days: int = 30, summary_only: bool = False) -> dict:
    """Daily event counts by domain and feature adoption percentages."""
    result = _call_full(_handle_analytics_usage, {"days": days})
    return _result_with_optional_cache(
        "analytics_usage",
        result,
        summary_only=summary_only,
        extra_data={"total_events": result.get("summary", {}).get("total_events", 0)},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def analytics_session_stats(days: int = 30) -> dict:
    """Average chat session duration, messages, and tool calls."""
    return _call(_handle_analytics_session_stats, {"days": days})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def perf_summary(days: int = 7) -> dict:
    """Latency percentiles by metric plus top slow tools and slow query fingerprints."""
    return _call(_handle_perf_summary, {"days": days})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def perf_slow_queries(days: int = 7, summary_only: bool = False) -> dict:
    """Grouped slow-query samples by SQL fingerprint."""
    result = _call_full(_handle_perf_slow_queries, {"days": days})
    return _result_with_optional_cache(
        "perf_slow_queries",
        result,
        summary_only=summary_only,
        extra_data={
            "fingerprint_count": result.get("summary", {}).get("fingerprint_count", 0)
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def perf_tool_stats(days: int = 7, summary_only: bool = False) -> dict:
    """Per-tool call counts, latency percentiles, and error rates."""
    result = _call_full(_handle_perf_tool_stats, {"days": days})
    return _result_with_optional_cache(
        "perf_tool_stats",
        result,
        summary_only=summary_only,
        extra_data={"tool_count": result.get("summary", {}).get("tool_count", 0)},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def error_list(
    status: str = "open",
    severity: Optional[str] = None,
    source: Optional[str] = None,
    days: int = 7,
    summary_only: bool = False,
) -> dict:
    """List runtime errors with status/severity/source filters."""
    result = _call_full(
        _handle_error_list,
        {"status": status, "severity": severity, "source": source, "days": days},
    )
    return _result_with_optional_cache(
        "error_list",
        result,
        summary_only=summary_only,
        extra_data={"total_errors": result.get("summary", {}).get("total_errors", 0)},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def error_show(error_id: str) -> dict:
    """Show full error detail with redacted traceback and occurrence timeline.

    Discovery: use error_list with status filters to choose error_id before updating or reading an error.
    """
    return _call(_handle_error_show, {"error_id": error_id})


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def error_update(error_id: str, status: str, resolution: Optional[str] = None) -> dict:
    """Update runtime error triage status and optional resolution note.

    Discovery: use error_list with status filters to choose error_id before updating or reading an error.
    """
    return _call(
        _handle_error_update,
        {"error_id": error_id, "status": status, "resolution": resolution},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def error_stats(days: int = 30) -> dict:
    """Error counts by source and severity plus top recurring fingerprints."""
    return _call(_handle_error_stats, {"days": days})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def issue_list(status: str = "open", summary_only: bool = False) -> dict:
    """List agent-reported issues from issue_reports."""
    result = _call_full(_handle_issue_list, {"status": status})
    return _result_with_optional_cache(
        "issue_list",
        result,
        summary_only=summary_only,
        extra_data={"total_issues": result.get("summary", {}).get("total_issues", 0)},
    )


@mcp.tool(sync_behavior="no_sync", approval_required=True)
def issue_update(issue_id: str, status: str, resolution: Optional[str] = None) -> dict:
    """Update issue_reports triage status and optional resolution note.

    Discovery: use issue_list to choose issue_id before updating an issue.
    """
    return _call(
        _handle_issue_update,
        {"issue_id": issue_id, "status": status, "resolution": resolution},
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cost_summary(months: int = 1) -> dict:
    """Total cost breakdown by provider and operation for the current user."""
    return _call(_handle_cost_summary, {"months": months})


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cost_daily(days: int = 30, summary_only: bool = False) -> dict:
    """Day-by-day user cost breakdown."""
    result = _call_full(_handle_cost_daily, {"days": days})
    return _result_with_optional_cache(
        "cost_daily",
        result,
        summary_only=summary_only,
        extra_data={
            "days_with_cost": result.get("summary", {}).get("days_with_cost", 0)
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cost_limits_show() -> dict:
    """Show active cost limits and current spend proximity.

    Related tools: cost_limits_set.
    """
    return _call(_handle_cost_limits_show, {})


@mcp.tool(sync_behavior="db_write", approval_required=True)
def cost_limits_set(provider: str, period: str, limit_usd6: int, action: str) -> dict:
    """Create or update a cost limit.

    Discovery: run `cost_limits_show` first to obtain `provider` and `period` values.

    Valid provider values: claude | openai | plaid | all
    Valid period values: daily | monthly
    Valid action values: warn | block
    """
    return _call(
        _handle_cost_limits_set,
        {
            "provider": provider,
            "period": period,
            "limit_usd6": limit_usd6,
            "action": action,
        },
    )


@mcp.tool(sync_behavior="no_sync", read_only=True)
def cost_unit_economics(
    months: int = 3,
    price_points: str = "5,10,15,20",
    summary_only: bool = False,
) -> dict:
    """Plan-level AI unit economics from shared ops cost rollups."""
    result = _call_full(
        _handle_cost_unit_economics,
        {"months": months, "price_points": price_points},
        allow_missing_user_db=True,
    )
    return _result_with_optional_cache(
        "cost_unit_economics",
        result,
        summary_only=summary_only,
        extra_data={
            "available": result.get("summary", {}).get("available", False),
            "period_days": result.get("summary", {}).get("period_days", months * 30),
            "plan_count": result.get("summary", {}).get("plan_count", 0),
        },
    )


REGISTERED_TOOL_NAMES = frozenset(tool_registry._REGISTERED_TOOL_NAMES)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _main() -> None:
    """Compatibility stdio entrypoint for direct `python -m finance_cli.mcp_server`.

    Shared tool definitions live in this module, but startup-specific behavior
    belongs in dedicated entrypoints like ``finance_cli.mcp_local`` and
    ``finance_cli.mcp_gateway``.
    """
    load_dotenv()
    mcp.run()


if __name__ == "__main__":
    _main()
