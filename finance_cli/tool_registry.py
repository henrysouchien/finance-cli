"""Central tool metadata registry for MCP tool classification."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from typing import Iterable, Literal

logger = logging.getLogger(__name__)

SyncBehavior = Literal["db_write", "server_proxied", "no_sync"]


@dataclass(frozen=True)
class ToolMetadata:
    sync_behavior: SyncBehavior
    read_only: bool = False
    approval_required: bool = False
    excluded_from_agent: bool = False
    normalizer: bool = False
    onboarding_auto_approved: bool = False
    coach_debt_payoff_auto_approved: bool = False
    coach_emergency_fund_auto_approved: bool = False
    coach_savings_goal_auto_approved: bool = False
    coach_spending_plan_auto_approved: bool = False
    coach_tax_readiness_auto_approved: bool = False
    coach_homebuying_readiness_auto_approved: bool = False
    coach_retirement_contribution_readiness_auto_approved: bool = False
    coach_retirement_income_readiness_auto_approved: bool = False
    coach_investment_readiness_auto_approved: bool = False
    coach_estate_document_readiness_auto_approved: bool = False
    coach_financial_plan_intake_auto_approved: bool = False
    coach_risk_insurance_readiness_auto_approved: bool = False
    coach_advisor_handoff_readiness_auto_approved: bool = False

    def __post_init__(self) -> None:
        if self.sync_behavior not in {"db_write", "server_proxied", "no_sync"}:
            raise ValueError(f"Invalid sync_behavior: {self.sync_behavior!r}")
        if self.onboarding_auto_approved and not self.approval_required:
            raise ValueError("onboarding_auto_approved requires approval_required=True")
        if self.coach_debt_payoff_auto_approved and not self.approval_required:
            raise ValueError("coach_debt_payoff_auto_approved requires approval_required=True")
        if self.coach_emergency_fund_auto_approved and not self.approval_required:
            raise ValueError("coach_emergency_fund_auto_approved requires approval_required=True")
        if self.coach_savings_goal_auto_approved and not self.approval_required:
            raise ValueError("coach_savings_goal_auto_approved requires approval_required=True")
        if self.coach_spending_plan_auto_approved and not self.approval_required:
            raise ValueError("coach_spending_plan_auto_approved requires approval_required=True")
        if self.coach_tax_readiness_auto_approved and not self.approval_required:
            raise ValueError("coach_tax_readiness_auto_approved requires approval_required=True")
        if self.coach_homebuying_readiness_auto_approved and not self.approval_required:
            raise ValueError(
                "coach_homebuying_readiness_auto_approved requires approval_required=True"
            )
        if (
            self.coach_retirement_contribution_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_retirement_contribution_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_retirement_income_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_retirement_income_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_investment_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_investment_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_estate_document_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_estate_document_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_financial_plan_intake_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_financial_plan_intake_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_risk_insurance_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_risk_insurance_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            self.coach_advisor_handoff_readiness_auto_approved
            and not self.approval_required
        ):
            raise ValueError(
                "coach_advisor_handoff_readiness_auto_approved "
                "requires approval_required=True"
            )
        if self.sync_behavior == "db_write" and self.read_only:
            raise ValueError("db_write tools cannot be read_only")


_REGISTERED_TOOL_NAMES: set[str] = set()
_TOOL_REGISTRY: dict[str, ToolMetadata] = {}


def _register_name(name: str) -> None:
    if name in _REGISTERED_TOOL_NAMES:
        raise RuntimeError(f"Tool {name!r} registered twice")
    _REGISTERED_TOOL_NAMES.add(name)


def register(name: str, meta: ToolMetadata) -> None:
    if name in _TOOL_REGISTRY:
        raise RuntimeError(f"Tool metadata for {name!r} registered twice")
    _TOOL_REGISTRY[name] = meta


def iter_registry() -> Iterable[tuple[str, ToolMetadata]]:
    return iter(_TOOL_REGISTRY.items())


def clear() -> None:
    """Reset the registry. Intended for mcp_server module re-execution (tests)."""
    _REGISTERED_TOOL_NAMES.clear()
    _TOOL_REGISTRY.clear()
    # Invalidate derived caches so the next access recomputes from the fresh registry.
    for module_name in (
        "finance_cli.gateway.tools",
        "finance_cli.sync.tool_classification",
    ):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        module._all.cache_clear()
        module._derived.cache_clear()


def validate_registry(registered_names: Iterable[str], *, strict: bool = False) -> None:
    registered = frozenset(registered_names)
    classified = frozenset(_TOOL_REGISTRY)
    issues: list[str] = []

    unclassified = registered - classified
    if unclassified:
        issues.append(f"{len(unclassified)} unclassified tools")

    unknown = classified - registered
    if unknown:
        issues.append(f"{len(unknown)} classified tools not registered: {sorted(unknown)}")

    for name, meta in sorted(iter_registry()):
        if meta.onboarding_auto_approved and not meta.approval_required:
            issues.append(f"{name}: onboarding_auto_approved requires approval_required=True")
        if meta.coach_debt_payoff_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_debt_payoff_auto_approved requires approval_required=True"
            )
        if meta.coach_emergency_fund_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_emergency_fund_auto_approved requires approval_required=True"
            )
        if meta.coach_savings_goal_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_savings_goal_auto_approved requires approval_required=True"
            )
        if meta.coach_spending_plan_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_spending_plan_auto_approved requires approval_required=True"
            )
        if meta.coach_tax_readiness_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_tax_readiness_auto_approved requires approval_required=True"
            )
        if meta.coach_homebuying_readiness_auto_approved and not meta.approval_required:
            issues.append(
                f"{name}: coach_homebuying_readiness_auto_approved requires approval_required=True"
            )
        if (
            meta.coach_retirement_contribution_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_retirement_contribution_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_retirement_income_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_retirement_income_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_investment_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_investment_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_estate_document_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_estate_document_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_financial_plan_intake_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_financial_plan_intake_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_risk_insurance_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_risk_insurance_readiness_auto_approved "
                "requires approval_required=True"
            )
        if (
            meta.coach_advisor_handoff_readiness_auto_approved
            and not meta.approval_required
        ):
            issues.append(
                f"{name}: coach_advisor_handoff_readiness_auto_approved "
                "requires approval_required=True"
            )
        if meta.sync_behavior == "db_write" and meta.read_only:
            issues.append(f"{name}: db_write tools cannot be read_only")

    if not issues:
        return

    if strict:
        raise RuntimeError("; ".join(issues))

    for issue in issues:
        logger.warning("tool_registry warn: %s", issue)
