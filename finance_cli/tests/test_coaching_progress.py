from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from finance_cli import coaching_progress as subject
from finance_cli.db import connect, initialize_database
from finance_cli.gateway.tools import BRIDGE_TOOLS, READ_ONLY_TOOLS
from finance_cli.skill_state import SkillStateStore
from finance_cli.sync.tool_classification import NO_SYNC_TOOLS


_NOW = datetime(2026, 6, 26, tzinfo=timezone.utc)


def _init_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "finance.db"
    initialize_database(db_path)
    return db_path


def _store(tmp_path: Path) -> SkillStateStore:
    return SkillStateStore(tmp_path / "skill_state.json")


def _recommendations(*skills: str) -> dict:
    return {
        "data": {
            "recommendations": [
                {
                    "skill": skill,
                    "source": "test",
                    "reason": f"{skill} reason",
                    "action": {"tool": "get_skill", "params": {"name": skill}},
                }
                for skill in skills
            ]
        },
        "summary": {"count": len(skills), "top_skill": skills[0] if skills else None},
    }


def _expected_check_in(skill: str, title: str, next_check_in: str) -> dict[str, object]:
    parsed = date.fromisoformat(next_check_in)
    days_until = (parsed - _NOW.date()).days
    if days_until < 0:
        check_in_status = "overdue"
        days_overdue = abs(days_until)
        days_until_value = None
    else:
        check_in_status = "due_today" if days_until == 0 else "upcoming"
        days_overdue = 0
        days_until_value = days_until
    return {
        "skill": skill,
        "title": title,
        "next_check_in": next_check_in,
        "check_in_status": check_in_status,
        "days_overdue": days_overdue,
        "days_until": days_until_value,
    }


def test_progress_reports_not_started_skills(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    monkeypatch.setattr(subject, "recommend_skills", lambda *_args, **_kwargs: _recommendations())

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    assert result["summary"]["skills_total"] == len(subject.COACHING_SKILLS)
    assert result["summary"]["skills_started"] == 0
    assert result["summary"]["plans_saved"] == 0
    assert result["summary"]["status_counts"] == {
        "not_started": len(subject.COACHING_SKILLS)
    }
    assert {item["status"] for item in result["data"]["skills"]} == {"not_started"}


def test_progress_uses_skill_state_and_session_markers(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    store.set("coach_debt_payoff", {"phase": "prioritize", "last_active_at": "2026-05-01T09:00:00"})
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    (sessions_dir / "2026-05-02.md").write_text(
        "## 10:00\ncoach_debt_payoff:phase0_diagnose_complete\n"
        "coach_debt_payoff:phase1_surface_complete\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_debt_payoff"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    debt = next(item for item in result["data"]["skills"] if item["skill"] == "coach_debt_payoff")
    assert debt["status"] == "in_progress"
    assert debt["acted_on_recommendation"] is True
    assert debt["recommendation"]["rank"] == 1
    assert debt["phase"] == "prioritize"
    assert debt["completed_phase_numbers"] == [0, 1]
    assert debt["phase_progress_pct"] == 20
    assert debt["first_session_date"] == "2026-05-02"
    assert debt["last_session_date"] == "2026-05-02"


def test_progress_reads_latest_artifact_and_outcomes(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_debt_payoff"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260501.md").write_text(
        "## Debt Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-05-01T12:00:00Z'\n"
        "monthly_commitment_cents: 42000\n"
        "target_debt_free_date: '2026-12-31'\n"
        "monitoring_cadence: monthly\n"
        "next_check_in: '2026-06-01'\n"
        "strategy: avalanche\n"
        "debts_in_scope:\n"
        "  - label: High APR Card\n"
        "    balance_cents: 100000\n"
        "check_ins:\n"
        "  - date: '2026-06-01'\n"
        "    progress_summary: Paid as planned.\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_debt_payoff"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    debt = next(item for item in result["data"]["skills"] if item["skill"] == "coach_debt_payoff")
    assert debt["status"] == "plan_saved"
    assert debt["phase_progress_pct"] == 90
    assert debt["artifact"]["found"] is True
    assert debt["artifact"]["artifact_name"] == "20260501.md"
    assert debt["outcomes"] == {
        "monthly_commitment_cents": 42000,
        "target_date": "2026-12-31",
        "monitoring_cadence": "monthly",
        "next_check_in": "2026-06-01",
        "strategy": "avalanche",
        "debts_in_scope_count": 1,
        "check_in_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["summary"]["overdue_check_in_count"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in("coach_debt_payoff", "Debt-payoff coaching", "2026-06-01")
    ]
    assert result["data"]["next_check_ins"][0]["check_in_status"] == "overdue"
    assert result["data"]["next_check_ins"][0]["days_overdue"] == 25


def test_progress_reads_homebuying_readiness_outcomes(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_homebuying_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260621.md").write_text(
        "## Homebuying Readiness Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-21T12:00:00Z'\n"
        "household_profile:\n"
        "  timeline: 3_12_months\n"
        "affordability_scenarios:\n"
        "  - scenario_id: baseline\n"
        "    monthly_homeownership_cost_cents: 334200\n"
        "cash_to_close:\n"
        "  cash_to_close_total_cents: 5710000\n"
        "  reserve_gap_cents: 710000\n"
        "ratios:\n"
        "  full_homeownership_cost_ratio_pct: 37.1\n"
        "readiness_status: fix_first\n"
        "readiness_flags:\n"
        "  - reserve_gap\n"
        "next_actions:\n"
        "  - Build reserves before preapproval.\n"
        "referrals:\n"
        "  - referrals.hud-approved-housing-counselor\n"
        "next_check_in: '2026-07-21'\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_homebuying_readiness"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    homebuying = next(
        item for item in result["data"]["skills"] if item["skill"] == "coach_homebuying_readiness"
    )
    assert homebuying["title"] == "Homebuying-readiness coaching"
    assert homebuying["status"] == "plan_saved"
    assert homebuying["outcomes"] == {
        "readiness_status": "fix_first",
        "timeline": "3_12_months",
        "next_check_in": "2026-07-21",
        "scenario_count": 1,
        "cash_to_close_total_cents": 5710000,
        "reserve_gap_cents": 710000,
        "monthly_homeownership_cost_cents": 334200,
        "full_homeownership_cost_ratio_pct": 37.1,
        "readiness_flag_count": 1,
        "next_action_count": 1,
        "referral_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_homebuying_readiness",
            "Homebuying-readiness coaching",
            "2026-07-21",
        )
    ]


def test_progress_reads_retirement_contribution_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_retirement_contribution_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "## Retirement Contribution Readiness Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "tax_year: 2026\n"
        "readiness_status: contribution_ready\n"
        "priority_result:\n"
        "  helper: advisory_contribution_priority\n"
        "  source_tax_year: 2026\n"
        "  steps:\n"
        "    - account: workplace_plan_match\n"
        "selected_commitment:\n"
        "  account_type: workplace_plan_match\n"
        "  monthly_target_cents: 60000\n"
        "  write_status: not_requested\n"
        "readiness_flags:\n"
        "  - match_available\n"
        "next_actions:\n"
        "  - Confirm payroll contribution setting.\n"
        "referrals: []\n"
        "next_check_in: '2026-07-22'\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations(
            "coach_retirement_contribution_readiness"
        ),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    retirement = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_retirement_contribution_readiness"
    )
    assert retirement["title"] == "Retirement contribution-readiness coaching"
    assert retirement["status"] == "plan_saved"
    assert retirement["outcomes"] == {
        "readiness_status": "contribution_ready",
        "tax_year": 2026,
        "next_check_in": "2026-07-22",
        "selected_account_type": "workplace_plan_match",
        "monthly_target_cents": 60000,
        "write_status": "not_requested",
        "source_tax_year": 2026,
        "priority_step_count": 1,
        "readiness_flag_count": 1,
        "next_action_count": 1,
        "referral_count": 0,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_retirement_contribution_readiness",
            "Retirement contribution-readiness coaching",
            "2026-07-22",
        )
    ]


def test_progress_reads_retirement_income_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_retirement_income_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "## Retirement Income Readiness Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "readiness_status: professional_review_needed\n"
        "next_check_in: '2026-07-22'\n"
        "income_sources:\n"
        "  social_security_estimate_status: sourced\n"
        "  pension_status: needs_plan_document\n"
        "  retirement_account_status: partial\n"
        "rmd_context:\n"
        "  relevance: future\n"
        "boundary_response:\n"
        "  prohibited_request_detected: true\n"
        "milestones:\n"
        "  - name: social_security_claiming_window\n"
        "professional_handoffs:\n"
        "  - type: fiduciary\n"
        "    trigger: Timing decision.\n"
        "    question_to_ask: What should I review before deciding?\n"
        "questions_to_ask:\n"
        "  - What documents should I bring?\n"
        "documents_to_gather:\n"
        "  - Social Security statement\n"
        "data_gaps:\n"
        "  - Pension estimate missing.\n"
        "next_actions:\n"
        "  - label: Gather income-source documents\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations(
            "coach_retirement_income_readiness"
        ),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    retirement = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_retirement_income_readiness"
    )
    assert retirement["title"] == "Retirement income-readiness coaching"
    assert retirement["status"] == "plan_saved"
    assert retirement["outcomes"] == {
        "readiness_status": "professional_review_needed",
        "next_check_in": "2026-07-22",
        "social_security_estimate_status": "sourced",
        "pension_status": "needs_plan_document",
        "retirement_account_status": "partial",
        "rmd_relevance": "future",
        "prohibited_request_detected": True,
        "milestone_count": 1,
        "professional_handoff_count": 1,
        "question_count": 1,
        "document_count": 1,
        "data_gap_count": 1,
        "next_action_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_retirement_income_readiness",
            "Retirement income-readiness coaching",
            "2026-07-22",
        )
    ]


def test_progress_reads_investment_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_investment_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "## Investment Readiness Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "readiness_status: account_funding_ready\n"
        "user_goal:\n"
        "  target_account_type: taxable_brokerage\n"
        "selected_action:\n"
        "  action_type: fund_investment_account\n"
        "  write_status: not_requested\n"
        "boundary:\n"
        "  cash_movement_only: true\n"
        "  no_security_selection: true\n"
        "  professional_handoff_recommended: false\n"
        "candidate_actions:\n"
        "  - action_type: fund_investment_account\n"
        "data_gaps:\n"
        "  - Confirm emergency-fund minimum.\n"
        "next_actions:\n"
        "  - Confirm funding account.\n"
        "monitoring:\n"
        "  next_check_in: '2026-07-22'\n"
        "  review_triggers:\n"
        "    - Cash-flow changes.\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_investment_readiness"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    investment = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_investment_readiness"
    )
    assert investment["title"] == "Investment-readiness coaching"
    assert investment["status"] == "plan_saved"
    assert investment["outcomes"] == {
        "readiness_status": "account_funding_ready",
        "next_check_in": "2026-07-22",
        "target_account_type": "taxable_brokerage",
        "selected_action_type": "fund_investment_account",
        "write_status": "not_requested",
        "cash_movement_only": True,
        "no_security_selection": True,
        "professional_handoff_recommended": False,
        "candidate_action_count": 1,
        "data_gap_count": 1,
        "next_action_count": 1,
        "review_trigger_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_investment_readiness",
            "Investment-readiness coaching",
            "2026-07-22",
        )
    ]


def test_progress_reads_estate_document_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_estate_document_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "## Estate Document Readiness Checklist\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "readiness_status: checklist_ready\n"
        "legal_boundary_acknowledged: true\n"
        "document_inventory:\n"
        "  will:\n"
        "    status: unknown\n"
        "  financial_power_of_attorney:\n"
        "    status: missing\n"
        "  beneficiary_designations:\n"
        "    status: stale\n"
        "beneficiary_review:\n"
        "  accounts_to_review:\n"
        "    - account_type: 401k\n"
        "  mismatch_flags:\n"
        "    - stale_review\n"
        "  user_tasks:\n"
        "    - Check plan-provider beneficiary page.\n"
        "referral_context:\n"
        "  attorney_recommended: true\n"
        "  reasons:\n"
        "    - User asked about trust wording.\n"
        "next_actions:\n"
        "  - Locate current documents.\n"
        "scope_notes:\n"
        "  - Metadata only.\n"
        "next_check_in: '2026-07-22'\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations(
            "coach_estate_document_readiness"
        ),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    estate = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_estate_document_readiness"
    )
    assert estate["title"] == "Estate document-readiness coaching"
    assert estate["status"] == "plan_saved"
    assert estate["outcomes"] == {
        "readiness_status": "checklist_ready",
        "next_check_in": "2026-07-22",
        "legal_boundary_acknowledged": True,
        "document_count": 3,
        "document_status_counts": {"missing": 1, "stale": 1, "unknown": 1},
        "accounts_to_review_count": 1,
        "mismatch_flag_count": 1,
        "beneficiary_task_count": 1,
        "attorney_recommended": True,
        "attorney_reason_count": 1,
        "next_action_count": 1,
        "scope_note_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_estate_document_readiness",
            "Estate document-readiness coaching",
            "2026-07-22",
        )
    ]


def test_progress_reads_financial_plan_intake_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_financial_plan_intake"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "## Financial Planning Snapshot\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "snapshot_status: complete\n"
        "goals:\n"
        "  - name: Fund brokerage account\n"
        "sibling_artifacts:\n"
        "  - skill: coach_investment_readiness\n"
        "planning_sequence:\n"
        "  - next_skill: coach_investment_readiness\n"
        "professional_handoffs:\n"
        "  - type: cfp\n"
        "    reason: Cross-domain planning review.\n"
        "data_gaps:\n"
        "  - Confirm insurance coverage.\n"
        "domain_readiness:\n"
        "  debt: ready\n"
        "  emergency_fund: ready\n"
        "  investment: data_needed\n"
        "  retirement: data_needed\n"
        "  tax: ready\n"
        "monitoring:\n"
        "  next_review_date: '2026-07-22'\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_financial_plan_intake"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    intake = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_financial_plan_intake"
    )
    assert intake["title"] == "Financial planning snapshot"
    assert intake["status"] == "plan_saved"
    assert intake["outcomes"] == {
        "snapshot_status": "complete",
        "next_check_in": "2026-07-22",
        "goal_count": 1,
        "sibling_artifact_count": 1,
        "planning_sequence_count": 1,
        "professional_handoff_count": 1,
        "data_gap_count": 1,
        "domains_ready_count": 3,
        "domains_data_needed_count": 2,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_financial_plan_intake",
            "Financial planning snapshot",
            "2026-07-22",
        )
    ]


def test_progress_reads_risk_insurance_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_risk_insurance_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "# Risk and Insurance Readiness Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "readiness_status: review_recommended\n"
        "next_check_in: '2026-07-22'\n"
        "household_context:\n"
        "  dependents_count: 2\n"
        "  homeowner: yes\n"
        "liquidity_context:\n"
        "  emergency_fund_months: 2.4\n"
        "  essential_monthly_expenses_cents: 420000\n"
        "coverage_inventory:\n"
        "  health:\n"
        "    known: true\n"
        "  disability:\n"
        "    known: false\n"
        "  life:\n"
        "    known: true\n"
        "  property_liability:\n"
        "    known: true\n"
        "risk_flags:\n"
        "  - flag_id: missing_disability_income_context\n"
        "professional_handoffs:\n"
        "  - type: insurance_agent\n"
        "    reason: Review disability and property/liability coverage details.\n"
        "planning_implications:\n"
        "  - Pause aggressive investing until disability income context is known.\n"
        "data_gaps:\n"
        "  - Confirm disability-income benefit period.\n"
        "next_actions:\n"
        "  - label: Find benefits summary\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations("coach_risk_insurance_readiness"),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    risk = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_risk_insurance_readiness"
    )
    assert risk["title"] == "Risk and insurance readiness coaching"
    assert risk["status"] == "plan_saved"
    assert risk["outcomes"] == {
        "readiness_status": "review_recommended",
        "next_check_in": "2026-07-22",
        "dependents_count": 2,
        "emergency_fund_months": 2.4,
        "essential_monthly_expenses_cents": 420000,
        "coverage_inventory_count": 4,
        "known_coverage_count": 3,
        "risk_flag_count": 1,
        "professional_handoff_count": 1,
        "planning_implication_count": 1,
        "data_gap_count": 1,
        "next_action_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_risk_insurance_readiness",
            "Risk and insurance readiness coaching",
            "2026-07-22",
        )
    ]


def test_progress_reads_advisor_handoff_readiness_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_advisor_handoff_readiness"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260622.md").write_text(
        "# Advisor Handoff Readiness Packet\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-06-22T12:00:00Z'\n"
        "handoff_status: handoff_ready\n"
        "next_check_in: '2026-07-22'\n"
        "request_classification:\n"
        "  user_request: Should I buy VOO?\n"
        "  release_mode: referral_handoff\n"
        "  prohibited_response_if_unsupervised: true\n"
        "professional_type:\n"
        "  primary: ria\n"
        "  rationale: Specific securities question.\n"
        "cashnerd_context:\n"
        "  relevant_artifacts:\n"
        "    - coach_investment_readiness:20260622\n"
        "  key_facts:\n"
        "    - User has no supervised advice path.\n"
        "  user_questions:\n"
        "    - Should I buy VOO?\n"
        "handoff_questions:\n"
        "  - Are you acting as a fiduciary?\n"
        "  - How are you compensated?\n"
        "documents_to_bring:\n"
        "  - Account statement\n"
        "disclosures_to_surface:\n"
        "  - scope_boundary\n"
        "  - conflict_of_interest\n"
        "boundary_response:\n"
        "  user_facing_summary: CashNerd can prepare facts, not choose a security.\n"
        "  refused_topics:\n"
        "    - specific ETF recommendation\n"
        "  allowed_help:\n"
        "    - prepare a fiduciary-review packet\n"
        "next_actions:\n"
        "  - label: Schedule a fiduciary review\n"
        "```\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        subject,
        "recommend_skills",
        lambda *_args, **_kwargs: _recommendations(
            "coach_advisor_handoff_readiness"
        ),
    )

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    handoff = next(
        item
        for item in result["data"]["skills"]
        if item["skill"] == "coach_advisor_handoff_readiness"
    )
    assert handoff["title"] == "Advisor handoff readiness"
    assert handoff["status"] == "plan_saved"
    assert handoff["outcomes"] == {
        "handoff_status": "handoff_ready",
        "next_check_in": "2026-07-22",
        "release_mode": "referral_handoff",
        "professional_type": "ria",
        "prohibited_response_if_unsupervised": True,
        "relevant_artifact_count": 1,
        "key_fact_count": 1,
        "user_question_count": 1,
        "handoff_question_count": 2,
        "document_count": 1,
        "disclosure_count": 2,
        "refused_topic_count": 1,
        "allowed_help_count": 1,
        "next_action_count": 1,
    }
    assert result["summary"]["plans_saved"] == 1
    assert result["data"]["next_check_ins"] == [
        _expected_check_in(
            "coach_advisor_handoff_readiness",
            "Advisor handoff readiness",
            "2026-07-22",
        )
    ]


def test_progress_marks_monitoring_when_phase9_marker_exists(tmp_path: Path, monkeypatch) -> None:
    db_path = _init_db(tmp_path)
    store = _store(tmp_path)
    artifact_dir = tmp_path / "artifacts" / "coach_spending_plan"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "20260510.md").write_text(
        "## Spending Plan\n\n"
        "## Generated machine-readable footer\n"
        "```yaml\n"
        "generated_at: '2026-05-10T12:00:00Z'\n"
        "strategy: calendar\n"
        "expected_monthly_income_cents: 500000\n"
        "expected_monthly_expenses_cents: 420000\n"
        "review_cadence: monthly\n"
        "next_review_at: '2026-06-10'\n"
        "```\n",
        encoding="utf-8",
    )
    sessions_dir = tmp_path / "sessions"
    sessions_dir.mkdir()
    (sessions_dir / "2026-06-10.md").write_text(
        "## 09:00\ncoach_spending_plan:phase9_monitor_check_in_2026-06-10\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(subject, "recommend_skills", lambda *_args, **_kwargs: _recommendations())

    with connect(db_path) as conn:
        result = subject.build_coaching_progress(
            conn,
            skill_state_store=store,
            data_dir=tmp_path,
            now=_NOW,
        )

    spending = next(item for item in result["data"]["skills"] if item["skill"] == "coach_spending_plan")
    assert spending["status"] == "monitoring"
    assert spending["phase_progress_pct"] == 100
    assert spending["outcomes"]["next_check_in"] == "2026-06-10"


def test_coaching_progress_tool_classification() -> None:
    assert "coaching_progress" in READ_ONLY_TOOLS
    assert "coaching_progress" in NO_SYNC_TOOLS
    assert "coaching_progress" not in BRIDGE_TOOLS
