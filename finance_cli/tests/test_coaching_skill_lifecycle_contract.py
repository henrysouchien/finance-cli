from __future__ import annotations

import re
from pathlib import Path

from finance_cli.coaching_progress import COACHING_SKILLS, _TOTAL_PHASES
from finance_cli.skill_constants import NON_ACTIVATABLE_SKILLS
from finance_cli.skills import SKILL_FILES

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SKILLS_DIR = _REPO_ROOT / "docs" / "skills"


def _skill_markdown(skill: str) -> str:
    return (_SKILLS_DIR / SKILL_FILES[skill]).read_text(encoding="utf-8")


def _phase_headings(markdown: str) -> set[int]:
    return {int(match) for match in re.findall(r"^## Phase (\d+):", markdown, flags=re.MULTILINE)}


def _marker_phases(markdown: str, skill: str) -> set[int]:
    pattern = re.compile(
        r'agent_session_write\("' + re.escape(skill) + r':phase(?P<phase>\d+)_[^"]+"\)'
    )
    return {int(match.group("phase")) for match in pattern.finditer(markdown)}


def test_core_coaching_skill_playbooks_cover_progress_contract() -> None:
    expected_phases = set(range(_TOTAL_PHASES))

    for skill in COACHING_SKILLS:
        markdown = _skill_markdown(skill)

        assert f'skill_state_get("{skill}")' in markdown, skill
        assert f'skill_state_set("{skill}"' in markdown, skill
        assert _phase_headings(markdown) == expected_phases, skill
        assert _marker_phases(markdown, skill) == expected_phases, skill


def test_retirement_contribution_readiness_is_session_start_only() -> None:
    assert "coach_retirement_contribution_readiness" in COACHING_SKILLS
    assert "coach_retirement_contribution_readiness" in NON_ACTIVATABLE_SKILLS


def test_retirement_income_readiness_is_session_start_only() -> None:
    assert "coach_retirement_income_readiness" in COACHING_SKILLS
    assert "coach_retirement_income_readiness" in NON_ACTIVATABLE_SKILLS


def test_estate_document_readiness_is_session_start_only() -> None:
    assert "coach_estate_document_readiness" in COACHING_SKILLS
    assert "coach_estate_document_readiness" in NON_ACTIVATABLE_SKILLS


def test_financial_plan_intake_is_session_start_only() -> None:
    assert "coach_financial_plan_intake" in COACHING_SKILLS
    assert "coach_financial_plan_intake" in NON_ACTIVATABLE_SKILLS


def test_risk_insurance_readiness_is_session_start_only() -> None:
    assert "coach_risk_insurance_readiness" in COACHING_SKILLS
    assert "coach_risk_insurance_readiness" in NON_ACTIVATABLE_SKILLS


def test_advisor_handoff_readiness_is_session_start_only() -> None:
    assert "coach_advisor_handoff_readiness" in COACHING_SKILLS
    assert "coach_advisor_handoff_readiness" in NON_ACTIVATABLE_SKILLS
