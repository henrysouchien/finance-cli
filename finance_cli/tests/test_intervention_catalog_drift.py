from __future__ import annotations

from finance_cli.config import PROJECT_ROOT
import finance_cli.interventions  # noqa: F401
from finance_cli.interventions.catalog_drift import extract_pattern_ids
from finance_cli.interventions.registry import (
    CFP_TAXONOMY_REVIEWED_AT,
    CFPDomain,
    CFPProcessStep,
    PATTERN_REGISTRY,
)
import yaml


PLAYBOOK_PATH = PROJECT_ROOT / "docs" / "COACHING_PLAYBOOK.md"
TAG_SIDECAR_PATH = PROJECT_ROOT / "docs" / "COACHING_PLAYBOOK_TAGS.yaml"


def test_extract_pattern_ids_matches_heading_format() -> None:
    markdown = """
#### D-1: APR avalanche recommendation
- copy
#### C-5: Buffer health check
- copy
"""
    assert extract_pattern_ids(markdown) == {"D-1", "C-5"}


def test_extract_pattern_ids_ignores_level_five_notes_outside_skill_sections() -> None:
    markdown = """
#### D-1: APR avalanche recommendation
- **Move:** Prescribe

The patterns below are entry surfaces for the multi-phase `coach_debt_payoff` skill.

##### minimum_only_payments
- **Move:** Diagnose
- **Trigger:** Minimum-only payments for several months

#### C-5: Buffer health check
- **Move:** Warn

##### notes
This heading is just local explanation, not a semantic intervention ID.
"""

    assert extract_pattern_ids(markdown) == {
        "D-1",
        "C-5",
        "minimum_only_payments",
    }


def test_catalog_matches_playbook_exactly() -> None:
    code_ids = set(PATTERN_REGISTRY.keys())
    playbook_text = PLAYBOOK_PATH.read_text(encoding="utf-8")
    playbook_ids = extract_pattern_ids(playbook_text)

    assert code_ids == playbook_ids, (
        f"Pattern registry/playbook drift. "
        f"Only in code: {code_ids - playbook_ids}; only in playbook: {playbook_ids - code_ids}"
    )


def test_cfp_tag_sidecar_matches_playbook_and_registry() -> None:
    playbook_ids = extract_pattern_ids(PLAYBOOK_PATH.read_text(encoding="utf-8"))
    sidecar = yaml.safe_load(TAG_SIDECAR_PATH.read_text(encoding="utf-8"))

    assert sidecar["version"] == 1
    assert sidecar["cfp_taxonomy_reviewed_at"] == CFP_TAXONOMY_REVIEWED_AT

    sidecar_patterns = sidecar["patterns"]
    assert set(sidecar_patterns) == playbook_ids, (
        f"CFP tag sidecar/playbook drift. "
        f"Only in sidecar: {set(sidecar_patterns) - playbook_ids}; "
        f"only in playbook: {playbook_ids - set(sidecar_patterns)}"
    )

    for pattern_id, sidecar_entry in sidecar_patterns.items():
        assert isinstance(sidecar_entry["cfp_applicable"], bool), pattern_id
        assert isinstance(sidecar_entry["cfp_domains"], list), pattern_id
        assert isinstance(sidecar_entry["cfp_steps"], list), pattern_id
        assert isinstance(sidecar_entry["rationale"], str), pattern_id
        assert sidecar_entry["rationale"].strip(), pattern_id

        sidecar_domains = tuple(CFPDomain(value) for value in sidecar_entry["cfp_domains"])
        sidecar_steps = tuple(CFPProcessStep(value) for value in sidecar_entry["cfp_steps"])

        if sidecar_entry["cfp_applicable"]:
            assert sidecar_domains, pattern_id
        else:
            assert not sidecar_domains, pattern_id
        assert sidecar_steps, pattern_id

        registered_pattern = PATTERN_REGISTRY[pattern_id]
        assert sidecar_domains == registered_pattern.cfp_domains, pattern_id
        assert sidecar_steps == registered_pattern.cfp_steps, pattern_id


def test_catalog_drift_set_math_catches_extra_and_missing() -> None:
    playbook_ids = {"D-1", "C-1", "C-5", "T-2", "I-1"}
    code_ids = {"D-1", "C-1", "C-5", "T-2", "Z-99"}

    extra_in_code = code_ids - playbook_ids
    missing = {"I-1"} - code_ids

    assert extra_in_code == {"Z-99"}
    assert missing == {"I-1"}
