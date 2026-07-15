from __future__ import annotations

import re


# Catalog patterns use the canonical letter-dash-number convention (D-1, C-5, T-2, ...).
# Skill-coupled patterns use semantic snake_case identifiers under a deeper heading
# (##### dti_threshold_36, etc.) — they're entry surfaces for multi-phase coaching
# skills rather than one-shot recommendations, so they don't fit the catalog ID scheme.
_CATALOG_PATTERN_RE = re.compile(r"^#### ([A-Z]-\d+):", re.MULTILINE)
_SECTION_HEADING_RE = re.compile(r"^#{1,4}\s+")
_SKILL_COUPLED_INTRO_RE = re.compile(r"\bpatterns below\b.*\bskill\b", re.IGNORECASE)
_SKILL_COUPLED_PATTERN_RE = re.compile(r"^##### ([a-z][a-z0-9_]*)\s*$")


def _is_pattern_body(lines: list[str]) -> bool:
    return any(line.startswith("- **Move:**") for line in lines) and any(
        line.startswith("- **Trigger:**") for line in lines
    )


def _extract_skill_coupled_pattern_ids(markdown_text: str) -> set[str]:
    lines = markdown_text.splitlines()
    ids: set[str] = set()
    in_skill_coupled_section = False
    index = 0

    while index < len(lines):
        line = lines[index]
        if _SECTION_HEADING_RE.match(line):
            in_skill_coupled_section = False
        if _SKILL_COUPLED_INTRO_RE.search(line):
            in_skill_coupled_section = True
            index += 1
            continue

        match = _SKILL_COUPLED_PATTERN_RE.match(line)
        if match is None or not in_skill_coupled_section:
            index += 1
            continue

        body_start = index + 1
        body_end = body_start
        while body_end < len(lines) and not lines[body_end].startswith("#"):
            body_end += 1
        if _is_pattern_body(lines[body_start:body_end]):
            ids.add(match.group(1))
        index = body_end

    return ids


def extract_pattern_ids(markdown_text: str) -> set[str]:
    return set(_CATALOG_PATTERN_RE.findall(markdown_text)) | _extract_skill_coupled_pattern_ids(
        markdown_text
    )
