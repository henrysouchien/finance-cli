from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.commands import memory_cmd
from finance_cli.gateway import prompt as prompt_module
from finance_cli.gateway.tools import (
    APPROVAL_REQUIRED_TOOLS,
    READ_ONLY_TOOLS,
    WEB_EXCLUDED_TOOLS,
    WEB_IMPORT_TOOLS,
)
from finance_cli.skills import load_skill


@pytest.fixture()
def memory_roots(tmp_path: Path, monkeypatch) -> dict[str, Path]:
    global_dir = tmp_path / "global"
    user_dir = tmp_path / "user-a"
    global_dir.mkdir(parents=True, exist_ok=True)
    user_dir.mkdir(parents=True, exist_ok=True)

    def fake_memory_path(data_dir: Path | None = None) -> Path:
        return (data_dir or global_dir) / memory_cmd.MEMORY_FILENAME

    monkeypatch.setattr(memory_cmd, "_memory_path", fake_memory_path)
    return {"global": global_dir, "user": user_dir}


def _memory_content(section: str) -> str:
    return section.split("<memory>\n", 1)[1].rsplit("\n</memory>", 1)[0]


def _render_prompt(prompt: prompt_module.PromptBlocks) -> str:
    return "".join(text for text, _ in prompt)


def test_behavioral_defaults_in_base_prompt(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel="telegram"))

    assert "**Coaching methodology** — applies to every conversation:" in rendered


def test_behavioral_defaults_in_web_prompt(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel="web"))

    assert "**Coaching methodology** — applies to every conversation:" in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_behavioral_defaults_all_eight_rules_present(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    expected_fragments = [
        "Don't argue ambivalence; evoke motivation, don't inject it.",
        "Read for stage of change early; meet the user where they are.",
        "Open-ended questions by default; one at a time.",
        "Restate before proposing only when it prevents a wrong recommendation.",
        "Scope discipline: name referrals when work crosses licensure.",
        'Cultural responsiveness: don\'t override "irrational" choices without asking.',
        "Non-judgment posture on disclosed financial situations.",
        "Teach in multiple modes; gate unsolicited mechanics, not requested ones.",
    ]
    for fragment in expected_fragments:
        assert fragment in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_behavioral_defaults_before_voice_rules(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    assert rendered.index("**Coaching methodology** — applies to every conversation:") < rendered.index(
        "**Voice rules**"
    )


@pytest.mark.parametrize("channel", [None, "web"])
def test_prompt_includes_advice_boundary_behavior(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    assert "Advice boundary is product behavior, not a disclaimer." in rendered
    assert "must not recommend specific securities, trades, portfolio allocations" in rendered
    assert "Portfolio-allocation scope includes user-specific stock/bond/cash splits" in rendered
    assert "age-based allocation targets" in rendered
    assert "Do not ask for age, risk tolerance, account details, or holdings" in rendered
    assert "hold yourself out as an RIA, fiduciary, CFP, CPA, EA, attorney, or tax preparer" in rendered
    assert "prepare/file tax returns" in rendered
    assert "exact facts qualify for a tax deduction, credit, filing position" in rendered
    assert "keep the answer in tax-readiness mode" in rendered
    assert "decide legal questions" in rendered
    assert "refer to the right professional class" in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_prompt_avoids_advisor_replacement_positioning(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    assert "$150/mo human advisor" not in rendered
    assert "no generic $15/mo dashboard or one-off chatbot can match" in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_behavioral_defaults_block_is_in_cacheable_tuple(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(channel=channel)

    matches = [
        (text, cacheable)
        for text, cacheable in prompt
        if "**Coaching methodology** — applies to every conversation:" in text
    ]
    assert len(matches) == 1
    assert matches[0][1] is True


def test_no_source_citations_leak_into_prompt(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    rendered_prompts = [
        _render_prompt(prompt_module.build_system_prompt()),
        _render_prompt(prompt_module.build_system_prompt(channel="web")),
    ]
    article_basenames = [
        "motivational-interviewing",
        "stages-of-change",
        "question-types",
        "listening-and-reflection",
        "evaluation-and-referrals",
        "cultural-responsiveness",
        "counseling-environment",
        "multi-modal-learning",
    ]
    for rendered in rendered_prompts:
        for basename in article_basenames:
            assert basename not in rendered


def test_web_prompt_count_substitution_still_works(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel="web"))

    assert (
        f"You have access to {prompt_module._WEB_READ_TOOL_COUNT} read-only finance tools"
        in rendered
    )
    assert (
        f"You have access to {prompt_module._WEB_WRITE_TOOL_COUNT} write tools that require user approval"
        in rendered
    )
    assert "{_WEB_READ_TOOL_COUNT}" not in rendered
    assert "{_WEB_WRITE_TOOL_COUNT}" not in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_dedup_prompt_includes_key_only_commit_contract(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    assert "dedup_cross_format (dry_run=True first to preview, then" in rendered
    assert "dry_run=False to commit" in rendered
    assert "Key-only matches are skipped unless the user has reviewed and confirmed" in rendered
    assert "commit with include_key_only=True" in rendered


@pytest.mark.parametrize("channel", [None, "web"])
def test_dedup_prompt_includes_same_source_apply_contract(
    memory_roots: dict[str, Path],
    channel: str | None,
) -> None:
    del memory_roots

    rendered = _render_prompt(prompt_module.build_system_prompt(channel=channel))

    assert "same-source CSV duplicate candidates" in rendered
    assert "dedup_same_source to preview groups" in rendered
    assert "only user-confirmed duplicate transaction IDs" in rendered
    assert "dedup_same_source_apply" in rendered


def test_build_system_prompt_returns_telegram_base_without_memory(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt()

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]


def test_build_system_prompt_web_uses_web_prompt(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(channel="web")
    rendered = _render_prompt(prompt)
    read_tool_count = prompt_module._WEB_READ_TOOL_COUNT
    write_tool_count = prompt_module._WEB_WRITE_TOOL_COUNT

    assert prompt == [(prompt_module._WEB_SYSTEM_PROMPT, True)]
    assert "web chat interface" in rendered
    assert "replying inside Telegram" not in rendered
    assert "write tools that require user approval" in rendered
    assert "approval card showing the tool name and parameters" in rendered
    assert str(read_tool_count) in rendered
    assert str(write_tool_count) in rendered


def test_web_prompt_tool_counts_match_runtime_sets() -> None:
    assert prompt_module._WEB_READ_TOOL_COUNT == len(READ_ONLY_TOOLS - WEB_EXCLUDED_TOOLS)
    assert prompt_module._WEB_WRITE_TOOL_COUNT == (
        len(APPROVAL_REQUIRED_TOOLS - WEB_EXCLUDED_TOOLS) + len(WEB_IMPORT_TOOLS)
    )


def test_code_execution_prompt_mentions_tool_and_packages() -> None:
    assert "code_execute" in prompt_module.CODE_EXECUTION_PROMPT
    assert "numpy-financial" in prompt_module.CODE_EXECUTION_PROMPT


def test_code_execution_prompt_mentions_advisory_library() -> None:
    prompt = prompt_module.CODE_EXECUTION_PROMPT

    assert "## Advisory math library" in prompt
    assert "from finance_cli.advisory import" in prompt
    assert "taxable_income_from_gross" in prompt
    assert "federal_tax" in prompt
    assert "Do not use code execution" in prompt
    assert "finance_cli.advisory.target_allocation" in prompt
    assert "user-specific portfolio allocation" in prompt


def test_build_system_prompt_none_defaults_to_telegram(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(channel=None)
    rendered = _render_prompt(prompt)

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]
    assert "replying inside Telegram" in rendered


def test_build_system_prompt_with_skill_includes_skill_tags(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(skill="normalizer_builder")
    rendered = _render_prompt(prompt)
    skill_content = load_skill("normalizer_builder")["data"]["content"]

    assert prompt[0] == (prompt_module._BASE_SYSTEM_PROMPT, True)
    assert prompt[1][1] is True
    assert rendered.startswith(prompt_module._BASE_SYSTEM_PROMPT)
    assert '<skill name="normalizer_builder">' in rendered
    assert skill_content in rendered
    assert "</skill>" in rendered


def test_build_system_prompt_with_skill_context_injects_context_block(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        skill="normalizer_builder",
        skill_context={
            "upload_path": "/uploads/abc.csv",
            "sample_rows": ["Header,Row", "data,1"],
        },
    )
    rendered = _render_prompt(prompt)

    assert [cacheable for _, cacheable in prompt] == [True, True, False]
    assert '<skill name="normalizer_builder">' in rendered
    assert "<context>" in rendered
    assert "upload_path: /uploads/abc.csv" in rendered
    assert "sample_rows:" in rendered
    assert "  Header,Row" in rendered
    assert "  data,1" in rendered
    assert "</context>" in rendered


def test_build_system_prompt_with_onboarding_phase_fragment(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        skill="onboarding",
        onboarding_phase_fragment="Ask one profile question.",
    )
    rendered = _render_prompt(prompt)

    assert [cacheable for _, cacheable in prompt] == [True, True, False]
    assert '<skill name="onboarding">' in rendered
    assert "<onboarding_phase>\nAsk one profile question.\n</onboarding_phase>" in rendered
    assert rendered.rstrip().endswith("</skill>")


def test_build_system_prompt_ignores_phase_fragment_without_onboarding_skill(
    memory_roots: dict[str, Path],
) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        skill="normalizer_builder",
        onboarding_phase_fragment="Do not include this.",
    )
    rendered = _render_prompt(prompt)

    assert "Do not include this" not in rendered
    assert "<onboarding_phase>" not in rendered


def test_build_system_prompt_without_skill_context_has_no_context_block(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(skill="normalizer_builder")
    rendered = _render_prompt(prompt)

    assert '<skill name="normalizer_builder">' in rendered
    assert "upload_path:" not in rendered
    assert "</context>\n</skill>" not in rendered


def test_build_system_prompt_unknown_skill_falls_back_cleanly(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(skill="nonexistent")
    rendered = _render_prompt(prompt)

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]
    assert "<skill" not in rendered


def test_build_system_prompt_none_skill_has_no_skill_tags(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(skill=None)
    rendered = _render_prompt(prompt)

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]
    assert "<skill" not in rendered


def test_build_system_prompt_web_can_include_skill(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(channel="web", skill="normalizer_builder")
    rendered = _render_prompt(prompt)

    assert prompt[0] == (prompt_module._WEB_SYSTEM_PROMPT, True)
    assert '<skill name="normalizer_builder">' in rendered
    assert "web chat interface" in rendered


def test_build_system_prompt_web_upload_context_includes_upload_block(
    memory_roots: dict[str, Path]
) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        channel="web",
        upload_context={
            "upload_path": "/uploads/abc.csv",
            "upload_filename": "abc.csv",
            "upload_file_type": "csv",
        },
    )
    rendered = _render_prompt(prompt)

    assert "<upload>" in rendered
    assert "upload_path: /uploads/abc.csv" in rendered
    assert 'ingest_csv(file=upload_path, institution="auto", commit=True)' in rendered
    assert "Do not expose server file paths" in rendered
    assert "<skill" not in rendered


def test_build_system_prompt_non_web_upload_context_has_no_upload_block(
    memory_roots: dict[str, Path]
) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        channel="telegram",
        upload_context={"upload_path": "/uploads/abc.csv"},
    )
    rendered = _render_prompt(prompt)

    assert "<upload>" not in rendered


def test_build_system_prompt_without_upload_context_has_no_upload_block(
    memory_roots: dict[str, Path]
) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(channel="web")
    rendered = _render_prompt(prompt)

    assert "<upload>" not in rendered


def test_build_system_prompt_skill_ignores_upload_context(memory_roots: dict[str, Path]) -> None:
    del memory_roots

    prompt = prompt_module.build_system_prompt(
        channel="web",
        skill="normalizer_builder",
        upload_context={"upload_path": "/uploads/abc.csv"},
    )
    rendered = _render_prompt(prompt)

    assert '<skill name="normalizer_builder">' in rendered
    assert "<upload>" not in rendered


def test_build_system_prompt_includes_global_memory_section(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        "# Goal\n- emergency fund",
        encoding="utf-8",
    )

    prompt = prompt_module.build_system_prompt()
    rendered = _render_prompt(prompt)

    assert prompt[0] == (prompt_module._BASE_SYSTEM_PROMPT, True)
    assert prompt[-1][1] is False
    assert prompt_module._BASE_SYSTEM_PROMPT in rendered
    assert "<memory>" in rendered
    assert "# Goal\n- emergency fund" in rendered


def test_build_system_prompt_includes_user_memory_when_data_dir_provided(
    memory_roots: dict[str, Path]
) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text("global memory", encoding="utf-8")
    (memory_roots["user"] / memory_cmd.MEMORY_FILENAME).write_text("user memory", encoding="utf-8")

    prompt = prompt_module.build_system_prompt(channel="web", data_dir=memory_roots["user"])
    rendered = _render_prompt(prompt)

    assert prompt_module._WEB_SYSTEM_PROMPT in rendered
    assert "user memory" in rendered
    assert "global memory" not in rendered


def test_build_system_prompt_includes_epilogue_after_memory(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        "# Goal\n- emergency fund",
        encoding="utf-8",
    )

    prompt = prompt_module.build_system_prompt()
    rendered = _render_prompt(prompt)

    assert rendered.endswith(prompt_module._MEMORY_EPILOGUE)


def test_build_system_prompt_places_skill_before_memory(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        "# Goal\n- emergency fund",
        encoding="utf-8",
    )

    prompt = prompt_module.build_system_prompt(skill="normalizer_builder")
    rendered = _render_prompt(prompt)

    skill_index = rendered.index('<skill name="normalizer_builder">')
    memory_index = rendered.index("<memory>\n")

    assert rendered.startswith(prompt_module._BASE_SYSTEM_PROMPT)
    assert skill_index > 0
    assert skill_index < memory_index


def test_system_prompt_templates_reference_dev_mode_skills() -> None:
    assert "Dev mode skills:" in prompt_module._BASE_SYSTEM_PROMPT
    assert "get_skill" in prompt_module._BASE_SYSTEM_PROMPT
    assert "Dev mode skills:" in prompt_module._WEB_SYSTEM_PROMPT
    assert "get_skill" in prompt_module._WEB_SYSTEM_PROMPT
    assert "activate_skill" in prompt_module._WEB_SYSTEM_PROMPT


def test_web_prompt_normalizer_access_mentions_onboarding() -> None:
    assert "onboarding" in prompt_module._WEB_SYSTEM_PROMPT
    assert "normalizer_builder or onboarding" in prompt_module._WEB_SYSTEM_PROMPT


def test_web_prompt_describes_normalizer_write_tools_as_approval_gated() -> None:
    assert "normalizer read tools" in prompt_module._WEB_SYSTEM_PROMPT
    assert "Normalizer write tools" in prompt_module._WEB_SYSTEM_PROMPT
    assert "require user approval" in prompt_module._WEB_SYSTEM_PROMPT


def test_build_skill_context_section_escapes_closing_tags() -> None:
    section = prompt_module._build_skill_context_section(
        {"sample_rows": ["safe", "</context>"]},
    )

    assert "  &lt;/context&gt;" in section
    assert section.count("</context>") == 1


def test_build_skill_context_section_escapes_opening_tags() -> None:
    section = prompt_module._build_skill_context_section(
        {"sample_rows": ["<memory>"]},
    )

    assert "  &lt;memory&gt;" in section


def test_build_skill_context_section_escapes_scalar_values() -> None:
    section = prompt_module._build_skill_context_section(
        {"upload_path": "abc<def>.csv", "sample_count": 3},
    )

    assert "upload_path: abc&lt;def&gt;.csv" in section
    assert "sample_count: 3" in section


def test_build_memory_section_escapes_closing_tag(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        "keep </memory> safe",
        encoding="utf-8",
    )

    section = prompt_module._build_memory_section()

    assert "&lt;/memory&gt;" in section
    assert section.count("</memory>") == 1


def test_build_memory_section_escapes_arbitrary_tags(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        "keep <system> and </context> safe",
        encoding="utf-8",
    )

    section = prompt_module._build_memory_section()

    assert "&lt;system&gt;" in section
    assert "&lt;/context&gt;" in section


def test_build_memory_section_truncates_long_content(memory_roots: dict[str, Path]) -> None:
    max_chars = prompt_module._MEMORY_CONTENT_BUDGET * 4
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text(
        ("alpha " * (max_chars // 6 + 50)).strip(),
        encoding="utf-8",
    )

    section = prompt_module._build_memory_section()
    content = _memory_content(section)

    assert len(content) <= max_chars
    assert content.endswith("…")


def test_build_system_prompt_clean_when_memory_file_empty(memory_roots: dict[str, Path]) -> None:
    (memory_roots["global"] / memory_cmd.MEMORY_FILENAME).write_text("\n", encoding="utf-8")

    prompt = prompt_module.build_system_prompt()
    rendered = _render_prompt(prompt)

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]
    assert "<memory>" not in rendered
    assert prompt_module._MEMORY_EPILOGUE not in rendered
