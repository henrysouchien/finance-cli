from __future__ import annotations

from pathlib import Path

import pytest

from finance_cli.commands import memory_cmd
from finance_cli.gateway import prompt as prompt_module


@pytest.fixture()
def memory_path(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / "agent_memory.md"
    monkeypatch.setattr(memory_cmd, "_memory_path", lambda data_dir=None: path)
    return path


def _memory_content(section: str) -> str:
    return section.split("<memory>\n", 1)[1].rsplit("\n</memory>", 1)[0]


def _render_prompt(prompt: prompt_module.PromptBlocks) -> str:
    return "".join(text for text, _ in prompt)


def test_memory_budget_updated() -> None:
    assert prompt_module._MEMORY_TOKEN_BUDGET == 700
    assert prompt_module._MEMORY_CONTENT_BUDGET == 700 - (prompt_module._MEMORY_FRAME_CHARS // 4 + 1)


def test_build_memory_section_empty_when_no_file(memory_path: Path) -> None:
    assert not memory_path.exists()
    assert prompt_module._build_memory_section() == ""


def test_build_memory_section_wraps_content(memory_path: Path) -> None:
    content = "# Preferences\n- dining: 400"
    memory_path.write_text(content, encoding="utf-8")

    assert prompt_module._build_memory_section() == (
        f"\n\n{prompt_module._MEMORY_PREAMBLE}\n<memory>\n{content}\n</memory>"
    )


def test_build_memory_section_escapes_closing_tag(memory_path: Path) -> None:
    memory_path.write_text("keep </memory> safe", encoding="utf-8")

    section = prompt_module._build_memory_section()

    assert "&lt;/memory&gt;" in section
    assert section.count("</memory>") == 1


def test_build_memory_section_trims_long_content(memory_path: Path) -> None:
    max_chars = prompt_module._MEMORY_CONTENT_BUDGET * 4
    memory_path.write_text(("alpha " * (max_chars // 6 + 50)).strip(), encoding="utf-8")

    section = prompt_module._build_memory_section()
    content = _memory_content(section)

    assert len(content) <= max_chars
    assert content.endswith("…")


def test_build_memory_section_prepends_compaction_warning_near_capacity(memory_path: Path) -> None:
    max_chars = prompt_module._MEMORY_CONTENT_BUDGET * 4
    content = ("alpha " * (max_chars // 6 + 50)).strip()[: max_chars - 10]
    memory_path.write_text(content, encoding="utf-8")

    section = prompt_module._build_memory_section()
    section_content = _memory_content(section)

    assert section_content.startswith(prompt_module._COMPACTION_WARNING)
    assert len(section_content) <= max_chars
    assert section_content.endswith("…")


def test_build_memory_section_read_error_returns_empty(monkeypatch) -> None:
    class BrokenPath:
        def exists(self) -> bool:
            return True

        def read_text(self, encoding: str = "utf-8") -> str:
            del encoding
            raise OSError("boom")

    monkeypatch.setattr(memory_cmd, "_memory_path", lambda data_dir=None: BrokenPath())

    assert prompt_module._build_memory_section() == ""


def test_build_system_prompt_includes_memory_and_epilogue(memory_path: Path) -> None:
    memory_path.write_text("# Goal\n- emergency fund", encoding="utf-8")

    prompt = prompt_module.build_system_prompt()
    rendered = _render_prompt(prompt)

    assert prompt[0] == (prompt_module._BASE_SYSTEM_PROMPT, True)
    assert prompt[-1][1] is False
    assert prompt_module._BASE_SYSTEM_PROMPT in rendered
    assert "<memory>" in rendered
    assert "# Goal\n- emergency fund" in rendered
    assert rendered.endswith(prompt_module._MEMORY_EPILOGUE)


def test_build_system_prompt_clean_when_no_memory(memory_path: Path) -> None:
    memory_path.write_text("\n", encoding="utf-8")

    prompt = prompt_module.build_system_prompt()
    rendered = _render_prompt(prompt)

    assert prompt == [(prompt_module._BASE_SYSTEM_PROMPT, True)]
    assert "<memory>" not in rendered
    assert prompt_module._MEMORY_EPILOGUE not in rendered


def test_base_system_prompt_mentions_session_memory_tools() -> None:
    assert "agent_session_write" in prompt_module._BASE_SYSTEM_PROMPT
    assert "agent_session_search / agent_session_read" in prompt_module._BASE_SYSTEM_PROMPT
    assert "When memory gets full, consolidate" in prompt_module._BASE_SYSTEM_PROMPT
