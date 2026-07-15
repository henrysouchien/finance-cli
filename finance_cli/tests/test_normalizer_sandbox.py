from __future__ import annotations

import importlib.util
import sys
from textwrap import indent
from pathlib import Path

import pytest

from finance_cli.exceptions import ValidationError as PackageValidationError
from finance_cli.importers.normalizer_sandbox import ModuleMetadata, SandboxViolation, validate_normalizer_source


_DEFAULT_NORMALIZE_BODY = """
reader = csv.DictReader(io.StringIO("".join(lines)))
rows = []
for row in reader:
    rows.append(
        {
            "Date": row["Date"],
            "Description": row["Description"],
            "Amount": row["Amount"],
            "Account Type": "checking",
            "Source": SOURCE_NAME,
        }
    )
return NormalizeResult(
    rows=rows,
    source_name=SOURCE_NAME,
    warnings=[],
    raw_row_count=len(rows),
    skipped_row_count=0,
)
""".strip()


def _module_source(
    *,
    primary_key: str | None = "demo_bank",
    aliases: tuple[str, ...] = ("demo",),
    source_name: str | None = "Demo Bank",
    imports: str = "import csv\nimport io\nfrom decimal import Decimal",
    detect_body: str | None = 'return any("Demo Header" in line for line in lines)',
    normalize_body: str | None = _DEFAULT_NORMALIZE_BODY,
) -> str:
    parts: list[str] = []
    if primary_key is not None:
        parts.append(f"PRIMARY_KEY = {primary_key!r}")
    parts.append(f"ALIASES = {[alias for alias in aliases]!r}")
    if source_name is not None:
        parts.append(f"SOURCE_NAME = {source_name!r}")
    if imports:
        parts.append(imports)
    if detect_body is not None:
        parts.append(f"def detect(lines):\n{indent(detect_body, '    ')}")
    if normalize_body is not None:
        parts.append(f"def normalize(lines, file_name):\n{indent(normalize_body, '    ')}")
    return "\n\n".join(parts) + "\n"


VALID_SOURCE = _module_source()


def _load_module_from_path(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        sys.modules.pop(module_name, None)


def test_validate_normalizer_source_accepts_valid_module() -> None:
    metadata = validate_normalizer_source(VALID_SOURCE)

    assert metadata == ModuleMetadata(
        primary_key="demo_bank",
        aliases=["demo"],
        source_name="Demo Bank",
    )


@pytest.mark.parametrize(
    ("snippet", "pattern"),
    [
        ("from csv import *\n", "star imports are prohibited"),
        ("import dataclasses\n", r"prohibited import 'dataclasses'"),
        ("from os import path\n", r"prohibited import 'os'"),
        ("import json\n", r"import 'json' is not allowlisted"),
        ("import pickle\n", r"prohibited import 'pickle'"),
    ],
)
def test_validate_normalizer_source_rejects_disallowed_imports(snippet: str, pattern: str) -> None:
    with pytest.raises(SandboxViolation, match=pattern):
        validate_normalizer_source(VALID_SOURCE + snippet)


@pytest.mark.parametrize("builtin_name", ["open", "getattr", "setattr", "vars", "dir"])
def test_validate_normalizer_source_rejects_banned_builtins(builtin_name: str) -> None:
    calls = {
        "dir": "dir(lines)",
        "open": "open(file_name)",
        "getattr": 'getattr(lines, "append")',
        "setattr": 'setattr(lines, "blocked", "value")',
        "vars": "vars(lines)",
    }
    source = _module_source(normalize_body=f"{calls[builtin_name]}\n\n{_DEFAULT_NORMALIZE_BODY}")

    with pytest.raises(SandboxViolation, match=rf"prohibited builtin '{builtin_name}\(\)'"):
        validate_normalizer_source(source)


@pytest.mark.parametrize("method_name", ["read_text", "write_bytes"])
def test_validate_normalizer_source_rejects_banned_path_methods(method_name: str) -> None:
    source = _module_source(normalize_body=f"path.{method_name}()\n\n{_DEFAULT_NORMALIZE_BODY}")

    with pytest.raises(SandboxViolation, match=rf"prohibited attribute call '\.{method_name}\(\)'"):
        validate_normalizer_source(source)


@pytest.mark.parametrize(
    ("source", "pattern"),
    [
        (_module_source(detect_body=None), r"missing required detect\(\) function"),
        (_module_source(normalize_body=None), r"missing required normalize\(\) function"),
        (_module_source(primary_key=None), r"PRIMARY_KEY must be a non-empty string literal"),
        (_module_source(source_name=None), r"SOURCE_NAME must be a non-empty string literal"),
    ],
)
def test_validate_normalizer_source_requires_metadata_and_entrypoints(source: str, pattern: str) -> None:
    with pytest.raises(SandboxViolation, match=pattern):
        validate_normalizer_source(source)


def test_validate_normalizer_source_rejects_syntax_error() -> None:
    with pytest.raises(SandboxViolation, match=r"expected ':'"):
        validate_normalizer_source("def detect(lines)\n    return True\n")


def test_validate_normalizer_source_rejects_dunder_and_type_patterns() -> None:
    dunder_source = VALID_SOURCE + "\nvalue = reader.__class__\n"
    type_source = VALID_SOURCE + "\nDemo = type('Demo', (), {})\n"

    with pytest.raises(SandboxViolation, match="dunder attribute access '__class__'"):
        validate_normalizer_source(dunder_source)
    with pytest.raises(SandboxViolation, match=r"type\(\) with three arguments"):
        validate_normalizer_source(type_source)


def test_normalizer_sandbox_standalone_import() -> None:
    module_path = Path(__file__).resolve().parents[1] / "importers" / "normalizer_sandbox.py"
    module = _load_module_from_path("standalone_normalizer_sandbox", module_path)

    assert issubclass(module.ValidationError, ValueError)
    assert module.ValidationError is not PackageValidationError
