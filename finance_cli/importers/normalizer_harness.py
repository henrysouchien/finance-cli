"""Subprocess runner for untrusted user-generated CSV normalizers."""

from __future__ import annotations

import argparse
import builtins
import contextlib
import csv
import decimal
import io
import json
import re
import sys
import types
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

try:
    from .normalizer_sandbox import ALLOWED_IMPORTS, ModuleMetadata, validate_normalizer_source
except ImportError:
    from normalizer_sandbox import ALLOWED_IMPORTS, ModuleMetadata, validate_normalizer_source


@dataclass
class NormalizeResult:
    # Keep this in sync with finance_cli.importers.csv_normalizers.NormalizeResult.
    rows: list[dict[str, str]]
    source_name: str
    warnings: list[str]
    raw_row_count: int
    skipped_row_count: int


def _row_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            return str(row[key]).strip()
    return ""


def _parse_amount(value: str) -> Decimal:
    cleaned = value.strip().replace("$", "").replace(",", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return Decimal(cleaned)
    except InvalidOperation as exc:
        raise ValueError(f"invalid amount '{value}'") from exc


def _format_amount(amount: Decimal) -> str:
    quantized = amount.quantize(Decimal("0.01"))
    if quantized == Decimal("-0.00"):
        quantized = Decimal("0.00")
    return f"{quantized:.2f}"


def _extract_card_ending(line: str) -> str:
    account_match = re.search(r"account\s*number\s*:?\s*(.+)", line, flags=re.IGNORECASE)
    if account_match:
        digits = re.sub(r"\D", "", account_match.group(1))
        if len(digits) >= 4:
            return digits[-4:]

    masked_match = re.search(r"[Xx*]{4,}\s*(\d{4})", line)
    if masked_match:
        return masked_match.group(1)

    return ""


def _safe_io_module() -> types.ModuleType:
    module = types.ModuleType("io")
    module.StringIO = io.StringIO
    return module


def _safe_import(name: str, globals=None, locals=None, fromlist=(), level: int = 0):
    if level:
        raise ImportError("relative imports are not allowed")
    root = name.split(".", 1)[0]
    if root not in ALLOWED_IMPORTS:
        raise ImportError(f"import '{root}' is not allowlisted")
    if root == "io":
        return _safe_io_module()
    return builtins.__import__(name, globals, locals, fromlist, level)


def _safe_builtins() -> dict[str, object]:
    allowed_names = {
        "ArithmeticError",
        "AssertionError",
        "AttributeError",
        "BaseException",
        "EOFError",
        "Exception",
        "False",
        "ImportError",
        "IndexError",
        "KeyError",
        "LookupError",
        "NameError",
        "None",
        "NotImplemented",
        "NotImplementedError",
        "OSError",
        "OverflowError",
        "RuntimeError",
        "StopIteration",
        "True",
        "TypeError",
        "ValueError",
        "ZeroDivisionError",
        "abs",
        "all",
        "any",
        "bool",
        "bytes",
        "callable",
        "chr",
        "classmethod",
        "complex",
        "dict",
        "divmod",
        "enumerate",
        "filter",
        "float",
        "format",
        "frozenset",
        "hasattr",
        "hash",
        "hex",
        "id",
        "int",
        "isinstance",
        "issubclass",
        "iter",
        "len",
        "list",
        "map",
        "max",
        "min",
        "next",
        "object",
        "oct",
        "ord",
        "pow",
        "print",
        "property",
        "range",
        "repr",
        "reversed",
        "round",
        "set",
        "slice",
        "sorted",
        "staticmethod",
        "str",
        "sum",
        "super",
        "tuple",
        "type",
        "zip",
    }
    safe = {name: getattr(builtins, name) for name in allowed_names if hasattr(builtins, name)}
    safe["__build_class__"] = builtins.__build_class__
    safe["__import__"] = _safe_import
    return safe


def _module_globals() -> dict[str, object]:
    return {
        "__builtins__": _safe_builtins(),
        "__doc__": None,
        "__name__": "__normalizer__",
        "Decimal": Decimal,
        "InvalidOperation": InvalidOperation,
        "NormalizeResult": NormalizeResult,
        "_extract_card_ending": _extract_card_ending,
        "_format_amount": _format_amount,
        "_parse_amount": _parse_amount,
        "_row_value": _row_value,
        "csv": csv,
        "decimal": decimal,
        "io": _safe_io_module(),
        "re": re,
    }


def _load_module(module_path: Path) -> tuple[types.ModuleType, ModuleMetadata]:
    source = module_path.read_text(encoding="utf-8")
    metadata = validate_normalizer_source(source, filename=str(module_path))
    module = types.ModuleType("__normalizer__")
    module.__dict__.update(_module_globals())
    compiled = compile(source, str(module_path), "exec")
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        exec(compiled, module.__dict__, module.__dict__)
    return module, metadata


def _normalize_result_payload(value: object, *, source_name: str) -> dict[str, object]:
    if isinstance(value, NormalizeResult):
        payload = asdict(value)
    elif is_dataclass(value):
        payload = asdict(value)
    elif isinstance(value, dict):
        payload = dict(value)
    else:
        raise TypeError("normalize() must return NormalizeResult or a compatible dict")

    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise TypeError("NormalizeResult.rows must be a list")

    normalized_rows: list[dict[str, str]] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            raise TypeError("each normalized row must be a dict")
        row = {
            str(key): "" if value is None else str(value)
            for key, value in raw_row.items()
            if key is not None and str(key).lower() != "source"
        }
        row["Source"] = source_name
        normalized_rows.append(row)

    warnings = payload.get("warnings", [])
    if not isinstance(warnings, list):
        raise TypeError("NormalizeResult.warnings must be a list")

    return {
        "rows": normalized_rows,
        "source_name": source_name,
        "warnings": [str(item) for item in warnings],
        "raw_row_count": int(payload.get("raw_row_count", 0)),
        "skipped_row_count": int(payload.get("skipped_row_count", 0)),
    }


def _read_lines_from_stdin() -> list[str]:
    payload = json.loads(sys.stdin.read() or "[]")
    if not isinstance(payload, list):
        raise TypeError("stdin payload must be a JSON list of strings")
    return [str(item) for item in payload]


def _run_detect(module_path: Path) -> dict[str, bool]:
    module, _metadata = _load_module(module_path)
    detect_fn = getattr(module, "detect", None)
    if not callable(detect_fn):
        raise TypeError("module missing callable detect()")
    lines = _read_lines_from_stdin()
    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        match = detect_fn(lines)
    return {"match": bool(match)}


def _run_normalize(module_path: Path, file_path: Path) -> dict[str, object]:
    module, metadata = _load_module(module_path)
    normalize_fn = getattr(module, "normalize", None)
    if not callable(normalize_fn):
        raise TypeError("module missing callable normalize()")

    with file_path.open("r", encoding="utf-8-sig", newline="") as fh:
        lines = fh.readlines()

    sink = io.StringIO()
    with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
        result = normalize_fn(lines, file_path.name)
    return _normalize_result_payload(result, source_name=metadata.source_name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--detect", action="store_true", help="run detect(lines) using stdin JSON")
    mode.add_argument("--normalize", action="store_true", help="run normalize(lines, file_name)")
    parser.add_argument("module_path", help="Path to the user-generated normalizer module")
    parser.add_argument("file_path", nargs="?", help="CSV file path for --normalize mode")
    args = parser.parse_args(argv)

    module_path = Path(args.module_path).expanduser().resolve()
    if args.normalize and not args.file_path:
        parser.error("file_path is required for --normalize")

    try:
        if args.detect:
            payload = _run_detect(module_path)
        else:
            payload = _run_normalize(module_path, Path(args.file_path).expanduser().resolve())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(json.dumps(payload, indent=None, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
