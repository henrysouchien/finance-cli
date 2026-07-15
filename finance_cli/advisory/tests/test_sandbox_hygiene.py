import ast
import pathlib

import pytest


ADVISORY_DIR = pathlib.Path(__file__).parent.parent

ALLOWED_ROOTS = frozenset({
    "decimal", "dataclasses", "typing", "enum", "math", "itertools",
    "functools", "collections", "__future__",
})
ALLOWED_PREFIXES = ("finance_cli.advisory",)

FORBIDDEN_ROOTS = frozenset({
    "os", "sys", "pathlib", "subprocess", "socket", "ssl", "http",
    "urllib", "requests", "httpx", "sqlite3", "psycopg2",
    "importlib", "ctypes", "pickle", "shelve", "shutil", "tempfile",
    "threading", "asyncio", "multiprocessing",
})

FORBIDDEN_NAMES = frozenset({
    "open", "exec", "eval", "compile", "__import__",
})


def _runtime_py_files() -> list[pathlib.Path]:
    return [p for p in ADVISORY_DIR.rglob("*.py") if "/tests/" not in str(p)]


def _assert_module_allowed(py_file: pathlib.Path, module_name: str) -> None:
    root = module_name.split(".")[0]
    assert root not in FORBIDDEN_ROOTS, (
        f"{py_file}: import of {module_name!r} is explicitly forbidden "
        f"(root {root!r} in FORBIDDEN_ROOTS)"
    )
    if root in ALLOWED_ROOTS:
        return
    if any(module_name == p or module_name.startswith(p + ".") for p in ALLOWED_PREFIXES):
        return
    raise AssertionError(
        f"{py_file}: import of {module_name!r} is not in the advisory allowlist. "
        f"Allowed roots: {sorted(ALLOWED_ROOTS)}; allowed prefixes: {ALLOWED_PREFIXES}"
    )


@pytest.mark.parametrize("py_file", _runtime_py_files(), ids=str)
def test_advisory_file_imports_are_safe(py_file: pathlib.Path) -> None:
    tree = ast.parse(py_file.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _assert_module_allowed(py_file, alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.level > 0:
                continue
            _assert_module_allowed(py_file, node.module or "")
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            raise AssertionError(
                f"{py_file}: reference to forbidden builtin {node.id!r}"
            )


@pytest.mark.parametrize("py_file", _runtime_py_files(), ids=str)
def test_advisory_no_toplevel_side_effects(py_file: pathlib.Path) -> None:
    """Advisory runtime files may only contain at module scope:
    - imports (checked above),
    - class defs, function defs,
    - docstring (a bare string literal as the first statement),
    - constant/dataclass assignments (no calls that do I/O — already enforced by imports),
    - type alias / annotated assignments.

    Raw top-level Expr nodes other than the module docstring are rejected —
    this catches mistakes like `print(...)` or `open(...)` at import time.
    """
    tree = ast.parse(py_file.read_text())
    for idx, node in enumerate(tree.body):
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef,
                              ast.AsyncFunctionDef, ast.ClassDef,
                              ast.Assign, ast.AnnAssign, ast.AugAssign)):
            continue
        if isinstance(node, ast.Expr):
            if idx == 0 and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                continue
            raise AssertionError(
                f"{py_file}: top-level bare expression at position {idx} is not a docstring; "
                f"got {ast.dump(node.value)!r}"
            )
        raise AssertionError(
            f"{py_file}: top-level statement of type {type(node).__name__} is not allowed"
        )
