"""Static analysis gate for untrusted user-generated CSV normalizers."""

from __future__ import annotations

import ast
from dataclasses import dataclass

try:
    from ..exceptions import ValidationError
except (ImportError, SystemError):
    class ValidationError(ValueError):
        """Standalone fallback when the package hierarchy is unavailable."""

        pass

ALLOWED_IMPORTS = {"csv", "decimal", "io", "re"}
BANNED_IMPORTS = {
    "code",
    "codeop",
    "ctypes",
    "dataclasses",
    "http",
    "importlib",
    "logging",
    "marshal",
    "os",
    "pathlib",
    "pickle",
    "shutil",
    "socket",
    "subprocess",
    "sys",
    "urllib",
}
BANNED_BUILTINS = {
    "__import__",
    "breakpoint",
    "compile",
    "delattr",
    "dir",
    "eval",
    "exec",
    "getattr",
    "globals",
    "locals",
    "open",
    "setattr",
    "vars",
}
BANNED_PATH_METHODS = {
    "mkdir",
    "open",
    "read_bytes",
    "read_text",
    "rename",
    "replace",
    "rmdir",
    "touch",
    "unlink",
    "write_bytes",
    "write_text",
}
ALLOWED_DUNDERS = {"__doc__", "__name__"}


@dataclass(frozen=True)
class ModuleMetadata:
    primary_key: str
    aliases: list[str]
    source_name: str


class SandboxViolation(ValidationError):
    """Raised when a user-generated normalizer violates the sandbox policy."""


def _line_col(node: ast.AST) -> str:
    line = getattr(node, "lineno", 0)
    col = getattr(node, "col_offset", 0) + 1
    return f"{line}:{col}"


class _SandboxAnalyzer(ast.NodeVisitor):
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.errors: list[str] = []

    def _error(self, node: ast.AST, message: str) -> None:
        self.errors.append(f"{self.filename}:{_line_col(node)}: {message}")

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            root = alias.name.split(".", 1)[0]
            if root in BANNED_IMPORTS:
                self._error(node, f"prohibited import '{root}'")
            elif root not in ALLOWED_IMPORTS:
                self._error(node, f"import '{root}' is not allowlisted")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if any(alias.name == "*" for alias in node.names):
            self._error(node, "star imports are prohibited")
        root = (node.module or "").split(".", 1)[0]
        if root in BANNED_IMPORTS:
            self._error(node, f"prohibited import '{root}'")
        elif root not in ALLOWED_IMPORTS:
            self._error(node, f"import '{root}' is not allowlisted")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name):
            if func.id in BANNED_BUILTINS:
                self._error(node, f"prohibited builtin '{func.id}()'")
            if func.id == "type" and len(node.args) == 3:
                self._error(node, "type() with three arguments is prohibited")
        if isinstance(func, ast.Attribute) and func.attr in BANNED_PATH_METHODS:
            self._error(node, f"prohibited attribute call '.{func.attr}()'")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr.startswith("__") and node.attr.endswith("__") and node.attr not in ALLOWED_DUNDERS:
            self._error(node, f"dunder attribute access '{node.attr}' is prohibited")
        self.generic_visit(node)


def _extract_assignment_value(node: ast.Assign | ast.AnnAssign) -> ast.AST:
    if isinstance(node, ast.Assign):
        return node.value
    return node.value


def extract_module_metadata(tree: ast.AST, *, filename: str = "<string>") -> ModuleMetadata:
    values: dict[str, object] = {}
    has_detect = False
    has_normalize = False

    for node in getattr(tree, "body", []):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == "detect":
                has_detect = True
            elif node.name == "normalize":
                has_normalize = True
            continue

        target_names: list[str] = []
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    target_names.append(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_names.append(node.target.id)
        else:
            continue

        interested = [name for name in target_names if name in {"PRIMARY_KEY", "ALIASES", "SOURCE_NAME"}]
        if not interested:
            continue

        value_node = _extract_assignment_value(node)
        if value_node is None:
            raise SandboxViolation(f"{filename}:{_line_col(node)}: missing value for metadata assignment")
        try:
            literal_value = ast.literal_eval(value_node)
        except Exception as exc:
            raise SandboxViolation(
                f"{filename}:{_line_col(node)}: metadata values must be literals"
            ) from exc

        for name in interested:
            if name in values:
                raise SandboxViolation(f"{filename}:{_line_col(node)}: duplicate {name} assignment")
            values[name] = literal_value

    if not has_detect:
        raise SandboxViolation(f"{filename}: missing required detect() function")
    if not has_normalize:
        raise SandboxViolation(f"{filename}: missing required normalize() function")

    primary_key = str(values.get("PRIMARY_KEY") or "").strip()
    source_name = str(values.get("SOURCE_NAME") or "").strip()
    raw_aliases = values.get("ALIASES", [])

    if not primary_key:
        raise SandboxViolation(f"{filename}: PRIMARY_KEY must be a non-empty string literal")
    if not source_name:
        raise SandboxViolation(f"{filename}: SOURCE_NAME must be a non-empty string literal")
    if not isinstance(raw_aliases, (list, tuple)) or any(not isinstance(item, str) for item in raw_aliases):
        raise SandboxViolation(f"{filename}: ALIASES must be a list of string literals")

    aliases = [item.strip() for item in raw_aliases if str(item).strip()]
    return ModuleMetadata(primary_key=primary_key, aliases=aliases, source_name=source_name)


def validate_normalizer_source(source: str, *, filename: str = "<string>") -> ModuleMetadata:
    """Parse and validate an untrusted normalizer module."""
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as exc:
        raise SandboxViolation(f"{filename}:{exc.lineno}:{exc.offset}: {exc.msg}") from exc

    analyzer = _SandboxAnalyzer(filename)
    analyzer.visit(tree)
    if analyzer.errors:
        raise SandboxViolation("\n".join(analyzer.errors))
    return extract_module_metadata(tree, filename=filename)
