import ast
from pathlib import Path


PACKAGE_INIT = Path(__file__).resolve().parents[2] / "__init__.py"


def test_advisory_image_init_has_no_side_effects() -> None:
    tree = ast.parse(PACKAGE_INIT.read_text(encoding="utf-8"))

    for idx, node in enumerate(tree.body):
        if isinstance(
            node,
            (
                ast.Import,
                ast.ImportFrom,
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
                ast.Assign,
                ast.AnnAssign,
                ast.AugAssign,
            ),
        ):
            continue
        if isinstance(node, ast.Expr):
            if idx == 0 and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                continue
            raise AssertionError(
                f"{PACKAGE_INIT}: top-level bare expression at position {idx} is not a docstring; "
                f"got {ast.dump(node.value)!r}"
            )
        raise AssertionError(
            f"{PACKAGE_INIT}: top-level statement of type {type(node).__name__} is not allowed"
        )
