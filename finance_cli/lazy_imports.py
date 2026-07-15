"""Small runtime helpers for import-boundary control."""

from __future__ import annotations

import importlib
from typing import Any


class LazyModule:
    """Import a module on first attribute access."""

    def __init__(self, module_name: str) -> None:
        super().__setattr__("_module_name", module_name)
        super().__setattr__("_module", None)

    def _load(self):
        module = super().__getattribute__("_module")
        if module is None:
            module = importlib.import_module(super().__getattribute__("_module_name"))
            super().__setattr__("_module", module)
        return module

    def __getattr__(self, name: str):
        return getattr(self._load(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {"_module_name", "_module"}:
            super().__setattr__(name, value)
            return
        setattr(self._load(), name, value)

    def __repr__(self) -> str:
        return f"<lazy module {super().__getattribute__('_module_name')}>"
