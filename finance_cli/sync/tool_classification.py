"""Tool classification for local MCP sync behavior."""

from __future__ import annotations

from collections.abc import Callable
from functools import cache

from finance_cli import tool_registry


@cache
def _all() -> tuple[tuple[str, tool_registry.ToolMetadata], ...]:
    import finance_cli.mcp_server  # noqa: F401

    return tuple(tool_registry.iter_registry())


def _from_registry(
    predicate: Callable[[str, tool_registry.ToolMetadata], bool],
) -> frozenset[str]:
    return frozenset(name for name, meta in _all() if predicate(name, meta))


_DERIVATIONS: dict[str, Callable[[], frozenset[str]]] = {
    "DB_WRITE_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.sync_behavior == "db_write"
    ),
    "SERVER_PROXIED_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.sync_behavior == "server_proxied"
    ),
    "SERVER_PROXIED_MUTATING_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.sync_behavior == "server_proxied"
        and not meta.read_only
    ),
    "NO_SYNC_TOOLS": lambda: _from_registry(
        lambda _name, meta: meta.sync_behavior == "no_sync"
    ),
}


@cache
def _derived(name: str) -> frozenset[str]:
    try:
        return _DERIVATIONS[name]()
    except KeyError as exc:
        raise AttributeError(name) from exc


def __getattr__(name: str) -> frozenset[str]:
    return _derived(name)
