"""sqlite3-style cursor backed by the storage server proxy."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from . import _params
from ._generated import storage_server_pb2 as pb2
from ._session import starts_with_insert

if TYPE_CHECKING:
    from .connection import StorageConnection


class StorageRow:
    """Small sqlite3.Row-compatible adapter for proxy result rows."""

    def __init__(self, columns: tuple[str, ...], values: tuple[object, ...]) -> None:
        self._columns = columns
        self._values = values
        self._index: dict[str, int] = {}
        for idx, column in enumerate(columns):
            self._index.setdefault(column, idx)
            self._index.setdefault(column.lower(), idx)

    def __getitem__(self, key):
        if isinstance(key, str):
            index = self._index[key] if key in self._index else self._index[key.lower()]
            return self._values[index]
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._columns)

    def get(self, key: str, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default


class StorageCursor(Iterator[object]):
    """Buffered cursor implementing the sqlite3 cursor subset used by finance_cli."""

    def __init__(self, connection: "StorageConnection") -> None:
        self._connection = connection
        self._rows: list[object] = []
        self._index = 0
        self._rowcount = -1
        self._lastrowid: int | None = None
        self._description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None
        self._closed = False
        self.arraysize = 1

    def execute(self, sql: str, params: _params.Params = None) -> "StorageCursor":
        self._ensure_open()
        response = self._connection._execute(sql, params)
        self._populate_from_execute_response(response, sql=sql)
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[_params.Params]) -> "StorageCursor":
        self._ensure_open()
        response = self._connection._execute_many(sql, seq_of_params)
        self._populate_from_execute_response(response, sql=sql)
        return self

    def executescript(self, sql: str) -> "StorageCursor":
        self._ensure_open()
        response = self._connection._execute_script(sql)
        self._rows = []
        self._index = 0
        self._rowcount = -1
        self._lastrowid = None
        self._description = None
        self._connection._session.update_after_execute(sql, response.in_transaction, response)
        return self

    def fetchone(self):
        self._ensure_open()
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[object]:
        self._ensure_open()
        limit = self.arraysize if size is None else int(size)
        if limit <= 0:
            return self.fetchall()
        end = min(self._index + limit, len(self._rows))
        rows = self._rows[self._index : end]
        self._index = end
        return rows

    def fetchall(self) -> list[object]:
        self._ensure_open()
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def __iter__(self) -> "StorageCursor":
        self._ensure_open()
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._lastrowid

    @property
    def description(self):
        return self._description

    def _populate_from_execute_response(self, response: pb2.ExecuteResponse, *, sql: str) -> None:
        column_names = tuple(response.column_names)
        self._description = (
            tuple((name, None, None, None, None, None, None) for name in column_names)
            if column_names
            else None
        )
        self._rows = [
            self._make_row(
                column_names,
                tuple(_params.from_proto_value(value) for value in row.values),
            )
            for row in response.rows
        ]
        self._index = 0
        self._rowcount = int(response.rowcount)
        self._lastrowid = int(response.lastrowid) if starts_with_insert(sql) else None
        self._connection._session.update_after_execute(sql, response.in_transaction, response)

    def _make_row(self, column_names: tuple[str, ...], values: tuple[object, ...]) -> object:
        factory = self._connection.row_factory
        if factory is None:
            return values
        if factory is sqlite3.Row:
            return StorageRow(column_names, values)
        return factory(self, values)

    def _ensure_open(self) -> None:
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed cursor.")
        self._connection._ensure_open()
