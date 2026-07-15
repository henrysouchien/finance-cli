"""Client-side storage session state and retry guard."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


_SQL_LEADER_RE = re.compile(r"^\s*(?:--[^\n]*(?:\n|$)|/\*.*?\*/\s*)*", re.DOTALL)
_FIRST_WORD_RE = re.compile(r"([A-Za-z_]+)")
_TAINT_PATTERNS = (
    re.compile(r"\bPRAGMA\s+\w+\s*=", re.IGNORECASE),
    re.compile(r"\bPRAGMA\s+\w+\s*\(", re.IGNORECASE),
    re.compile(r"\bCREATE\s+TEMP(?:ORARY)?\s+(?:TABLE|VIEW)\b", re.IGNORECASE),
    re.compile(r"\bCREATE\s+(?:TABLE|VIEW)\s+temp\.\w+\b", re.IGNORECASE),
)


@dataclass
class SessionState:
    session_id: str | None = None
    last_in_transaction: bool = False
    explicit_begin_depth: int = 0
    savepoint_names: list[str] = field(default_factory=list)
    last_kid: str | None = None
    last_response: Any | None = None
    tainted: bool = False

    def reset_for_reopen(self) -> None:
        self.session_id = None
        self.last_in_transaction = False
        self.explicit_begin_depth = 0
        self.savepoint_names.clear()
        self.last_response = None
        self.tainted = False

    def update_after_execute(
        self,
        sql: str,
        response_in_transaction: bool,
        response: Any | None = None,
    ) -> None:
        self.last_in_transaction = bool(response_in_transaction)
        self.last_response = response
        self._update_taint_from_sql(sql)
        self._update_depth_from_sql(sql)

    def should_retry_session_expired(self) -> bool:
        return not self.last_in_transaction and self.explicit_begin_depth == 0

    def _update_depth_from_sql(self, sql: str) -> None:
        word = first_sql_word(sql)
        if not word:
            return
        upper = word.upper()
        if upper == "BEGIN":
            self.explicit_begin_depth = max(self.explicit_begin_depth, 1)
            return
        if upper == "SAVEPOINT":
            self.explicit_begin_depth += 1
            name = _word_after(sql, "SAVEPOINT")
            if name:
                self.savepoint_names.append(name)
            return
        if upper == "RELEASE":
            if self.explicit_begin_depth > 0:
                self.explicit_begin_depth -= 1
            if self.savepoint_names:
                self.savepoint_names.pop()
            return
        if upper == "COMMIT":
            self.explicit_begin_depth = 0
            self.savepoint_names.clear()
            return
        if upper == "ROLLBACK" and not _is_rollback_to_savepoint(sql):
            self.explicit_begin_depth = 0
            self.savepoint_names.clear()

    def _update_taint_from_sql(self, sql: str) -> None:
        if self.tainted:
            return
        text = sql or ""
        self.tainted = any(pattern.search(text) for pattern in _TAINT_PATTERNS)


def first_sql_word(sql: str) -> str:
    match = _SQL_LEADER_RE.match(sql or "")
    start = match.end() if match else 0
    word = _FIRST_WORD_RE.match(sql[start:])
    return word.group(1) if word else ""


def starts_with_insert(sql: str) -> bool:
    return first_sql_word(sql).upper() == "INSERT"


def starts_implicit_dml(sql: str) -> bool:
    return first_sql_word(sql).upper() in {"INSERT", "UPDATE", "DELETE", "REPLACE"}


def _is_rollback_to_savepoint(sql: str) -> bool:
    return bool(re.match(r"^\s*ROLLBACK\s+TO(?:\s+SAVEPOINT)?\b", sql or "", re.IGNORECASE))


def _word_after(sql: str, keyword: str) -> str | None:
    pattern = rf"^\s*{re.escape(keyword)}\s+([A-Za-z_][A-Za-z0-9_]*)"
    match = re.match(pattern, sql or "", re.IGNORECASE)
    return match.group(1) if match else None
