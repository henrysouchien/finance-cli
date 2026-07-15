"""Shared PII redaction helpers."""

from __future__ import annotations

import re

_ASCII_LEFT = r"(?<![A-Za-z0-9])"
_ASCII_RIGHT = r"(?![A-Za-z0-9])"
_SECRET_KEY = (
    r"token|secret|password|passwd|auth|access[_-]?token|refresh[_-]?token|"
    r"id[_-]?token|api[_-]?key|apikey|client[_-]?secret|session|cookie|authorization"
)
_URL_SECRET_KEY = (
    r"code|state|token|secret|password|passwd|auth|key|access[_-]?token|"
    r"refresh[_-]?token|id[_-]?token|api[_-]?key|apikey|client[_-]?secret|"
    r"session|cookie|authorization"
)
_KEY_ASSIGNMENT_KEY = (
    r"api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"client[_-]?secret|secret|token|password|passwd|authorization|auth|session|cookie"
)

_JSON_SECRET_RE = re.compile(
    rf'("({_SECRET_KEY})")(\s*:\s*)"(?:\\.|[^"\\]){{1,1024}}"',
    re.IGNORECASE,
)
_URL_QUERY_SECRET_RE = re.compile(
    rf"([?&]|\A)({_URL_SECRET_KEY})=[^&\s\"'#)(\]\[<>,;]+",
    re.IGNORECASE,
)
_CARD_RE = re.compile(rf"{_ASCII_LEFT}(?:\d{{4}}[-\s]?){{3}}\d{{4}}{_ASCII_RIGHT}")
_SSN_RE = re.compile(rf"{_ASCII_LEFT}\d{{3}}-\d{{2}}-\d{{4}}{_ASCII_RIGHT}")
_ACCOUNT_RE = re.compile(rf"{_ASCII_LEFT}\d{{9,12}}{_ASCII_RIGHT}")
_EMAIL_RE = re.compile(
    r"(?<![A-Za-z0-9._+-])[^@/\s]+@[A-Za-z0-9.-]+\.[A-Za-z0-9]+(?![A-Za-z0-9])"
)
_JWT_RE = re.compile(
    rf"{_ASCII_LEFT}eyJ[A-Za-z0-9_-]{{10,}}\.eyJ[A-Za-z0-9_-]{{10,}}\.[A-Za-z0-9_-]{{10,}}{_ASCII_RIGHT}"
)
_KEY_ASSIGNMENT_RE = re.compile(
    rf"(?<![A-Za-z0-9_\-\"'?&])(?:{_KEY_ASSIGNMENT_KEY})\s*[:=]\s*(?!\[REDACTED\])[^\s,;&)\]\}}]+",
    re.IGNORECASE,
)
_KEY_PREFIX_RE = re.compile(
    r"(?<![A-Za-z0-9\"'])(?:sk|pk|rk|ak|token|access|key)[-_][A-Za-z0-9._-]{3,}(?![A-Za-z0-9._\-\u0080-\uffff])",
    re.IGNORECASE,
)
_IP_RE = re.compile(rf"{_ASCII_LEFT}\d{{1,3}}(?:\.\d{{1,3}}){{3}}{_ASCII_RIGHT}")
_PATH_TOKEN_RE = re.compile(
    r"""(?P<prefix>^|[\s'"([{=,;])(?P<path>/(?!/)[^\s:'")\]}]+)"""
)
_USER_PATH_PREFIXES = ("/Users/", "/home/")
_USER_DATA_PATH_SEGMENT_RE = re.compile(r"(?:^|/)data/users(?:/|$)")
_SERVER_PATH_SEGMENT_RE = re.compile(r"(?:^|/)(?:data|var)(?:/|$)")
_WINDOWS_USER_PATH_RE = re.compile(r"[A-Za-z]:\\Users\\[^\s:]+")


def _redact_path_tokens(text: str) -> str:
    def _replace_path(match: re.Match[str]) -> str:
        path = match.group("path")
        if (
            path.startswith(_USER_PATH_PREFIXES)
            or _USER_DATA_PATH_SEGMENT_RE.search(path)
        ):
            return f"{match.group('prefix')}[USER_PATH]"
        if _SERVER_PATH_SEGMENT_RE.search(path):
            return f"{match.group('prefix')}[SERVER_PATH]"
        return match.group(0)

    return _PATH_TOKEN_RE.sub(_replace_path, text)


def redact_text(text: str) -> str:
    """Strip likely secrets and user-identifying strings from text."""
    redacted = str(text or "")
    redacted = _JSON_SECRET_RE.sub(r'\1\3"[REDACTED]"', redacted)
    redacted = _URL_QUERY_SECRET_RE.sub(r"\1\2=[REDACTED]", redacted)
    redacted = _CARD_RE.sub("[CARD]", redacted)
    redacted = _SSN_RE.sub("[SSN]", redacted)
    redacted = _ACCOUNT_RE.sub("[ACCT]", redacted)
    redacted = _EMAIL_RE.sub("[EMAIL]", redacted)
    redacted = _JWT_RE.sub("[JWT]", redacted)
    redacted = _KEY_ASSIGNMENT_RE.sub("[KEY]", redacted)
    redacted = _KEY_PREFIX_RE.sub("[KEY]", redacted)
    redacted = _IP_RE.sub("[IP]", redacted)
    redacted = _redact_path_tokens(redacted)
    redacted = _WINDOWS_USER_PATH_RE.sub("[USER_PATH]", redacted)
    return redacted


__all__ = ["redact_text"]
