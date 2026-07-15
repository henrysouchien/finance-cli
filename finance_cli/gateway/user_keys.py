"""Identity-bound gateway user key parsing."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Literal

GATEWAY_USER_KEYS_ENV = "GATEWAY_USER_KEYS"

GatewayKeyChannel = Literal["web", "telegram", "cli"]
GatewayKeyRole = Literal["owner", "invite"]

VALID_GATEWAY_KEY_CHANNELS: frozenset[str] = frozenset({"web", "telegram", "cli"})
VALID_GATEWAY_KEY_ROLES: frozenset[str] = frozenset({"owner", "invite"})


@dataclass(frozen=True, slots=True)
class GatewayUserKey:
    key: str
    channel: GatewayKeyChannel
    user_id: str
    email: str
    role: GatewayKeyRole

    @property
    def key_hash(self) -> str:
        return hash_gateway_user_key(self.key)

    @property
    def risk_user_id(self) -> int:
        return int(self.user_id)


class GatewayUserKeySet:
    def __init__(self, entries: list[GatewayUserKey]) -> None:
        if not entries:
            raise ValueError(f"{GATEWAY_USER_KEYS_ENV} must contain at least one key")

        by_hash: dict[str, GatewayUserKey] = {}
        by_identity: dict[tuple[str, str], GatewayUserKey] = {}
        for entry in entries:
            if entry.key_hash in by_hash:
                raise ValueError(f"{GATEWAY_USER_KEYS_ENV} contains duplicate key material")
            identity = (entry.channel, entry.user_id)
            if identity in by_identity:
                raise ValueError(
                    f"{GATEWAY_USER_KEYS_ENV} contains duplicate key for "
                    f"channel={entry.channel!r} user_id={entry.user_id!r}"
                )
            by_hash[entry.key_hash] = entry
            by_identity[identity] = entry

        self._entries = tuple(entries)
        self._by_hash = by_hash
        self._by_identity = by_identity

    @property
    def entries(self) -> tuple[GatewayUserKey, ...]:
        return self._entries

    @property
    def valid_api_keys(self) -> set[str]:
        return {entry.key for entry in self._entries}

    def entry_for_key(self, api_key: str) -> GatewayUserKey | None:
        return self._by_hash.get(hash_gateway_user_key(api_key.strip()))

    def key_for(self, channel: str, user_id: str | int) -> str:
        normalized_channel = normalize_gateway_key_channel(channel)
        normalized_user_id = normalize_gateway_key_user_id(user_id)
        entry = self._by_identity.get((normalized_channel, normalized_user_id))
        if entry is None:
            raise KeyError(
                f"No gateway user key configured for channel={normalized_channel!r} "
                f"user_id={normalized_user_id!r}"
            )
        return entry.key


def hash_gateway_user_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def load_gateway_user_key_set(raw: str | None = None) -> GatewayUserKeySet:
    return GatewayUserKeySet(load_gateway_user_keys(raw))


def load_gateway_user_keys(raw: str | None = None) -> list[GatewayUserKey]:
    value = os.environ.get(GATEWAY_USER_KEYS_ENV, "") if raw is None else raw
    value = value.strip()
    if not value:
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV} is required")

    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV} must be valid JSON") from exc

    if not isinstance(payload, list):
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV} must be a JSON array")

    return [_parse_entry(item, index=index) for index, item in enumerate(payload)]


def normalize_gateway_key_channel(value: str) -> GatewayKeyChannel:
    channel = value.strip().lower()
    if channel not in VALID_GATEWAY_KEY_CHANNELS:
        raise ValueError(
            f"channel must be one of {sorted(VALID_GATEWAY_KEY_CHANNELS)} (got {value!r})"
        )
    return channel  # type: ignore[return-value]


def normalize_gateway_key_user_id(value: str | int) -> str:
    if isinstance(value, bool):
        raise ValueError("user_id must be a positive integer")
    text = str(value).strip()
    if not text:
        raise ValueError("user_id is required")
    try:
        numeric = int(text)
    except ValueError as exc:
        raise ValueError(f"user_id must be a positive integer (got {text!r})") from exc
    if numeric <= 0:
        raise ValueError(f"user_id must be positive (got {numeric})")
    return str(numeric)


def _parse_entry(item: Any, *, index: int) -> GatewayUserKey:
    if not isinstance(item, dict):
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV}[{index}] must be an object")

    key = _required_string(item, "key", index)
    channel = normalize_gateway_key_channel(_required_string(item, "channel", index))
    user_id = normalize_gateway_key_user_id(item.get("user_id"))
    email = _required_string(item, "email", index)
    role = _required_role(_required_string(item, "role", index), index)

    return GatewayUserKey(
        key=key,
        channel=channel,
        user_id=user_id,
        email=email,
        role=role,
    )


def _required_string(item: dict[str, Any], field: str, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str):
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV}[{index}].{field} must be a string")
    value = value.strip()
    if not value:
        raise ValueError(f"{GATEWAY_USER_KEYS_ENV}[{index}].{field} is required")
    return value


def _required_role(value: str, index: int) -> GatewayKeyRole:
    role = value.strip().lower()
    if role not in VALID_GATEWAY_KEY_ROLES:
        raise ValueError(
            f"{GATEWAY_USER_KEYS_ENV}[{index}].role must be one of "
            f"{sorted(VALID_GATEWAY_KEY_ROLES)}"
        )
    return role  # type: ignore[return-value]
