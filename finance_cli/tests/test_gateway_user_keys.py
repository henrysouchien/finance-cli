from __future__ import annotations

import json

import pytest

from finance_cli.gateway.user_keys import load_gateway_user_key_set


def _raw_keys() -> str:
    return json.dumps(
        [
            {
                "key": "web-user-1",
                "channel": "web",
                "user_id": 1,
                "email": "user1@example.test",
                "role": "owner",
            },
            {
                "key": "telegram-user-2",
                "channel": "telegram",
                "user_id": "2",
                "email": "user2@example.test",
                "role": "invite",
            },
        ]
    )


def test_gateway_user_keys_selects_key_by_channel_and_user() -> None:
    key_set = load_gateway_user_key_set(_raw_keys())

    assert key_set.key_for("web", "1") == "web-user-1"
    assert key_set.key_for("telegram", 2) == "telegram-user-2"
    assert key_set.entry_for_key("web-user-1").user_id == "1"  # type: ignore[union-attr]


def test_gateway_user_keys_rejects_duplicate_channel_user() -> None:
    raw = json.dumps(
        [
            {
                "key": "first",
                "channel": "web",
                "user_id": 1,
                "email": "user1@example.test",
                "role": "owner",
            },
            {
                "key": "second",
                "channel": "web",
                "user_id": "1",
                "email": "user1@example.test",
                "role": "owner",
            },
        ]
    )

    with pytest.raises(ValueError, match="duplicate key for channel"):
        load_gateway_user_key_set(raw)


def test_gateway_user_keys_rejects_unknown_channel() -> None:
    raw = json.dumps(
        [
            {
                "key": "bad",
                "channel": "mobile",
                "user_id": 1,
                "email": "user1@example.test",
                "role": "owner",
            }
        ]
    )

    with pytest.raises(ValueError, match="channel must be one of"):
        load_gateway_user_key_set(raw)
