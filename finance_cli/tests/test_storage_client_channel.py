from __future__ import annotations

from finance_cli.storage_client.channel import ChannelPool, get_channel, _CHANNEL_OPTIONS, _default_pool


def test_get_channel_returns_same_instance_for_same_target() -> None:
    pool = ChannelPool()
    try:
        first = pool.get("localhost:50051")
        second = pool.get("localhost:50051")

        assert second is first
        assert id(second) == id(first)
    finally:
        pool.close_all()


def test_different_targets_get_different_channels() -> None:
    pool = ChannelPool()
    try:
        first = pool.get("localhost:50051")
        second = pool.get("localhost:50052")

        assert second is not first
    finally:
        pool.close_all()


def test_close_all_releases_cached_channels() -> None:
    pool = ChannelPool()
    first = pool.get("localhost:50051")
    pool.close_all()
    second = pool.get("localhost:50051")
    try:
        assert second is not first
    finally:
        pool.close_all()


def test_module_level_get_channel_uses_default_pool() -> None:
    try:
        first = get_channel("localhost:50051")
        second = get_channel("localhost:50051")

        assert second is first
    finally:
        _default_pool.close_all()


def test_channel_options_do_not_send_idle_keepalive_pings() -> None:
    option_names = {name for name, _value in _CHANNEL_OPTIONS}

    assert "grpc.keepalive_time_ms" not in option_names
    assert "grpc.keepalive_timeout_ms" not in option_names
    assert "grpc.keepalive_permit_without_calls" not in option_names
