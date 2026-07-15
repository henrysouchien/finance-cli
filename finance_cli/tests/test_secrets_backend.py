from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from finance_cli import secrets_backend


@pytest.fixture(autouse=True)
def _reset_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    secrets_backend._client = None
    yield
    secrets_backend._client = None


@mock_aws
def test_put_and_get_round_trip() -> None:
    secrets_backend.put_secret("finance-cli/users/u1/anthropic-api-key", "sk-test-1")

    assert secrets_backend.get_secret("finance-cli/users/u1/anthropic-api-key") == "sk-test-1"


@mock_aws
def test_get_returns_none_when_not_found_and_missing_ok() -> None:
    assert secrets_backend.get_secret("finance-cli/users/missing/anthropic-api-key", missing_ok=True) is None


@mock_aws
def test_get_raises_when_not_found_and_not_missing_ok() -> None:
    with pytest.raises(RuntimeError, match="Failed to retrieve secret"):
        secrets_backend.get_secret("finance-cli/users/missing/anthropic-api-key")


def test_get_returns_none_for_scheduled_for_deletion_when_missing_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    client = SimpleNamespace()

    def _raise(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise ClientError(
            {
                "Error": {
                    "Code": "InvalidRequestException",
                    "Message": "You can't perform this operation on the secret because it was scheduled for deletion.",
                }
            },
            "GetSecretValue",
        )

    monkeypatch.setattr(secrets_backend, "_get_client", lambda: client)
    monkeypatch.setattr(client, "get_secret_value", _raise, raising=False)

    assert secrets_backend.get_secret("finance-cli/users/u1/stripe-api-key", missing_ok=True) is None


@mock_aws
def test_put_on_soft_deleted_secret_restores_then_updates() -> None:
    name = "finance-cli/users/u1/stripe-api-key"
    secrets_backend.put_secret(name, "sk-old")
    secrets_backend.delete_secret(name)

    secrets_backend.put_secret(name, "sk-new")

    assert secrets_backend.get_secret(name) == "sk-new"


@mock_aws
def test_delete_then_restore() -> None:
    name = "finance-cli/users/u1/anthropic-api-key"
    secrets_backend.put_secret(name, "sk-test-restore")
    secrets_backend.delete_secret(name)

    assert secrets_backend.get_secret(name, missing_ok=True) is None

    secrets_backend.restore_secret(name)

    assert secrets_backend.get_secret(name) == "sk-test-restore"


@mock_aws
def test_delete_force_skips_recovery_window() -> None:
    name = "finance-cli/users/u1/anthropic-api-key"
    secrets_backend.put_secret(name, "sk-test-force")

    secrets_backend.delete_secret(name, force=True)

    assert secrets_backend.get_secret(name, missing_ok=True) is None
