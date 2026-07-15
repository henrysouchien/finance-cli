"""AWS Secrets Manager wrapper shared by finance_cli modules."""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)

_DEFAULT_REGION = "us-east-1"
_DEFAULT_RECOVERY_WINDOW_DAYS = 7
_client: Any | None = None


def _region() -> str:
    return str(os.getenv("AWS_REGION") or _DEFAULT_REGION).strip() or _DEFAULT_REGION


def _get_client() -> Any:
    global _client
    if _client is None:
        _client = importlib.import_module("boto3").client("secretsmanager", region_name=_region())
    return _client


def _client_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code") or "").strip()


def _client_error_message(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Message") or "").strip()


def _scheduled_for_deletion(exc: ClientError) -> bool:
    if _client_error_code(exc) != "InvalidRequestException":
        return False
    message = _client_error_message(exc).lower()
    return "scheduled for deletion" in message or "marked for deletion" in message or "marked deleted" in message


def _restore_if_deleted(client: Any, secret_name: str) -> None:
    try:
        response = client.describe_secret(SecretId=secret_name)
    except ClientError:
        return
    except BotoCoreError:
        return
    if response.get("DeletedDate") is not None:
        restore_secret(secret_name)


def get_secret(secret_name: str, *, missing_ok: bool = False) -> str | None:
    """Return a plaintext secret string from AWS Secrets Manager."""
    client = _get_client()
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        code = _client_error_code(exc)
        if missing_ok and code == "ResourceNotFoundException":
            return None
        if missing_ok and _scheduled_for_deletion(exc):
            return None
        raise RuntimeError(f"Failed to retrieve secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to retrieve secret {secret_name}") from exc

    raw = response.get("SecretString")
    if raw is None:
        return None
    return str(raw)


def describe_secret(secret_name: str, *, missing_ok: bool = False) -> dict[str, Any] | None:
    """Return Secrets Manager metadata without reading the secret value."""
    client = _get_client()
    try:
        response = client.describe_secret(SecretId=secret_name)
    except ClientError as exc:
        if missing_ok and _client_error_code(exc) == "ResourceNotFoundException":
            return None
        raise RuntimeError(f"Failed to describe secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to describe secret {secret_name}") from exc
    return dict(response)


def put_secret(secret_name: str, value: str, *, description: str | None = None) -> None:
    """Create or update a secret, restoring soft-deleted entries when needed."""
    client = _get_client()
    create_kwargs: dict[str, Any] = {
        "Name": secret_name,
        "SecretString": value,
    }
    if description is not None:
        create_kwargs["Description"] = description

    try:
        client.create_secret(**create_kwargs)
        return
    except ClientError as exc:
        code = _client_error_code(exc)
        if _scheduled_for_deletion(exc):
            restore_secret(secret_name)
        elif code == "ResourceExistsException":
            _restore_if_deleted(client, secret_name)
        else:
            raise RuntimeError(f"Failed to store secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to store secret {secret_name}") from exc

    try:
        if description is not None:
            client.update_secret(
                SecretId=secret_name,
                SecretString=value,
                Description=description,
            )
        else:
            client.put_secret_value(SecretId=secret_name, SecretString=value)
    except ClientError as exc:
        if _scheduled_for_deletion(exc):
            restore_secret(secret_name)
            if description is not None:
                client.update_secret(
                    SecretId=secret_name,
                    SecretString=value,
                    Description=description,
                )
            else:
                client.put_secret_value(SecretId=secret_name, SecretString=value)
            return
        raise RuntimeError(f"Failed to store secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to store secret {secret_name}") from exc


def delete_secret(
    secret_name: str,
    recovery_window_days: int = _DEFAULT_RECOVERY_WINDOW_DAYS,
    *,
    force: bool = False,
) -> None:
    """Delete a secret, defaulting to a soft-delete recovery window."""
    client = _get_client()
    kwargs: dict[str, Any] = {"SecretId": secret_name}
    if force:
        kwargs["ForceDeleteWithoutRecovery"] = True
    else:
        kwargs["RecoveryWindowInDays"] = int(recovery_window_days)

    try:
        client.delete_secret(**kwargs)
    except ClientError as exc:
        code = _client_error_code(exc)
        if code == "ResourceNotFoundException" or _scheduled_for_deletion(exc):
            return
        raise RuntimeError(f"Failed to delete secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to delete secret {secret_name}") from exc


def restore_secret(secret_name: str) -> None:
    """Restore a secret that is pending deletion."""
    client = _get_client()
    try:
        client.restore_secret(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(f"Failed to restore secret {secret_name}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Failed to restore secret {secret_name}") from exc
