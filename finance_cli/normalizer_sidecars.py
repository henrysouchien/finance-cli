"""Storage-backed sidecar helpers for custom normalizers and registries."""

from __future__ import annotations

from .storage_client import _dispatch as storage_dispatch
from .storage_lease import current_lease_scope
from .user_context import get_user_context

PRODUCT = "finance_cli"


class StorageSidecarUnavailable(RuntimeError):
    """Raised when remote sidecars are required but cannot be reached safely."""


def remote_sidecar_target() -> tuple[str, str] | None:
    """Return (target, user_id) when sidecars should use remote storage."""
    scope = current_lease_scope()
    context = get_user_context()
    requested_mode = (
        str(context.storage_mode or "").strip().lower() if context is not None else ""
    )
    if scope is None:
        if requested_mode == "remote":
            raise StorageSidecarUnavailable(
                "remote normalizer sidecars require an active remote storage lease"
            )
        return None
    if scope.storage_mode != "remote":
        if requested_mode == "remote":
            raise StorageSidecarUnavailable(
                f"remote normalizer sidecars cannot use active {scope.storage_mode!r} storage lease"
            )
        return None

    user_id = str(scope.user_id).strip()
    if not user_id:
        return None

    if context is not None:
        if context.local_mode:
            return None
        if (
            context.expected_user_id is not None
            and str(context.expected_user_id) != user_id
        ):
            raise StorageSidecarUnavailable(
                f"remote normalizer sidecar user {user_id!r} does not match context user {context.expected_user_id!r}"
            )

    target = storage_dispatch.storage_server_target()
    if not target or not storage_dispatch.storage_client_enabled():
        raise StorageSidecarUnavailable(
            "remote normalizer sidecars require enabled storage client configuration"
        )
    return target, user_id


def read_text(
    relative_path: str,
    *,
    target_info: tuple[str, str],
    missing_ok: bool = False,
) -> str | None:
    from . import storage_files
    from .storage_client import errors as storage_errors

    target, user_id = target_info
    try:
        content = storage_files.read_file(
            target,
            user_id=user_id,
            product=PRODUCT,
            relative_path=relative_path,
        )
    except storage_errors.StorageClientError as exc:
        if missing_ok and is_not_found(exc, relative_path):
            return None
        raise
    return content.decode("utf-8")


def write_text(
    relative_path: str,
    text: str,
    *,
    target_info: tuple[str, str],
) -> None:
    from . import storage_files

    target, user_id = target_info
    storage_files.write_file(
        target,
        user_id=user_id,
        product=PRODUCT,
        relative_path=relative_path,
        content=text.encode("utf-8"),
    )


def delete_file(relative_path: str, *, target_info: tuple[str, str]) -> None:
    from . import storage_files

    target, user_id = target_info
    storage_files.delete_file(
        target,
        user_id=user_id,
        product=PRODUCT,
        relative_path=relative_path,
    )


def list_paths(prefix: str, *, target_info: tuple[str, str]) -> list[str]:
    from . import storage_files

    target, user_id = target_info
    return storage_files.list_files(
        target,
        user_id=user_id,
        product=PRODUCT,
        prefix=prefix,
    )


def exists(relative_path: str, *, target_info: tuple[str, str]) -> bool:
    try:
        read_text(relative_path, target_info=target_info)
    except Exception as exc:
        if is_not_found(exc, relative_path):
            return False
        raise
    return True


def is_not_found(exc: Exception, relative_path: str | None = None) -> bool:
    reason = str(getattr(exc, "reason", "") or str(exc)).strip()
    if not reason:
        return False
    not_found_reasons = {"not_found", "FileNotFoundError"}
    if relative_path:
        not_found_reasons.add(str(relative_path))
    return reason in not_found_reasons


__all__ = [
    "PRODUCT",
    "StorageSidecarUnavailable",
    "delete_file",
    "exists",
    "is_not_found",
    "list_paths",
    "read_text",
    "remote_sidecar_target",
    "write_text",
]
