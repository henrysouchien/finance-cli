"""Remote storage bootstrap for users that never had local per-user files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Any

import grpc

from finance_cli.category_seed import seed_canonical_categories
from finance_cli.db import SCHEMA_VERSION, initialize_connection
from finance_cli.user_provisioning import stamp_tenant_marker

from . import auth as storage_auth
from . import channel as storage_channel
from . import errors
from ._generated import storage_server_pb2 as pb2
from ._generated import storage_server_pb2_grpc as pb2_grpc
from .connection import StorageConnection


class RemoteBootstrapError(RuntimeError):
    """Raised when an empty-user remote bootstrap cannot be verified."""

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        self.details = details or {}
        super().__init__(message)


@dataclass(frozen=True)
class RemoteBootstrapResult:
    user_id: str
    product: str
    created: bool
    user_dir: str
    schema_version: int
    tenant_marker: str
    rules_present: bool
    rules_bytes: int
    canonical_categories: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "product": self.product,
            "created": self.created,
            "user_dir": self.user_dir,
            "schema_version": self.schema_version,
            "tenant_marker": self.tenant_marker,
            "rules_present": self.rules_present,
            "rules_bytes": self.rules_bytes,
            "canonical_categories": self.canonical_categories,
        }


def bootstrap_remote_empty_user(
    *,
    target: str,
    user_id: str | int,
    product: str = "finance_cli",
    template_rules_path: Path | str | None = None,
    ensure_canonical_categories: bool = False,
    require_synthetic_only_false: bool = False,
    cleanup_on_failure: bool = False,
    auth_provider=None,
    channel_pool=None,
) -> RemoteBootstrapResult:
    """Provision and initialize a remote-only user database.

    This path is for users whose local per-user directory never existed. It does
    not export or restore local files and should stay separate from normal
    cutover, which must continue to require local source files.
    """

    target_text = _required_text(target, "target")
    product_text = _required_text(product, "product")
    user_id_text = _required_text(user_id, "user_id")
    provider = auth_provider or storage_auth.get_default_provider()
    pool = channel_pool or storage_channel._default_pool
    stub = pb2_grpc.SqliteProxyStub(pool.get(target_text))
    provisioned = None

    try:
        if require_synthetic_only_false:
            health = _admin_health(
                stub=stub,
                product=product_text,
                user_id=user_id_text,
                auth_provider=provider,
            )
            if bool(getattr(health, "synthetic_only", True)) is not False:
                raise RemoteBootstrapError("storage proxy synthetic_only=true")

        provisioned = _provision_remote_user(
            stub=stub,
            product=product_text,
            user_id=user_id_text,
            auth_provider=provider,
        )
        if not bool(getattr(provisioned, "created", False)):
            raise RemoteBootstrapError(
                "remote user already exists; empty-user bootstrap requires a newly "
                "provisioned remote user",
                details={"user_dir": str(getattr(provisioned, "user_dir", ""))},
            )

        rules_bytes = _ensure_rules_file(
            stub=stub,
            product=product_text,
            user_id=user_id_text,
            auth_provider=provider,
            template_rules_path=template_rules_path,
        )

        canonical_categories: dict[str, Any] | None = None
        conn = StorageConnection(
            target_text,
            user_id=user_id_text,
            product=product_text,
            auth_provider=provider,
            channel_pool=pool,
        )
        try:
            conn.row_factory = sqlite3.Row
            _assert_remote_empty_bootstrap_target(conn, user_id=user_id_text)
            initialize_connection(conn, create_migration_backup=False)
            conn.commit()
            stamp_tenant_marker(
                conn,
                user_id_text,
                db_path=f"storage://{product_text}/{user_id_text}",
            )
            if ensure_canonical_categories:
                canonical_categories = seed_canonical_categories(conn, dry_run=False)
            schema_version, tenant_marker = _verify_bootstrap(conn, user_id=user_id_text)
        finally:
            conn.close()
    except Exception as exc:
        cleanup = _cleanup_remote_user_after_failure(
            stub=stub,
            product=product_text,
            user_id=user_id_text,
            auth_provider=provider,
            provisioned=provisioned,
            cleanup_on_failure=cleanup_on_failure,
        )
        if isinstance(exc, RemoteBootstrapError):
            exc.details.setdefault("cleanup", cleanup)
            raise
        raise RemoteBootstrapError(str(exc), details={"cleanup": cleanup}) from exc

    return RemoteBootstrapResult(
        user_id=user_id_text,
        product=product_text,
        created=bool(provisioned.created),
        user_dir=str(provisioned.user_dir),
        schema_version=schema_version,
        tenant_marker=tenant_marker,
        rules_present=True,
        rules_bytes=rules_bytes,
        canonical_categories=canonical_categories,
    )


def _required_text(value: str | int, name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _metadata(
    *,
    auth_provider,
    product: str,
    user_id: str,
    scopes: list[str] | None = None,
) -> tuple[tuple[str, str], ...]:
    token = auth_provider.get_token(product, user_id, scopes or [])
    return (("authorization", f"Bearer {token}"),)


def _admin_health(*, stub, product: str, user_id: str, auth_provider):
    try:
        return stub.AdminHealth(
            pb2.AdminHealthRequest(),
            metadata=_metadata(
                auth_provider=auth_provider,
                product=product,
                user_id=user_id,
                scopes=["admin"],
            ),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="AdminHealth") from exc


def _provision_remote_user(*, stub, product: str, user_id: str, auth_provider):
    try:
        return stub.ProvisionUser(
            pb2.ProvisionUserRequest(product=product, user_id=user_id),
            metadata=_metadata(
                auth_provider=auth_provider,
                product=product,
                user_id=user_id,
                scopes=["admin"],
            ),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="ProvisionUser") from exc


def _delete_remote_user(*, stub, product: str, user_id: str, auth_provider):
    try:
        return stub.DeleteUser(
            pb2.DeleteUserRequest(product=product, user_id=user_id),
            metadata=_metadata(
                auth_provider=auth_provider,
                product=product,
                user_id=user_id,
                scopes=["admin"],
            ),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="DeleteUser") from exc


def _cleanup_remote_user_after_failure(
    *,
    stub,
    product: str,
    user_id: str,
    auth_provider,
    provisioned,
    cleanup_on_failure: bool,
) -> dict[str, Any]:
    if not cleanup_on_failure:
        return {"attempted": False, "reason": "disabled"}
    if provisioned is None:
        return {"attempted": False, "reason": "not_provisioned"}
    user_dir = str(getattr(provisioned, "user_dir", ""))
    if not bool(getattr(provisioned, "created", False)):
        return {
            "attempted": False,
            "reason": "remote_user_preexisted",
            "user_dir": user_dir,
        }

    try:
        response = _delete_remote_user(
            stub=stub,
            product=product,
            user_id=user_id,
            auth_provider=auth_provider,
        )
    except Exception as cleanup_exc:
        return {
            "attempted": True,
            "deleted": False,
            "error_type": cleanup_exc.__class__.__name__,
            "error": str(cleanup_exc),
            "user_dir": user_dir,
        }
    return {
        "attempted": True,
        "deleted": bool(response.deleted),
        "user_dir": user_dir,
    }


def _ensure_rules_file(
    *,
    stub,
    product: str,
    user_id: str,
    auth_provider,
    template_rules_path: Path | str | None,
) -> int:
    size = _remote_rules_size(
        stub=stub,
        product=product,
        user_id=user_id,
        auth_provider=auth_provider,
    )
    if size is not None:
        return size
    if template_rules_path is None:
        raise RemoteBootstrapError("remote rules.yaml missing after ProvisionUser")

    content = Path(template_rules_path).expanduser().resolve().read_bytes()
    try:
        response = stub.WriteFile(
            iter(
                [
                    pb2.FileWriteChunk(
                        product=product,
                        user_id=user_id,
                        path="rules.yaml",
                        content=content,
                    )
                ]
            ),
            metadata=_metadata(auth_provider=auth_provider, product=product, user_id=user_id),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="WriteFile") from exc
    return int(response.bytes_written)


def _remote_rules_size(*, stub, product: str, user_id: str, auth_provider) -> int | None:
    try:
        response = stub.ListFiles(
            pb2.FileListRequest(product=product, user_id=user_id, path=""),
            metadata=_metadata(auth_provider=auth_provider, product=product, user_id=user_id),
        )
    except grpc.RpcError as exc:
        raise errors.from_grpc_error(exc, rpc="ListFiles") from exc
    for file_info in response.files:
        if str(file_info.path) == "rules.yaml" and not bool(file_info.is_directory):
            return int(file_info.size_bytes)
    return None


def _verify_bootstrap(conn, *, user_id: str) -> tuple[int, str]:
    schema_row = conn.execute("SELECT MAX(version) AS version FROM schema_version").fetchone()
    schema_version = int(_row_value(schema_row, "version", 0) or 0)
    if schema_version < SCHEMA_VERSION:
        raise RemoteBootstrapError(
            f"remote schema_version {schema_version} is below expected {SCHEMA_VERSION}"
        )

    marker_row = conn.execute(
        "SELECT user_id FROM tenant_marker WHERE singleton = 1"
    ).fetchone()
    tenant_marker = str(_row_value(marker_row, "user_id", 0) or "")
    if tenant_marker != user_id:
        raise RemoteBootstrapError(
            f"remote tenant marker {tenant_marker!r} does not match {user_id!r}"
        )
    return schema_version, tenant_marker


def _assert_remote_empty_bootstrap_target(conn, *, user_id: str) -> None:
    rows = conn.execute(
        """
        SELECT name
          FROM sqlite_master
         WHERE type = 'table'
           AND name NOT LIKE 'sqlite_%'
         ORDER BY name
        """
    ).fetchall()
    table_names = [str(_row_value(row, "name", 0) or "") for row in rows]
    table_names = [name for name in table_names if name]
    if not table_names:
        return

    if "tenant_marker" in table_names:
        marker = _read_existing_tenant_marker(conn)
        raise RemoteBootstrapError(
            "remote tenant_marker already exists before empty-user bootstrap",
            details={
                "expected_user_id": user_id,
                "tenant_marker": marker,
                "existing_tables": table_names,
            },
        )

    if "schema_version" in table_names:
        version = _read_existing_schema_version(conn)
        raise RemoteBootstrapError(
            "remote schema_version already exists before empty-user bootstrap",
            details={
                "expected_user_id": user_id,
                "schema_version": version,
                "existing_tables": table_names,
            },
        )

    non_empty_tables = []
    for table_name in table_names:
        try:
            row = conn.execute(
                f"SELECT 1 FROM {_quote_identifier(table_name)} LIMIT 1"
            ).fetchone()
        except sqlite3.Error as exc:
            raise RemoteBootstrapError(
                "failed inspecting remote bootstrap target table",
                details={
                    "expected_user_id": user_id,
                    "table": table_name,
                    "error": str(exc),
                    "existing_tables": table_names,
                },
            ) from exc
        if row is not None:
            non_empty_tables.append(table_name)

    raise RemoteBootstrapError(
        "remote database is not empty before empty-user bootstrap",
        details={
            "expected_user_id": user_id,
            "existing_tables": table_names,
            "non_empty_tables": non_empty_tables,
        },
    )


def _read_existing_tenant_marker(conn) -> str | None:
    try:
        row = conn.execute(
            "SELECT user_id FROM tenant_marker WHERE singleton = 1"
        ).fetchone()
    except sqlite3.Error:
        return None
    marker = _row_value(row, "user_id", 0)
    return str(marker) if marker is not None else None


def _read_existing_schema_version(conn) -> int:
    try:
        row = conn.execute("SELECT MAX(version) AS version FROM schema_version").fetchone()
    except sqlite3.Error:
        return 0
    try:
        return int(_row_value(row, "version", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _row_value(row, key: str, index: int):
    if row is None:
        return None
    try:
        return row[key]
    except Exception:
        pass
    try:
        return row[index]
    except Exception:
        return getattr(row, key, None)


__all__ = [
    "RemoteBootstrapError",
    "RemoteBootstrapResult",
    "bootstrap_remote_empty_user",
]
