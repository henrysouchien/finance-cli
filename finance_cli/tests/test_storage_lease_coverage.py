from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

SOURCE_ROOTS = (
    ROOT / "finance_cli",
    ROOT / "finance-web" / "server",
)
LEASE_CATALOG_FILES = (
    "finance-web/server/dependencies.py",
    "finance-web/server/routers/plaid_router.py",
    "finance-web/server/telegram_webhook.py",
    "finance-web/server/routers/telegram_router.py",
    "finance-web/server/routers/txn_router.py",
    "finance-web/server/routers/chat_router.py",
    "finance-web/server/routers/sync_router.py",
    "finance-web/server/cleanup.py",
    "finance_cli/telegram_bot/store.py",
    "finance-web/server/app.py",
    "finance_cli/gateway/server.py",
    "finance_cli/sync/subscriber.py",
    "finance_cli/mcp_server.py",
    "finance_cli/commands/memory_cmd.py",
    "finance_cli/commands/rules.py",
    "finance-web/server/sync_service.py",
    "finance_cli/cost_tracking.py",
    "finance_cli/analytics.py",
    "finance_cli/frontend_logs.py",
    "finance_cli/error_capture.py",
    "finance-web/server/user_db.py",
)

LEASE_IMPORT_NAMES = frozenset(
    {
        "LeaseScope",
        "enforce_active_lease_if_required",
        "get_user_conn",
        "optional_lease_scope",
        "require_active_lease",
        "storage_lease_scope",
    }
)
STORAGE_FILE_FUNCTIONS = frozenset({"read_file", "write_file", "list_files", "delete_file"})
MUTATION_LOCK_FUNCTIONS = frozenset({"mutation_lock", "async_mutation_lock"})
CLEANUP_FUNCTIONS = frozenset({"cleanup_old_uploads", "maybe_cleanup"})

DIRECT_RESOURCE_TEST_PATH_ALLOWLIST = (
    "finance_cli/tests/",  # why-allowed: pure unit tests intentionally exercise direct DB/storage APIs.
    "finance_cli/advisory/tests/",  # why-allowed: pure unit tests intentionally exercise direct DB/storage APIs.
    "finance_cli/gateway/tests/",  # why-allowed: pure unit tests intentionally exercise direct DB/storage APIs.
    "finance-web/server/tests/",  # why-allowed: pure API tests monkeypatch and exercise direct DB/storage APIs.
)
DIRECT_RESOURCE_CALL_ALLOWLIST = {
    (
        "finance-web/server/routers/billing_router.py",
        "seed_plan_caps_via_db",
        "db.connect",
    ): "billing admin helper seeds local plan caps outside user cutover flow",  # why-allowed
    (
        "finance-web/server/routers/billing_router.py",
        "billing_status",
        "db.connect",
    ): "billing status reads local credit ledger; route has no storage cutover mutation",  # why-allowed
    (
        "finance-web/server/routers/billing_router.py",
        "get_credits",
        "db.connect",
    ): "billing credits reads local credit ledger; route has no storage cutover mutation",  # why-allowed
    (
        "finance-web/server/routers/billing_router.py",
        "_handle_credit_pack_grant",
        "db.connect",
    ): "Stripe webhook credit grant is serialized by Stripe event handling, not user storage cutover",  # why-allowed
    (
        "finance-web/server/routers/billing_router.py",
        "_handle_charge_refunded",
        "db.connect",
    ): "Stripe webhook refund adjustment is outside Phase 5 user storage routing",  # why-allowed
    (
        "finance_cli/__main__.py",
        "main",
        "db.connect",
    ): "single-user local CLI entrypoint has no PG lease/session manager",  # why-allowed
    (
        "finance_cli/backup.py",
        "_verify_integrity",
        "db.connect",
    ): "backup integrity opens explicit archive/local DB paths outside request routing",  # why-allowed
    (
        "finance_cli/backup.py",
        "list_bundles_for_user",
        "db.connect",
    ): "backup listing reads local per-user backup metadata outside cutover traffic",  # why-allowed
    (
        "finance_cli/backup.py",
        "verify_backup",
        "db.connect",
    ): "backup verification opens extracted archive DBs, not live user request storage",  # why-allowed
    (
        "finance_cli/backup.py",
        "restore_backup",
        "db.connect",
    ): "backup restore manages its own exclusive local restore flow",  # why-allowed
    (
        "finance_cli/billing.py",
        "resolve_request",
        "db.connect",
    ): "billing request accounting can run from local CLI paths without PG lease manager",  # why-allowed
    (
        "finance_cli/billing.py",
        "apply_trial_cost_cap",
        "db.connect",
    ): "billing cap seeding is provisioning/admin setup outside live cutover requests",  # why-allowed
    (
        "finance_cli/billing.py",
        "restore_default_cost_cap",
        "db.connect",
    ): "billing cap restore is provisioning/admin setup outside live cutover requests",  # why-allowed
    (
        "finance_cli/billing.py",
        "apply_lifetime_cost_cap",
        "db.connect",
    ): "billing cap update is provisioning/admin setup outside live cutover requests",  # why-allowed
    (
        "finance_cli/commands/ops_cmd.py",
        "_connect_user_db",
        "db.connect",
    ): "ops command explicitly targets local user DBs for manual administration",  # why-allowed
    (
        "finance_cli/commands/ops_cmd.py",
        "handle_plan_caps_reseed",
        "db.connect",
    ): "ops reseed command is manual local administration outside request routing",  # why-allowed
    (
        "finance_cli/perf.py",
        "_open_perf_connection",
        "db.connect",
    ): "perf logging may run before request lease setup and falls back to local telemetry",  # why-allowed
    (
        "finance_cli/scripts/cost_rollup_job.py",
        "aggregate_user_costs",
        "db.connect",
    ): "offline rollup job routes users through its lease-aware wrapper before opening user DBs",  # why-allowed
    (
        "finance_cli/scripts/migrate_db_dek_to_vault.py",
        "_verify_db_open",
        "db.connect",
    ): "one-shot key migration script verifies local DB access outside request routing",  # why-allowed
    (
        "finance_cli/scripts/migrate_db_dek_to_vault.py",
        "_cleanup_gate_allows_sm_delete",
        "db.connect",
    ): "one-shot key migration script checks local cleanup durability outside request routing",  # why-allowed
    (
        "finance_cli/scripts/migrate_user_dbs_job.py",
        "_schema_versions",
        "db.connect",
    ): "deploy migration job opens explicit user DB paths; remote users are routed through LeaseScope first",  # why-allowed
    (
        "finance_cli/scripts/redact_historical_logs.py",
        "redact_database",
        "db.connect",
    ): "one-shot redaction script operates on explicit local DB paths",  # why-allowed
    (
        "finance_cli/scripts/backup_prune_job.py",
        "prune_user_backups",
        "db.connect",
    ): "offline backup retention job iterates explicit local user DB paths outside request routing",  # why-allowed
    (
        "finance_cli/scripts/reminder_delivery_job.py",
        "deliver_user_reminders",
        "db.connect",
    ): "offline reminder job wraps user DB access in optional_lease_scope unless explicitly disabled",  # why-allowed
    (
        "finance_cli/sync/engine.py",
        "SyncEngine._commit_staged_files_sync",
        "storage_files.write_file",
    ): "outer optional_lease_scope wraps the remote session restore loop",  # why-allowed
    (
        "finance_cli/user_provisioning.py",
        "_stamp_tenant_marker",
        "db.connect",
    ): "provisioning is called under get_user_conn/sync LeaseScope after routing",  # why-allowed
    (
        "finance_cli/user_provisioning.py",
        "_provision_db_dek_envelope",
        "db.connect",
    ): "DEK verification reuses the provisioning LeaseScope through ContextVar",  # why-allowed
    (
        "finance-web/server/dependencies.py",
        "get_user_conn",
        "db.connect",
    ): "fallback local connect is only allowed when lease infra is unavailable and enforcement is disabled",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "create_plaintext_snapshot",
        "db.connect",
    ): "sync router/app callers acquire the per-user LeaseScope before snapshot reads",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "apply_changeset",
        "db.connect",
    ): "sync push callers acquire the per-user LeaseScope before applying local changes",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "get_schema_version",
        "db.connect",
    ): "sync schema callers acquire the per-user LeaseScope before reading local schema state",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "get_migration_count",
        "db.connect",
    ): "sync schema callers acquire the per-user LeaseScope before reading local migration state",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "write_rules_yaml",
        "db.connect",
    ): "sync rules callers acquire the per-user LeaseScope before writing rules and changelog state",  # why-allowed
    (
        "finance-web/server/routers/sync_router.py",
        "_record_local_mcp_engagement_gate_block",
        "db.connect",
    ): "local MCP engagement-gate telemetry returns before db.connect for remote-storage users",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "read_synced_sidecar",
        "storage_files.list_files",
    ): "sync sidecar callers acquire the per-user sync LeaseScope before remote file reads",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "read_synced_sidecar",
        "storage_files.read_file",
    ): "sync sidecar callers acquire the per-user sync LeaseScope before remote file reads",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "write_synced_sidecar",
        "storage_files.write_file",
    ): "sync sidecar callers acquire the per-user sync LeaseScope before remote file writes",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "delete_synced_sidecar",
        "storage_files.delete_file",
    ): "sync sidecar callers acquire the per-user sync LeaseScope before remote file deletes",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "list_synced_sidecars",
        "storage_files.list_files",
    ): "sync sidecar callers acquire the per-user sync LeaseScope before remote file listing",  # why-allowed
    (
        "finance-web/server/sync_service.py",
        "prune_changelog",
        "db.connect",
    ): "startup prune iterates users under an explicit LeaseScope before local changelog pruning",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_connect_user_db",
        "db.connect",
    ): "central Telegram webhook connection helper reuses injected replay conn or the surrounding process_webhook_update LeaseScope",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_run_locked_replicated_write",
        "db.connect",
    ): "Telegram webhook processing acquires the webhook LeaseScope before replicated local writes",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_fetch_config_row",
        "db.connect",
    ): "Telegram webhook processing acquires the webhook LeaseScope before config reads",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_link_attempt_lock_remaining_seconds",
        "db.connect",
    ): "Telegram webhook link handling runs under the process_webhook_update LeaseScope",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_record_failed_link_attempt",
        "db.connect",
    ): "Telegram webhook link handling runs under the process_webhook_update LeaseScope",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_clear_link_attempts",
        "db.connect",
    ): "Telegram webhook link handling runs under the process_webhook_update LeaseScope",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_cleanup_processed_updates",
        "db.connect",
    ): "Telegram webhook processing acquires the webhook LeaseScope before cleanup writes",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_cancel_requested",
        "db.connect",
    ): "Telegram webhook processing acquires the webhook LeaseScope before cancellation reads",  # why-allowed
    (
        "finance-web/server/routers/telegram_router.py",
        "_legacy_local_telegram_secret_and_claim_update",
        "db.connect",
    ): "legacy non-BIGINT TEST fallback only; production refuses LeaseUnavailable before local DB access",  # why-allowed
    (
        "finance_cli/analytics.py",
        "log_event",
        "db.connect",
    ): "web analytics writes run under the request LeaseScope; local telemetry may fall back outside PG",  # why-allowed
    (
        "finance_cli/cost_tracking.py",
        "_open_db",
        "db.connect",
    ): "web cost writes run under the request LeaseScope; local CLI telemetry has no PG lease manager",  # why-allowed
    (
        "finance_cli/error_capture.py",
        "_sqlite_connect",
        "db.connect",
    ): "web error capture runs under the request LeaseScope; local diagnostics may fall back outside PG",  # why-allowed
    (
        "finance_cli/frontend_logs.py",
        "record_frontend_log",
        "db.connect",
    ): "frontend log writes run under the request LeaseScope established by get_user_conn",  # why-allowed
    (
        "finance_cli/gateway/server.py",
        "_make_build_chat_runtime._build_chat_runtime",
        "db.connect",
    ): "gateway user chat acquires a request lease before runtime DB reads when lease infra is available",  # why-allowed
    (
        "finance_cli/mcp_server.py",
        "_get_conn",
        "db.connect",
    ): "MCP requests inherit gateway user context/lease; standalone local MCP use has no PG manager",  # why-allowed
    (
        "finance_cli/mcp_server.py",
        "_RemoteAwareSkillStateStore._read_all",
        "storage_files.list_files",
    ): "remote skill state is only installed for routed remote user contexts from the gateway",  # why-allowed
    (
        "finance_cli/mcp_server.py",
        "_RemoteAwareSkillStateStore._read_all",
        "storage_files.read_file",
    ): "remote skill state is only installed for routed remote user contexts from the gateway",  # why-allowed
    (
        "finance_cli/mcp_server.py",
        "_RemoteAwareSkillStateStore._write_all",
        "storage_files.write_file",
    ): "remote skill state is only installed for routed remote user contexts from the gateway",  # why-allowed
    (
        "finance_cli/mcp_server.py",
        "_scan_credit_topup_user_hashes",
        "db.connect",
    ): "credit top-up scan is a global billing helper outside per-user cutover traffic",  # why-allowed
    (
        "finance_cli/normalizer_sidecars.py",
        "read_text",
        "storage_files.read_file",
    ): "callers pass target_info from remote_sidecar_target; tuple provenance is not statically inferred",  # why-allowed
    (
        "finance_cli/normalizer_sidecars.py",
        "write_text",
        "storage_files.write_file",
    ): "callers pass target_info from remote_sidecar_target; tuple provenance is not statically inferred",  # why-allowed
    (
        "finance_cli/normalizer_sidecars.py",
        "delete_file",
        "storage_files.delete_file",
    ): "callers pass target_info from remote_sidecar_target; tuple provenance is not statically inferred",  # why-allowed
    (
        "finance_cli/normalizer_sidecars.py",
        "list_paths",
        "storage_files.list_files",
    ): "callers pass target_info from remote_sidecar_target; tuple provenance is not statically inferred",  # why-allowed
    (
        "finance_cli/sync/engine.py",
        "SyncEngine.schema_version",
        "db.connect",
    ): "desktop sync engine accesses the local client DB outside server-side PG cutover",  # why-allowed
    (
        "finance_cli/sync/engine.py",
        "SyncEngine.bump_last_applied",
        "db.connect",
    ): "desktop sync engine updates the local client DB outside server-side PG cutover",  # why-allowed
    (
        "finance_cli/sync/engine.py",
        "SyncEngine._read_current_sync_state_sync",
        "db.connect",
    ): "desktop sync engine reads the local client DB outside server-side PG cutover",  # why-allowed
    (
        "finance_cli/sync/engine.py",
        "SyncEngine._set_subscriber_status",
        "db.connect",
    ): "desktop sync engine updates the local client DB outside server-side PG cutover",  # why-allowed
    (
        "finance_cli/telegram_bot/store.py",
        "BotStore.startup",
        "db.connect",
    ): "Telegram bot web callers establish a LeaseScope before starting the per-user store",  # why-allowed
    (
        "finance-web/server/telegram_webhook.py",
        "_load_token_payload",
        "storage_files.read_file",
    ): "Remote Telegram token reads require an active RemoteLease before calling storage_files.read_file",  # why-allowed
    (
        "finance-web/server/telegram_secrets.py",
        "read_legacy_token_payload",
        "storage_files.read_file",
    ): "remote legacy token callers run under Telegram route/webhook/delete LeaseScope before storage_files.read_file",  # why-allowed
    (
        "finance-web/server/telegram_secrets.py",
        "delete_legacy_token_payload",
        "storage_files.delete_file",
    ): "remote legacy token cleanup runs under Telegram route/webhook/delete LeaseScope before storage_files.delete_file",  # why-allowed
    (
        "finance_cli/user_rules.py",
        "_load_remote_rules_payload",
        "storage_files.read_file",
    ): "Remote rules.yaml reads require an active RemoteLease before calling storage_files.read_file",  # why-allowed
    (
        "finance_cli/user_provisioning.py",
        "_seed_user_categories",
        "db.connect",
    ): "provisioning category seeding is called under get_user_conn/sync/auth LeaseScope after routing",  # why-allowed
}


@dataclass(frozen=True)
class _ResourceCall:
    qualname: str
    lineno: int
    kind: str
    scoped: bool


class _ResourceCallVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.db_connect_names: set[str] = set()
        self.db_module_names: set[str] = set()
        self.storage_module_names: set[str] = set()
        self.storage_function_names: set[str] = set()
        self.mutation_lock_names: set[str] = set()
        self.mutation_lock_module_names: set[str] = set()
        self.cleanup_names: set[str] = set()
        self.cleanup_module_names: set[str] = set()
        self.depends_names: set[str] = {"Depends"}
        self.get_user_conn_names: set[str] = {"get_user_conn"}
        self.class_qualnames: set[str] = set()
        self.lease_scope_names: set[str] = {
            "LeaseScope",
            "optional_lease_scope",
            "storage_lease_scope",
            "_plaid_lease_scope",
            "_sync_local_lease_scope",
            "_sync_storage_scope",
            "_telegram_lease_scope",
        }
        self.lease_scope_module_names: set[str] = set()
        self.acquire_or_route_names: set[str] = {"acquire_or_route"}
        self.calls: list[_ResourceCall] = []
        self.local_function_names: set[str] = set()
        self.function_lease_scopes: dict[str, bool] = {}
        self.local_calls: list[tuple[str, str, bool]] = []
        self._scope: list[str] = []
        self._function_lease_scopes: list[bool] = []
        self._lease_context_stack: list[bool] = []

    def collect_imports(self, tree: ast.AST) -> None:
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.local_function_names.add(node.name)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    asname = alias.asname or alias.name.split(".", 1)[0]
                    if alias.name == "finance_cli.db":
                        self.db_module_names.add(asname)
                    elif alias.name == "finance_cli.storage_files":
                        self.storage_module_names.add(asname)
                    elif alias.name.endswith(".mutation_lock"):
                        self.mutation_lock_module_names.add(asname)
                    elif alias.name.endswith(".cleanup"):
                        self.cleanup_module_names.add(asname)
                    elif alias.name.endswith(".storage_lease"):
                        self.lease_scope_module_names.add(asname)
                    elif alias.name.endswith(".dependencies"):
                        self.lease_scope_module_names.add(asname)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                for alias in node.names:
                    asname = alias.asname or alias.name
                    if module == "fastapi" and alias.name == "Depends":
                        self.depends_names.add(asname)
                    if module in {"finance_cli.db", "db"} or module.endswith(".db"):
                        if alias.name == "connect":
                            self.db_connect_names.add(asname)
                    elif module == "finance_cli" and alias.name == "db":
                        self.db_module_names.add(asname)
                    elif module == "finance_cli" and alias.name == "storage_files":
                        self.storage_module_names.add(asname)
                    elif module in {"finance_cli.storage_files", "storage_files"} or module.endswith(".storage_files"):
                        if alias.name in STORAGE_FILE_FUNCTIONS:
                            self.storage_function_names.add(asname)
                    elif module == "" and alias.name == "storage_files":
                        self.storage_module_names.add(asname)
                    if module.endswith(".mutation_lock") and alias.name in MUTATION_LOCK_FUNCTIONS:
                        self.mutation_lock_names.add(asname)
                    if module.endswith(".cleanup") and alias.name in CLEANUP_FUNCTIONS:
                        self.cleanup_names.add(asname)
                    if module.endswith(".dependencies"):
                        if alias.name == "get_user_conn":
                            self.get_user_conn_names.add(asname)
                        if alias.name == "storage_lease_scope":
                            self.lease_scope_names.add(asname)
                    if module.endswith(".storage_lease"):
                        if alias.name in LEASE_IMPORT_NAMES:
                            self.lease_scope_names.add(asname)
                        if alias.name == "acquire_or_route":
                            self.acquire_or_route_names.add(asname)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self.class_qualnames.add(self._qualname())
        self.generic_visit(node)
        self._scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scope.append(node.name)
        has_lease_dependency = self._function_has_get_user_conn_dependency(node)
        self.function_lease_scopes[self._qualname()] = has_lease_dependency
        self._function_lease_scopes.append(has_lease_dependency)
        self.generic_visit(node)
        self._function_lease_scopes.pop()
        self._scope.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scope.append(node.name)
        has_lease_dependency = self._function_has_get_user_conn_dependency(node)
        self.function_lease_scopes[self._qualname()] = has_lease_dependency
        self._function_lease_scopes.append(has_lease_dependency)
        self.generic_visit(node)
        self._function_lease_scopes.pop()
        self._scope.pop()

    def visit_With(self, node: ast.With) -> None:
        self._visit_with_body(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        self._visit_with_body(node)

    def visit_Call(self, node: ast.Call) -> None:
        local_callee = self._local_callee_name(node.func)
        if local_callee is not None and self._scope:
            self.local_calls.append((self._qualname(), local_callee, self._in_lease_scope()))
        for passed_callee in self._thread_submitted_local_callees(node):
            if self._scope:
                self.local_calls.append((self._qualname(), passed_callee, self._in_lease_scope()))
        kind = self._resource_call_kind(node.func)
        if kind is not None:
            self.calls.append(
                _ResourceCall(
                    qualname=self._qualname(),
                    lineno=int(node.lineno),
                    kind=kind,
                    scoped=self._in_lease_scope(),
                )
            )
        self.generic_visit(node)

    def _visit_with_body(self, node: ast.With | ast.AsyncWith) -> None:
        scoped = any(self._is_lease_context_expr(item.context_expr) for item in node.items)
        for item in node.items:
            self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.visit(item.optional_vars)
        self._lease_context_stack.append(scoped or self._in_lease_scope())
        for child in node.body:
            self.visit(child)
        self._lease_context_stack.pop()

    def _qualname(self) -> str:
        return ".".join(self._scope) if self._scope else "<module>"

    def _in_lease_scope(self) -> bool:
        return any(self._function_lease_scopes) or any(self._lease_context_stack)

    def _function_has_get_user_conn_dependency(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        defaults = [default for default in node.args.defaults if default is not None]
        defaults.extend(default for default in node.args.kw_defaults if default is not None)
        return any(self._is_get_user_conn_dependency(default) for default in defaults)

    def _is_get_user_conn_dependency(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Call):
            return False
        func_name = _callable_name(node.func)
        if func_name not in self.depends_names and not (func_name or "").endswith(".Depends"):
            return False
        if not node.args:
            return False
        dep_name = _callable_name(node.args[0])
        return dep_name in self.get_user_conn_names or (dep_name or "").endswith(".get_user_conn")

    def _is_lease_context_expr(self, node: ast.AST) -> bool:
        call = node if isinstance(node, ast.Call) else None
        func = call.func if call is not None else node
        name = _callable_name(func)
        if name in self.lease_scope_names:
            return True
        if name in self.acquire_or_route_names:
            return True
        if name in {"LeaseScope.acquire", "LeaseScope"}:
            return True
        if name and (
            name.endswith(".LeaseScope.acquire")
            or name.endswith(".LeaseScope")
            or name.endswith(".optional_lease_scope")
            or name.endswith(".storage_lease_scope")
            or name.endswith("_lease_scope")
        ):
            return True
        path = _attribute_path(func)
        return bool(
            len(path) >= 2
            and path[0] in self.lease_scope_module_names
            and path[-1] in {"LeaseScope", "optional_lease_scope", "storage_lease_scope"}
        )

    def _local_callee_name(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name) and node.id in self.local_function_names:
            return node.id
        return None

    def _thread_submitted_local_callees(self, node: ast.Call) -> list[str]:
        name = _callable_name(node.func)
        if name not in {"asyncio.to_thread", "to_thread"}:
            return []
        if not node.args:
            return []
        callee = self._local_callee_name(node.args[0])
        return [callee] if callee is not None else []

    def _resource_call_kind(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            if node.id in self.db_connect_names:
                return "db.connect"
            if node.id in self.storage_function_names:
                return f"storage_files.{node.id}"
            if node.id in self.mutation_lock_names:
                return "mutation_lock"
            if node.id in self.cleanup_names:
                return f"cleanup.{node.id}"
            return None

        path = _attribute_path(node)
        if not path:
            return None
        if len(path) == 2 and path[0] in self.db_module_names and path[1] == "connect":
            return "db.connect"
        if path == ("finance_cli", "db", "connect"):
            return "db.connect"
        if len(path) == 2 and path[0] in self.storage_module_names and path[1] in STORAGE_FILE_FUNCTIONS:
            return f"storage_files.{path[1]}"
        if len(path) == 3 and path[:2] == ("finance_cli", "storage_files") and path[2] in STORAGE_FILE_FUNCTIONS:
            return f"storage_files.{path[2]}"
        if len(path) == 2 and path[0] in self.mutation_lock_module_names and path[1] in MUTATION_LOCK_FUNCTIONS:
            return "mutation_lock"
        if len(path) == 2 and path[0] in self.cleanup_module_names and path[1] in CLEANUP_FUNCTIONS:
            return f"cleanup.{path[1]}"
        return None


def _attribute_path(node: ast.AST) -> tuple[str, ...]:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return tuple(reversed(parts))
    return ()


def _callable_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    path = _attribute_path(node)
    if path:
        return ".".join(path)
    return None


def _callee_qualnames(
    functions_by_name: dict[str, list[str]],
    class_qualnames: set[str],
    caller: str,
    callee_name: str,
) -> list[str]:
    candidates = functions_by_name.get(callee_name, [])
    candidate_set = set(candidates)
    caller_parts = [] if caller == "<module>" else caller.split(".")
    for end in range(len(caller_parts), -1, -1):
        container = ".".join(caller_parts[:end])
        if container in class_qualnames:
            continue
        qualname = f"{container}.{callee_name}" if container else callee_name
        if qualname in candidate_set:
            return [qualname]

    return []


def _source_paths() -> list[Path]:
    paths: list[Path] = []
    for root in SOURCE_ROOTS:
        paths.extend(
            path
            for path in root.rglob("*.py")
            if not any(part.endswith("-dist") for part in path.parts)
        )
    return sorted(paths)


def _module_imports_lease_helper(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.Import):
            if any(alias.name == "storage_lease" or alias.name.endswith(".storage_lease") for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "storage_lease" or module.endswith(".storage_lease"):
                return True
            if any(alias.name == "storage_lease" for alias in node.names):
                return True
            if any(alias.name in LEASE_IMPORT_NAMES for alias in node.names):
                return True
    return False


def _direct_resource_calls(path: Path, *, root: Path = ROOT) -> list[tuple[str, str, int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    visitor = _ResourceCallVisitor()
    visitor.collect_imports(tree)
    visitor.visit(tree)
    relative = str(path.relative_to(root))
    return [(relative, call.qualname, call.lineno, call.kind) for call in visitor.calls]


def _unscoped_resource_calls(path: Path, *, root: Path = ROOT) -> list[tuple[str, str, int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    visitor = _ResourceCallVisitor()
    visitor.collect_imports(tree)
    visitor.visit(tree)
    relative = str(path.relative_to(root))
    functions_by_name: dict[str, list[str]] = {}
    for qualname in visitor.function_lease_scopes:
        functions_by_name.setdefault(qualname.rsplit(".", 1)[-1], []).append(qualname)

    scoped_functions = {
        qualname for qualname, scoped in visitor.function_lease_scopes.items() if scoped
    }
    incoming_calls: set[str] = set()
    for caller, callee_name, _call_scoped in visitor.local_calls:
        incoming_calls.update(
            _callee_qualnames(
                functions_by_name,
                visitor.class_qualnames,
                caller,
                callee_name,
            )
        )

    unscoped_functions = {
        qualname
        for qualname, scoped in visitor.function_lease_scopes.items()
        if not scoped and qualname not in incoming_calls
    }
    changed = True
    while changed:
        changed = False
        for caller, callee_name, call_scoped in visitor.local_calls:
            for callee in _callee_qualnames(
                functions_by_name,
                visitor.class_qualnames,
                caller,
                callee_name,
            ):
                if (call_scoped or caller in scoped_functions) and callee not in scoped_functions:
                    scoped_functions.add(callee)
                    changed = True
                if (
                    not call_scoped
                    and caller in unscoped_functions
                    and callee not in unscoped_functions
                    and not visitor.function_lease_scopes.get(callee, False)
                ):
                    unscoped_functions.add(callee)
                    changed = True

    return [
        (relative, call.qualname, call.lineno, call.kind)
        for call in visitor.calls
        if not call.scoped and (
            call.qualname not in scoped_functions or call.qualname in unscoped_functions
        )
    ]


def _is_test_allowlisted(relative: str) -> bool:
    return any(relative.startswith(prefix) for prefix in DIRECT_RESOURCE_TEST_PATH_ALLOWLIST)


def _is_call_allowlisted(relative: str, qualname: str, kind: str) -> bool:
    return (relative, qualname, kind) in DIRECT_RESOURCE_CALL_ALLOWLIST


def _unscoped_direct_resource_calls(paths: list[Path], *, root: Path = ROOT) -> list[str]:
    offenders: list[str] = []
    for path in paths:
        calls = _unscoped_resource_calls(path, root=root)
        if not calls:
            continue
        for relative, qualname, lineno, kind in calls:
            if _is_test_allowlisted(relative):
                continue
            if _is_call_allowlisted(relative, qualname, kind):
                continue
            offenders.append(f"{relative}:{qualname}:{lineno}: {kind}")
    return offenders


def test_catalog_resource_files_import_lease_helper():
    missing = []
    for relative in LEASE_CATALOG_FILES:
        path = ROOT / relative
        if _direct_resource_calls(path):
            if not _module_imports_lease_helper(path):
                missing.append(relative)
    assert missing == []


def test_direct_storage_calls_are_lease_scoped_or_allowlisted():
    assert _unscoped_direct_resource_calls(_source_paths()) == []


def test_ast_backstop_detects_aliased_connect_without_lease(tmp_path):
    path = tmp_path / "bad_alias.py"
    path.write_text(
        "from finance_cli.db import connect as open_db\n\n"
        "def unleased(path):\n"
        "    return open_db(path)\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == [
        "bad_alias.py:unleased:4: db.connect"
    ]


def test_ast_backstop_detects_router_mutation_lock_without_get_user_conn(tmp_path):
    path = tmp_path / "bad_router.py"
    path.write_text(
        "from fastapi import Depends, Request\n"
        "from server.dependencies import get_user_paths, require_active_subscription\n"
        "from server.mutation_lock import mutation_lock\n\n"
        "def bad(request: Request, user=Depends(require_active_subscription), paths=Depends(get_user_paths)):\n"
        "    with mutation_lock(request.app.state.settings, user['user_id'], timeout=10.0):\n"
        "        return None\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == [
        "bad_router.py:bad:6: mutation_lock"
    ]


def test_ast_backstop_detects_helper_reused_by_unscoped_path(tmp_path):
    path = tmp_path / "mixed_helper.py"
    path.write_text(
        "from fastapi import Depends\n"
        "from finance_cli.db import connect\n"
        "from server.dependencies import get_user_conn\n\n"
        "def helper(path):\n"
        "    return connect(path)\n\n"
        "def scoped_route(path, conn=Depends(get_user_conn)):\n"
        "    return helper(path)\n\n"
        "def unscoped_cli(path):\n"
        "    return helper(path)\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == [
        "mixed_helper.py:helper:6: db.connect"
    ]


def test_ast_backstop_keeps_same_name_helpers_separate(tmp_path):
    path = tmp_path / "same_name_helper.py"
    path.write_text(
        "from fastapi import Depends\n"
        "from finance_cli.db import connect\n"
        "from server.dependencies import get_user_conn\n\n"
        "def helper(path):\n"
        "    return connect(path)\n\n"
        "def scoped_route(path, conn=Depends(get_user_conn)):\n"
        "    def helper(path):\n"
        "        return connect(path)\n"
        "    return helper(path)\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == [
        "same_name_helper.py:helper:6: db.connect"
    ]


def test_ast_backstop_ignores_imported_name_colliding_with_local_method(tmp_path):
    path = tmp_path / "import_collision.py"
    path.write_text(
        "from fastapi import Depends\n"
        "from external import helper\n"
        "from finance_cli.db import connect\n"
        "from server.dependencies import get_user_conn\n\n"
        "class Other:\n"
        "    def helper(self, path):\n"
        "        return connect(path)\n\n"
        "def scoped_route(path, conn=Depends(get_user_conn)):\n"
        "    return helper(path)\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == [
        "import_collision.py:Other.helper:8: db.connect"
    ]


def test_ast_backstop_allows_helper_called_only_from_scoped_path(tmp_path):
    path = tmp_path / "scoped_helper.py"
    path.write_text(
        "from fastapi import Depends\n"
        "from finance_cli.db import connect\n"
        "from server.dependencies import get_user_conn\n\n"
        "def helper(path):\n"
        "    return connect(path)\n\n"
        "def scoped_route(path, conn=Depends(get_user_conn)):\n"
        "    return helper(path)\n",
        encoding="utf-8",
    )

    assert _unscoped_direct_resource_calls([path], root=tmp_path) == []


def test_no_run_in_executor_without_context_helper():
    offenders = []
    for path in _source_paths():
        text = path.read_text(encoding="utf-8")
        if (
            "run_in_executor(" in text
            and "LeaseScope.run_in_context" not in text
            and "LeaseScope.bind_context" not in text
        ):
            offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []


def test_remote_mcp_modules_do_not_use_threadpool_hops_blocked_by_ci():
    forbidden_patterns = (
        "asyncio.to_thread",
        "run_in_executor",
        "ThreadPoolExecutor",
        "anyio.to_thread",
    )
    offenders = []
    for relative_path in ("finance_cli/mcp_server.py", "finance_cli/mcp_remote.py"):
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        hits = [pattern for pattern in forbidden_patterns if pattern in text]
        if hits:
            offenders.append(f"{relative_path}: {', '.join(hits)}")

    assert offenders == []
