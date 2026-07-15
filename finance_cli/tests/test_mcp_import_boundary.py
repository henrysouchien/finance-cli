from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_mcp_server_import_does_not_load_distribution_excluded_modules() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """\
                import sys

                for name in list(sys.modules):
                    if name == "finance_cli.mcp_server" or name.startswith("finance_cli.gateway"):
                        sys.modules.pop(name)

                import finance_cli.mcp_server

                blocked = {
                    "finance_cli.commands.account_cmd",
                    "finance_cli.commands.db_cmd",
                    "finance_cli.commands.ingest",
                    "finance_cli.commands.plaid_cmd",
                    "finance_cli.commands.stripe_cmd",
                    "finance_cli.commands.txn",
                    "finance_cli.gateway.tools",
                    "finance_cli.gateway.server",
                    "finance_cli.mcp_remote",
                    "finance_cli.telegram_bot.bot",
                    "finance_cli.frontend_logs",
                    "finance_cli.plaid_client",
                    "finance_cli.stripe_client",
                    "finance_cli.schwab_client",
                    "finance_cli.sync.tool_classification",
                    "finance_cli.user_provisioning",
                    "finance_cli.storage_files",
                    "finance_cli.storage_client.connection",
                    "finance_cli.storage_client.cursor",
                    "finance_cli.storage_client.errors",
                    "finance_cli.storage_client.session_pool",
                    "finance_cli.storage_client.sync_snapshot",
                    "finance_cli.secrets_backend",
                    "finance_cli.secrets_store",
                    "boto3",
                    "botocore",
                    "grpc",
                    "grpc.aio",
                }
                loaded = sorted(name for name in blocked if name in sys.modules)
                assert not loaded, loaded
                """
            ),
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_storage_dispatch_import_does_not_load_grpc_transport() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """\
                import sys

                import finance_cli.storage_client._dispatch

                blocked = {
                    "finance_cli.storage_client.connection",
                    "finance_cli.storage_client.errors",
                    "finance_cli.storage_client._generated.storage_server_pb2",
                    "finance_cli.storage_client._generated.storage_server_pb2_grpc",
                    "grpc",
                    "grpc.aio",
                }
                loaded = sorted(name for name in blocked if name in sys.modules)
                assert not loaded, loaded
                """
            ),
        ],
        capture_output=True,
        cwd=_PROJECT_ROOT,
        text=True,
    )

    assert result.returncode == 0, result.stderr
