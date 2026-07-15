from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import psycopg2
import psycopg2.extras
import pytest
import pytest_asyncio
from app_platform.db.migration import run_migrations_dir
from fastmcp.client import Client
from fastmcp.client.transports import StdioTransport

from finance_cli import db as db_module
from finance_cli.db import connect
from finance_cli.user_provisioning import provision_user, user_db_path

from .helpers.fake_auth_service import FakeAuthService, SYNTHETIC_TOKEN
from .helpers.fake_secrets_backend import FakeSecretsBackend
from .helpers.server_runtime import start_server, stop_server

REPO_ROOT = Path(__file__).resolve().parents[3]
FINANCE_WEB_ROOT = REPO_ROOT / "finance-web"
if str(FINANCE_WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(FINANCE_WEB_ROOT))

from server.app import create_app  # noqa: E402
from server.config import FINANCE_CLI_ROOT, Settings  # noqa: E402

TEST_GOOGLE_CLIENT_ID = "test-client-id"
TEST_SESSION_SECRET = "test-secret"


def _require_integration() -> bool:
    return str(os.environ.get("CI_REQUIRE_INTEGRATION") or "").strip() == "1"


def _merge_search_path(database_url: str, schema: str) -> str:
    parts = urlsplit(database_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    existing_opts = query.get("options", "")
    merged_opts = (existing_opts + " " if existing_opts else "") + f"-csearch_path={schema},public"
    query["options"] = merged_opts
    # CRITICAL (R4 finding): use quote_via=quote so spaces in multi-option `options`
    # values become %20, not +. libpq does not decode + back to space in query
    # strings — leaving + would fuse two libpq flags into a single malformed one.
    return urlunsplit(parts._replace(query=urlencode(query, quote_via=quote)))


def _schema_name() -> str:
    return f"test_local_mcp_e2e_{os.getpid()}_{int(time.time())}"


@pytest.fixture(autouse=True)
def _stub_db_keys():
    yield  # no-op shadow


@pytest.fixture(autouse=True)
def _clean_data_dir_env():
    yield


@pytest.fixture(autouse=True)
def _suppress_alerts():
    yield


@pytest.fixture(scope="session")
def postgres_url() -> str:
    value = str(os.environ.get("TEST_POSTGRES_URL") or "").strip()
    if not value:
        message = "integration tests require TEST_POSTGRES_URL"
        if _require_integration():
            pytest.fail(message)
        pytest.skip(message)

    try:
        with psycopg2.connect(value):
            pass
    except psycopg2.OperationalError as exc:
        message = f"Unable to connect to TEST_POSTGRES_URL: {exc}"
        if _require_integration():
            pytest.fail(message)
        pytest.skip(message)

    return value


@pytest.fixture()
def seeded_users_row(postgres_url: str):
    schema = _schema_name()
    database_url = _merge_search_path(postgres_url, schema)
    migrations_dir = FINANCE_WEB_ROOT / "server" / "migrations"

    with psycopg2.connect(
        postgres_url,
        cursor_factory=psycopg2.extras.RealDictCursor,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'SET search_path TO "{schema}", public')
            run_migrations_dir(migrations_dir, conn)
            cursor.execute(
                """
                INSERT INTO users (email, name, google_user_id, auth_provider)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                ("e2e@test", "E2E User", "test-google-id", "google"),
            )
            row = cursor.fetchone()
        conn.commit()

    try:
        yield {
            "id": str(row["id"]),
            "email": "e2e@test",
            "name": "E2E User",
            "google_user_id": "test-google-id",
            "schema": schema,
            "database_url": database_url,
        }
    finally:
        with psycopg2.connect(postgres_url) as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.fixture()
def fake_secrets_backend() -> FakeSecretsBackend:
    return FakeSecretsBackend()


@pytest.fixture()
def encrypted_server_mode(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FINANCE_CLI_REQUIRE_DB_ENCRYPTION", "provision")
    assert db_module.db_encryption_mode() == "provision"
    yield


@pytest_asyncio.fixture()
async def in_process_server(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seeded_users_row: dict[str, str],
    fake_secrets_backend: FakeSecretsBackend,
    encrypted_server_mode,
):
    del encrypted_server_mode

    settings = Settings(
        app_name="finance-web",
        **{
            "APP_ENV": "test",
            "HOST": "127.0.0.1",
            "PORT": 8001,
            "DATABASE_URL": seeded_users_row["database_url"],
            "GOOGLE_CLIENT_ID": TEST_GOOGLE_CLIENT_ID,
            "SESSION_SECRET": TEST_SESSION_SECRET,
            "SESSION_COOKIE_NAME": "cashnerd_session",
            "COOKIE_SECURE": False,
            "FRONTEND_ORIGIN": "http://localhost:5173",
            "CORS_ORIGINS": ["http://localhost:5173"],
            "FINANCE_WEB_DATA_ROOT": tmp_path / "users",
            "FINANCE_WEB_RULES_TEMPLATE": (
                FINANCE_CLI_ROOT / "finance_cli" / "data" / "rules_template.yaml"
            ).resolve(),
            "APP_PLATFORM_SRC": None,
            "AUTO_RUN_MIGRATIONS": False,
            "GATEWAY_URL": "https://gateway.example.com",
            "GATEWAY_USER_KEYS": json.dumps(
                [
                    {
                        "key": "web-key-1",
                        "channel": "web",
                        "user_id": 1,
                        "email": "user1@example.test",
                        "role": "owner",
                    }
                ]
            ),
            "GATEWAY_SSL_VERIFY": True,
            "RATE_LIMIT_WINDOW_SECONDS": 60,
            "ANON_RATE_LIMIT": 60,
            "FREE_TIER_RATE_LIMIT": 100,
            "PAID_TIER_RATE_LIMIT": 1000,
        }
    )

    monkeypatch.setattr("finance_cli.secrets_backend._get_client", lambda: fake_secrets_backend)
    app = create_app(settings)
    app.state.auth_service = FakeAuthService()
    monkeypatch.setattr(
        "server.routers.sync_router._resolve_user_id",
        lambda *args, **kwargs: seeded_users_row["id"],
    )

    provision_user(
        data_root=settings.data_root,
        user_id=seeded_users_row["id"],
        template_rules_path=settings.template_rules_path,
    )
    server_db_path = user_db_path(settings.data_root, seeded_users_row["id"])
    with connect(server_db_path, expected_user_id=seeded_users_row["id"]) as conn:
        conn.execute(
            """
            INSERT INTO transactions (id, date, description, amount_cents, source, is_active)
            VALUES ('txn-server-seed', '2026-04-16', 'Server Seed', -500, 'manual', 1)
            """
        )
        conn.commit()

    runtime = await start_server(app)
    try:
        yield SimpleNamespace(
            app=app,
            base_url=runtime.base_url,
            port=runtime.port,
            server=runtime.server,
            task=runtime.task,
            settings=settings,
            user_id=seeded_users_row["id"],
            server_db_path=server_db_path,
            schema=seeded_users_row["schema"],
            database_url=seeded_users_row["database_url"],
        )
    finally:
        await stop_server(runtime)
        app.state.pool_manager.close()


@pytest.fixture()
def test_home_dir(tmp_path: Path) -> Path:
    home_dir = tmp_path / "home"
    home_dir.mkdir(parents=True, exist_ok=True)
    return home_dir


@pytest.fixture()
def seeded_token_file(test_home_dir: Path) -> Path:
    token_path = test_home_dir / ".cashnerd" / "auth" / "token.json"
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(
        json.dumps(
            {
                "id_token": SYNTHETIC_TOKEN,
                "access_token": "n/a",
                "refresh_token": "",
                "expires_at": "2099-01-01T00:00:00Z",
                "google_client_id": TEST_GOOGLE_CLIENT_ID,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    token_path.chmod(0o600)
    return token_path


@pytest_asyncio.fixture()
async def mcp_client(
    test_home_dir: Path,
    seeded_token_file: Path,
    in_process_server,
):
    del seeded_token_file

    transport = StdioTransport(
        command=sys.executable,
        args=["-m", "finance_cli.mcp_local"],
        env={
            **os.environ,
            "HOME": str(test_home_dir),
            "FINANCE_CLI_DISABLE_DOTENV": "1",
            "GOOGLE_CLIENT_ID": TEST_GOOGLE_CLIENT_ID,
            "CASHNERD_SERVER_URL": in_process_server.base_url,
            "PYTHONUNBUFFERED": "1",
        },
        keep_alive=False,
    )
    async with Client(transport) as client:
        yield client
