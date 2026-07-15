"""Remote MCP entry point with Google OAuth and per-user context isolation."""

from __future__ import annotations

import logging
import os
import html as html_module
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import mcp.types as mt
from fastmcp.server.auth.oauth_proxy import consent as fastmcp_oauth_consent
from fastmcp.server.auth.providers.google import GoogleProvider
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.tools.tool import ToolResult

try:
    import psycopg2
except ImportError as exc:  # pragma: no cover - environment dependency
    raise ImportError(
        "finance_cli.mcp_remote requires psycopg2 for PostgreSQL user resolution. "
        "Install the finance-web dependencies on the remote host."
    ) from exc

from finance_cli.config import PACKAGE_TEMPLATE_DIR, load_dotenv
from finance_cli.billing import (
    apply_trial_cost_cap,
    effective_tier,
    mcp_tool_allowed_for_user,
)
from finance_cli.gateway.tools import ALL_NORMALIZER_TOOLS, EXCLUDED_TOOLS
from finance_cli.mcp_remote_config import McpRemoteSettings
from finance_cli.mcp_server import (
    REGISTERED_TOOL_NAMES,
    mcp,
)
from finance_cli.plaid_client import register_revocation_failure_handler
from finance_cli.storage_client import _dispatch as storage_dispatch
from finance_cli.tool_registry import validate_registry
from finance_cli.user_context import UserContext, reset_user_context, set_user_context
from finance_cli.user_provisioning import provision_user, user_db_path, user_rules_path

_TEMPLATE_RULES_PATH = PACKAGE_TEMPLATE_DIR / "rules_template.yaml"
_REMOTE_RESERVED_ARG_KEYS = {"_request_id", "_session_id"}
_EXTRA_FILE_TOOLS = frozenset(
    {
        "db_import_preferences",
        "db_backup_verify",
        "db_backup",
        "db_export_preferences",
    }
)
REMOTE_EXCLUDED_TOOLS = EXCLUDED_TOOLS | ALL_NORMALIZER_TOOLS | _EXTRA_FILE_TOOLS
logger = logging.getLogger(__name__)
_STORAGE_POOL_LIFESPAN_INSTALLED = False

_CONSENT_SCOPE_LABELS = {
    "openid": "Verify your Google sign-in",
    "https://www.googleapis.com/auth/userinfo.email": "Read your email address",
    "https://www.googleapis.com/auth/userinfo.profile": "Read your name and profile",
}


def _display_host(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme and parsed.netloc:
        return parsed.netloc
    return uri


def _create_cashnerd_consent_html(
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    txn_id: str,
    csrf_token: str,
    client_name: str | None = None,
    title: str = "Connect CashNerd",
    server_name: str | None = None,
    server_icon_url: str | None = None,
    server_website_url: str | None = None,
    client_website_url: str | None = None,
    csp_policy: str | None = None,
    is_cimd_client: bool = False,
    cimd_domain: str | None = None,
) -> str:
    del title, server_name, server_icon_url, server_website_url, client_website_url
    del is_cimd_client, cimd_domain

    client_display = html_module.escape(client_name or client_id)
    client_id_escaped = html_module.escape(client_id)
    redirect_uri_escaped = html_module.escape(redirect_uri)
    redirect_host = html_module.escape(_display_host(redirect_uri))
    scope_items = (
        "\n".join(
            f"<li>{html_module.escape(_CONSENT_SCOPE_LABELS.get(scope, scope))}</li>"
            for scope in scopes
        )
        or "<li>Use this MCP client with your CashNerd account</li>"
    )
    txn_id_escaped = html_module.escape(txn_id, quote=True)
    csrf_token_escaped = html_module.escape(csrf_token, quote=True)

    if csp_policy is None:
        csp_policy = "default-src 'none'; style-src 'unsafe-inline'; img-src https: data:; base-uri 'none'"
    csp_meta = (
        '<meta http-equiv="Content-Security-Policy" '
        f'content="{html_module.escape(csp_policy, quote=True)}" />'
        if csp_policy
        else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Connect CashNerd</title>
  {csp_meta}
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8f2ea;
      --surface: #fdf8f2;
      --panel: #f3eadf;
      --rule: #d8ccbd;
      --ink: #1c2028;
      --ink-soft: #495047;
      --ink-faint: #6f766d;
      --spruce: #1f5c4f;
      --spruce-dark: #17483d;
      --coral: #d96a4f;
      --gold: #c39a43;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-width: 320px;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 32px 16px;
      background: linear-gradient(180deg, var(--bg), var(--panel));
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.5;
    }}
    main {{
      width: min(100%, 620px);
      border: 1px solid var(--rule);
      border-radius: 8px;
      background: var(--surface);
      box-shadow: 0 1px 3px rgb(28 32 40 / 0.04), 0 16px 48px rgb(28 32 40 / 0.08);
      overflow: hidden;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 20px 24px;
      border-bottom: 1px solid var(--rule);
      background: #fffaf4;
    }}
    .mark {{
      display: grid;
      place-items: center;
      width: 36px;
      height: 36px;
      border-radius: 8px;
      background: var(--spruce);
      color: #fffaf4;
      font-weight: 700;
      letter-spacing: 0;
    }}
    .brand {{ font-weight: 700; }}
    .eyebrow {{
      margin: 0;
      color: var(--ink-faint);
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
    }}
    .content {{ padding: 28px 24px 24px; }}
    h1 {{
      margin: 0;
      color: var(--ink);
      font-size: 28px;
      line-height: 1.14;
      font-weight: 650;
      letter-spacing: 0;
    }}
    .lead {{
      margin: 12px 0 0;
      color: var(--ink-soft);
      font-size: 15px;
    }}
    .client {{
      margin-top: 22px;
      padding: 16px;
      border: 1px solid var(--rule);
      border-left: 4px solid var(--spruce);
      border-radius: 8px;
      background: var(--panel);
    }}
    .label {{
      margin: 0;
      color: var(--ink-faint);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }}
    .client-name {{
      margin: 4px 0 0;
      overflow-wrap: anywhere;
      font-weight: 650;
    }}
    .section {{
      margin-top: 20px;
      padding-top: 18px;
      border-top: 1px solid var(--rule);
    }}
    ul {{
      margin: 10px 0 0;
      padding-left: 20px;
      color: var(--ink-soft);
    }}
    li + li {{ margin-top: 6px; }}
    .redirect {{
      margin-top: 10px;
      padding: 12px;
      border: 1px solid #e4d7c6;
      border-radius: 8px;
      background: #fffaf4;
      color: var(--ink-soft);
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 13px;
      overflow-wrap: anywhere;
    }}
    details {{
      margin-top: 16px;
      color: var(--ink-soft);
      font-size: 13px;
    }}
    summary {{
      cursor: pointer;
      color: var(--spruce-dark);
      font-weight: 650;
    }}
    .detail-grid {{
      display: grid;
      gap: 8px;
      margin-top: 10px;
      padding: 12px;
      border: 1px solid var(--rule);
      border-radius: 8px;
      background: #fffaf4;
    }}
    .detail-grid div {{
      display: grid;
      grid-template-columns: 132px minmax(0, 1fr);
      gap: 10px;
    }}
    .detail-grid dt {{
      color: var(--ink-faint);
      font-weight: 650;
    }}
    .detail-grid dd {{
      margin: 0;
      overflow-wrap: anywhere;
    }}
    form {{ margin-top: 24px; }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    button {{
      min-height: 44px;
      border-radius: 8px;
      border: 1px solid transparent;
      padding: 0 18px;
      cursor: pointer;
      font-weight: 700;
    }}
    .approve {{
      background: var(--spruce);
      color: #fffaf4;
    }}
    .approve:hover {{ background: var(--spruce-dark); }}
    .deny {{
      background: transparent;
      color: var(--ink-soft);
      border-color: var(--rule);
    }}
    .deny:hover {{ border-color: var(--coral); color: var(--ink); }}
    .note {{
      margin: 18px 0 0;
      color: var(--ink-faint);
      font-size: 13px;
    }}
    @media (max-width: 520px) {{
      body {{ padding: 0; place-items: stretch; }}
      main {{ min-height: 100vh; border-radius: 0; border-left: 0; border-right: 0; }}
      .topbar, .content {{ padding-left: 18px; padding-right: 18px; }}
      .detail-grid div {{ grid-template-columns: 1fr; gap: 2px; }}
      button {{ flex: 1 1 100%; }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="topbar">
      <div class="mark" aria-hidden="true">C</div>
      <div>
        <div class="brand">CashNerd</div>
        <p class="eyebrow">Secure MCP authorization</p>
      </div>
    </div>
    <section class="content">
      <h1>Connect CashNerd to your MCP client</h1>
      <p class="lead">{client_display} is requesting permission to connect with your CashNerd account.</p>

      <div class="client">
        <p class="label">Requesting client</p>
        <p class="client-name">{client_display}</p>
      </div>

      <div class="section">
        <p class="label">Requested access</p>
        <ul>{scope_items}</ul>
      </div>

      <div class="section">
        <p class="label">Callback destination</p>
        <p class="lead">After Google sign-in, credentials return to {redirect_host}.</p>
        <div class="redirect">{redirect_uri_escaped}</div>
      </div>

      <details>
        <summary>Connection details</summary>
        <dl class="detail-grid">
          <div><dt>Client ID</dt><dd>{client_id_escaped}</dd></div>
          <div><dt>Redirect URI</dt><dd>{redirect_uri_escaped}</dd></div>
        </dl>
      </details>

      <form id="consentForm" method="POST" action="">
        <input type="hidden" name="txn_id" value="{txn_id_escaped}" />
        <input type="hidden" name="csrf_token" value="{csrf_token_escaped}" />
        <input type="hidden" name="submit" value="true" />
        <div class="actions">
          <button type="submit" name="action" value="approve" class="approve">Allow Access</button>
          <button type="submit" name="action" value="deny" class="deny">Deny</button>
        </div>
      </form>
      <p class="note">Only approve clients you recognize. Denying returns you to the requesting application without granting access.</p>
    </section>
  </main>
</body>
</html>"""


class GoogleProviderNoCIMD(GoogleProvider):
    """GoogleProvider with CIMD disabled.

    Claude Code's CIMD document lists redirect_uris without wildcard ports
    (http://localhost/callback), but uses dynamic ports for OAuth callbacks.
    FastMCP's port matching rejects these as port 80 != dynamic port.
    Disabling CIMD forces clients to use Dynamic Client Registration instead.

    This relies on OAuthProxy._cimd_manager (private attribute). Remove this
    subclass once GoogleProvider exposes enable_cimd (tracked upstream).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._cimd_manager = None


def _raise_if_deleted_user(cur, user_id: object) -> None:
    cur.execute(
        "SELECT deleted_at FROM users WHERE id = %s",
        (user_id,),
    )
    row = cur.fetchone()
    deleted_at = (
        row.get("deleted_at") if hasattr(row, "get") else (row[0] if row else None)
    )
    if deleted_at is not None:
        raise PermissionError("Account has been deleted.")


def _row_get(row: object, key: str, index: int, default: object = None) -> object:
    if row is None:
        return default
    if hasattr(row, "get"):
        return row.get(key, default)  # type: ignore[union-attr]
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, TypeError):
        return default


def _start_trial_if_registered(cur, user_id: str) -> None:
    cur.execute(
        """
        UPDATE users
           SET tier = 'trial',
               trial_ends_at = NOW() + INTERVAL '21 days',
               updated_at = NOW()
         WHERE id = %s
           AND tier = 'registered'
           AND trial_ends_at IS NULL
        """,
        (user_id,),
    )


def _apply_trial_bridge_for_new_user(
    *,
    data_root: Path | None,
    template_rules_path: Path | None,
    user_id: str,
) -> None:
    if data_root is None:
        return
    paths = provision_user(
        data_root=data_root,
        user_id=user_id,
        template_rules_path=template_rules_path or _TEMPLATE_RULES_PATH,
    )
    apply_trial_cost_cap(Path(paths["db_path"]))


def _load_user_billing_snapshot(user_id: str) -> dict[str, object]:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL required for user billing enforcement")

    try:
        conn = psycopg2.connect(database_url)
    except psycopg2.OperationalError as exc:
        raise RuntimeError(f"Cannot connect to user database: {exc}") from exc

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tier, trial_ends_at, deleted_at, lifetime_deal "
                "FROM users WHERE id = %s",
                (user_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None or _row_get(row, "deleted_at", 2) is not None:
        raise PermissionError("Account is not accessible")

    return {
        "tier": _row_get(row, "tier", 0),
        "trial_ends_at": _row_get(row, "trial_ends_at", 1),
        "lifetime_deal": _row_get(row, "lifetime_deal", 3),
    }


def _resolve_user_id(
    google_sub: str,
    email: str | None = None,
    name: str | None = None,
    *,
    data_root: Path | None = None,
    template_rules_path: Path | None = None,
) -> str:
    """Map Google sub claims to PostgreSQL users.id values."""

    google_sub = google_sub.strip()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL required for user resolution")

    email = (email or "").strip() or None
    name = (name or "").strip() or None

    try:
        conn = psycopg2.connect(database_url)
    except psycopg2.OperationalError as exc:
        raise RuntimeError(f"Cannot connect to user database: {exc}") from exc

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE google_user_id = %s",
                (google_sub,),
            )
            row = cur.fetchone()
            if row:
                raw_user_id = row[0]
                user_id = str(raw_user_id)
                _raise_if_deleted_user(cur, raw_user_id)
                _start_trial_if_registered(cur, user_id)
                conn.commit()
                return user_id

            if email:
                cur.execute(
                    "SELECT id FROM users WHERE email = %s",
                    (email,),
                )
                row = cur.fetchone()
                if row:
                    raw_user_id = row[0]
                    user_id = str(raw_user_id)
                    _raise_if_deleted_user(cur, raw_user_id)
                    cur.execute(
                        "UPDATE users SET google_user_id = %s, updated_at = NOW() WHERE id = %s",
                        (google_sub, user_id),
                    )
                    _start_trial_if_registered(cur, user_id)
                    conn.commit()
                    return user_id

            if not email:
                raise PermissionError("New user requires email scope for registration")

            display_name = name or email.split("@", 1)[0]
            cur.execute(
                """
                INSERT INTO users (email, name, google_user_id, auth_provider,
                                   tier, trial_ends_at)
                VALUES (%s, %s, %s, 'google', 'trial', NOW() + INTERVAL '21 days')
                ON CONFLICT (google_user_id) DO UPDATE SET updated_at = NOW()
                RETURNING id, (xmax = 0) AS inserted
                """,
                (email, display_name, google_sub),
            )
            row = cur.fetchone()
            conn.commit()
            if row:
                user_id = str(row[0])
                if bool(_row_get(row, "inserted", 1)):
                    _apply_trial_bridge_for_new_user(
                        data_root=data_root,
                        template_rules_path=template_rules_path,
                        user_id=user_id,
                    )
                return user_id
    except psycopg2.IntegrityError:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE google_user_id = %s",
                (google_sub,),
            )
            row = cur.fetchone()
            if row:
                raw_user_id = row[0]
                user_id = str(raw_user_id)
                _raise_if_deleted_user(cur, raw_user_id)
                _start_trial_if_registered(cur, user_id)
                conn.commit()
                return user_id
        raise RuntimeError("Failed to resolve user after concurrent insert")
    finally:
        conn.close()

    raise RuntimeError("Failed to resolve or create user record")


class RemoteUserMiddleware(Middleware):
    """Bind authenticated users to per-user DB, rules, and uploads paths."""

    def __init__(self, *, data_root: Path, template_rules_path: Path):
        self._data_root = Path(data_root)
        self._template_rules_path = Path(template_rules_path)

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, ToolResult],
    ) -> ToolResult:
        access_token = get_access_token()
        google_sub = (
            str(access_token.claims.get("sub") or "").strip()
            if access_token is not None
            else None
        )
        if not google_sub or google_sub == "unknown":
            raise PermissionError(
                "Google authentication did not return a valid user identity"
            )
        email = access_token.claims.get("email") if access_token is not None else None
        name = access_token.claims.get("name") if access_token is not None else None
        user_id = _resolve_user_id(
            google_sub,
            email,
            name,
            data_root=self._data_root,
            template_rules_path=self._template_rules_path,
        )

        user_snapshot = _load_user_billing_snapshot(user_id)
        tool_name = str(context.message.name or "")
        if not mcp_tool_allowed_for_user(tool_name, user_snapshot):
            raise PermissionError(
                f"Tool '{tool_name}' is not available on your current plan "
                f"(tier: {effective_tier(user_snapshot)}). "
                "Resubscribe at https://cashnerd.ai to restore full access."
            )

        args = dict(context.message.arguments or {})
        clean_message = mt.CallToolRequestParams(
            name=context.message.name,
            arguments={
                key: value
                for key, value in args.items()
                if not str(key).startswith("_user_")
                and key not in _REMOTE_RESERVED_ARG_KEYS
            },
            task=context.message.task,
            meta=context.message.meta,
        )
        clean_context = context.copy(message=clean_message)

        provision_user(
            data_root=self._data_root,
            user_id=user_id,
            template_rules_path=self._template_rules_path,
            ensure_canonical_categories=True,
        )
        db_path = user_db_path(self._data_root, user_id)
        rules_path = user_rules_path(self._data_root, user_id)
        uploads_dir = db_path.parent / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        storage_mode = storage_dispatch.storage_mode_for_user(user_id)

        token_user_context = set_user_context(
            UserContext.from_paths(
                db_path=db_path,
                expected_user_id=user_id,
                rules_path=rules_path,
                uploads_dir=uploads_dir,
                local_mode=False,
                storage_mode=storage_mode,
            )
        )
        try:
            return await call_next(clean_context)
        finally:
            reset_user_context(token_user_context)


async def _drain_storage_session_pool() -> None:
    from finance_cli.storage_client import auth, channel, errors
    from finance_cli.storage_client._generated import (
        storage_server_pb2_grpc as pb2_grpc,
    )
    from finance_cli.storage_client.session_pool import get_default_pool

    pool = get_default_pool()
    if pool.size() <= 0:
        errors.record_storage_session_pool_event("session_pool_close_all", count=0)
        return

    provider = auth.get_default_provider()

    def _make_stub(target: str):
        return pb2_grpc.SqliteProxyStub(channel._default_pool.get(target))

    def _make_metadata(
        product: str,
        user_id: str,
        auth_kid: str | None,
    ) -> tuple[tuple[str, str], ...]:
        del auth_kid
        token = provider.get_token(product, user_id, [])
        return (("authorization", f"Bearer {token}"),)

    count = pool.close_all(_make_stub, _make_metadata)
    errors.record_storage_session_pool_event("session_pool_close_all", count=count)


def _install_storage_pool_lifespan() -> None:
    global _STORAGE_POOL_LIFESPAN_INSTALLED
    if _STORAGE_POOL_LIFESPAN_INSTALLED:
        return
    original_lifespan = mcp._lifespan

    @asynccontextmanager
    async def _storage_pool_lifespan(server):
        async with original_lifespan(server):
            try:
                yield
            finally:
                with suppress(Exception):
                    await _drain_storage_session_pool()

    mcp._lifespan = _storage_pool_lifespan
    _STORAGE_POOL_LIFESPAN_INSTALLED = True


def configure_remote(settings: McpRemoteSettings) -> None:
    if settings.database_url:
        from server.revocation_queue import make_postgres_handler

        register_revocation_failure_handler(
            make_postgres_handler(SimpleNamespace(database_url=settings.database_url))
        )
        logger.info("plaid_revocation_handler_registered process=finance-mcp-remote")

    fastmcp_oauth_consent.create_consent_html = _create_cashnerd_consent_html
    mcp.auth = GoogleProviderNoCIMD(
        client_id=settings.google_client_id,
        client_secret=settings.google_client_secret,
        base_url=settings.base_url,
        required_scopes=[
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ],
        require_authorization_consent=True,
    )
    mcp.middleware.insert(
        0,
        RemoteUserMiddleware(
            data_root=settings.data_root,
            template_rules_path=_TEMPLATE_RULES_PATH,
        ),
    )
    validate_registry(REGISTERED_TOOL_NAMES, strict=True)
    for tool_name in sorted(REMOTE_EXCLUDED_TOOLS):
        try:
            mcp.local_provider.remove_tool(tool_name)
        except Exception:
            pass
    _install_storage_pool_lifespan()


def main() -> None:
    load_dotenv()
    settings = McpRemoteSettings.from_env()
    configure_remote(settings)
    mcp.run(transport="streamable-http", host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
