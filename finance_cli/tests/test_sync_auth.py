from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from finance_cli.sync import auth as sync_auth
from finance_cli.sync import config as sync_config
from finance_cli.sync import login as sync_login
from finance_cli.sync.exceptions import SyncAuthError


def _patch_cashnerd_paths(monkeypatch, base_dir: Path) -> None:
    monkeypatch.setattr(sync_config, "CASHNERD_DIR", base_dir)
    monkeypatch.setattr(sync_config, "CASHNERD_CONFIG_PATH", base_dir / "config.json")
    monkeypatch.setattr(sync_config, "CASHNERD_AUTH_DIR", base_dir / "auth")
    monkeypatch.setattr(sync_config, "CASHNERD_TOKEN_PATH", base_dir / "auth" / "token.json")
    monkeypatch.setattr(sync_config, "CASHNERD_DATA_DIR", base_dir / "data")
    monkeypatch.setattr(sync_config, "CASHNERD_UPLOADS_DIR", base_dir / "data" / "uploads")
    monkeypatch.setattr(sync_config, "CASHNERD_SYNC_DIR", base_dir / "sync")


def _jwt(exp: int) -> str:
    def _b64(payload: dict[str, object]) -> str:
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f"{_b64({'alg': 'none'})}.{_b64({'exp': exp})}.signature"


def test_token_storage_and_expiry_check(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        time_fn=lambda: 1_000,
    )

    auth._token_data = {
        "id_token": _jwt(2_000),
        "refresh_token": "refresh",
        "expires_at": "1970-01-01T00:33:20Z",
        "google_client_id": "desktop-client.apps.googleusercontent.com",
        "google_client_source": "local",
    }
    auth._save_token()

    reloaded = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        time_fn=lambda: 1_000,
    )
    assert reloaded.is_authenticated() is True
    assert token_path.stat().st_mode & 0o777 == 0o600

    expired = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        time_fn=lambda: 1_999,
    )
    assert expired.is_authenticated() is False


def test_ensure_authenticated_refreshes_expired_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "legacy-web-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://cashnerd.example/api/sync/oauth/refresh"
        assert json.loads(request.content.decode("utf-8")) == {"refresh_token": "refresh-token"}
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "id_token": _jwt(5_000),
                "expires_in": 3600,
            },
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        google_client_id="client-id.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(900),
        "refresh_token": "refresh-token",
        "expires_at": "1970-01-01T00:15:00Z",
        "google_client_id": "client-id.apps.googleusercontent.com",
        "google_client_source": "local",
    }
    auth._save_token()

    asyncio.run(auth.ensure_authenticated())

    assert auth.is_authenticated() is True
    assert auth._token_data["access_token"] == "new-access"
    assert auth._token_data["refresh_token"] == "refresh-token"


def test_refresh_token_uses_server_token_broker(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "legacy-web-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://cashnerd.example/api/sync/oauth/refresh"
        assert json.loads(request.content.decode("utf-8")) == {"refresh_token": "refresh-token"}
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "id_token": _jwt(5_000),
                "expires_in": 3600,
            },
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        google_client_id="client-id.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(900),
        "refresh_token": "refresh-token",
        "expires_at": "1970-01-01T00:15:00Z",
        "google_client_id": "client-id.apps.googleusercontent.com",
        "google_client_source": "local",
    }
    auth._save_token()

    asyncio.run(auth.refresh_token())

    assert auth._token_data["access_token"] == "new-access"
    assert auth._token_data["refresh_token"] == "refresh-token"


def test_refresh_token_persists_rotated_refresh_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    monkeypatch.setenv("GOOGLE_CLIENT_SECRET", "legacy-web-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://cashnerd.example/api/sync/oauth/refresh"
        assert json.loads(request.content.decode("utf-8")) == {"refresh_token": "refresh-token"}
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "new-refresh-token",
                "id_token": _jwt(5_000),
                "expires_in": 3600,
            },
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        google_client_id="client-id.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(900),
        "refresh_token": "refresh-token",
        "expires_at": "1970-01-01T00:15:00Z",
        "google_client_id": "client-id.apps.googleusercontent.com",
        "google_client_source": "local",
    }
    auth._save_token()

    asyncio.run(auth.refresh_token())

    assert auth._token_data["access_token"] == "new-access"
    assert auth._token_data["refresh_token"] == "new-refresh-token"
    assert auth._token_data["google_client_source"] == "local"


def test_ensure_authenticated_rejects_pre_cutover_web_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected token refresh request: {request.url}")

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        google_client_id="desktop-client.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(900),
        "refresh_token": "old-web-refresh-token",
        "expires_at": "1970-01-01T00:15:00Z",
        "google_client_id": "web-client.apps.googleusercontent.com",
    }
    auth._save_token()

    with pytest.raises(SyncAuthError, match="Desktop OAuth cutover"):
        asyncio.run(auth.ensure_authenticated())


def test_ensure_authenticated_requires_prior_login(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        time_fn=lambda: 1_000,
    )

    with pytest.raises(SyncAuthError, match="python3 -m finance_cli.sync.login"):
        asyncio.run(auth.ensure_authenticated())


def test_local_oauth_prefers_local_client_id_env_over_web_env(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "web-client.apps.googleusercontent.com")
    monkeypatch.setenv("CASHNERD_LOCAL_GOOGLE_CLIENT_ID", "local-client.apps.googleusercontent.com")

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=tmp_path / ".cashnerd" / "auth" / "token.json",
    )

    client_id = asyncio.run(auth._resolve_google_client_id(allow_stored=False))

    assert client_id == "local-client.apps.googleusercontent.com"


def test_browser_oauth_does_not_reuse_untyped_stored_web_client_id(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    monkeypatch.delenv("CASHNERD_LOCAL_GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "web-client.apps.googleusercontent.com")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/sync/client-id"
        return httpx.Response(404, json={"detail": "not configured"})

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    auth._token_data = {"google_client_id": "web-client.apps.googleusercontent.com"}
    auth._save_token()

    with pytest.raises(SyncAuthError, match="CASHNERD_LOCAL_GOOGLE_CLIENT_ID"):
        asyncio.run(auth._resolve_google_client_id(allow_stored=False))


def test_oauth_callback_server_uses_available_loopback_port_by_default(monkeypatch) -> None:
    monkeypatch.delenv("CASHNERD_OAUTH_CALLBACK_PORT", raising=False)

    server = sync_auth._OAuthCallbackServer()
    server.start()
    try:
        parsed = urlparse(server.redirect_uri)
        assert parsed.scheme == "http"
        assert parsed.hostname == "127.0.0.1"
        assert parsed.path == "/callback"
        assert parsed.port is not None
        assert parsed.port > 0
        assert sync_auth._OAuthCallbackServer.OAUTH_CALLBACK_PORT == 0
    finally:
        server.close()


def test_oauth_callback_server_accepts_explicit_port_env(monkeypatch) -> None:
    monkeypatch.setenv("CASHNERD_OAUTH_CALLBACK_PORT", "0")

    server = sync_auth._OAuthCallbackServer()
    server.start()
    try:
        assert urlparse(server.redirect_uri).port is not None
    finally:
        server.close()


def test_oauth_callback_server_rejects_invalid_port_env(monkeypatch) -> None:
    monkeypatch.setenv("CASHNERD_OAUTH_CALLBACK_PORT", "not-a-port")

    with pytest.raises(SyncAuthError, match="CASHNERD_OAUTH_CALLBACK_PORT"):
        sync_auth._OAuthCallbackServer()


def test_run_browser_oauth_exchanges_code_and_persists_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    opened_urls: list[str] = []

    class FakeCallbackServer:
        def __init__(self) -> None:
            self.redirect_uri = "http://127.0.0.1:43123/callback"
            self.started = False
            self.closed = False

        def start(self) -> None:
            self.started = True

        def close(self) -> None:
            self.closed = True

        async def wait_for_result(self, timeout_seconds: int) -> dict[str, str | None]:
            assert timeout_seconds == 300
            return {
                "code": "oauth-code",
                "state": "oauth-state",
                "error": None,
                "error_description": None,
            }

    fake_server = FakeCallbackServer()
    monkeypatch.setattr(sync_auth, "_OAuthCallbackServer", lambda: fake_server)

    tokens = iter(["oauth-state"])
    monkeypatch.setattr(sync_auth.secrets, "token_urlsafe", lambda _n: next(tokens))
    monkeypatch.setattr(sync_auth, "_pkce_code_verifier", lambda: "verifier-123")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://cashnerd.example/api/sync/oauth/token"
        body = json.loads(request.content.decode("utf-8"))
        assert body == {
            "code": "oauth-code",
            "redirect_uri": "http://127.0.0.1:43123/callback",
            "code_verifier": "verifier-123",
        }
        return httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "id_token": _jwt(5_000),
                "expires_in": 3600,
            },
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        google_client_id="client-id.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        browser_opener=lambda url: opened_urls.append(url) or True,
        time_fn=lambda: 1_000,
    )

    asyncio.run(auth.run_browser_oauth())

    assert fake_server.started is True
    assert fake_server.closed is True
    assert token_path.exists()
    assert token_path.stat().st_mode & 0o777 == 0o600
    assert auth._token_data["access_token"] == "new-access"
    assert auth._token_data["refresh_token"] == "new-refresh"
    assert auth._token_data["google_client_id"] == "client-id.apps.googleusercontent.com"
    assert auth._token_data["google_client_source"] == "local"

    parsed = urlparse(opened_urls[0])
    query = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert parsed.netloc == "accounts.google.com"
    assert query["client_id"] == ["client-id.apps.googleusercontent.com"]
    assert query["redirect_uri"] == ["http://127.0.0.1:43123/callback"]
    assert query["response_type"] == ["code"]
    assert query["scope"] == ["openid email profile"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["state"] == ["oauth-state"]


def test_run_browser_oauth_reports_google_token_error_detail(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")

    class FakeCallbackServer:
        redirect_uri = "http://127.0.0.1:43123/callback"

        def start(self) -> None:
            return

        def close(self) -> None:
            return

        async def wait_for_result(self, timeout_seconds: int) -> dict[str, str | None]:
            return {
                "code": "oauth-code",
                "state": "oauth-state",
                "error": None,
                "error_description": None,
            }

    monkeypatch.setattr(sync_auth, "_OAuthCallbackServer", FakeCallbackServer)
    tokens = iter(["oauth-state"])
    monkeypatch.setattr(sync_auth.secrets, "token_urlsafe", lambda _n: next(tokens))
    monkeypatch.setattr(sync_auth, "_pkce_code_verifier", lambda: "verifier-123")

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://cashnerd.example/api/sync/oauth/token"
        return httpx.Response(
            400,
            json={
                "detail": "Google token exchange failed: invalid_client - client secret is missing",
            },
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        google_client_id="client-id.apps.googleusercontent.com",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        browser_opener=lambda _url: True,
    )

    with pytest.raises(SyncAuthError, match="invalid_client - client secret is missing"):
        asyncio.run(auth.run_browser_oauth())


def test_get_sync_token_uses_cached_session_and_persists_server_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return httpx.Response(
            200,
            headers={
                "X-CashNerd-Sync-Token": _jwt(10_000),
                "X-CashNerd-User-Id": "42",
            },
            content=b"tarball",
        )

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(5_000),
        "expires_at": "1970-01-01T01:23:20Z",
        "google_client_id": "desktop-client.apps.googleusercontent.com",
        "google_client_source": "local",
    }
    auth._save_token()

    first = asyncio.run(auth.get_sync_token())
    second = asyncio.run(auth.get_sync_token())

    assert first == second
    assert calls == ["/api/sync/auth"]
    assert auth._token_data["user_id"] == "42"


def test_get_sync_token_rejects_pre_cutover_cached_sync_token(monkeypatch, tmp_path: Path) -> None:
    _patch_cashnerd_paths(monkeypatch, tmp_path / ".cashnerd")
    token_path = tmp_path / ".cashnerd" / "auth" / "token.json"

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected sync request: {request.url}")

    auth = sync_auth.LocalAuth(
        "https://cashnerd.example",
        token_path=token_path,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        time_fn=lambda: 1_000,
    )
    auth._token_data = {
        "id_token": _jwt(5_000),
        "expires_at": "1970-01-01T01:23:20Z",
        "google_client_id": "web-client.apps.googleusercontent.com",
        "refresh_token": "old-web-refresh-token",
        "sync_token": _jwt(10_000),
    }
    auth._save_token()

    with pytest.raises(SyncAuthError, match="Desktop OAuth cutover"):
        asyncio.run(auth.get_sync_token())


def test_login_main_runs_browser_auth(monkeypatch, capsys) -> None:
    class FakeAuth:
        def __init__(self, server_url: str) -> None:
            assert server_url == "https://cashnerd.example"
            self.called = False

        async def run_browser_oauth(self) -> None:
            self.called = True

    fake_auth = FakeAuth("https://cashnerd.example")
    monkeypatch.setattr(sync_login, "load_config", lambda: sync_config.SyncConfig(server_url="https://cashnerd.example"))
    monkeypatch.setattr(sync_login, "LocalAuth", lambda server_url: fake_auth)

    status = sync_login.main()

    captured = capsys.readouterr()
    assert status == 0
    assert fake_auth.called is True
    assert "Authentication succeeded" in captured.out
