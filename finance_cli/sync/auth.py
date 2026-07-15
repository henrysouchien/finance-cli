"""Local auth token storage and sync-session bootstrapping."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import http.server
import json
import os
import queue
import secrets
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from .config import CASHNERD_TOKEN_PATH, ensure_dirs
from .exceptions import SyncAuthError, SyncServerUnreachableError

_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_SYNC_AUTH_PATH = "/api/sync/auth"
_SYNC_CLIENT_ID_PATH = "/api/sync/client-id"
_SYNC_OAUTH_REFRESH_PATH = "/api/sync/oauth/refresh"
_SYNC_OAUTH_TOKEN_PATH = "/api/sync/oauth/token"
_SYNC_REFRESH_WINDOW_SECONDS = 300
_OAUTH_CALLBACK_TIMEOUT_SECONDS = 300
_AUTH_REQUIRED_MESSAGE = "Not authenticated. Run: python3 -m finance_cli.sync.login"
_LOCAL_GOOGLE_CLIENT_ID_ENV = "CASHNERD_LOCAL_GOOGLE_CLIENT_ID"


def _utc_epoch() -> int:
    return int(time.time())


def _parse_iso_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    normalized = str(value).strip().replace("Z", "+00:00")
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return int(parsed.timestamp())


def _iso_from_epoch(epoch_seconds: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _jwt_exp(token: str | None) -> int | None:
    if not token or token.count(".") < 2:
        return None
    payload = token.split(".", 2)[1]
    payload += "=" * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
    except Exception:
        return None
    exp = data.get("exp")
    return int(exp) if isinstance(exp, int) else None


def _response_error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        text = response.text.strip()
        return f": {text[:240]}" if text else ""

    parts = []
    for key in ("error", "error_description", "detail"):
        value = str(body.get(key) or "").strip()
        if value:
            parts.append(value)
    return f": {' - '.join(parts)[:240]}" if parts else ""


def _pkce_code_verifier() -> str:
    return secrets.token_urlsafe(64)


def _pkce_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


class _OAuthCallbackServer:
    """One-shot localhost callback listener for the browser OAuth flow."""

    OAUTH_CALLBACK_HOST = "127.0.0.1"
    OAUTH_CALLBACK_PORT = 0
    OAUTH_CALLBACK_PORT_ENV = "CASHNERD_OAUTH_CALLBACK_PORT"

    def __init__(self, port: int | None = None) -> None:
        self._result_queue: queue.Queue[dict[str, str | None]] = queue.Queue(maxsize=1)
        callback_port = self._resolve_port(port)
        self._server = http.server.ThreadingHTTPServer(
            (self.OAUTH_CALLBACK_HOST, callback_port),
            self._build_handler(),
        )
        host, port = self._server.server_address
        self.redirect_uri = f"http://{host}:{port}/callback"
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.1},
            daemon=True,
        )

    @classmethod
    def _resolve_port(cls, port: int | None) -> int:
        if port is not None:
            return port

        raw_port = str(os.environ.get(cls.OAUTH_CALLBACK_PORT_ENV) or "").strip()
        if not raw_port:
            return cls.OAUTH_CALLBACK_PORT

        try:
            parsed = int(raw_port)
        except ValueError as exc:
            raise SyncAuthError(
                f"{cls.OAUTH_CALLBACK_PORT_ENV} must be an integer port from 0 to 65535."
            ) from exc
        if parsed < 0 or parsed > 65535:
            raise SyncAuthError(
                f"{cls.OAUTH_CALLBACK_PORT_ENV} must be an integer port from 0 to 65535."
            )
        return parsed

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)

    async def wait_for_result(self, timeout_seconds: int) -> dict[str, str | None]:
        try:
            return await asyncio.to_thread(self._result_queue.get, True, timeout_seconds)
        except queue.Empty as exc:
            raise SyncAuthError("Timed out waiting for the Google OAuth callback") from exc

    def _build_handler(self) -> type[http.server.BaseHTTPRequestHandler]:
        result_queue = self._result_queue

        class CallbackHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - stdlib naming
                parsed = urlparse(self.path)
                if parsed.path != "/callback":
                    self.send_error(404)
                    return

                query = parse_qs(parsed.query)
                code = str((query.get("code") or [None])[0] or "").strip() or None
                state = str((query.get("state") or [None])[0] or "").strip() or None
                error = str((query.get("error") or [None])[0] or "").strip() or None
                error_description = (
                    str((query.get("error_description") or [None])[0] or "").strip() or None
                )

                if error:
                    payload = {
                        "code": None,
                        "state": state,
                        "error": error,
                        "error_description": error_description,
                    }
                    body = (
                        "<html><body><h1>CashNerd login failed</h1>"
                        "<p>You can close this window and return to the terminal.</p></body></html>"
                    )
                    status_code = 400
                elif not code:
                    payload = {
                        "code": None,
                        "state": state,
                        "error": "missing_code",
                        "error_description": "OAuth callback did not include an authorization code.",
                    }
                    body = (
                        "<html><body><h1>CashNerd login failed</h1>"
                        "<p>The callback did not include an authorization code.</p></body></html>"
                    )
                    status_code = 400
                else:
                    payload = {
                        "code": code,
                        "state": state,
                        "error": None,
                        "error_description": None,
                    }
                    body = (
                        "<html><body><h1>CashNerd login complete</h1>"
                        "<p>You can close this window and return to the terminal.</p></body></html>"
                    )
                    status_code = 200

                try:
                    result_queue.put_nowait(payload)
                except queue.Full:
                    pass

                encoded = body.encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: object) -> None:
                return

        return CallbackHandler


class LocalAuth:
    """Manage Google tokens and exchange them for sync-session JWTs."""

    def __init__(
        self,
        server_url: str,
        token_path: Path | None = None,
        *,
        google_client_id: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        browser_opener=webbrowser.open,
        time_fn=_utc_epoch,
    ) -> None:
        self._server_url = str(server_url).rstrip("/")
        self._token_path = Path(token_path or CASHNERD_TOKEN_PATH)
        self._google_client_id = str(
            google_client_id or os.environ.get(_LOCAL_GOOGLE_CLIENT_ID_ENV) or ""
        ).strip()
        self._http_client = http_client
        self._browser_opener = browser_opener
        self._time_fn = time_fn
        self._token_data = self._load_token()

    def _load_token(self) -> dict[str, Any]:
        ensure_dirs()
        if not self._token_path.exists():
            return {}
        return json.loads(self._token_path.read_text(encoding="utf-8"))

    def _save_token(self) -> None:
        ensure_dirs()
        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(
            json.dumps(self._token_data, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        self._token_path.chmod(0o600)

    def _expires_at_epoch(self) -> int | None:
        return _parse_iso_timestamp(self._token_data.get("expires_at"))

    def _sync_token_valid(self) -> bool:
        if not self._uses_local_google_client():
            return False
        exp = _jwt_exp(self._token_data.get("sync_token"))
        if exp is None:
            return False
        return exp - self._time_fn() > _SYNC_REFRESH_WINDOW_SECONDS

    def _stored_google_client_id(self) -> str:
        client_id = str(self._token_data.get("google_client_id") or "").strip()
        if client_id:
            return client_id
        return ""

    def _stored_local_google_client_id(self) -> str:
        if str(self._token_data.get("google_client_source") or "") != "local":
            return ""
        return self._stored_google_client_id()

    def _uses_local_google_client(self) -> bool:
        return str(self._token_data.get("google_client_source") or "") == "local"

    async def _resolve_server_google_client_id(self) -> str:
        try:
            response = await self._request("GET", f"{self._server_url}{_SYNC_CLIENT_ID_PATH}")
        except SyncServerUnreachableError:
            response = None

        if response is not None and response.status_code < 400:
            try:
                body = response.json()
            except ValueError:
                body = {}
            client_id = str(body.get("client_id") or body.get("google_client_id") or "").strip()
            if client_id:
                return client_id

        return ""

    async def _resolve_google_client_id(self, *, allow_stored: bool = True) -> str:
        if self._google_client_id:
            return self._google_client_id

        if allow_stored:
            client_id = self._stored_local_google_client_id()
            if client_id:
                return client_id

        client_id = await self._resolve_server_google_client_id()
        if client_id:
            self._google_client_id = client_id
            return client_id

        if not allow_stored:
            client_id = self._stored_local_google_client_id()
            if client_id:
                return client_id

        raise SyncAuthError(
            "Unable to determine the local Google OAuth client ID. "
            f"Set {_LOCAL_GOOGLE_CLIENT_ID_ENV} to a Desktop app client ID or "
            "expose GET /api/sync/client-id on the sync server."
        )

    def invalidate_sync_token(self) -> None:
        self._token_data.pop("sync_token", None)
        self._save_token()

    def is_authenticated(self) -> bool:
        if not self._uses_local_google_client():
            return False
        expires_at = self._expires_at_epoch()
        id_token = str(self._token_data.get("id_token") or "").strip()
        if not id_token or expires_at is None:
            return False
        return expires_at - self._time_fn() > _SYNC_REFRESH_WINDOW_SECONDS

    async def ensure_authenticated(self) -> None:
        if self.is_authenticated():
            return

        refresh_token = str(self._token_data.get("refresh_token") or "").strip()
        if refresh_token:
            if not self._uses_local_google_client():
                raise SyncAuthError(
                    "Stored Google credentials were created before the local Desktop OAuth cutover. "
                    "Run: python3 -m finance_cli.sync.login"
                )
            try:
                await self.refresh_token()
            except (SyncAuthError, SyncServerUnreachableError) as exc:
                raise SyncAuthError(
                    "Stored Google credentials are expired and could not be refreshed. "
                    f"Run: python3 -m finance_cli.sync.login ({exc})"
                ) from exc

        if not self.is_authenticated():
            raise SyncAuthError(_AUTH_REQUIRED_MESSAGE)

    async def refresh_token(self) -> None:
        refresh_token = str(self._token_data.get("refresh_token") or "").strip()
        if not refresh_token:
            raise SyncAuthError("No refresh token is stored")
        if not self._uses_local_google_client():
            raise SyncAuthError(
                "Stored Google credentials were not created with the local Desktop OAuth client. "
                "Run: python3 -m finance_cli.sync.login"
            )

        client_id = await self._resolve_google_client_id()
        payload = {"refresh_token": refresh_token}
        try:
            response = await self._request(
                "POST",
                f"{self._server_url}{_SYNC_OAUTH_REFRESH_PATH}",
                json=payload,
            )
        except SyncServerUnreachableError:
            raise
        except Exception as exc:
            raise SyncAuthError(f"Failed to refresh Google token: {exc}") from exc

        if response.status_code >= 400:
            raise SyncAuthError(
                f"Google token refresh failed with HTTP {response.status_code}"
                f"{_response_error_detail(response)}"
            )

        self._store_google_tokens(
            response.json(),
            client_id=client_id,
            client_source="local",
            allow_missing_refresh_token=True,
        )

    async def get_credential(self) -> str:
        await self.ensure_authenticated()
        credential = str(self._token_data.get("id_token") or "").strip()
        if not credential:
            raise SyncAuthError("No Google credential is available")
        return credential

    async def get_sync_token(self, *, force_refresh: bool = False) -> str:
        if not force_refresh and self._sync_token_valid():
            return str(self._token_data["sync_token"])

        credential = await self.get_credential()
        try:
            response = await self._request(
                "POST",
                f"{self._server_url}{_SYNC_AUTH_PATH}",
                json={"credential": credential},
            )
        except SyncServerUnreachableError:
            raise

        if response.status_code == 401:
            raise SyncAuthError("Google credential was rejected by the sync server")
        response.raise_for_status()
        token = str(response.headers.get("X-CashNerd-Sync-Token") or "").strip()
        if not token:
            raise SyncAuthError("Sync server did not return a sync session token")
        self.record_sync_session(
            token=token,
            user_id=response.headers.get("X-CashNerd-User-Id"),
        )
        return token

    def record_sync_session(self, *, token: str, user_id: str | None = None) -> None:
        self._token_data["sync_token"] = str(token)
        self._token_data["server_url"] = self._server_url
        if user_id is not None:
            self._token_data["user_id"] = str(user_id)
        exp = _jwt_exp(token)
        if exp is not None:
            self._token_data["sync_token_expires_at"] = _iso_from_epoch(exp)
        self._save_token()

    async def run_browser_oauth(self) -> None:
        client_id = await self._resolve_google_client_id(allow_stored=False)
        state = secrets.token_urlsafe(32)
        code_verifier = _pkce_code_verifier()
        code_challenge = _pkce_code_challenge(code_verifier)

        callback_server = _OAuthCallbackServer()
        callback_server.start()
        redirect_uri = callback_server.redirect_uri
        auth_url = _GOOGLE_AUTH_URL + "?" + urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "response_type": "code",
                "scope": "openid email profile",
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
                "state": state,
                "access_type": "offline",
                "prompt": "consent",
            }
        )

        print("Opening browser for CashNerd sync login...", file=sys.stderr, flush=True)
        print(f"If the browser does not open, visit:\n{auth_url}", file=sys.stderr, flush=True)
        try:
            self._browser_opener(auth_url)
        except Exception:
            pass

        try:
            callback = await callback_server.wait_for_result(_OAUTH_CALLBACK_TIMEOUT_SECONDS)
        finally:
            callback_server.close()

        if callback.get("error"):
            detail = str(callback.get("error_description") or callback["error"])
            raise SyncAuthError(f"Google OAuth failed: {detail}")
        if callback.get("state") != state:
            raise SyncAuthError("Google OAuth state verification failed")

        code = str(callback.get("code") or "").strip()
        if not code:
            raise SyncAuthError("Google OAuth callback did not include an authorization code")

        payload = {
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        }
        try:
            response = await self._request(
                "POST",
                f"{self._server_url}{_SYNC_OAUTH_TOKEN_PATH}",
                json=payload,
            )
        except SyncServerUnreachableError as exc:
            raise SyncAuthError(f"Failed to exchange the OAuth authorization code: {exc}") from exc

        if response.status_code >= 400:
            raise SyncAuthError(
                f"Google token exchange failed with HTTP {response.status_code}"
                f"{_response_error_detail(response)}"
            )

        self._store_google_tokens(
            response.json(),
            client_id=client_id,
            client_source="local",
            allow_missing_refresh_token=True,
        )

    def _store_google_tokens(
        self,
        token_response: dict[str, Any],
        *,
        client_id: str,
        client_source: str | None = None,
        allow_missing_refresh_token: bool,
    ) -> None:
        access_token = str(token_response.get("access_token") or "").strip()
        id_token = str(token_response.get("id_token") or self._token_data.get("id_token") or "").strip()
        refresh_token = str(
            token_response.get("refresh_token") or self._token_data.get("refresh_token") or ""
        ).strip()
        expires_in = int(token_response.get("expires_in") or 3600)

        if not access_token or not id_token:
            raise SyncAuthError("Google token response did not include usable tokens")
        if not refresh_token and not allow_missing_refresh_token:
            raise SyncAuthError("Google token response did not include a refresh token")

        jwt_exp = _jwt_exp(id_token)
        expires_at = (
            _iso_from_epoch(jwt_exp)
            if jwt_exp is not None
            else _iso_from_epoch(self._time_fn() + expires_in)
        )

        self._token_data.update(
            {
                "access_token": access_token,
                "expires_at": expires_at,
                "google_client_id": client_id,
                "id_token": id_token,
                "refresh_token": refresh_token,
                "server_url": self._server_url,
            }
        )
        if client_source:
            self._token_data["google_client_source"] = client_source
        self._token_data.pop("sync_token", None)
        self._token_data.pop("sync_token_expires_at", None)
        self._save_token()

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        client = self._http_client
        owns_client = client is None
        if client is None:
            client = httpx.AsyncClient(timeout=30.0)
        try:
            response = await client.request(method, url, **kwargs)
            return response
        except httpx.HTTPError as exc:
            raise SyncServerUnreachableError(str(exc)) from exc
        finally:
            if owns_client:
                await client.aclose()
