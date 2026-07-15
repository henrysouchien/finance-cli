"""JWT minting and Secrets Manager key refresh for the storage server client."""

from __future__ import annotations

import os
import threading
import time
from typing import Any

import boto3
import jwt

DEFAULT_SECRET_ID = "finance-cli/jwt/app-server/private_key_pem"
DEFAULT_AWS_REGION = "us-east-2"
DEFAULT_REFRESH_INTERVAL_SECONDS = 6 * 60 * 60
TOKEN_LIFETIME_SECONDS = 30
TOKEN_CACHE_FRESH_BUFFER_SECONDS = 10


class JWTAuthProvider:
    """Fetch an Ed25519 private key and mint short-lived storage JWTs."""

    def __init__(
        self,
        *,
        secret_id: str = DEFAULT_SECRET_ID,
        aws_region: str | None = None,
        refresh_interval_seconds: int = DEFAULT_REFRESH_INTERVAL_SECONDS,
        secrets_client: Any | None = None,
    ) -> None:
        self.secret_id = secret_id
        self.aws_region = aws_region or os.getenv("AWS_REGION") or DEFAULT_AWS_REGION
        self.refresh_interval_seconds = int(refresh_interval_seconds)
        self._secrets_client = secrets_client or boto3.client(
            "secretsmanager",
            region_name=self.aws_region,
        )
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._token_cache: dict[tuple[str, str, tuple[str, ...]], tuple[str, int]] = {}
        self._private_key_pem = ""
        self._kid = ""

        self._refresh_key_material()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name="storage-client-jwt-refresh",
            daemon=True,
        )
        self._refresh_thread.start()

    @property
    def kid(self) -> str:
        with self._lock:
            return self._kid

    def get_token(
        self,
        product: str,
        user_id: str,
        scopes: list[str] | None = None,
    ) -> str:
        scopes_tuple = tuple(scopes or ())
        cache_key = (product, user_id, scopes_tuple)
        now = time.time()
        with self._lock:
            cached = self._token_cache.get(cache_key)
            if cached is not None:
                token, exp_timestamp = cached
                if exp_timestamp - now > TOKEN_CACHE_FRESH_BUFFER_SECONDS:
                    return token
            token, exp_timestamp = self._mint_token_locked(
                product=product,
                user_id=user_id,
                scopes=scopes_tuple,
                now=now,
            )
            self._token_cache[cache_key] = (token, exp_timestamp)
            return token

    def close(self) -> None:
        self._stop_event.set()
        self._refresh_thread.join(timeout=1.0)

    def _refresh_loop(self) -> None:
        while not self._stop_event.wait(self.refresh_interval_seconds):
            self._refresh_key_material()

    def _refresh_key_material(self) -> None:
        private_key_pem = self._read_secret_string()
        kid = self._read_kid_tag()
        if not private_key_pem:
            raise RuntimeError(f"Secret {self.secret_id} did not contain a private key PEM")
        if not kid:
            raise RuntimeError(f"Secret {self.secret_id} is missing required kid tag")
        with self._lock:
            changed = private_key_pem != self._private_key_pem or kid != self._kid
            self._private_key_pem = private_key_pem
            self._kid = kid
            if changed:
                self._token_cache.clear()

    def _read_secret_string(self) -> str:
        response = self._secrets_client.get_secret_value(SecretId=self.secret_id)
        if "SecretString" in response and response["SecretString"] is not None:
            return str(response["SecretString"])
        secret_binary = response.get("SecretBinary")
        if isinstance(secret_binary, bytes):
            return secret_binary.decode("utf-8")
        if secret_binary is not None:
            return str(secret_binary)
        return ""

    def _read_kid_tag(self) -> str:
        response = self._secrets_client.describe_secret(SecretId=self.secret_id)
        for tag in response.get("Tags", []) or []:
            if str(tag.get("Key", "")) == "kid":
                return str(tag.get("Value", "")).strip()
        return ""

    def _mint_token_locked(
        self,
        *,
        product: str,
        user_id: str,
        scopes: tuple[str, ...],
        now: float,
    ) -> tuple[str, int]:
        exp_timestamp = int(now + TOKEN_LIFETIME_SECONDS)
        claims = {
            "exp": exp_timestamp,
            "kid": self._kid,
            "product": product,
            "user_id": user_id,
            "scopes": list(scopes),
        }
        token = jwt.encode(
            claims,
            self._private_key_pem,
            algorithm="EdDSA",
            headers={"kid": self._kid},
        )
        return str(token), exp_timestamp


_default_provider: JWTAuthProvider | None = None
_default_provider_lock = threading.RLock()


def get_default_provider() -> JWTAuthProvider:
    global _default_provider
    with _default_provider_lock:
        if _default_provider is None:
            _default_provider = JWTAuthProvider()
        return _default_provider


def reset_default_provider() -> None:
    global _default_provider
    with _default_provider_lock:
        if _default_provider is not None:
            _default_provider.close()
        _default_provider = None
