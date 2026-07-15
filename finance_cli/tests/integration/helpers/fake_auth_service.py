from __future__ import annotations

import base64
import json


def _b64(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


SYNTHETIC_TOKEN = (
    f"{_b64({'alg': 'none', 'typ': 'JWT'})}."
    f"{_b64({'exp': 4070908800, 'email': 'e2e@test'})}."
    "signature"
)


class FakeAuthService:
    def __init__(self, expected_token: str = SYNTHETIC_TOKEN) -> None:
        self._expected_token = expected_token

    def verify_token(self, credential: str) -> tuple[dict[str, str] | None, str | None]:
        if credential != self._expected_token:
            return None, "invalid"
        return (
            {
                "google_user_id": "test-google-id",
                "email": "e2e@test",
                "name": "E2E",
            },
            None,
        )
