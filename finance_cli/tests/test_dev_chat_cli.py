from __future__ import annotations

import io
import json
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

import finance_cli.dev.chat_cli as chat_cli
from finance_cli.dev.chat_cli import CLIConfig, _save_config, main


class ChunkStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[str]) -> None:
        self._chunks = [chunk.encode("utf-8") for chunk in chunks]

    def __iter__(self):
        yield from self._chunks


def _wrapped_sse(seq: int, event: dict[str, object]) -> str:
    return (
        "data: "
        + json.dumps(
            {
                "seq": seq,
                "session_id": "sess-1",
                "schema_version": 1,
                "event": event,
            },
            separators=(",", ":"),
        )
        + "\n\n"
    )


@pytest.fixture(autouse=True)
def clear_dev_chat_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.delenv("GATEWAY_API_KEY", raising=False)
    monkeypatch.delenv("GATEWAY_USER_KEY", raising=False)
    monkeypatch.delenv("CASHNERD_USER_ID", raising=False)
    monkeypatch.delenv("GATEWAY_BASE_URL", raising=False)


def test_login_verifies_init_and_saves_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(
            {
                "path": request.url.path,
                "json": json.loads(request.content.decode("utf-8")),
            }
        )
        return httpx.Response(
            200,
            json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
        )

    answers = iter(["user-key-123", "user-uuid-123"])
    stdout = io.StringIO()
    stderr = io.StringIO()

    code = main(
        ["login", "--base-url", "http://127.0.0.1:8002"],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: next(answers),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved == {
        "base_url": "http://127.0.0.1:8002",
        "gateway_user_key": "user-key-123",
        "user_id": "user-uuid-123",
    }
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert requests == [
        {
            "path": "/api/chat/init",
            "json": {
                "api_key": "user-key-123",
                "user_id": "user-uuid-123",
                "context": {"channel": "cli"},
            },
        }
    ]
    rendered = stdout.getvalue()
    assert "[login] verified init session sess-1" in rendered
    assert f"[login] config saved to {config_path}" in rendered


def test_login_migrates_gateway_api_key_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "base_url": "http://127.0.0.1:8002",
                "config_namespace": "cashnerd",
                "gateway_api_key": "legacy-key-123",
                "schema_version": 1,
                "user_id": "2",
            }
        ),
        encoding="utf-8",
    )

    requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content.decode("utf-8")))
        return httpx.Response(
            200,
            json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
        )

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["login"],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: pytest.fail("login should reuse saved credentials"),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert requests == [
        {"api_key": "legacy-key-123", "user_id": "2", "context": {"channel": "cli"}}
    ]
    saved = json.loads(config_path.read_text(encoding="utf-8"))
    assert saved == {
        "base_url": "http://127.0.0.1:8002",
        "gateway_user_key": "legacy-key-123",
        "user_id": "2",
    }


def test_chat_cli_streams_gateway_direct_and_posts_approval(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    init_posts: list[dict[str, object]] = []
    chat_posts: list[dict[str, object]] = []
    approval_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            init_posts.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            chat_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": json.loads(request.content.decode("utf-8")),
                }
            )
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        'data: {"type":"text_delta","text":"Hello"}\n\n',
                        'data: {"type":"tool_call_start","tool_name":"plaid_link","tool_call_id":"tool-1","tool_input":{"wait":false}}\n\n',
                        'data: {"type":"tool_approval_request","tool_name":"plaid_link","tool_call_id":"tool-1","nonce":"nonce-1","tool_input":{"wait":false,"include_balance":true},"expires_at":1760000000}\n\n',
                        'data: {"type":"tool_call_complete","tool_name":"plaid_link","tool_call_id":"tool-1","result":{"url":"https://example.test"}}\n\n',
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        if request.url.path == "/api/chat/tool-approval":
            approval_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": json.loads(request.content.decode("utf-8")),
                }
            )
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["chat", "--skill", "onboarding", "connect", "my", "bank"],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: "y",
        time_fn=lambda: 1759999950.0,
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert init_posts == [
        {"api_key": "user-key-123", "user_id": "user-123", "context": {"channel": "cli"}}
    ]
    assert chat_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "messages": [{"role": "user", "content": "connect my bank"}],
                "context": {"channel": "cli", "skill": "onboarding"},
                "user_id": "user-123",
            },
        }
    ]
    assert approval_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "tool_call_id": "tool-1",
                "nonce": "nonce-1",
                "approved": True,
            },
        }
    ]

    rendered = stdout.getvalue()
    assert "You> connect my bank" in rendered
    assert "[text] Hello" in rendered
    assert '[tool_call_start] plaid_link  call_id=tool-1 input={"wait":false}' in rendered
    assert "[tool_approval_request] plaid_link  call_id=tool-1  nonce=nonce-1  expires_in=0:45 remaining" in rendered
    assert '"include_balance": true' in rendered
    assert "  -> Allow? [y/N]: [approval_submitted] approved" in rendered
    assert "[tool_call_complete] plaid_link  call_id=tool-1  result_bytes=" in rendered
    assert "[stream_complete]" in rendered
    assert "[done] 0.0s" in rendered


@pytest.mark.parametrize(
    ("event_type", "expected_rendered"),
    [
        ("error", "[error] Overloaded"),
        ("stream_error", "[stream_error] Overloaded"),
    ],
)
def test_chat_cli_exits_nonzero_on_streamed_gateway_error(
    monkeypatch,
    tmp_path: Path,
    event_type: str,
    expected_rendered: str,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    capture_path = tmp_path / "captures" / f"provider-{event_type}.jsonl"
    session_name = f"provider-{event_type}"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            event = {
                "type": event_type,
                "error": {"type": "overloaded_error", "message": "Overloaded"},
            }
            return httpx.Response(
                200,
                stream=ChunkStream([f"data: {json.dumps(event, separators=(',', ':'))}\n\n"]),
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "chat",
            "--session",
            session_name,
            "--capture-jsonl",
            str(capture_path),
            "trigger",
            "provider",
            event_type,
        ],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: pytest.fail("one-shot prompt should not prompt again"),
        time_fn=lambda: 1000.0,
        transport=httpx.MockTransport(handler),
    )

    assert code == 1
    assert stderr.getvalue() == ""
    rendered = stdout.getvalue()
    assert expected_rendered in rendered
    assert "[done] 0.0s" in rendered

    session_path = tmp_path / "cashnerd" / "sessions" / f"{session_name}.json"
    saved_session = json.loads(session_path.read_text(encoding="utf-8"))
    assert saved_session["messages"] == []

    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [event_type]


def test_chat_cli_auto_approves_allowlisted_tool_and_captures_decision(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    capture_path = tmp_path / "captures" / "coach-debt.jsonl"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    approval_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        'data: {"type":"text_delta","text":"Working"}\n\n',
                        (
                            'data: {"type":"tool_approval_request",'
                            '"tool_name":"goal_set","tool_call_id":"tool-1",'
                            '"nonce":"nonce-1","tool_input":{"title":"Pay off card"},'
                            '"expires_at":1760000000}\n\n'
                        ),
                        (
                            'data: {"type":"tool_call_complete","tool_name":"goal_set",'
                            '"tool_call_id":"tool-1","result":{"summary":{"ok":true}}}\n\n'
                        ),
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        if request.url.path == "/api/chat/tool-approval":
            approval_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": json.loads(request.content.decode("utf-8")),
                }
            )
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "chat",
            "--capture-jsonl",
            str(capture_path),
            "--auto-approve-tool",
            "goal_set",
            "advance",
            "the",
            "plan",
        ],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: pytest.fail("auto-approved tools should not prompt"),
        time_fn=_incrementing_time(7000),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert approval_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "tool_call_id": "tool-1",
                "nonce": "nonce-1",
                "approved": True,
            },
        }
    ]

    rendered = stdout.getvalue()
    assert "[tool_approval_request] goal_set" in rendered
    assert "[auto_approval] approving goal_set via --auto-approve-tool" in rendered
    assert "[approval_submitted] approved" in rendered
    assert "  -> Allow? [y/N]:" not in rendered

    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [
        "text_delta",
        "tool_approval_request",
        "dev_chat_cli_approval_decision",
        "tool_call_complete",
        "stream_complete",
    ]
    assert rows[1]["capture"]["source"] == "gateway_sse"
    assert rows[2] == {
        "type": "dev_chat_cli_approval_decision",
        "tool_name": "goal_set",
        "resolved_qualifier": None,
        "approval_key": "goal_set",
        "tool_call_id": "tool-1",
        "nonce": "nonce-1",
        "approved": True,
        "outcome": "approved",
        "submitted": True,
        "decision_source": "auto_approve_tool",
        "capture": rows[2]["capture"],
    }
    assert rows[2]["capture"] == {
        "source": "dev_chat_cli_auto_approval",
        "session_name": "default",
        "session_id": "sess-1",
        "turn_index": 1,
        "attempt": 0,
        "event_index": 3,
        "captured_at": rows[2]["capture"]["captured_at"],
    }


def test_chat_cli_handles_wrapped_gateway_events_for_rendering_and_approval(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    capture_path = tmp_path / "captures" / "wrapped.jsonl"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    approval_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        _wrapped_sse(1, {"type": "text_delta", "text": "Working"}),
                        _wrapped_sse(
                            2,
                            {
                                "type": "tool_approval_request",
                                "tool_name": "budget_set",
                                "tool_call_id": "tool-1",
                                "nonce": "nonce-1",
                                "tool_input": {"category": "Rent", "amount": 2100},
                                "expires_at": 1760000000,
                            },
                        ),
                        _wrapped_sse(
                            3,
                            {
                                "type": "tool_call_complete",
                                "tool_name": "budget_set",
                                "tool_call_id": "tool-1",
                                "result": {"summary": {"total_budgets": 1}},
                            },
                        ),
                        _wrapped_sse(4, {"type": "stream_complete"}),
                    ]
                ),
            )
        if request.url.path == "/api/chat/tool-approval":
            approval_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": json.loads(request.content.decode("utf-8")),
                }
            )
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "chat",
            "--capture-jsonl",
            str(capture_path),
            "--auto-approve-tool",
            "budget_set",
            "mirror",
            "rent",
        ],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: pytest.fail("wrapped auto-approved tools should not prompt"),
        time_fn=_incrementing_time(7000),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert approval_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "tool_call_id": "tool-1",
                "nonce": "nonce-1",
                "approved": True,
            },
        }
    ]

    rendered = stdout.getvalue()
    assert "[text] Working" in rendered
    assert "[tool_approval_request] budget_set" in rendered
    assert "[auto_approval] approving budget_set via --auto-approve-tool" in rendered
    assert "[approval_submitted] approved" in rendered
    assert "[tool_call_complete] budget_set  call_id=tool-1" in rendered
    assert "[stream_complete]" in rendered

    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert rows[0]["event"]["type"] == "text_delta"
    assert rows[1]["event"]["type"] == "tool_approval_request"
    assert rows[2]["type"] == "dev_chat_cli_approval_decision"
    assert rows[2]["approval_key"] == "budget_set"
    assert rows[2]["submitted"] is True


def test_chat_cli_approval_uses_separate_connection_while_stream_open(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    approval_submitted = threading.Event()
    approval_posts: list[dict[str, object]] = []
    chat_posts: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def _read_json(self) -> dict[str, object]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode("utf-8"))

        def _send_json(self, status: int, payload: dict[str, object]) -> None:
            raw = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_POST(self) -> None:
            if self.path == "/api/chat/init":
                self._read_json()
                self._send_json(
                    200,
                    {
                        "session_token": "tok-1",
                        "session_id": "sess-1",
                        "expires_at": 5000,
                    },
                )
                return

            if self.path == "/api/chat/tool-approval":
                approval_posts.append(
                    {
                        "authorization": self.headers.get("Authorization"),
                        "json": self._read_json(),
                    }
                )
                approval_submitted.set()
                self._send_json(200, {"success": True})
                return

            if self.path == "/api/chat":
                chat_posts.append(
                    {
                        "authorization": self.headers.get("Authorization"),
                        "json": self._read_json(),
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(
                    (
                        'data: {"type":"tool_approval_request",'
                        '"tool_name":"budget_set","tool_call_id":"tool-1",'
                        '"nonce":"nonce-1","tool_input":{"category":"groceries"},'
                        '"expires_at":1760000000}\n\n'
                    ).encode("utf-8")
                )
                self.wfile.flush()
                if approval_submitted.wait(3.0):
                    self.wfile.write(
                        (
                            'data: {"type":"tool_call_complete","tool_name":"budget_set",'
                            '"tool_call_id":"tool-1","result":{"ok":true}}\n\n'
                            'data: {"type":"stream_complete"}\n\n'
                        ).encode("utf-8")
                    )
                else:
                    self.wfile.write(
                        b'data: {"type":"stream_error","error":"approval did not arrive"}\n\n'
                    )
                self.wfile.flush()
                return

            self._send_json(404, {"error": self.path})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        config_path = tmp_path / "cashnerd" / "cli_config.json"
        _save_config(
            config_path,
            CLIConfig(
                gateway_user_key="user-key-123",
                user_id="user-123",
                base_url=f"http://127.0.0.1:{server.server_port}",
            ),
        )
        capture_path = tmp_path / "captures" / "budget.jsonl"
        stdout = io.StringIO()
        cli = chat_cli.DevChatCLI(
            stdout=stdout,
            stderr=io.StringIO(),
            input_fn=lambda: pytest.fail("auto-approved tools should not prompt"),
            time_fn=_incrementing_time(7000),
            client_limits=httpx.Limits(max_connections=1),
        )

        code = cli.chat(
            message="set budget",
            skill=None,
            raw=False,
            new_history=False,
            session_name="default",
            capture_jsonl=str(capture_path),
            auto_approve_tools=frozenset({"budget_set"}),
            base_url=None,
            user_key=None,
            user_id=None,
            config_path=config_path,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)

    assert code == 0
    assert chat_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "messages": [{"role": "user", "content": "set budget"}],
                "context": {"channel": "cli"},
                "user_id": "user-123",
            },
        }
    ]
    assert approval_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "tool_call_id": "tool-1",
                "nonce": "nonce-1",
                "approved": True,
            },
        }
    ]
    assert "[approval_submitted] approved" in stdout.getvalue()
    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [
        "tool_approval_request",
        "dev_chat_cli_approval_decision",
        "tool_call_complete",
        "stream_complete",
    ]


def test_chat_cli_auto_approves_qualified_tool_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    capture_path = tmp_path / "captures" / "qualified.jsonl"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    approval_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        (
                            'data: {"type":"tool_approval_request",'
                            '"tool_name":"code_execute","resolved_qualifier":"docker",'
                            '"tool_call_id":"tool-1","nonce":"nonce-1",'
                            '"tool_input":{"cmd":"true"},"expires_at":1760000000}\n\n'
                        ),
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        if request.url.path == "/api/chat/tool-approval":
            approval_posts.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    code = main(
        [
            "chat",
            "--capture-jsonl",
            str(capture_path),
            "--auto-approve-tool",
            "code_execute:docker",
            "run",
        ],
        stdout=stdout,
        stderr=io.StringIO(),
        input_fn=lambda: pytest.fail("qualified auto-approved tools should not prompt"),
        time_fn=_incrementing_time(7000),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert approval_posts == [
        {"tool_call_id": "tool-1", "nonce": "nonce-1", "approved": True}
    ]
    rendered = stdout.getvalue()
    assert "[tool_approval_request] code_execute  qualifier=docker" in rendered
    assert "[auto_approval] approving code_execute:docker via --auto-approve-tool" in rendered
    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [
        "tool_approval_request",
        "dev_chat_cli_approval_decision",
        "stream_complete",
    ]
    assert rows[1]["approval_key"] == "code_execute:docker"
    assert rows[1]["resolved_qualifier"] == "docker"


def test_chat_cli_auto_approve_requires_qualified_key_for_qualified_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    capture_path = tmp_path / "captures" / "qualified-deny.jsonl"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    approval_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        (
                            'data: {"type":"tool_approval_request",'
                            '"tool_name":"code_execute","resolved_qualifier":"docker",'
                            '"tool_call_id":"tool-1","nonce":"nonce-1",'
                            '"tool_input":{"cmd":"true"},"expires_at":1760000000}\n\n'
                        ),
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        if request.url.path == "/api/chat/tool-approval":
            approval_posts.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(200, json={"success": True})
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    code = main(
        [
            "chat",
            "--capture-jsonl",
            str(capture_path),
            "--auto-approve-tool",
            "code_execute",
            "run",
        ],
        stdout=stdout,
        stderr=io.StringIO(),
        input_fn=lambda: "n",
        time_fn=_incrementing_time(7000),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert approval_posts == [
        {"tool_call_id": "tool-1", "nonce": "nonce-1", "approved": False}
    ]
    rendered = stdout.getvalue()
    assert "[auto_approval]" not in rendered
    assert "  -> Allow? [y/N]:" in rendered
    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [
        "tool_approval_request",
        "stream_complete",
    ]


def test_chat_cli_auto_approve_tool_requires_capture_jsonl(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(
        ["chat", "--auto-approve-tool", "goal_set", "hello"],
        stdout=io.StringIO(),
        stderr=stderr,
        transport=httpx.MockTransport(lambda request: pytest.fail(str(request.url))),
    )

    assert code == 1
    assert "--auto-approve-tool requires --capture-jsonl" in stderr.getvalue()


def test_chat_cli_auto_approve_tool_rejects_invalid_name(tmp_path: Path) -> None:
    stderr = io.StringIO()
    code = main(
        ["chat", "--auto-approve-tool", "goal_set,*", "hello"],
        stdout=io.StringIO(),
        stderr=stderr,
        transport=httpx.MockTransport(lambda request: pytest.fail(str(request.url))),
    )

    assert code == 1
    assert "Invalid auto-approval tool name" in stderr.getvalue()


def test_chat_cli_retries_once_on_401_and_resends_full_history(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    config_path = tmp_path / "cashnerd" / "cli_config.json"
    _save_config(
        config_path,
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )

    init_calls = 0
    chat_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal init_calls
        if request.url.path == "/api/chat/init":
            init_calls += 1
            return httpx.Response(
                200,
                json={
                    "session_token": f"tok-{init_calls}",
                    "session_id": f"sess-{init_calls}",
                    "expires_at": 5000,
                },
            )
        if request.url.path == "/api/chat":
            payload = json.loads(request.content.decode("utf-8"))
            chat_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": payload,
                }
            )
            if len(chat_posts) == 1:
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            'data: {"type":"text_delta","text":"First reply"}\n\n',
                            'data: {"type":"stream_complete"}\n\n',
                        ]
                    ),
                )
            if len(chat_posts) == 2:
                return httpx.Response(401, text="expired")
            if len(chat_posts) == 3:
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            'data: {"type":"text_delta","text":"Second reply"}\n\n',
                            'data: {"type":"stream_complete"}\n\n',
                        ]
                    ),
                )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    answers = iter(["first", "second", ""])
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["chat"],
        stdout=stdout,
        stderr=stderr,
        input_fn=lambda: next(answers),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert init_calls == 2
    assert chat_posts == [
        {
            "authorization": "Bearer tok-1",
            "json": {
                "messages": [{"role": "user", "content": "first"}],
                "context": {"channel": "cli"},
                "user_id": "user-123",
            },
        },
        {
            "authorization": "Bearer tok-1",
            "json": {
                "messages": [
                    {"role": "user", "content": "first"},
                    {"role": "assistant", "content": "First reply"},
                    {"role": "user", "content": "second"},
                ],
                "context": {"channel": "cli"},
                "user_id": "user-123",
            },
        },
        {
            "authorization": "Bearer tok-2",
            "json": {
                "messages": [
                    {"role": "user", "content": "first"},
                    {"role": "assistant", "content": "First reply"},
                    {"role": "user", "content": "second"},
                ],
                "context": {"channel": "cli"},
                "user_id": "user-123",
            },
        },
    ]

    rendered = stdout.getvalue()
    assert "First reply" in rendered
    assert "Second reply" in rendered


def test_smoke_prod_requires_explicit_production_ack(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stderr = io.StringIO()
    code = main(
        [
            "smoke-prod",
            "--base-url",
            "http://127.0.0.1:8002",
            "--user-key",
            "user-key-123",
            "--user-id",
            "user-123",
        ],
        stdout=io.StringIO(),
        stderr=stderr,
        transport=httpx.MockTransport(handler),
    )

    assert code == 1
    assert "smoke-prod requires --allow-production" in stderr.getvalue()


def test_smoke_prod_runs_no_tool_and_tool_checks(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    init_posts: list[dict[str, object]] = []
    chat_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            init_posts.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            payload = json.loads(request.content.decode("utf-8"))
            chat_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": payload,
                }
            )
            if len(chat_posts) == 1:
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            'data: {"type":"text_delta","text":"LIVE_AGENT_OK"}\n\n',
                            'data: {"type":"turn_complete"}\n\n',
                            'data: {"type":"stream_complete"}\n\n',
                        ]
                    ),
                )
            if len(chat_posts) == 2:
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            'data: {"type":"tool_call_start","tool_name":"provider_status","tool_call_id":"tool-1"}\n\n',
                            'data: {"type":"tool_call_complete","tool_name":"provider_status","tool_call_id":"tool-1","result":{"summary":{"ok":true}}}\n\n',
                            'data: {"type":"text_delta","text":"LIVE_AGENT_TOOL_OK"}\n\n',
                            'data: {"type":"stream_complete"}\n\n',
                        ]
                    ),
                )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "smoke-prod",
            "--base-url",
            "http://127.0.0.1:8002",
            "--user-key",
            "user-key-123",
            "--user-id",
            "user-123",
            "--allow-production",
        ],
        stdout=stdout,
        stderr=stderr,
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert init_posts == [
        {"api_key": "user-key-123", "user_id": "user-123", "context": {"channel": "cli"}}
    ]
    assert [post["authorization"] for post in chat_posts] == ["Bearer tok-1", "Bearer tok-1"]
    assert chat_posts[0]["json"] == {
        "messages": [{"role": "user", "content": chat_cli._PROD_SMOKE_NO_TOOL_MESSAGE}],
        "context": {"channel": "cli", "purpose": "prod_smoke_no_tool"},
        "user_id": "user-123",
    }
    assert chat_posts[1]["json"] == {
        "messages": [{"role": "user", "content": chat_cli._PROD_SMOKE_TOOL_MESSAGE}],
        "context": {"channel": "cli", "purpose": "prod_smoke_tool"},
        "user_id": "user-123",
    }
    rendered = stdout.getvalue()
    assert "[smoke:no-tool] PASS" in rendered
    assert "[smoke:tool] PASS" in rendered
    assert "[smoke] PASS" in rendered


def test_smoke_prod_handles_wrapped_gateway_events(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    chat_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal chat_count
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            chat_count += 1
            if chat_count == 1:
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            _wrapped_sse(1, {"type": "text_delta", "text": "LIVE_AGENT_OK"}),
                            _wrapped_sse(2, {"type": "stream_complete"}),
                        ]
                    ),
                )
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        _wrapped_sse(
                            3,
                            {
                                "type": "tool_call_start",
                                "tool_name": "provider_status",
                                "tool_call_id": "tool-1",
                            },
                        ),
                        _wrapped_sse(
                            4,
                            {
                                "type": "tool_call_complete",
                                "tool_name": "provider_status",
                                "tool_call_id": "tool-1",
                                "result": {"summary": {"ok": True}},
                            },
                        ),
                        _wrapped_sse(5, {"type": "text_delta", "text": "LIVE_AGENT_TOOL_OK"}),
                        _wrapped_sse(6, {"type": "stream_complete"}),
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "smoke-prod",
            "--base-url",
            "http://127.0.0.1:8002",
            "--user-key",
            "user-key-123",
            "--user-id",
            "user-123",
            "--allow-production",
        ],
        stdout=stdout,
        stderr=stderr,
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    rendered = stdout.getvalue()
    assert "[smoke:no-tool] PASS" in rendered
    assert "[smoke:tool] PASS" in rendered
    assert "[smoke] PASS" in rendered


def test_smoke_prod_fails_on_tool_error(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            payload = json.loads(request.content.decode("utf-8"))
            if payload["context"]["purpose"] == "prod_smoke_no_tool":
                return httpx.Response(
                    200,
                    stream=ChunkStream(
                        [
                            'data: {"type":"text_delta","text":"LIVE_AGENT_OK"}\n\n',
                            'data: {"type":"stream_complete"}\n\n',
                        ]
                    ),
                )
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        'data: {"type":"tool_call_start","tool_name":"provider_status","tool_call_id":"tool-1"}\n\n',
                        'data: {"type":"tool_call_complete","tool_name":"provider_status","tool_call_id":"tool-1","error":{"message":"boom"}}\n\n',
                        'data: {"type":"text_delta","text":"LIVE_AGENT_TOOL_OK"}\n\n',
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    code = main(
        [
            "smoke-prod",
            "--base-url",
            "http://127.0.0.1:8002",
            "--user-key",
            "user-key-123",
            "--user-id",
            "user-123",
            "--allow-production",
        ],
        stdout=stdout,
        stderr=io.StringIO(),
        transport=httpx.MockTransport(handler),
    )

    assert code == 1
    rendered = stdout.getvalue()
    assert "[smoke:tool] FAIL" in rendered
    assert "errors=boom" in rendered
    assert "[smoke] FAIL" in rendered


def _write_cli_config(tmp_path: Path) -> None:
    _save_config(
        tmp_path / "cashnerd" / "cli_config.json",
        CLIConfig(
            gateway_user_key="user-key-123",
            user_id="user-123",
            base_url="http://127.0.0.1:8002",
        ),
    )


def _session_file(tmp_path: Path, name: str = "default") -> Path:
    return tmp_path / "cashnerd" / "sessions" / f"{name}.json"


def _incrementing_time(start: int = 1000):
    current = start - 1

    def time_fn() -> float:
        nonlocal current
        current += 1
        return float(current)

    return time_fn


def _gateway_transport(
    chat_posts: list[dict[str, object]],
    *,
    replies: list[str] | None = None,
) -> httpx.MockTransport:
    response_texts = replies or []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            chat_posts.append(
                {
                    "authorization": request.headers.get("Authorization"),
                    "json": json.loads(request.content.decode("utf-8")),
                }
            )
            index = len(chat_posts) - 1
            text = response_texts[index] if index < len(response_texts) else f"Reply {index + 1}"
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        f"data: {json.dumps({'type': 'text_delta', 'text': text})}\n\n",
                        'data: {"type":"stream_complete"}\n\n',
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    return httpx.MockTransport(handler)


def test_chat_capture_jsonl_writes_parsed_gateway_events(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)
    capture_path = tmp_path / "captures" / "coach-debt.jsonl"
    chat_posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/chat/init":
            return httpx.Response(
                200,
                json={"session_token": "tok-1", "session_id": "sess-1", "expires_at": 5000},
            )
        if request.url.path == "/api/chat":
            chat_posts.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(
                200,
                stream=ChunkStream(
                    [
                        'data: {"type":"text_delta","text":"ok"}\n\n',
                        (
                            'data: {"type":"tool_call_start","tool_name":"skill_state_get",'
                            '"tool_call_id":"tool-1","tool_input":{"name":"coach_debt_payoff"}}\n\n'
                        ),
                        (
                            'data: {"type":"tool_call_complete","tool_name":"skill_state_get",'
                            '"tool_call_id":"tool-1","result":{"state":{"phase":"diagnose"}}}\n\n'
                        ),
                        'data: {"type":"stream_complete"}',
                    ]
                ),
            )
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        [
            "chat",
            "--skill",
            "coach_debt_payoff",
            "--capture-jsonl",
            str(capture_path),
            "start harness run",
        ],
        stdout=stdout,
        stderr=stderr,
        time_fn=_incrementing_time(6000),
        transport=httpx.MockTransport(handler),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert chat_posts == [
        {
            "messages": [{"role": "user", "content": "start harness run"}],
            "context": {"channel": "cli", "skill": "coach_debt_payoff"},
            "user_id": "user-123",
        }
    ]
    assert f"[capture] writing JSONL to {capture_path}" in stdout.getvalue()
    assert stat.S_IMODE(capture_path.stat().st_mode) == 0o600

    rows = [
        json.loads(line)
        for line in capture_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["type"] for row in rows] == [
        "text_delta",
        "tool_call_start",
        "tool_call_complete",
        "stream_complete",
    ]
    assert rows[1]["tool_name"] == "skill_state_get"
    assert rows[1]["tool_input"] == {"name": "coach_debt_payoff"}
    assert rows[2]["result"] == {"state": {"phase": "diagnose"}}
    assert rows[3]["capture"]["event_index"] == 4
    for row in rows:
        assert row["capture"] == {
            "source": "gateway_sse",
            "session_name": "default",
            "session_id": "sess-1",
            "turn_index": 1,
            "attempt": 0,
            "event_index": row["capture"]["event_index"],
            "captured_at": row["capture"]["captured_at"],
        }
        assert isinstance(row["capture"]["captured_at"], int)

    from finance_cli.coaching_skill_harness import normalize_tool_calls

    tool_calls = normalize_tool_calls(rows)
    assert [call.tool_name for call in tool_calls] == ["skill_state_get"]
    assert tool_calls[0].tool_input == {"name": "coach_debt_payoff"}
    assert tool_calls[0].succeeded is True

    assert "user-key-123" not in capture_path.read_text(encoding="utf-8")


def test_chat_persists_session_history(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)

    chat_posts: list[dict[str, object]] = []
    transport = _gateway_transport(chat_posts, replies=["First reply", "Second reply"])
    time_fn = _incrementing_time(1000)

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["chat", "first"],
        stdout=stdout,
        stderr=stderr,
        time_fn=time_fn,
        transport=transport,
    )

    assert code == 0
    assert stderr.getvalue() == ""
    session_path = _session_file(tmp_path)
    assert session_path.exists()
    assert stat.S_IMODE(session_path.stat().st_mode) == 0o600
    first_saved = json.loads(session_path.read_text(encoding="utf-8"))
    assert first_saved["name"] == "default"
    assert isinstance(first_saved["created_at"], int)
    assert isinstance(first_saved["updated_at"], int)
    assert first_saved["messages"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "First reply"},
    ]

    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["chat", "second"],
        stdout=stdout,
        stderr=stderr,
        time_fn=time_fn,
        transport=transport,
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert chat_posts[1]["json"]["messages"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "First reply"},
        {"role": "user", "content": "second"},
    ]
    second_saved = json.loads(session_path.read_text(encoding="utf-8"))
    assert second_saved["created_at"] == first_saved["created_at"]
    assert second_saved["updated_at"] > first_saved["updated_at"]
    assert second_saved["messages"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "First reply"},
        {"role": "user", "content": "second"},
        {"role": "assistant", "content": "Second reply"},
    ]


def test_chat_session_named_isolation(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)

    chat_posts: list[dict[str, object]] = []
    transport = _gateway_transport(chat_posts, replies=["Foo reply", "Bar reply"])
    time_fn = _incrementing_time(2000)

    code = main(
        ["chat", "--session", "foo", "hello foo"],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        time_fn=time_fn,
        transport=transport,
    )
    assert code == 0

    code = main(
        ["chat", "--session", "bar", "hello bar"],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        time_fn=time_fn,
        transport=transport,
    )
    assert code == 0

    assert _session_file(tmp_path, "foo").exists()
    assert _session_file(tmp_path, "bar").exists()
    assert chat_posts[0]["json"]["messages"] == [
        {"role": "user", "content": "hello foo"}
    ]
    assert chat_posts[1]["json"]["messages"] == [
        {"role": "user", "content": "hello bar"}
    ]
    foo_saved = json.loads(_session_file(tmp_path, "foo").read_text(encoding="utf-8"))
    bar_saved = json.loads(_session_file(tmp_path, "bar").read_text(encoding="utf-8"))
    assert foo_saved["messages"] == [
        {"role": "user", "content": "hello foo"},
        {"role": "assistant", "content": "Foo reply"},
    ]
    assert bar_saved["messages"] == [
        {"role": "user", "content": "hello bar"},
        {"role": "assistant", "content": "Bar reply"},
    ]


def test_chat_new_truncates_session(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)
    session_path = _session_file(tmp_path)
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(
        json.dumps(
            {
                "name": "default",
                "created_at": 123,
                "updated_at": 124,
                "messages": [
                    {"role": "user", "content": "old"},
                    {"role": "assistant", "content": "older"},
                ],
            }
        ),
        encoding="utf-8",
    )

    chat_posts: list[dict[str, object]] = []
    code = main(
        ["chat", "--new", "fresh"],
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        time_fn=_incrementing_time(3000),
        transport=_gateway_transport(chat_posts, replies=["Fresh reply"]),
    )

    assert code == 0
    assert chat_posts[0]["json"]["messages"] == [
        {"role": "user", "content": "fresh"}
    ]
    saved = json.loads(session_path.read_text(encoding="utf-8"))
    assert saved["created_at"] == 123
    assert saved["messages"] == [
        {"role": "user", "content": "fresh"},
        {"role": "assistant", "content": "Fresh reply"},
    ]


def test_chat_new_recovers_from_corrupted_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)
    session_path = _session_file(tmp_path)
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text("{not json", encoding="utf-8")

    chat_posts: list[dict[str, object]] = []
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = main(
        ["chat", "--new", "fresh"],
        stdout=stdout,
        stderr=stderr,
        time_fn=_incrementing_time(4000),
        transport=_gateway_transport(chat_posts, replies=["Recovered reply"]),
    )

    assert code == 0
    assert stderr.getvalue() == ""
    assert chat_posts[0]["json"]["messages"] == [
        {"role": "user", "content": "fresh"}
    ]
    saved = json.loads(session_path.read_text(encoding="utf-8"))
    assert saved["messages"] == [
        {"role": "user", "content": "fresh"},
        {"role": "assistant", "content": "Recovered reply"},
    ]


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "a/b",
        "with spaces",
        " leading",
        "trailing ",
        "a" * 65,
        "name.json",
    ],
)
def test_chat_invalid_session_name_fails_before_credentials(
    monkeypatch,
    tmp_path: Path,
    name: str,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stderr = io.StringIO()
    code = main(
        ["chat", "--session", name, "hello"],
        stdout=io.StringIO(),
        stderr=stderr,
        transport=httpx.MockTransport(handler),
    )

    assert code == 1
    assert "Invalid session name" in stderr.getvalue()
    assert "Missing gateway credentials" not in stderr.getvalue()
    assert not (tmp_path / "cashnerd" / "sessions").exists()


@pytest.mark.parametrize(
    "payload",
    [
        "{not json",
        [],
        {"name": "default", "created_at": 123, "updated_at": 124},
        {"name": "default", "created_at": 123, "updated_at": 124, "messages": "bad"},
        {"name": "default", "created_at": 123, "updated_at": 124, "messages": ["bad"]},
        {
            "name": "default",
            "created_at": 123,
            "updated_at": 124,
            "messages": [{"content": "missing role"}],
        },
        {
            "name": "default",
            "created_at": 123,
            "updated_at": 124,
            "messages": [{"role": "user"}],
        },
        {
            "name": "default",
            "created_at": 123,
            "updated_at": 124,
            "messages": [{"role": 1, "content": "bad role"}],
        },
        {
            "name": "default",
            "created_at": 123,
            "updated_at": 124,
            "messages": [{"role": "user", "content": 1}],
        },
        {
            "name": "default",
            "created_at": 123,
            "updated_at": 124,
            "messages": [{"role": "system", "content": "unsupported"}],
        },
    ],
)
def test_chat_corrupted_session_file_errors_clearly(
    monkeypatch,
    tmp_path: Path,
    payload: object,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)
    session_path = _session_file(tmp_path)
    session_path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        session_path.write_text(payload, encoding="utf-8")
    else:
        session_path.write_text(json.dumps(payload), encoding="utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"Unexpected request path: {request.url.path}")

    stderr = io.StringIO()
    code = main(
        ["chat", "hello"],
        stdout=io.StringIO(),
        stderr=stderr,
        transport=httpx.MockTransport(handler),
    )

    assert code == 1
    rendered_error = stderr.getvalue()
    assert str(session_path) in rendered_error
    assert "--new" in rendered_error


def test_chat_persist_failure_warns_and_continues(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    _write_cli_config(tmp_path)

    original_save_session = chat_cli._save_session
    save_calls = 0

    def flaky_save_session(state: chat_cli.SessionState, *, time_fn) -> None:
        nonlocal save_calls
        save_calls += 1
        if save_calls == 1:
            raise OSError("boom")
        original_save_session(state, time_fn=time_fn)

    monkeypatch.setattr(chat_cli, "_save_session", flaky_save_session)

    answers = iter(["first", "second", ""])
    chat_posts: list[dict[str, object]] = []
    stderr = io.StringIO()
    code = main(
        ["chat"],
        stdout=io.StringIO(),
        stderr=stderr,
        input_fn=lambda: next(answers),
        time_fn=_incrementing_time(5000),
        transport=_gateway_transport(chat_posts, replies=["First reply", "Second reply"]),
    )

    assert code == 0
    assert "[session_warn] failed to persist: boom" in stderr.getvalue()
    assert len(chat_posts) == 2
    assert chat_posts[0]["json"]["messages"] == [
        {"role": "user", "content": "first"}
    ]
    assert chat_posts[1]["json"]["messages"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "First reply"},
        {"role": "user", "content": "second"},
    ]
    saved = json.loads(_session_file(tmp_path).read_text(encoding="utf-8"))
    assert saved["messages"] == [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "First reply"},
        {"role": "user", "content": "second"},
        {"role": "assistant", "content": "Second reply"},
    ]
