from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_storage_architecture_gate.py"
SPEC = importlib.util.spec_from_file_location("check_storage_architecture_gate", SCRIPT_PATH)
assert SPEC is not None
check_storage_architecture_gate = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = check_storage_architecture_gate
SPEC.loader.exec_module(check_storage_architecture_gate)


Window = check_storage_architecture_gate.Window


class FakeAws:
    def __init__(
        self,
        *,
        requests: dict[str, int] | None = None,
        errors: dict[str, int] | None = None,
        server: dict[str, tuple[int, float]] | None = None,
        client: dict[str, tuple[int, float]] | None = None,
        kms: dict[str, tuple[int, float]] | None = None,
        gap_rows: list[list[dict[str, str]]] | None = None,
        client_gap_rows: list[list[dict[str, str]]] | None = None,
        server_gap_rows: list[list[dict[str, str]]] | None = None,
    ) -> None:
        self.requests = requests or {}
        self.errors = errors or {}
        self.server = server or {}
        self.client = client or {}
        self.kms = kms or {}
        self.gap_rows = gap_rows or []
        self.client_gap_rows = client_gap_rows or []
        self.server_gap_rows = server_gap_rows or []
        self.commands: list[list[str]] = []
        self._query_results: dict[str, list[list[dict[str, str]]]] = {}
        self._query_count = 0

    def run(self, args: list[str], *, timeout_seconds: int | None = None) -> dict[str, Any]:
        self.commands.append(args)
        if args[:2] == ["logs", "start-query"]:
            del timeout_seconds
            self._query_count += 1
            query_id = f"query-{self._query_count}"
            log_group = args[args.index("--log-group-name") + 1]
            query_string = args[args.index("--query-string") + 1]
            if "join type=inner" in query_string:
                rows = self.gap_rows
            elif log_group == "/finance-cli/finance-web/metrics":
                rows = self.client_gap_rows
            elif log_group == "/finance-cli/storage-server/access":
                rows = self.server_gap_rows
            else:
                rows = []
            self._query_results[query_id] = rows
            return {"queryId": query_id}
        if args[:2] == ["logs", "get-query-results"]:
            query_id = args[args.index("--query-id") + 1]
            return {"status": "Complete", "results": self._query_results.get(query_id, [])}

        metric = args[args.index("--metric-name") + 1]
        dimensions = _parse_dimensions(args)
        if metric == "RpcRequests":
            return {"Datapoints": [{"Sum": self.requests.get(dimensions["rpc"], 0)}]}
        if metric == "RpcErrors":
            return {"Datapoints": [{"Sum": self.errors.get(dimensions["rpc"], 0)}]}
        if metric == "RemoteUserLatencyUs":
            samples, p99 = self.server.get(dimensions["rpc"], (0, 0.0))
            return _latency_payload(samples, p99)
        if metric == "ClientRpcLatencyUs":
            samples, p99 = self.client.get(dimensions["rpc"], (0, 0.0))
            return _latency_payload(samples, p99)
        if metric == "KmsCallLatencyUs":
            samples, p99 = self.kms.get(dimensions["operation"], (0, 0.0))
            return _latency_payload(samples, p99)
        raise AssertionError(f"unexpected metric {metric}")


def _parse_dimensions(args: list[str]) -> dict[str, str]:
    if "--dimensions" not in args:
        return {}
    index = args.index("--dimensions") + 1
    dimensions: dict[str, str] = {}
    while index < len(args) and not args[index].startswith("--"):
        raw = args[index]
        parts = dict(part.split("=", 1) for part in raw.split(","))
        dimensions[parts["Name"]] = parts["Value"]
        index += 1
    return dimensions


def _latency_payload(samples: int, p99: float) -> dict[str, Any]:
    return {"Datapoints": [{"SampleCount": samples, "ExtendedStatistics": {"p99": p99}}]}


def _gap_event_row(call_id: str, *, rpc: str, duration_us: int) -> list[dict[str, str]]:
    return [
        {"field": "call_id", "value": call_id},
        {"field": "rpc", "value": rpc},
        {"field": "duration_us", "value": str(duration_us)},
    ]


def _window() -> Window:
    return Window(
        start=datetime(2026, 6, 25, tzinfo=UTC),
        end=datetime(2026, 6, 26, tzinfo=UTC),
        period_seconds=86_400,
    )


def test_collect_gate_reports_not_ready_when_required_samples_are_low() -> None:
    fake = FakeAws(
        requests={"Execute": 500, "OpenSession": 100},
        server={"Execute": (500, 15_000), "OpenSession": (100, 240_000)},
        client={"Execute": (500, 22_000), "OpenSession": (100, 250_000)},
        kms={"Decrypt": (100, 90_000)},
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
    )

    assert result["ok"] is False
    assert result["decision"] == "not_ready"
    assert result["sample_gate"]["below_min_sample_rpcs"] == ["Execute", "OpenSession"]


def test_collect_gate_passes_with_clean_metrics_and_gap_query() -> None:
    fake = FakeAws(
        requests={"Execute": 1200, "OpenSession": 1200},
        server={"Execute": (1200, 15_000), "OpenSession": (1200, 240_000)},
        client={"Execute": (1200, 22_000), "OpenSession": (1200, 250_000)},
        kms={"Decrypt": (1200, 90_000)},
        gap_rows=[
            [
                {"field": "rpc", "value": "Execute"},
                {"field": "matched_calls", "value": "1200"},
                {"field": "gap_p99_us", "value": "8000"},
            ]
        ],
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
        include_gap_query=True,
    )

    assert result["ok"] is True
    assert result["decision"] == "pass"
    assert result["gap_query"]["method"] == "logs_insights_join"
    assert result["gap_query"]["results"]["Execute"]["gap_p99_us"] == 8000.0


def test_collect_gate_falls_back_to_client_side_gap_join_when_logs_join_under_matches() -> None:
    fake = FakeAws(
        requests={"Execute": 3, "OpenSession": 3},
        server={"Execute": (3, 15_000), "OpenSession": (3, 240_000)},
        client={"Execute": (3, 22_000), "OpenSession": (3, 250_000)},
        kms={"Decrypt": (3, 90_000)},
        gap_rows=[],
        client_gap_rows=[
            _gap_event_row("call-1", rpc="Execute", duration_us=15_000),
            _gap_event_row("call-2", rpc="Execute", duration_us=18_000),
            _gap_event_row("call-3", rpc="Execute", duration_us=21_000),
        ],
        server_gap_rows=[
            _gap_event_row("call-1", rpc="Execute", duration_us=10_000),
            _gap_event_row("call-2", rpc="Execute", duration_us=11_000),
            _gap_event_row("call-3", rpc="Execute", duration_us=12_000),
        ],
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
        min_ok_samples=3,
        include_gap_query=True,
    )

    assert result["ok"] is True
    assert result["decision"] == "pass"
    assert result["gap_query"]["method"] == "client_side_call_id_join"
    assert result["gap_query"]["source_event_counts"] == {
        "client": 3,
        "server": 3,
        "intersection": 3,
    }
    assert result["gap_query"]["results"]["Execute"] == {
        "matched_calls": 3,
        "gap_p99_us": 9000.0,
    }
    assert result["gap_query"]["logs_insights_join"]["method"] == "logs_insights_join"


def test_collect_gate_fallback_fails_closed_when_raw_result_limit_is_reached() -> None:
    fake = FakeAws(
        requests={"Execute": 1, "OpenSession": 1},
        server={"Execute": (1, 15_000), "OpenSession": (1, 240_000)},
        client={"Execute": (1, 22_000), "OpenSession": (1, 250_000)},
        kms={"Decrypt": (1, 90_000)},
        gap_rows=[],
        client_gap_rows=[
            _gap_event_row("call-1", rpc="Execute", duration_us=15_000)
            for _ in range(check_storage_architecture_gate.GAP_EVENT_QUERY_LIMIT)
        ],
        server_gap_rows=[
            _gap_event_row("call-1", rpc="Execute", duration_us=10_000)
        ],
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
        min_ok_samples=1,
        include_gap_query=True,
    )

    assert result["ok"] is False
    assert result["decision"] == "review_required"
    assert result["gap_query"]["status"] == "Partial"
    assert result["gap_query"]["result_limit_reached"] is True
    assert result["gap_query"]["source_row_counts"] == {
        "client": check_storage_architecture_gate.GAP_EVENT_QUERY_LIMIT,
        "server": 1,
    }
    assert result["gap_query"]["source_event_counts"] == {
        "client": 1,
        "server": 1,
        "intersection": 1,
    }
    assert "true per-call gap query did not complete: Partial" in result["findings"]


def test_collect_gate_rejects_gap_query_without_execute_result() -> None:
    fake = FakeAws(
        requests={"Execute": 1200, "OpenSession": 1200},
        server={"Execute": (1200, 15_000), "OpenSession": (1200, 240_000)},
        client={"Execute": (1200, 22_000), "OpenSession": (1200, 250_000)},
        kms={"Decrypt": (1200, 90_000)},
        gap_rows=[
            [
                {"field": "rpc", "value": "OpenSession"},
                {"field": "matched_calls", "value": "1200"},
                {"field": "gap_p99_us", "value": "5000"},
            ]
        ],
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
        include_gap_query=True,
    )

    assert result["ok"] is False
    assert result["decision"] == "review_required"
    assert "Execute true client-server gap p99 is missing" in result["findings"]
    assert (
        "Execute true client-server gap query matched fewer than 1000 calls"
        in result["findings"]
    )


def test_collect_gate_rejects_gap_query_with_too_few_matched_execute_calls() -> None:
    fake = FakeAws(
        requests={"Execute": 1200, "OpenSession": 1200},
        server={"Execute": (1200, 15_000), "OpenSession": (1200, 240_000)},
        client={"Execute": (1200, 22_000), "OpenSession": (1200, 250_000)},
        kms={"Decrypt": (1200, 90_000)},
        gap_rows=[
            [
                {"field": "rpc", "value": "Execute"},
                {"field": "matched_calls", "value": "42"},
                {"field": "gap_p99_us", "value": "8000"},
            ]
        ],
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
        include_gap_query=True,
    )

    assert result["ok"] is False
    assert result["decision"] == "review_required"
    assert (
        "Execute true client-server gap query matched fewer than 1000 calls"
        in result["findings"]
    )


def test_collect_gate_requires_gap_query_before_passing_architecture_gate() -> None:
    fake = FakeAws(
        requests={"Execute": 1200, "OpenSession": 1200},
        server={"Execute": (1200, 15_000), "OpenSession": (1200, 240_000)},
        client={"Execute": (1200, 22_000), "OpenSession": (1200, 250_000)},
        kms={"Decrypt": (1200, 90_000)},
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
    )

    assert result["ok"] is False
    assert result["decision"] == "gap_query_required"
    assert "include-gap-query" in result["findings"][-1]


def test_collect_gate_flags_execute_latency_threshold() -> None:
    fake = FakeAws(
        requests={"Execute": 1200, "OpenSession": 1200},
        server={"Execute": (1200, 15_000), "OpenSession": (1200, 240_000)},
        client={"Execute": (1200, 101_000), "OpenSession": (1200, 250_000)},
        kms={"Decrypt": (1200, 90_000)},
    )

    result = check_storage_architecture_gate.collect_gate(
        aws=fake,
        window=_window(),
        rpcs=("Execute", "OpenSession"),
        required_rpcs=("Execute", "OpenSession"),
    )

    assert result["ok"] is False
    assert result["decision"] == "review_required"
    assert "Execute client p99 exceeds 100ms" in result["findings"]


def test_parse_window_defaults_to_last_full_utc_day() -> None:
    window = check_storage_architecture_gate._default_window(
        datetime(2026, 6, 26, 3, 10, tzinfo=UTC)
    )

    assert window.start.isoformat() == "2026-06-25T00:00:00+00:00"
    assert window.end.isoformat() == "2026-06-26T00:00:00+00:00"
    assert window.period_seconds == 86_400
