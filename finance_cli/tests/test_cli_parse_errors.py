from __future__ import annotations

import json
from pathlib import Path

import pytest

from finance_cli.__main__ import main


@pytest.mark.parametrize(
    "argv",
    [
        ["nonexistent"],
        ["txn", "--bogus"],
        ["balance", "bogus"],
    ],
)
def test_parse_errors_return_json_envelope(tmp_path: Path, monkeypatch, capsys, argv: list[str]) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(argv)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert payload["status"] == "error"
    assert "error" in payload
    assert "Traceback" not in captured.err


def test_help_still_exits_normally(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
