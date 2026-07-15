from __future__ import annotations

import json
from pathlib import Path

from finance_cli.__main__ import main


def test_json_envelope_contract(tmp_path: Path, capsys, monkeypatch) -> None:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))

    code = main(["cat", "list"])
    assert code == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["status"] == "success"
    assert payload["command"] == "cat.list"
    from finance_cli import __version__
    assert payload["version"] == __version__
    assert "data" in payload
    assert "summary" in payload
