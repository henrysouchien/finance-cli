from __future__ import annotations

import json
from pathlib import Path


def test_main_loads_dotenv_and_runs_without_db_bootstrap(monkeypatch) -> None:
    import finance_cli.mcp_gateway as mod
    import finance_cli.mcp_server as mcp_server

    calls = {"dotenv": 0, "run": 0}

    monkeypatch.setattr(mod, "load_dotenv", lambda *a, **k: calls.__setitem__("dotenv", calls["dotenv"] + 1))
    monkeypatch.setattr(mod.mcp, "run", lambda *a, **k: calls.__setitem__("run", calls["run"] + 1))

    assert not hasattr(mcp_server, "auto_migrate_data")
    assert not hasattr(mcp_server, "initialize_database")

    mod.main()

    assert calls == {"dotenv": 1, "run": 1}


def test_gateway_mcp_config_uses_lazy_tiny_db_pool() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / "infra" / "mcp" / "gateway-mcp-config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    server_config = config["mcpServers"]["finance-cli"]

    assert server_config["env"]["DB_POOL_MIN"] == "0"
    assert server_config["env"]["DB_POOL_MAX"] == "1"
