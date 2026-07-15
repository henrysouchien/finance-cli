import importlib
import sys


def test_module_import_does_not_run_bootstrap(monkeypatch):
    calls = {
        "load_dotenv": 0,
        "auto_migrate_data": 0,
        "initialize_database": 0,
    }

    def _count(name):
        def _inner(*args, **kwargs):
            calls[name] += 1

        return _inner

    monkeypatch.setattr("finance_cli.config.load_dotenv", _count("load_dotenv"))
    monkeypatch.setattr(
        "finance_cli.config.auto_migrate_data",
        _count("auto_migrate_data"),
    )
    monkeypatch.setattr(
        "finance_cli.db.initialize_database",
        _count("initialize_database"),
    )

    sys.modules.pop("finance_cli.mcp_server", None)
    importlib.import_module("finance_cli.mcp_server")

    assert calls == {
        "load_dotenv": 0,
        "auto_migrate_data": 0,
        "initialize_database": 0,
    }
