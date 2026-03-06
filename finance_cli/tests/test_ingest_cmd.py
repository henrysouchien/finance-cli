from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import pytest

from finance_cli.__main__ import main
from finance_cli.ai_statement_parser import AIParseResult, PROMPT_VERSION, _default_model
from finance_cli.db import connect, initialize_database
from finance_cli.extractors import ExtractorMeta, ExtractorOutput
from finance_cli.importers.pdf import ExtractResult
from finance_cli.ingest_validation import validate_ai_parse


def _run_cli(args: list[str], capsys) -> tuple[int, dict[str, Any]]:
    code = main(args)
    payload = json.loads(capsys.readouterr().out)
    return code, payload


def _setup_db(tmp_path: Path, monkeypatch) -> Path:
    db_path = tmp_path / "finance.db"
    monkeypatch.setenv("FINANCE_CLI_DB", str(db_path))
    initialize_database(db_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: openai
          model: gpt-4o-mini
        ai_categorizer:
          provider: openai
        """,
    )
    return db_path


def _write_pdf(path: Path, name: str = "stmt.pdf", content: bytes = b"dummy") -> Path:
    pdf_path = path / name
    pdf_path.write_bytes(content)
    return pdf_path


def _write_rules(path: Path, body: str) -> Path:
    rules_path = path / "rules.yaml"
    rules_path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    return rules_path


def _write_csv(path: Path, name: str, body: str) -> Path:
    csv_path = path / name
    csv_path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    return csv_path


def _write_apple_csv(path: Path, name: str = "apple.csv") -> Path:
    return _write_csv(
        path,
        name,
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        02/17/2026,02/17/2026,ACH DEPOSIT,ACH DEPOSIT,Payment,Payment,-100.00,Henry
        """,
    )


def _write_bofa_checking_csv(path: Path, name: str = "bofa.csv") -> Path:
    return _write_csv(
        path,
        name,
        """
        Description,,Summary Amt.
        Beginning balance as of 01/01/2026,,"5,000.00"
        Total credits,,"1,000.00"
        Total debits,,"-500.00"
        Ending balance as of 01/31/2026,,"5,500.00"

        Date,Description,Amount,Running Bal.
        01/15/2026,"PAYPAL DES:INST XFER","-32.00","4,968.00"
        """,
    )


def _base_payload() -> dict[str, Any]:
    return {
        "statement": {
            "institution": "Citi",
            "account_label": "Card",
            "statement_period_start": "2025-01-01",
            "statement_period_end": "2025-01-31",
            "new_balance": 130.00,
            "currency": "USD",
        },
        "transactions": [
            {
                "date": "2025-01-03",
                "description": "COFFEE SHOP",
                "amount": -10.00,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.95,
                "evidence": None,
            },
            {
                "date": "2025-01-05",
                "description": "BOOK STORE",
                "amount": -20.00,
                "card_ending": "1234",
                "transaction_id": None,
                "confidence": 0.92,
                "evidence": None,
            },
        ],
        "extraction_meta": {
            "model": "fake-model",
            "prompt_version": "v1",
            "notes": None,
            "expected_transaction_count": 2,
        },
    }


@pytest.fixture(autouse=True)
def ai_stub(monkeypatch):
    state: dict[str, Any] = {
        "extractor": lambda _path: "statement text",
        "response": _base_payload(),
        "usage": {"input_tokens": 13, "output_tokens": 7},
        "calls": [],
    }

    def _fake_extract(pdf_path: Path) -> str:
        extractor = state["extractor"]
        return str(extractor(Path(pdf_path)))

    def _fake_send(
        provider: str,
        system_prompt: str,
        user_prompt: str,
        model: str,
        max_tokens: int = 16384,
    ) -> tuple[str, dict[str, int]]:
        state["calls"].append(
            {
                "provider": provider,
                "model": model,
                "user_prompt": user_prompt,
                "system_prompt": system_prompt,
            }
        )

        response = state["response"]
        value = response(provider, system_prompt, user_prompt, model) if callable(response) else response
        usage = dict(state.get("usage") or {})
        if isinstance(value, str):
            return value, usage
        return json.dumps(value), usage

    monkeypatch.setattr("finance_cli.ai_statement_parser.extract_pdf_text", _fake_extract)
    monkeypatch.setattr("finance_cli.ai_statement_parser._send_parse_request", _fake_send)
    return state


def test_ingest_statement_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["inserted"] > 0
    assert payload["data"]["input_tokens"] == 13
    assert payload["data"]["output_tokens"] == 7
    assert payload["data"]["elapsed_ms"] >= 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == 0


def test_ingest_statement_commit(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is False
    assert payload["data"]["inserted"] > 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == payload["data"]["inserted"]


def test_ingest_statement_records_ai_model_version_in_import_batch(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT ai_model
              FROM import_batches
             ORDER BY created_at DESC
             LIMIT 1
            """
        ).fetchone()

    assert row["ai_model"] == payload["data"]["model"]


def test_ingest_statement_persists_bumped_prompt_version_and_hash(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"

    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT ai_prompt_version, ai_prompt_hash
              FROM import_batches
             ORDER BY created_at DESC
             LIMIT 1
            """
        ).fetchone()

    assert row["ai_prompt_version"] == PROMPT_VERSION
    assert isinstance(row["ai_prompt_hash"], str)
    assert len(row["ai_prompt_hash"]) == 64


def test_ingest_statement_cli_report_includes_tokens(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code = main(["ingest", "statement", "--file", str(pdf_path), "--format", "cli"])
    output = capsys.readouterr().out
    assert code == 0
    assert "tokens=in:13/out:7" in output
    assert "elapsed=" in output


def test_ingest_statement_replace(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    replacement = _base_payload()
    replacement["transactions"] = [
        {
            "date": "2025-01-06",
            "description": "GROCERY",
            "amount": -15.00,
            "card_ending": "1234",
            "transaction_id": None,
            "confidence": 0.97,
            "evidence": None,
        }
    ]
    responses = [_base_payload(), replacement]
    ai_stub["response"] = lambda *_args: responses.pop(0)

    first_code, first_payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert first_code == 0
    assert first_payload["status"] == "success"
    # First commit moves file to processed/; move it back for --replace test
    processed = pdf_path.parent / "processed" / pdf_path.name
    if processed.exists():
        processed.rename(pdf_path)

    second_code, second_payload = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--commit", "--replace"],
        capsys,
    )
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["inserted"] == 1
    assert Path(second_payload["data"]["backup_path"]).exists()

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 1
    assert batch_count == 1


def test_ingest_statement_already_imported(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    first_code, first_payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert first_code == 0
    assert first_payload["status"] == "success"
    # First commit moves file to processed/; move it back for re-import test
    processed = pdf_path.parent / "processed" / pdf_path.name
    if processed.exists():
        processed.rename(pdf_path)

    second_code, second_payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["already_imported"] is True
    assert second_payload["data"]["inserted"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == 2


def test_ingest_statement_commit_moves_to_processed(tmp_path: Path, monkeypatch, capsys) -> None:
    """Single-file ingest with --commit moves file to processed/ subdirectory."""
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert not pdf_path.exists(), "file should have been moved"
    assert (tmp_path / "processed" / pdf_path.name).exists()


def test_ingest_statement_dry_run_does_not_move(tmp_path: Path, monkeypatch, capsys) -> None:
    """Single-file ingest without --commit should NOT move the file."""
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 0
    assert pdf_path.exists(), "file should remain in place for dry-run"
    assert not (tmp_path / "processed").exists()


def test_ingest_statement_allow_partial(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    payload = _base_payload()
    payload["transactions"][0]["confidence"] = 0.55
    ai_stub["response"] = payload

    code, result = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--allow-partial", "--commit"],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["inserted"] == 1

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == 1


def test_ingest_statement_require_reconciled_rejects_no_totals_for_ai(
    tmp_path: Path,
    monkeypatch,
    capsys,
    ai_stub,
) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, result = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--require-reconciled"],
        capsys,
    )
    assert code == 1
    assert result["status"] == "error"
    assert "reconciliation status is 'no_totals'" in result["error"]


def test_ingest_statement_provider_from_rules(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: openai
          model: gpt-from-rules
        """,
    )

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["provider"] == "openai"
    assert result["data"]["model"] == "gpt-from-rules"
    assert ai_stub["calls"][0]["provider"] == "openai"
    assert ai_stub["calls"][0]["model"] == "gpt-from-rules"


def test_ingest_statement_cli_flag_overrides_rules(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: claude
          model: claude-rules-model
        """,
    )

    code, result = _run_cli(
        [
            "ingest",
            "statement",
            "--file",
            str(pdf_path),
            "--provider",
            "openai",
            "--model",
            "gpt-4o",
        ],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["provider"] == "openai"
    assert result["data"]["model"] == "gpt-4o"
    assert ai_stub["calls"][0]["provider"] == "openai"
    assert ai_stub["calls"][0]["model"] == "gpt-4o"


def test_ingest_statement_config_knobs_forwarded(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: claude
          model: claude-special
          max_text_chars: 12345
          confidence_warn: 0.91
          confidence_block: 0.74
        """,
    )

    captured: dict[str, Any] = {}

    def _fake_ai_parse_statement(
        _pdf_path,
        *,
        provider: str,
        model: str,
        max_text_chars: int,
        max_tokens: int,
        confidence_warn: float,
        confidence_block: float,
    ) -> AIParseResult:
        captured["provider"] = provider
        captured["model"] = model
        captured["max_text_chars"] = max_text_chars
        captured["max_tokens"] = max_tokens
        captured["confidence_warn"] = confidence_warn
        captured["confidence_block"] = confidence_block

        parsed = _base_payload()
        validation = validate_ai_parse(
            parsed,
            confidence_warn=confidence_warn,
            confidence_block=confidence_block,
        )
        return AIParseResult(
            raw_json=json.dumps(parsed),
            parsed=parsed,
            validation=validation,
            provider=provider,
            model=model,
            prompt_version="v1",
            prompt_hash="a" * 64,
        )

    monkeypatch.setattr("finance_cli.extractors.ai_extractor.ai_parse_statement", _fake_ai_parse_statement)

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 0
    assert result["status"] == "success"
    assert captured == {
        "provider": "claude",
        "model": "claude-special",
        "max_text_chars": 12345,
        "max_tokens": 16384,
        "confidence_warn": 0.91,
        "confidence_block": 0.74,
    }


def test_ingest_statement_dir_batch(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "statements"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    _write_pdf(folder, "b.pdf", b"b")

    code, result = _run_cli(["ingest", "statement", "--dir", str(folder)], capsys)
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["inserted"] == 4
    assert result["data"]["skipped_duplicates"] == 0
    assert result["data"]["errors"] == 0
    assert result["data"]["total_input_tokens"] == 26
    assert result["data"]["total_output_tokens"] == 14
    assert result["data"]["total_elapsed_ms"] >= 0
    assert len(result["data"]["reports"]) == 2
    assert all("input_tokens" in item for item in result["data"]["reports"])


def test_ingest_statement_dir_partial_failure(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "statements"
    folder.mkdir()
    _write_pdf(folder, "good.pdf", b"a")
    _write_pdf(folder, "bad.pdf", b"b")

    def _extractor(pdf_path: Path) -> str:
        if pdf_path.name == "bad.pdf":
            raise ValueError("broken PDF")
        return "statement text"

    ai_stub["extractor"] = _extractor

    code, result = _run_cli(["ingest", "statement", "--dir", str(folder)], capsys)
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["inserted"] == 2
    assert result["data"]["errors"] == 1
    assert len(result["data"]["reports"]) == 2
    assert any("error" in item for item in result["data"]["reports"])


def test_ingest_statement_dir_failure_rolls_back_partial_writes(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "statements"
    folder.mkdir()
    _write_pdf(folder, "a_good.pdf", b"a")
    _write_pdf(folder, "b_bad.pdf", b"b")

    from finance_cli.importers.pdf import import_extracted_statement as _real_import_extracted_statement

    def _import_with_failure(*args, **kwargs):
        file_path = Path(kwargs.get("file_path"))
        report = _real_import_extracted_statement(*args, **kwargs)
        if file_path.name == "b_bad.pdf":
            raise RuntimeError("forced failure after write")
        return report

    monkeypatch.setattr("finance_cli.commands.ingest.import_extracted_statement", _import_with_failure)

    code, result = _run_cli(
        ["ingest", "statement", "--dir", str(folder), "--commit"],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["inserted"] == 2
    assert result["data"]["errors"] == 1

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 2
    assert batch_count == 1


def test_ingest_statement_dir_replace_failure_does_not_delete_existing_rows(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "statements"
    folder.mkdir()
    pdf_path = _write_pdf(folder, "a.pdf", b"a")

    first_code, first_payload = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--commit"],
        capsys,
    )
    assert first_code == 0
    assert first_payload["status"] == "success"
    # First commit moves file to processed/; move it back for dir-mode --replace test
    processed = folder / "processed" / pdf_path.name
    if processed.exists():
        processed.rename(pdf_path)

    from finance_cli.importers.pdf import import_extracted_statement as _real_import_extracted_statement

    def _import_fail_on_replace(*args, **kwargs):
        report = _real_import_extracted_statement(*args, **kwargs)
        if kwargs.get("replace_existing_hash"):
            raise RuntimeError("forced replace failure")
        return report

    monkeypatch.setattr("finance_cli.commands.ingest.import_extracted_statement", _import_fail_on_replace)

    second_code, second_payload = _run_cli(
        ["ingest", "statement", "--dir", str(folder), "--commit", "--replace"],
        capsys,
    )
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["inserted"] == 0
    assert second_payload["data"]["errors"] == 1

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 2
    assert batch_count == 1


def test_savepoint_inner_commit_suppressed(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "statements"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-03",
                "description": "COFFEE SHOP",
                "amount_cents": -1000,
                "card_ending": "1234",
                "source": "Citi",
            }
        ],
        statement_total_cents=None,
        extracted_total_cents=-1000,
        reconciled=False,
        warnings=[],
    )

    class _FakeExtractor:
        name = "ai"

        def extract(self, _path: Path, _options):
            return ExtractorOutput(
                result=extracted,
                meta=ExtractorMeta(
                    backend="ai",
                    bank_parser_label="ai:gpt-test",
                    provider="openai",
                    model_version="gpt-test",
                    reconcile_status="no_totals",
                    input_tokens=0,
                    output_tokens=0,
                    elapsed_ms=0,
                ),
            )

    captured_auto_commit: list[bool] = []

    def _fake_import(*_args, **kwargs):
        captured_auto_commit.append(bool(kwargs.get("auto_commit", True)))
        return {"inserted": 0, "skipped_duplicates": 0, "warnings": []}

    monkeypatch.setattr("finance_cli.commands.ingest.get_extractor", lambda *_a, **_k: _FakeExtractor())
    monkeypatch.setattr("finance_cli.commands.ingest.import_extracted_statement", _fake_import)

    code, result = _run_cli(
        ["ingest", "statement", "--dir", str(folder), "--commit", "--backend", "ai"],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert captured_auto_commit == [False]


def test_ingest_statement_no_file_or_dir_raises(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)

    code, result = _run_cli(["ingest", "statement"], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "Exactly one of --file or --dir" in result["error"]


def test_ingest_statement_both_file_and_dir_raises(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    folder = tmp_path / "statements"
    folder.mkdir()

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path), "--dir", str(folder)], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "Exactly one of --file or --dir" in result["error"]


def test_ingest_statement_dir_not_found_raises(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)

    code, result = _run_cli(["ingest", "statement", "--dir", str(tmp_path / "missing")], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "Directory not found" in result["error"]


def test_ingest_statement_dir_no_pdfs_raises(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "empty"
    folder.mkdir()

    code, result = _run_cli(["ingest", "statement", "--dir", str(folder)], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "No PDF files found" in result["error"]


def test_ingest_statement_validation_failure(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    payload = _base_payload()
    payload["transactions"][0]["amount"] = "bad"
    ai_stub["response"] = payload

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "AI parse validation failed" in result["error"]


def test_ingest_statement_invalid_account_id_raises(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, result = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--account-id", "missing-account"],
        capsys,
    )
    assert code == 1
    assert result["status"] == "error"
    assert "Account 'missing-account' not found" in result["error"]
    assert ai_stub["calls"] == []


def test_ingest_statement_provider_override_resets_model(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: claude
          model: claude-config-model
        """,
    )

    code, result = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--provider", "openai"],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["provider"] == "openai"
    assert result["data"]["model"] == _default_model("openai")
    assert result["data"]["model"] != "claude-config-model"
    assert ai_stub["calls"][0]["model"] == _default_model("openai")


def test_ingest_statement_mixed_case_provider(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    code, result = _run_cli(
        ["ingest", "statement", "--file", str(pdf_path), "--provider", "OpenAI"],
        capsys,
    )
    assert code == 0
    assert result["status"] == "success"
    assert result["data"]["provider"] == "openai"
    assert ai_stub["calls"][0]["provider"] == "openai"


def test_ingest_statement_missing_provider_in_rules_errors(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          model: gpt-4o-mini
        """,
    )

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "AI provider is required" in result["error"]


def test_ingest_statement_malformed_config_raises(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: openai
          confidence_warn: nope
        """,
    )

    code, result = _run_cli(["ingest", "statement", "--file", str(pdf_path)], capsys)
    assert code == 1
    assert result["status"] == "error"
    assert "Invalid ai_parser.confidence_warn" in result["error"]


def test_ingest_csv_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "apple.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        02/17/2026,02/17/2026,ACH DEPOSIT,ACH DEPOSIT,Payment,Payment,-100.00,Henry
        """,
    )

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card"],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["raw_row_count"] == 2
    assert payload["data"]["skipped_row_count"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 0
    assert batch_count == 0


def test_ingest_csv_commit(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "apple.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        02/17/2026,02/17/2026,ACH DEPOSIT,ACH DEPOSIT,Payment,Payment,-100.00,Henry
        """,
    )

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card", "--commit"],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is False
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == payload["data"]["inserted"]
    assert batch_count == 1


def test_ingest_csv_already_imported(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "apple.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        02/17/2026,02/17/2026,ACH DEPOSIT,ACH DEPOSIT,Payment,Payment,-100.00,Henry
        """,
    )

    first_code, first_payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card", "--commit"],
        capsys,
    )
    assert first_code == 0
    assert first_payload["status"] == "success"

    second_code, second_payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card", "--commit"],
        capsys,
    )
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["inserted"] == 0
    assert second_payload["data"]["skipped_duplicates"] == first_payload["data"]["inserted"]

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == first_payload["data"]["inserted"]
    assert batch_count == 1


def test_ingest_csv_unsupported_institution(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "sample.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        """,
    )

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "wells_fargo"],
        capsys,
    )
    assert code == 1
    assert payload["status"] == "error"
    assert "unsupported institution" in payload["error"]
    assert "supported institutions" in payload["error"]


def test_ingest_csv_file_not_found(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(tmp_path / "missing.csv"), "--institution", "apple_card"],
        capsys,
    )
    assert code == 1
    assert payload["status"] == "error"
    assert "CSV not found" in payload["error"]


def test_ingest_csv_empty_csv(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "empty.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        """,
    )

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card", "--commit"],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["raw_row_count"] == 0
    assert payload["data"]["inserted"] == 0
    assert payload["data"]["errors"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == 0


def test_ingest_csv_warnings_reported(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = _write_csv(
        tmp_path,
        "warn.csv",
        """
        Transaction Date,Clearing Date,Description,Merchant,Category,Type,Amount (USD),Purchased By
        02/18/2026,02/18/2026,DELTA 1030,DELTA,Airlines,Purchase,88.40,Henry
        02/17/2026,02/17/2026,BAD ROW,BAD,Other,Purchase,,Henry
        """,
    )

    code, payload = _run_cli(
        ["ingest", "csv", "--file", str(csv_path), "--institution", "apple_card"],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["warnings"]
    assert payload["data"]["skipped_row_count"] == 1


def test_ingest_csv_cli_report_includes_warnings(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    csv_path = _write_bofa_checking_csv(tmp_path, "bofa.csv")

    code = main(
        [
            "ingest",
            "csv",
            "--file",
            str(csv_path),
            "--institution",
            "bofa_checking",
            "--format",
            "cli",
        ]
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "Warnings (1):" in output
    assert "No card ending available for BofA Checking" in output


def test_ingest_batch_dry_run(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    _write_apple_csv(folder, "b.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["inserted"] == 4
    assert payload["data"]["skipped_duplicates"] == 0
    assert payload["data"]["errors"] == 0
    assert payload["data"]["files"] == 2
    assert payload["data"]["total_input_tokens"] == 13
    assert payload["data"]["total_output_tokens"] == 7
    assert payload["data"]["total_elapsed_ms"] >= 0
    assert len(payload["data"]["reports"]) == 2

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 0
    assert batch_count == 0


def test_ingest_batch_commit(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    _write_apple_csv(folder, "b.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["dry_run"] is False
    assert payload["data"]["inserted"] == 4
    assert payload["data"]["skipped_duplicates"] == 0
    assert payload["data"]["errors"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 4
    assert batch_count == 2


def test_ingest_batch_failure_rolls_back_partial_writes(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a_good.pdf", b"a")
    _write_pdf(folder, "b_bad.pdf", b"b")

    from finance_cli.importers.pdf import import_extracted_statement as _real_import_extracted_statement

    def _import_with_failure(*args, **kwargs):
        file_path = Path(kwargs.get("file_path"))
        report = _real_import_extracted_statement(*args, **kwargs)
        if file_path.name == "b_bad.pdf":
            raise RuntimeError("forced failure after write")
        return report

    monkeypatch.setattr("finance_cli.commands.ingest.import_extracted_statement", _import_with_failure)

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 1
    assert payload["data"]["ingest_errors"] == 1

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 2
    assert batch_count == 1


def test_ingest_batch_rename_failure_reports_move_error_without_aborting(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")

    real_rename = Path.rename

    def _failing_rename(self: Path, target: Path):
        if self.name == "a.pdf":
            raise OSError("simulated rename failure")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", _failing_rename)

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["errors"] == 0
    assert payload["data"]["ingest_errors"] == 0
    assert payload["data"]["move_errors"] == 1
    assert len(payload["data"]["move_error_files"]) == 1
    assert "simulated rename failure" in payload["data"]["move_error_files"][0]["error"]
    assert (folder / "a.pdf").exists()


def test_ingest_batch_rename_failure_rerun_is_idempotent(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")

    real_rename = Path.rename
    state = {"failed_once": False}

    def _flaky_rename(self: Path, target: Path):
        if self.name == "a.pdf" and not state["failed_once"]:
            state["failed_once"] = True
            raise OSError("temporary rename failure")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", _flaky_rename)

    first_code, first_payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert first_code == 0
    assert first_payload["status"] == "success"
    assert first_payload["data"]["inserted"] == 2
    assert first_payload["data"]["move_errors"] == 1

    second_code, second_payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["inserted"] == 0
    assert second_payload["data"]["errors"] == 0
    assert second_payload["data"]["move_errors"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
    assert txn_count == 2


def test_ingest_batch_rename_collision_reports_error(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    processed_dir = folder / "processed"
    processed_dir.mkdir()
    _write_pdf(processed_dir, "a.pdf", b"existing")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["errors"] == 0
    assert payload["data"]["move_errors"] == 1
    assert "destination already exists" in payload["data"]["move_error_files"][0]["error"]
    assert (folder / "a.pdf").exists()


def test_ingest_batch_mixed_ingest_and_move_errors(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a_good.pdf", b"a")
    _write_pdf(folder, "b_bad.pdf", b"b")
    processed_dir = folder / "processed"
    processed_dir.mkdir()
    _write_pdf(processed_dir, "a_good.pdf", b"existing")

    def _extractor(pdf_path: Path) -> str:
        if pdf_path.name == "b_bad.pdf":
            raise ValueError("broken PDF")
        return "statement text"

    ai_stub["extractor"] = _extractor

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["errors"] == 1
    assert payload["data"]["ingest_errors"] == 1
    assert payload["data"]["move_errors"] == 1
    assert any("error" in report for report in payload["data"]["reports"])
    assert any("destination already exists" in item["error"] for item in payload["data"]["move_error_files"])


def test_ingest_batch_csv_only(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_apple_csv(folder, "only.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 0
    assert payload["data"]["files"] == 1
    assert payload["data"]["reports"][0]["institution"] == "apple_card"
    assert ai_stub["calls"] == []


def test_ingest_batch_detects_uppercase_csv_extension(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_apple_csv(folder, "ONLY.CSV")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 0
    assert payload["data"]["files"] == 1
    assert payload["data"]["reports"][0]["institution"] == "apple_card"
    assert ai_stub["calls"] == []


def test_ingest_batch_pdf_only(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    _write_pdf(folder, "b.pdf", b"b")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 4
    assert payload["data"]["errors"] == 0
    assert payload["data"]["files"] == 2
    assert payload["data"]["total_input_tokens"] == 26
    assert payload["data"]["total_output_tokens"] == 14
    assert payload["data"]["total_elapsed_ms"] >= 0
    assert all("provider" in report for report in payload["data"]["reports"])


def test_ingest_batch_unknown_csv_isolated(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "good.pdf", b"a")
    _write_csv(
        folder,
        "unknown.csv",
        """
        date,description,amount
        2026-02-18,Unknown Merchant,-10.00
        """,
    )

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 1
    assert len(payload["data"]["reports"]) == 2
    assert any("error" in report for report in payload["data"]["reports"])
    assert any("provider" in report for report in payload["data"]["reports"])


def test_ingest_batch_cli_report_includes_warnings(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_bofa_checking_csv(folder, "stmt.csv")

    code = main(
        [
            "ingest",
            "batch",
            "--dir",
            str(folder),
            "--format",
            "cli",
        ]
    )
    output = capsys.readouterr().out

    assert code == 0
    assert "stmt.csv: inserted=1" in output
    assert "WARNING: No card ending available for BofA Checking" in output


def test_ingest_batch_empty_dir(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "empty"
    folder.mkdir()

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert "No PDF or CSV files found" in payload["error"]


def test_ingest_batch_dir_not_found(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)

    code, payload = _run_cli(["ingest", "batch", "--dir", str(tmp_path / "missing")], capsys)
    assert code == 1
    assert payload["status"] == "error"
    assert "Directory not found" in payload["error"]


def test_ingest_batch_csv_only_bad_ai_config(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: openai
          confidence_warn: nope
        """,
    )
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_apple_csv(folder, "only.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 0
    assert ai_stub["calls"] == []


def test_ingest_batch_mixed_bad_ai_config(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        ai_parser:
          provider: openai
          confidence_warn: nope
        """,
    )
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_pdf(folder, "a.pdf", b"a")
    _write_apple_csv(folder, "b.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 1
    assert any("Invalid ai_parser.confidence_warn" in str(report.get("error", "")) for report in payload["data"]["reports"])
    assert any(report.get("institution") == "apple_card" for report in payload["data"]["reports"])


def test_ingest_batch_commit_idempotent_csv_only(tmp_path: Path, monkeypatch, capsys) -> None:
    db_path = _setup_db(tmp_path, monkeypatch)
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_apple_csv(folder, "only.csv")

    first_code, first_payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert first_code == 0
    assert first_payload["status"] == "success"
    assert first_payload["data"]["inserted"] == 2
    assert first_payload["data"]["skipped_duplicates"] == 0

    # Files moved to processed/ after first commit; copy back for idempotency test
    import shutil
    for f in (folder / "processed").iterdir():
        shutil.copy2(f, folder / f.name)

    second_code, second_payload = _run_cli(["ingest", "batch", "--dir", str(folder), "--commit"], capsys)
    assert second_code == 0
    assert second_payload["status"] == "success"
    assert second_payload["data"]["inserted"] == 0
    assert second_payload["data"]["skipped_duplicates"] == 2
    assert second_payload["data"]["errors"] == 0

    with connect(db_path) as conn:
        txn_count = conn.execute("SELECT COUNT(*) AS n FROM transactions").fetchone()["n"]
        batch_count = conn.execute("SELECT COUNT(*) AS n FROM import_batches").fetchone()["n"]
    assert txn_count == 2
    assert batch_count == 1


def test_ingest_batch_csv_only_bad_extractors_config(tmp_path: Path, monkeypatch, capsys, ai_stub) -> None:
    _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        extractors:
          default_backend: not-real
        ai_parser:
          provider: openai
        """,
    )
    folder = tmp_path / "inbox"
    folder.mkdir()
    _write_apple_csv(folder, "only.csv")

    code, payload = _run_cli(["ingest", "batch", "--dir", str(folder)], capsys)
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["inserted"] == 2
    assert payload["data"]["errors"] == 0
    assert ai_stub["calls"] == []


def test_ingest_statement_require_reconciled_rejects_no_totals_for_non_ai(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _setup_db(tmp_path, monkeypatch)
    pdf_path = _write_pdf(tmp_path)

    extracted = ExtractResult(
        transactions=[
            {
                "date": "2025-01-03",
                "description": "COFFEE SHOP",
                "amount_cents": -1000,
                "card_ending": "1234",
                "source": "Citi",
            }
        ],
        statement_total_cents=None,
        extracted_total_cents=-1000,
        reconciled=False,
        warnings=[],
    )

    class _FakeExtractor:
        name = "bsc"

        def extract(self, _path: Path, _options):
            return ExtractorOutput(
                result=extracted,
                meta=ExtractorMeta(
                    backend="bsc",
                    bank_parser_label="bsc:api",
                    provider="bsc",
                    model_version="api-v1",
                    reconcile_status="no_totals",
                ),
            )

    monkeypatch.setattr("finance_cli.commands.ingest.get_extractor", lambda *_a, **_k: _FakeExtractor())

    code, payload = _run_cli(
        [
            "ingest",
            "statement",
            "--file",
            str(pdf_path),
            "--backend",
            "bsc",
            "--institution",
            "Citi",
            "--require-reconciled",
        ],
        capsys,
    )
    assert code == 1
    assert payload["status"] == "error"
    assert "reconciliation status is 'no_totals'" in payload["error"]


def test_ingest_statement_backend_resolution_cli_overrides_rules(tmp_path: Path, monkeypatch, capsys) -> None:
    _setup_db(tmp_path, monkeypatch)
    _write_rules(
        tmp_path,
        """
        extractors:
          default_backend: bsc
        ai_parser:
          provider: openai
          model: gpt-from-rules
        """,
    )
    pdf_path = _write_pdf(tmp_path)

    code, payload = _run_cli(
        [
            "ingest",
            "statement",
            "--file",
            str(pdf_path),
            "--backend",
            "ai",
        ],
        capsys,
    )
    assert code == 0
    assert payload["status"] == "success"
    assert payload["data"]["backend"] == "ai"
    assert payload["data"]["provider"] == "openai"
