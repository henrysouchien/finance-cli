"""AI extractor adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..ai_statement_parser import (
    DEFAULT_MAX_TEXT_CHARS,
    DEFAULT_MAX_TOKENS,
    _default_model,
    ai_parse_statement,
    ai_result_to_extract_result,
)
from . import ExtractOptions, ExtractorMeta, ExtractorOutput

_DEFAULT_CONFIDENCE_WARN = 0.80
_DEFAULT_CONFIDENCE_BLOCK = 0.60


class AIExtractor:
    name = "ai"

    def __init__(self, config: dict[str, Any]) -> None:
        cfg = config if isinstance(config, dict) else {}

        provider = str(cfg.get("provider") or "").strip().lower()
        if not provider:
            raise ValueError("AI provider is required; set --provider or ai_parser.provider in rules.yaml")

        model_raw = str(cfg.get("model") or "").strip()
        model = model_raw or _default_model(provider)

        max_text_chars = _coerce_positive_int(cfg.get("max_text_chars"), "max_text_chars", DEFAULT_MAX_TEXT_CHARS)
        max_tokens = _coerce_positive_int(cfg.get("max_tokens"), "max_tokens", DEFAULT_MAX_TOKENS)
        confidence_warn = _coerce_unit_float(
            cfg.get("confidence_warn"),
            "confidence_warn",
            _DEFAULT_CONFIDENCE_WARN,
        )
        confidence_block = _coerce_unit_float(
            cfg.get("confidence_block"),
            "confidence_block",
            _DEFAULT_CONFIDENCE_BLOCK,
        )
        if confidence_warn < confidence_block:
            raise ValueError(
                "Invalid ai_parser confidence thresholds: confidence_warn must be >= confidence_block"
            )

        self.provider = provider
        self.model = model
        self.max_text_chars = max_text_chars
        self.max_tokens = max_tokens
        self.confidence_warn = confidence_warn
        self.confidence_block = confidence_block

    def extract(self, pdf_path: Path, options: ExtractOptions) -> ExtractorOutput:
        result = ai_parse_statement(
            pdf_path,
            provider=self.provider,
            model=self.model,
            max_text_chars=self.max_text_chars,
            max_tokens=self.max_tokens,
            confidence_warn=self.confidence_warn,
            confidence_block=self.confidence_block,
        )

        if not result.validation.passed:
            raise ValueError(_validation_error_message(result.validation))

        extracted = ai_result_to_extract_result(
            result,
            allow_partial=options.allow_partial,
            require_reconciled=options.require_reconciled,
        )

        reconcile_status = _reconcile_status_from_extract(extracted)
        meta = ExtractorMeta(
            backend="ai",
            bank_parser_label=f"ai:{result.model}",
            provider=result.provider,
            model_version=result.model,
            reconcile_status=reconcile_status,
            content_text=result.extracted_text,
            raw_api_response=result.raw_json,
            validation_summary=result.validation.as_dict(),
            ai_prompt_version=result.prompt_version,
            ai_prompt_hash=result.prompt_hash,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            elapsed_ms=result.elapsed_ms,
        )
        return ExtractorOutput(result=extracted, meta=meta)


def _coerce_positive_int(raw: Any, key: str, default: int) -> int:
    value = default if raw is None else raw
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ai_parser.{key}: expected integer > 0, got {value!r}") from exc
    if out <= 0:
        raise ValueError(f"Invalid ai_parser.{key}: expected integer > 0, got {value!r}")
    return out


def _coerce_unit_float(raw: Any, key: str, default: float) -> float:
    value = default if raw is None else raw
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ai_parser.{key}: expected number in [0, 1], got {value!r}") from exc
    if out < 0 or out > 1:
        raise ValueError(f"Invalid ai_parser.{key}: expected number in [0, 1], got {value!r}")
    return out


def _validation_error_message(validation) -> str:
    fragments: list[str] = []
    for item in validation.errors[:8]:
        prefix = f"[{item.gate}]"
        if item.row_index is not None:
            prefix += f" row={item.row_index}"
        if item.field:
            prefix += f" field={item.field}"
        fragments.append(f"{prefix} {item.message}")
    more_count = max(len(validation.errors) - 8, 0)
    suffix = f" (+{more_count} more)" if more_count else ""
    return "AI parse validation failed: " + "; ".join(fragments) + suffix


def _reconcile_status_from_extract(extracted) -> str:
    if extracted.total_charges_cents is not None or extracted.total_payments_cents is not None:
        return "matched" if extracted.reconciled else "mismatch"
    return "no_totals"


__all__ = ["AIExtractor"]
