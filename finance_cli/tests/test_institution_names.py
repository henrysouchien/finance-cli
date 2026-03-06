from __future__ import annotations

from finance_cli.institution_names import CANONICAL_NAMES, canonicalize, is_known, normalize_key, similar_names


def test_canonicalize_known_variants() -> None:
    assert canonicalize("BofA Checking") == "Bank of America"
    assert canonicalize("Barclays - Cards") == "Barclays"
    assert canonicalize("Goldman Sachs Bank USA") == "Apple Card"
    assert canonicalize("Apple Card (Goldman Sachs Bank USA)") == "Apple Card"
    assert canonicalize("Amex") == "American Express"
    assert canonicalize("Bloomingdale's") == "Bloomingdale's"
    assert canonicalize("Merrill") == "Merrill"


def test_normalize_key_strips_punctuation_and_whitespace() -> None:
    assert normalize_key("  Barclays - Cards  ") == "barclays cards"
    assert normalize_key("Bloomingdale's") == "bloomingdale s"


def test_unknown_canonicalize_passthrough() -> None:
    assert canonicalize("Unknown Credit Union") == "Unknown Credit Union"


def test_is_known_true_and_false() -> None:
    assert is_known("BofA Checking") is True
    assert is_known("Apple Card (Goldman Sachs Bank USA)") is True
    assert is_known("Unknown Credit Union") is False


def test_similar_names_heuristics() -> None:
    assert similar_names("Goldman Sachs", "Goldman Sachs Bank USA") is True
    assert similar_names("First National Credit Union", "National Credit") is True
    assert similar_names("Merrill", "Bank of America") is False


def test_canonical_names_keys_are_prenormalized() -> None:
    bad_keys = []
    for key in CANONICAL_NAMES:
        expected = normalize_key(key)
        if key != expected:
            bad_keys.append((key, expected))

    assert not bad_keys, f"Keys not pre-normalized: {bad_keys}"
