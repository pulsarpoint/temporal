from __future__ import annotations

from corpscout_dagster.brreg.domain_enrichment import (
    DomainCandidate,
    extract_domain_candidates,
    normalize_domain,
)


def test_normalize_domain_handles_urls_and_www_prefixes() -> None:
    assert normalize_domain("https://www.example.no/path?q=1") == "example.no"
    assert normalize_domain("WWW.BORTIGARD.NO") == "bortigard.no"
    assert normalize_domain("not a domain") is None


def test_extract_domain_candidates_uses_website_field() -> None:
    candidates = extract_domain_candidates(
        raw_payload={"hjemmeside": "https://www.bortigard.no/om-oss"},
        website=None,
    )

    assert candidates == [
        DomainCandidate(
            domain="www.bortigard.no",
            normalized_domain="bortigard.no",
            signal="website_field",
            confidence=95,
            evidence={"website": "https://www.bortigard.no/om-oss"},
            metadata={"source_field": "hjemmeside"},
        )
    ]


def test_extract_domain_candidates_prefers_explicit_website_column() -> None:
    candidates = extract_domain_candidates(
        raw_payload={"hjemmeside": "https://payload.example.no"},
        website="https://column.example.no",
    )

    assert candidates[0].normalized_domain == "column.example.no"
    assert candidates[0].evidence == {"website": "https://column.example.no"}
