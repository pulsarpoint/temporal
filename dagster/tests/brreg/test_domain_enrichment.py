from __future__ import annotations

import pytest

import corpscout_dagster.brreg.domain_enrichment as domain_enrichment
from corpscout_dagster.brreg.domain_enrichment import (
    DomainCandidate,
    _candidate_domains,
    _should_back_off_external_signal,
    discover_domain_candidates,
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


def test_candidate_domains_remove_legal_suffix_and_use_norway_tld() -> None:
    assert _candidate_domains("BORTIGARD AS", "NO") == ["bortigard.no", "bortigard.com"]


def test_external_signal_backoff_statuses_cover_rate_limits_and_remote_outages() -> None:
    assert _should_back_off_external_signal(403) is True
    assert _should_back_off_external_signal(429) is True
    assert _should_back_off_external_signal(502) is True
    assert _should_back_off_external_signal(404) is False


@pytest.mark.asyncio
async def test_discover_domain_candidates_uses_temporal_signals(monkeypatch) -> None:
    async def fake_duckduckgo(company_name: str, country: str):
        return [("bortigard.no", "duckduckgo", 70)]

    async def fake_wikidata(company_name: str):
        return [("bortigard.com", "wikidata", 85)]

    async def fake_certsh(company_name: str):
        return []

    async def fake_heuristic(company_name: str, country: str):
        return []

    monkeypatch.setattr(domain_enrichment, "_duckduckgo_signal", fake_duckduckgo)
    monkeypatch.setattr(domain_enrichment, "_wikidata_signal", fake_wikidata)
    monkeypatch.setattr(domain_enrichment, "_certsh_signal", fake_certsh)
    monkeypatch.setattr(domain_enrichment, "_heuristic_signal", fake_heuristic)

    candidates = await discover_domain_candidates(
        raw_payload={"organisasjonsnummer": "810202572"},
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        website=None,
        country="NO",
    )

    assert [(candidate.normalized_domain, candidate.signal, candidate.confidence) for candidate in candidates] == [
        ("bortigard.no", "duckduckgo", 70),
        ("bortigard.com", "wikidata", 85),
    ]


@pytest.mark.asyncio
async def test_discover_domain_candidates_deduplicates_website_before_external_signals(monkeypatch) -> None:
    async def unexpected_signal(*args):
        raise AssertionError("external signals should not run when BRREG already has a website")

    monkeypatch.setattr(domain_enrichment, "_duckduckgo_signal", unexpected_signal)
    monkeypatch.setattr(domain_enrichment, "_wikidata_signal", unexpected_signal)
    monkeypatch.setattr(domain_enrichment, "_certsh_signal", unexpected_signal)
    monkeypatch.setattr(domain_enrichment, "_heuristic_signal", unexpected_signal)

    candidates = await discover_domain_candidates(
        raw_payload={"organisasjonsnummer": "810202572", "hjemmeside": "https://www.bortigard.no"},
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        website=None,
        country="NO",
    )

    assert len(candidates) == 1
    assert candidates[0].normalized_domain == "bortigard.no"
    assert candidates[0].signal == "website_field"
