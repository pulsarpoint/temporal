from __future__ import annotations

from dataclasses import dataclass

import pytest

from corpscout_dagster.brreg.domain_search_llm import (
    DomainSearchCompanyFacts,
    SearchResult,
    TriageDecision,
    VerificationDecision,
    build_domain_search_queries,
    domain_crawler_browser_config_from_env,
    discover_web_search_llm_domain_candidates,
    parse_domain_verification_response,
    parse_search_triage_response,
)


@dataclass
class FakeCrawlResult:
    links: dict
    markdown: str = ""


class FakeCrawler:
    def __init__(self) -> None:
        self.urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def arun(self, *, url: str):
        self.urls.append(url)
        if "duckduckgo.com" in url:
            return FakeCrawlResult(
                links={
                    "external": [
                        {
                            "href": "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.bortigard.no%2F",
                            "text": "Bortigard AS - official website",
                            "title": "Bortigard AS",
                            "description": "Norwegian property company in Holmestrand.",
                        },
                        {
                            "href": "https://wrong.example/no",
                            "text": "Wrong result",
                            "title": "Wrong Company",
                            "description": "Unrelated company.",
                        },
                    ]
                }
            )
        return FakeCrawlResult(
            links={},
            markdown="BORTIGARD AS\nLøkkeveien 18\nOrganization number 810202572\n",
        )


class FakeDomainSearchLLM:
    def __init__(self) -> None:
        self.triage_calls: list[list[SearchResult]] = []
        self.verification_calls: list[tuple[SearchResult, str]] = []

    def triage_search_results(
        self,
        *,
        company: DomainSearchCompanyFacts,
        results: list[SearchResult],
        prompt_version: str,
    ) -> list[TriageDecision]:
        self.triage_calls.append(results)
        return [
            TriageDecision(
                normalized_domain="bortigard.no",
                confidence=61,
                reason="Name and Norway description match.",
            ),
            TriageDecision(
                normalized_domain="wrong.example",
                confidence=35,
                reason="Different company.",
            ),
        ]

    def verify_candidate(
        self,
        *,
        company: DomainSearchCompanyFacts,
        result: SearchResult,
        markdown: str,
        prompt_version: str,
    ) -> VerificationDecision:
        self.verification_calls.append((result, markdown))
        return VerificationDecision(
            normalized_domain=result.normalized_domain,
            confidence=84,
            reason="Homepage includes the exact legal name, address, and organization number.",
            matched_evidence=["BORTIGARD AS", "Løkkeveien 18", "810202572"],
            reject_reason="",
        )


@pytest.mark.asyncio
async def test_web_search_llm_triages_first_page_then_verifies_selected_domain() -> None:
    crawler = FakeCrawler()
    llm = FakeDomainSearchLLM()

    candidates = await discover_web_search_llm_domain_candidates(
        raw_payload={
            "organisasjonsnummer": "810202572",
            "forretningsadresse": {
                "adresse": ["Løkkeveien 18"],
                "poststed": "HOLMESTRAND",
                "postnummer": "3085",
            },
        },
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        country="NO",
        classifier=llm,
        crawler_factory=lambda: crawler,
        triage_threshold=50,
        verification_threshold=60,
        max_verified_candidates=3,
        prompt_version="v1",
    )

    assert len(candidates) == 1
    assert candidates[0].normalized_domain == "bortigard.no"
    assert candidates[0].signal == "web_search_llm"
    assert candidates[0].confidence == 84
    assert candidates[0].evidence["triage"]["confidence"] == 61
    assert candidates[0].evidence["verification"]["matched_evidence"] == [
        "BORTIGARD AS",
        "Løkkeveien 18",
        "810202572",
    ]
    assert candidates[0].metadata["prompt_version"] == "v1"
    assert len(llm.triage_calls) == 1
    assert len(llm.verification_calls) == 1
    assert llm.verification_calls[0][0].normalized_domain == "bortigard.no"
    assert "Organization number 810202572" in llm.verification_calls[0][1]
    assert len(crawler.urls) == 2
    assert "html.duckduckgo.com/html/" in crawler.urls[0]
    assert crawler.urls[1] == "https://www.bortigard.no/"


def test_build_domain_search_queries_uses_org_number_and_address_fallbacks() -> None:
    queries = build_domain_search_queries(
        DomainSearchCompanyFacts(
            organization_number="810202572",
            organization_name="BORTIGARD AS",
            country="NO",
            address="Løkkeveien 18 HOLMESTRAND 3085",
        )
    )

    assert queries == [
        '"BORTIGARD AS" Norway official website',
        '"BORTIGARD AS" "810202572"',
        '"BORTIGARD AS" "Løkkeveien 18 HOLMESTRAND 3085"',
    ]


def test_domain_crawler_browser_config_defaults_to_headless_full_chromium(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DOMAIN_CRAWLER_HEADLESS", raising=False)
    monkeypatch.delenv("DOMAIN_CRAWLER_BROWSER_TYPE", raising=False)
    monkeypatch.delenv("DOMAIN_CRAWLER_CHROME_CHANNEL", raising=False)

    config = domain_crawler_browser_config_from_env()

    assert config.browser_type == "chromium"
    assert config.chrome_channel == "chromium"
    assert config.channel == "chromium"
    assert config.headless is True
    assert config.ignore_https_errors is True
    assert config.light_mode is True


def test_domain_crawler_browser_config_can_simulate_visible_chrome(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOMAIN_CRAWLER_HEADLESS", "false")
    monkeypatch.setenv("DOMAIN_CRAWLER_BROWSER_TYPE", "chromium")
    monkeypatch.setenv("DOMAIN_CRAWLER_CHROME_CHANNEL", "chrome")

    config = domain_crawler_browser_config_from_env()

    assert config.browser_type == "chromium"
    assert config.chrome_channel == "chrome"
    assert config.channel == "chrome"
    assert config.headless is False


def test_parse_search_triage_response_filters_unknown_domains_and_bad_confidence() -> None:
    decisions = parse_search_triage_response(
        """
        ```json
        {
          "candidates": [
            {"normalized_domain": "bortigard.no", "confidence": 72, "reason": "name match"},
            {"domain": "unknown.no", "confidence": 90, "reason": "not in results"},
            {"domain": "wrong.no", "confidence": "bad", "reason": "invalid confidence"}
          ]
        }
        ```
        """,
        allowed_domains={"bortigard.no", "wrong.no"},
    )

    assert decisions == [
        TriageDecision(normalized_domain="bortigard.no", confidence=72, reason="name match")
    ]


def test_parse_domain_verification_response_filters_mismatched_domain() -> None:
    decision = parse_domain_verification_response(
        """
        {"normalized_domain": "bortigard.no", "confidence": 82, "reason": "exact match",
         "matched_evidence": ["org number"], "reject_reason": ""}
        """,
        expected_domain="bortigard.no",
    )

    assert decision == VerificationDecision(
        normalized_domain="bortigard.no",
        confidence=82,
        reason="exact match",
        matched_evidence=["org number"],
        reject_reason="",
    )

    assert parse_domain_verification_response(
        '{"normalized_domain":"wrong.no","confidence":90}',
        expected_domain="bortigard.no",
    ) is None
