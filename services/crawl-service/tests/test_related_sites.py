from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from corpscout_crawl_service.crawl4ai_service import Crawl4AiRequest, Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService


@pytest.mark.asyncio
async def test_company_website_becomes_owned_domain_and_related_site() -> None:
    fake_crawler = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=SERVI%20GROUP%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=SERVI%20GROUP%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=SERVI%20GROUP%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://servi.no/"],
                duration_ms=7,
            ),
            "https://servi.no/": Crawl4AiResponse(
                url="https://servi.no/",
                final_url="https://servi.no/",
                status="succeeded",
                markdown="# Servi Group AS\nServi is Norway's largest hydraulics company.",
                markdown_hash="site-hash",
                links=[],
                duration_ms=11,
            ),
        }
    )
    analyzer = FakeSiteAnalyzer(
        {
            "https://servi.no/": {
                "decision": "accepted",
                "score": 92,
                "site_type": "company_website",
                "relationship": "primary_web_presence",
                "owned_domain": True,
                "reason": "The site names Servi Group AS.",
                "evidence": ["Servi Group AS"],
            }
        }
    )
    service = CrawlService(crawl4ai_service=fake_crawler, site_analyzer=analyzer)

    response = await service.discover_domains({"company_name": "SERVI GROUP AS", "country": "NO"})

    assert response.status == "succeeded"
    assert response.best_domain == "servi.no"
    assert response.domains[0].normalized_domain == "servi.no"
    assert response.related_sites[0].normalized_domain == "servi.no"
    assert response.related_sites[0].site_type == "company_website"
    assert response.related_sites[0].relationship == "primary_web_presence"
    assert response.related_sites[0].owned_domain is True
    assert response.primary_web_presence.normalized_domain == "servi.no"
    assert [request.llm_enabled for request in fake_crawler.requests] == [False, False]


@pytest.mark.asyncio
async def test_directory_profile_is_related_site_but_not_owned_domain() -> None:
    fake_crawler = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://www.dnb.com/business-directory/company-profiles.bortigard_as.html"],
                duration_ms=7,
            ),
            "https://www.dnb.com/business-directory/company-profiles.bortigard_as.html": Crawl4AiResponse(
                url="https://www.dnb.com/business-directory/company-profiles.bortigard_as.html",
                final_url="https://www.dnb.com/business-directory/company-profiles.bortigard_as.html",
                status="succeeded",
                markdown="# BORTIGARD AS\nBusiness directory profile for BORTIGARD AS.",
                markdown_hash="site-hash",
                links=[],
                duration_ms=11,
            ),
        }
    )
    analyzer = FakeSiteAnalyzer(
        {
            "https://www.dnb.com/business-directory/company-profiles.bortigard_as.html": {
                "decision": "accepted",
                "score": 88,
                "site_type": "directory_profile",
                "relationship": "evidence_profile",
                "owned_domain": True,
                "reason": "The page is about BORTIGARD AS, but DNB is a directory.",
                "evidence": ["BORTIGARD AS"],
            }
        }
    )
    service = CrawlService(crawl4ai_service=fake_crawler, site_analyzer=analyzer)

    response = await service.discover_domains({"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status == "succeeded"
    assert response.best_domain is None
    assert response.domains == []
    assert response.primary_web_presence is None
    assert response.related_sites[0].normalized_domain == "dnb.com"
    assert response.related_sites[0].site_type == "directory_profile"
    assert response.related_sites[0].relationship == "evidence_profile"
    assert response.related_sites[0].owned_domain is False


@pytest.mark.asyncio
async def test_social_profile_can_be_primary_web_presence_but_not_owned_domain() -> None:
    fake_crawler = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=ALSTRA%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=ALSTRA%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=ALSTRA%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://www.facebook.com/alstra"],
                duration_ms=7,
            ),
            "https://www.facebook.com/alstra": Crawl4AiResponse(
                url="https://www.facebook.com/alstra",
                final_url="https://www.facebook.com/alstra",
                status="succeeded",
                markdown="# ALSTRA AS Facebook page",
                markdown_hash="site-hash",
                links=[],
                duration_ms=11,
            ),
        }
    )
    analyzer = FakeSiteAnalyzer(
        {
            "https://www.facebook.com/alstra": {
                "decision": "accepted",
                "score": 86,
                "site_type": "social_profile",
                "relationship": "primary_web_presence",
                "owned_domain": True,
                "reason": "The Facebook page represents ALSTRA AS.",
                "evidence": ["ALSTRA AS"],
            }
        }
    )
    service = CrawlService(crawl4ai_service=fake_crawler, site_analyzer=analyzer)

    response = await service.discover_domains({"company_name": "ALSTRA AS", "country": "NO"})

    assert response.status == "succeeded"
    assert response.best_domain is None
    assert response.domains == []
    assert response.primary_web_presence.normalized_domain == "facebook.com"
    assert response.primary_web_presence.site_type == "social_profile"
    assert response.primary_web_presence.owned_domain is False
    assert response.related_sites[0].relationship == "primary_web_presence"


@pytest.mark.asyncio
async def test_brreg_business_context_is_passed_to_site_analyzer() -> None:
    fake_crawler = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://alstrays.com/testimonials/"],
                llm_output={
                    "candidates": [
                        {"url": "https://alstrays.com/testimonials/", "domain": "alstrays.com", "score": 90}
                    ]
                },
                duration_ms=7,
            ),
            "https://alstrays.com/testimonials/": Crawl4AiResponse(
                url="https://alstrays.com/testimonials/",
                final_url="https://alstrays.com/testimonials/",
                status="succeeded",
                markdown="# ALStrays Animal Welfare & Pet Ownership",
                markdown_hash="site-hash",
                links=[],
                duration_ms=11,
            ),
        }
    )
    analyzer = FakeSiteAnalyzer(
        {
            "https://alstrays.com/testimonials/": {
                "decision": "rejected",
                "score": 10,
                "site_type": "unrelated",
                "relationship": "unrelated",
                "owned_domain": False,
                "reason": "Animal welfare site conflicts with registered holding company context.",
                "evidence": [],
            }
        }
    )
    service = CrawlService(crawl4ai_service=fake_crawler, site_analyzer=analyzer)

    await service.discover_brreg_domain(
        {
            "record_id": "record-1",
            "organization_number": "810094532",
            "organization_name": "ALSTRAY AS",
            "raw_payload": {
                "navn": "ALSTRAY AS",
                "organisasjonsnummer": "810094532",
                "aktivitet": ["Holdingselskap."],
                "vedtektsfestetFormaal": [
                    "Drive virksomhet innenfor investeringer i og forvaltning av eiendom."
                ],
                "naeringskode1": {
                    "kode": "68.200",
                    "beskrivelse": "Utleie av egen eller leid fast eiendom",
                },
                "forretningsadresse": {
                    "adresse": ["Stoaveien 11"],
                    "postnummer": "4848",
                    "poststed": "ARENDAL",
                    "kommune": "ARENDAL",
                },
            },
            "country": "NO",
        }
    )

    payload = analyzer.requests[0]
    assert payload["address_lines"] == ["Stoaveien 11"]
    assert payload["city"] == "ARENDAL"
    assert payload["postal_code"] == "4848"
    assert payload["business_activity"] == ["Holdingselskap."]
    assert payload["statutory_purpose"] == [
        "Drive virksomhet innenfor investeringer i og forvaltning av eiendom."
    ]
    assert payload["industry_codes"] == ["68.200 Utleie av egen eller leid fast eiendom"]


@pytest.mark.asyncio
async def test_conflicting_activity_without_identity_signal_is_not_owned_domain() -> None:
    fake_crawler = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=ALSTRAY%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://alstrays.com/testimonials/"],
                llm_output={
                    "candidates": [
                        {"url": "https://alstrays.com/testimonials/", "domain": "alstrays.com", "score": 90}
                    ]
                },
                duration_ms=7,
            ),
            "https://alstrays.com/testimonials/": Crawl4AiResponse(
                url="https://alstrays.com/testimonials/",
                final_url="https://alstrays.com/testimonials/",
                status="succeeded",
                markdown="# ALStrays Animal Welfare & Pet Ownership",
                markdown_hash="site-hash",
                links=[],
                duration_ms=11,
            ),
        }
    )
    analyzer = FakeSiteAnalyzer(
        {
            "https://alstrays.com/testimonials/": {
                "decision": "accepted",
                "score": 90,
                "site_type": "company_website",
                "relationship": "primary_web_presence",
                "owned_domain": True,
                "activity_alignment": "conflicting",
                "identity_signals": {
                    "legal_name": False,
                    "organization_number": False,
                    "address_or_city": False,
                    "activity_or_industry": False,
                    "brand_or_domain": True,
                },
                "reason": "Domain name looks similar.",
                "evidence": ["ALStrays"],
            }
        }
    )
    service = CrawlService(crawl4ai_service=fake_crawler, site_analyzer=analyzer)

    response = await service.discover_domains(
        {
            "company_name": "ALSTRAY AS",
            "organization_number": "810094532",
            "country": "NO",
            "city": "ARENDAL",
            "address_lines": ["Stoaveien 11"],
            "business_activity": ["Holdingselskap."],
            "statutory_purpose": ["Investeringer i og forvaltning av eiendom."],
            "industry_codes": ["68.200 Utleie av egen eller leid fast eiendom"],
        }
    )

    assert response.status == "not_found"
    assert response.domains == []
    assert response.related_sites == []
    assert response.site_checks[0].owned_domain is False
    assert response.site_checks[0].relationship == "unrelated"
    assert response.site_checks[0].score < 70


@dataclass
class FakeSiteAnalyzer:
    outputs: dict[str, dict[str, Any]]
    requests: list[dict[str, Any]] = field(default_factory=list)

    async def analyze_site(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.requests.append(payload)
        return self.outputs[payload["url"]]


class FakeCrawl4AiService:
    def __init__(self, responses: dict[str, Crawl4AiResponse]) -> None:
        self._responses = responses
        self.requests: list[Crawl4AiRequest] = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def crawl(self, request: Crawl4AiRequest) -> Crawl4AiResponse:
        self.requests.append(request)
        if request.url not in self._responses:
            return Crawl4AiResponse(
                url=request.url,
                final_url=request.url,
                status="failed",
                error={"code": "not_found", "message": "No fake response configured."},
                duration_ms=0,
            )
        return self._responses[request.url]
