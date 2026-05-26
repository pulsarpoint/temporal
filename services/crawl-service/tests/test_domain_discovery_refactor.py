from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from corpscout_crawl_service.api import create_app
from corpscout_crawl_service.crawl4ai_service import Crawl4AiRequest, Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService


@pytest.mark.asyncio
async def test_domain_discovery_keeps_search_response_and_rejects_low_search_scores() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://maybe.example/"],
                llm_output={
                    "candidates": [
                        {
                            "url": "https://maybe.example/",
                            "domain": "maybe.example",
                            "score": 49,
                            "reason": "Weak match.",
                        }
                    ]
                },
                duration_ms=7,
            )
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains({"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status == "not_found"
    assert response.search.markdown_hash == "search-hash"
    assert response.links[0].score == 49
    assert response.site_checks == []
    assert [request.purpose for request in fake.requests] == ["domain_search"]


@pytest.mark.asyncio
async def test_domain_discovery_crawls_candidate_and_accepts_high_final_score() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://www.bortigard.no/"],
                llm_output={
                    "candidates": [
                        {
                            "url": "https://www.bortigard.no/",
                            "domain": "bortigard.no",
                            "score": 84,
                            "reason": "Search result matches company name.",
                            "evidence": ["BORTIGARD AS"],
                        }
                    ]
                },
                duration_ms=7,
            ),
            "https://www.bortigard.no/": Crawl4AiResponse(
                url="https://www.bortigard.no/",
                final_url="https://www.bortigard.no/",
                status="succeeded",
                markdown="# Bortigard AS",
                markdown_hash="site-hash",
                links=[],
                llm_output={
                    "decision": "accepted",
                    "score": 91,
                    "reason": "Company name and address match.",
                    "evidence": ["BORTIGARD AS", "Løkkeveien 18"],
                },
                duration_ms=11,
            ),
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains({"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status == "succeeded"
    assert response.best_domain == "bortigard.no"
    assert response.domains[0].domain == "bortigard.no"
    assert response.domains[0].score == 91
    assert response.site_checks[0].crawl.markdown_hash == "site-hash"
    assert [request.purpose for request in fake.requests] == ["domain_search", "domain_site_check"]


@pytest.mark.asyncio
async def test_domain_discovery_rejects_site_scores_below_domain_threshold() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://www.bortigard.no/"],
                llm_output={
                    "candidates": [
                        {
                            "url": "https://www.bortigard.no/",
                            "domain": "bortigard.no",
                            "score": 84,
                            "reason": "Search result matches company name.",
                        }
                    ]
                },
                duration_ms=7,
            ),
            "https://www.bortigard.no/": Crawl4AiResponse(
                url="https://www.bortigard.no/",
                final_url="https://www.bortigard.no/",
                status="succeeded",
                markdown="# Bortigard AS",
                markdown_hash="site-hash",
                links=[],
                llm_output={
                    "decision": "uncertain",
                    "score": 69,
                    "reason": "Only weak evidence is present.",
                },
                duration_ms=11,
            ),
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains({"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status == "not_found"
    assert response.domains == []
    assert response.site_checks[0].score == 69


@pytest.mark.asyncio
async def test_domain_discovery_falls_back_to_search_page_links_when_llm_returns_no_candidates() -> None:
    duckduckgo_redirect = (
        "https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.bortigard.no%2F&rut=abc123"
    )
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=[duckduckgo_redirect],
                llm_output=[],
                duration_ms=7,
            ),
            "https://www.bortigard.no/": Crawl4AiResponse(
                url="https://www.bortigard.no/",
                final_url="https://www.bortigard.no/",
                status="succeeded",
                markdown="# Bortigard AS",
                markdown_hash="site-hash",
                links=[],
                llm_output={"decision": "accepted", "score": 91, "reason": "Company name matches."},
                duration_ms=11,
            ),
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains({"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status == "succeeded"
    assert response.best_domain == "bortigard.no"
    assert response.links[0].url == "https://www.bortigard.no/"
    assert response.links[0].metadata["fallback"] == "search_response_links"
    assert response.warnings[0].code == "search_candidate_llm_empty"
    assert [request.url for request in fake.requests] == [
        "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
        "https://www.bortigard.no/",
    ]


@pytest.mark.asyncio
async def test_domain_discovery_uses_existing_website_without_searching() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://www.bortigard.no/": Crawl4AiResponse(
                url="https://www.bortigard.no/",
                final_url="https://www.bortigard.no/",
                status="succeeded",
                markdown="# Bortigard AS",
                markdown_hash="site-hash",
                links=[],
                llm_output={
                    "decision": "accepted",
                    "score": 95,
                    "reason": "Existing website matches company.",
                    "evidence": ["BORTIGARD AS"],
                },
                duration_ms=8,
            )
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains(
        {
            "company_name": "BORTIGARD AS",
            "country": "NO",
            "existing_website": "https://www.bortigard.no/",
        }
    )

    assert response.status == "succeeded"
    assert response.search is None
    assert response.best_domain == "bortigard.no"
    assert [request.url for request in fake.requests] == ["https://www.bortigard.no/"]


@pytest.mark.asyncio
async def test_domain_discovery_ignores_invalid_existing_website() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=[],
                llm_output={"candidates": []},
                duration_ms=7,
            )
        }
    )
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_domains(
        {
            "company_name": "BORTIGARD AS",
            "country": "NO",
            "existing_website": "not a url",
        }
    )

    assert response.status == "not_found"
    assert [request.url for request in fake.requests] == [
        "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website"
    ]


def test_domains_discover_endpoint_returns_search_links_and_site_checks() -> None:
    fake = FakeCrawl4AiService(
        {
            "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website": Crawl4AiResponse(
                url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                final_url="https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
                status="succeeded",
                markdown="# Search results",
                markdown_hash="search-hash",
                links=["https://www.bortigard.no/"],
                llm_output={
                    "candidates": [
                        {"url": "https://www.bortigard.no/", "domain": "bortigard.no", "score": 88}
                    ]
                },
                duration_ms=7,
            ),
            "https://www.bortigard.no/": Crawl4AiResponse(
                url="https://www.bortigard.no/",
                final_url="https://www.bortigard.no/",
                status="succeeded",
                markdown="# Bortigard AS",
                markdown_hash="site-hash",
                links=[],
                llm_output={"decision": "accepted", "score": 91, "reason": "Match."},
                duration_ms=11,
            ),
        }
    )
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=fake)))

    response = client.post("/v1/domains/discover", json={"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["search"]["markdown_hash"] == "search-hash"
    assert body["links"][0]["normalized_domain"] == "bortigard.no"
    assert body["site_checks"][0]["crawl"]["markdown_hash"] == "site-hash"
    assert body["domains"][0]["domain"] == "bortigard.no"


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
