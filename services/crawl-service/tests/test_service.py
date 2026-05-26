from __future__ import annotations

import pytest

from corpscout_crawl_service.crawl4ai_service import Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService

from tests.fakes import FakeCrawl4AiService


@pytest.mark.asyncio
async def test_service_closes_internal_crawl4ai_service() -> None:
    class ClosableFake(FakeCrawl4AiService):
        closed = False

        async def close(self) -> None:
            self.closed = True

    fake = ClosableFake({})
    service = CrawlService(crawl4ai_service=fake)

    await service.close()

    assert fake.closed is True


@pytest.mark.asyncio
async def test_brreg_endpoint_wrapper_uses_generic_domain_discovery() -> None:
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
    service = CrawlService(crawl4ai_service=fake)

    response = await service.discover_brreg_domain(
        {
            "record_id": "record-1",
            "organization_number": "810202572",
            "organization_name": "BORTIGARD AS",
            "raw_payload": {"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
            "country": "NO",
        }
    )

    assert response.status == "succeeded"
    assert response.best_domain == "bortigard.no"
    assert response.search is not None
    assert response.search.markdown_hash == "search-hash"
