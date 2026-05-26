from __future__ import annotations

from dataclasses import dataclass, field

from corpscout_crawl_service.models import Crawl4AiRequest, Crawl4AiResponse


@dataclass
class FakeCrawl4AiService:
    responses: dict[str, Crawl4AiResponse]
    requests: list[Crawl4AiRequest] = field(default_factory=list)

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def crawl(self, request: Crawl4AiRequest) -> Crawl4AiResponse:
        self.requests.append(request)
        if request.url not in self.responses:
            return Crawl4AiResponse(
                url=request.url,
                final_url=request.url,
                status="failed",
                error={"code": "not_found", "message": "No fake response configured."},
                duration_ms=0,
            )
        return self.responses[request.url]
