from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import Annotated

from fastapi import FastAPI, Query

from corpscout_crawl_service.models import BrregDomainDiscoveryRequest, DomainDiscoverRequest
from corpscout_crawl_service.service import CrawlService


def create_app(*, crawl_service: CrawlService | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service = crawl_service or CrawlService()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(title="Corpscout Crawl Service", version="0.1.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/domains/discover")
    async def discover_domains(request: DomainDiscoverRequest):
        return await service.discover_domains(request)

    @app.post("/v1/brreg/domain-discovery")
    async def discover_brreg_domain(
        request: BrregDomainDiscoveryRequest,
        prompt_version: Annotated[str | None, Query(min_length=1)] = None,
        search_provider: Annotated[str | None, Query(min_length=1)] = None,
    ):
        updates = {}
        if prompt_version is not None:
            updates["prompt_version"] = prompt_version
        if search_provider is not None:
            updates["search_provider"] = search_provider
        return await service.discover_brreg_domain(request.model_copy(update=updates))

    return app


app = create_app()
