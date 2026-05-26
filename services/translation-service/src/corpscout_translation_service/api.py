from __future__ import annotations

import logging
from typing import Annotated

from fastapi import FastAPI, Query

from corpscout_translation_service.models import BrregTranslateRequest, LLMSelection
from corpscout_translation_service.service import TranslationService


def create_app(*, translation_service: TranslationService | None = None) -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    app = FastAPI(title="Corpscout Translation Service", version="0.1.0")
    service = translation_service or TranslationService()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/translate/brreg-records")
    async def translate_brreg_records(
        request: BrregTranslateRequest,
        provider: Annotated[str | None, Query(min_length=1)] = None,
        model: Annotated[str | None, Query(min_length=1)] = None,
        prompt_version: Annotated[str | None, Query(min_length=1)] = None,
    ):
        llm = request.llm
        if provider is not None or model is not None:
            llm = LLMSelection(provider=provider or request.llm.provider, model=model or request.llm.model)
        if prompt_version is not None:
            request = request.model_copy(update={"llm": llm, "prompt_version": prompt_version})
        else:
            request = request.model_copy(update={"llm": llm})
        return await service.translate_brreg_records(request)

    return app


app = create_app()
