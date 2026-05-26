from __future__ import annotations

import os

import pytest

from corpscout_translation_service.llm import OpenAICompatibleLLMClient
from corpscout_translation_service.models import BrregRecord, BrregTranslateRequest, LLMSelection
from corpscout_translation_service.service import TranslationService

from tests.real_brreg_records import load_real_brreg_records


@pytest.mark.real_llm
@pytest.mark.asyncio
async def test_real_llm_can_translate_real_database_brreg_records() -> None:
    if os.environ.get("TRANSLATION_SERVICE_RUN_REAL_LLM_TESTS") != "1":
        pytest.skip("set TRANSLATION_SERVICE_RUN_REAL_LLM_TESTS=1 to run the real LLM stress test")

    provider = os.environ.get("TRANSLATION_SERVICE_TEST_PROVIDER", "default")
    model = os.environ.get("TRANSLATION_SERVICE_TEST_MODEL") or "qwen3:6b"

    records = int(os.environ.get("TRANSLATION_SERVICE_REAL_LLM_STRESS_RECORDS", "300"))
    service = TranslationService(llm_client=OpenAICompatibleLLMClient.from_env(), max_llm_items_per_request=40)

    response = await service.translate_brreg_records(
        BrregTranslateRequest(
            records=load_real_brreg_records(limit=records),
            llm=LLMSelection(provider=provider, model=model),
            max_retries=3,
        )
    )

    failed = [result for result in response.results if result.status == "failed"]
    assert not failed, [result.model_dump(mode="json") for result in failed[:5]]
