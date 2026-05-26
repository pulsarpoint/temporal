from __future__ import annotations

import pytest

from corpscout_translation_service.brreg import extract_translation_items, translation_item_id
from corpscout_translation_service.models import BrregRecord, BrregTranslateRequest, LLMSelection
from corpscout_translation_service.service import TranslationService

from tests.fakes import FakeLLMClient
from tests.fixtures import brreg_raw_payload, brreg_record_payload


@pytest.mark.asyncio
async def test_translation_service_returns_per_record_payload_and_model_metadata() -> None:
    service = TranslationService(llm_client=FakeLLMClient(), max_llm_items_per_request=50)
    request = BrregTranslateRequest(
        records=[
            BrregRecord(
                record_id="record-1",
                organization_number="810202572",
                raw_payload=brreg_raw_payload(),
            )
        ],
        llm=LLMSelection(provider="fake", model="fake-fast"),
    )

    response = await service.translate_brreg_records(request)

    assert response.status == "succeeded"
    assert response.provider == "fake"
    assert response.model == "fake-fast"
    assert response.results[0].status == "succeeded"
    assert response.results[0].translated_payload is not None
    assert response.results[0].translated_payload["model"] == "fake-fast"
    assert len(response.results[0].translated_payload["terms"]) == 6
    assert response.results[0].error is None


@pytest.mark.asyncio
async def test_translation_service_retries_missing_terms_in_smaller_chunks() -> None:
    raw_payload = brreg_raw_payload()
    missing_once_id = translation_item_id(extract_translation_items(raw_payload)[0])
    llm_client = FakeLLMClient(missing_once_ids={missing_once_id})
    service = TranslationService(llm_client=llm_client, max_llm_items_per_request=50)

    response = await service.translate_brreg_records(
        BrregTranslateRequest(
            records=[
                BrregRecord(record_id="record-1", organization_number="810202572", raw_payload=raw_payload),
            ],
            llm=LLMSelection(provider="fake", model="fake-fast"),
            max_retries=2,
        )
    )

    assert response.results[0].status == "succeeded"
    assert len(llm_client.calls) == 2
    assert [item.id for item in llm_client.calls[1].items] == [missing_once_id]


@pytest.mark.asyncio
async def test_translation_service_returns_structured_error_when_terms_stay_missing() -> None:
    raw_payload = brreg_raw_payload()
    missing_id = translation_item_id(extract_translation_items(raw_payload)[0])
    service = TranslationService(
        llm_client=FakeLLMClient(always_missing_ids={missing_id}),
        max_llm_items_per_request=50,
    )

    response = await service.translate_brreg_records(
        BrregTranslateRequest(
            records=[
                BrregRecord(record_id="record-1", organization_number="810202572", raw_payload=raw_payload),
            ],
            llm=LLMSelection(provider="fake", model="fake-fast"),
            max_retries=1,
        )
    )

    assert response.status == "failed"
    assert response.results[0].status == "failed"
    assert response.results[0].translated_payload is None
    assert response.results[0].error is not None
    assert response.results[0].error.code == "missing_translations"
    assert response.results[0].missing_terms == [missing_id]


@pytest.mark.asyncio
async def test_translation_service_handles_200_records_with_fake_llm() -> None:
    llm_client = FakeLLMClient()
    service = TranslationService(llm_client=llm_client, max_llm_items_per_request=40)
    request = BrregTranslateRequest(
        records=[BrregRecord(**brreg_record_payload(index)) for index in range(200)],
        llm=LLMSelection(provider="fake", model="fake-fast"),
        max_retries=2,
    )

    response = await service.translate_brreg_records(request)

    assert response.status == "succeeded"
    assert len(response.results) == 200
    assert all(result.status == "succeeded" for result in response.results)
    assert all(result.translated_payload for result in response.results)
    assert len(llm_client.calls) > 1
    assert sum(len(call.items) for call in llm_client.calls) >= 400


@pytest.mark.asyncio
async def test_translation_service_logs_llm_batch_progress(caplog) -> None:
    service = TranslationService(llm_client=FakeLLMClient(), max_llm_items_per_request=2)
    request = BrregTranslateRequest(
        records=[BrregRecord(**brreg_record_payload(index)) for index in range(3)],
        llm=LLMSelection(provider="fake", model="fake-fast"),
    )

    with caplog.at_level("INFO", logger="corpscout_translation_service.service"):
        response = await service.translate_brreg_records(request)

    assert response.status == "succeeded"
    messages = [record.message for record in caplog.records]
    assert "Translating BRREG terms with LLM" in messages
