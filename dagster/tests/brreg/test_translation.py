from __future__ import annotations

import httpx
import pytest

from corpscout_dagster.brreg.translation_terms import (
    CachedTermTranslation,
    HttpTranslationServiceTermTranslator,
    TranslationServiceError,
    TranslationItem,
    build_translation_payload,
    extract_translation_items,
    translation_cache_key,
    translation_item_id,
)


def test_extract_translation_items_reads_brreg_business_terms() -> None:
    payload = {
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "institusjonellSektorkode": {"kode": "2100", "beskrivelse": "Private aksjeselskaper mv."},
        "naeringskode1": {"kode": "41.000", "beskrivelse": "Oppføring av bygninger"},
        "kapital": {"type": "Aksjekapital"},
        "aktivitet": ["Drive utleie av fast eiendom", ""],
        "vedtektsfestetFormaal": ["Kjøp og salg av aksjer."],
        "frivilligMvaRegistrertBeskrivelser": ["Utleier av bygg eller anlegg"],
    }

    items = extract_translation_items(payload)

    assert items == [
        TranslationItem(category="org_form", text="Aksjeselskap"),
        TranslationItem(category="sector_code", text="Private aksjeselskaper mv."),
        TranslationItem(category="industry_code", text="Oppføring av bygninger"),
        TranslationItem(category="capital_type", text="Aksjekapital"),
        TranslationItem(category="activity", text="Drive utleie av fast eiendom"),
        TranslationItem(category="statutory_purpose", text="Kjøp og salg av aksjer."),
        TranslationItem(category="vat_description", text="Utleier av bygg eller anlegg"),
    ]


def test_translation_cache_key_hashes_normalized_text() -> None:
    left = translation_cache_key(TranslationItem(category="activity", text="  Regnskapstjenester "))
    right = translation_cache_key(TranslationItem(category="activity", text="regnskapstjenester"))

    assert left == right
    assert left.category == "activity"
    assert len(left.original_hash) == 64


def test_build_translation_payload_uses_cached_terms() -> None:
    items = [
        TranslationItem(category="activity", text="Drive utleie av fast eiendom"),
        TranslationItem(category="org_form", text="Aksjeselskap"),
    ]
    cache = {
        translation_cache_key(items[0]): CachedTermTranslation(
            category="activity",
            original_text="Drive utleie av fast eiendom",
            translated_text="Engage in rental of real estate",
            model="qwen3:6b",
            prompt_version="v1",
        ),
        translation_cache_key(items[1]): CachedTermTranslation(
            category="org_form",
            original_text="Aksjeselskap",
            translated_text="Limited Liability Company",
            model="qwen3:6b",
            prompt_version="v1",
        ),
    }

    payload = build_translation_payload(
        raw_payload={"organisasjonsnummer": "810202572"},
        items=items,
        cached_translations=cache,
        model="qwen3:6b",
        prompt_version="v1",
    )

    assert payload == {
        "schema_version": "brreg.translation_terms.v1",
        "source_language": "no",
        "target_language": "en",
        "model": "qwen3:6b",
        "prompt_version": "v1",
        "organization_number": "810202572",
        "terms": [
            {
                "category": "activity",
                "original_text": "Drive utleie av fast eiendom",
                "translated_text": "Engage in rental of real estate",
            },
            {
                "category": "org_form",
                "original_text": "Aksjeselskap",
                "translated_text": "Limited Liability Company",
            },
        ],
    }


def test_http_translation_service_term_translator_calls_external_service() -> None:
    item = TranslationItem(category="activity", text="Drive utleie av fast eiendom")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url == "http://translation-service.test/v1/translate/terms"
        assert request.headers["content-type"] == "application/json"
        body = json_from_request(request)
        assert body["provider"] == "deepseek"
        assert body["model"] == "deepseek-v4-flash"
        assert body["prompt_version"] == "v2"
        assert body["source_lang"] == "no"
        assert body["target_lang"] == "en"
        assert body["items"] == [
            {
                "id": translation_item_id(item),
                "category": "activity",
                "text": "Drive utleie av fast eiendom",
            }
        ]
        return httpx.Response(
            200,
            json={
                "schema_version": "translation-service.terms.v1",
                "status": "succeeded",
                "provider": "deepseek",
                "model": "deepseek-v4-flash",
                "prompt_version": "v2",
                "items_seen": 1,
                "items_completed": 1,
                "items_failed": 0,
                "translations": [
                    {"id": translation_item_id(item), "translation": "Engage in rental of real estate"},
                ],
                "missing_ids": [],
                "error": None,
                "duration_ms": 10,
            },
        )

    translator = HttpTranslationServiceTermTranslator(
        base_url="http://translation-service.test",
        provider="deepseek",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    translated = translator.translate_terms(
        category="mixed",
        items=[item],
        source_lang="no",
        target_lang="en",
        model="deepseek-v4-flash",
        prompt_version="v2",
    )

    assert translated == {translation_item_id(item): "Engage in rental of real estate"}
    assert len(requests) == 1


def test_http_translation_service_term_translator_preserves_structured_failure() -> None:
    item = TranslationItem(category="activity", text="Drive utleie")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "failed",
                "error": {
                    "message": "LLM returned malformed JSON",
                    "code": "malformed_llm_response",
                    "category": "invalid_llm_output",
                    "retry_strategy": "change_model_or_prompt",
                },
            },
        )

    translator = HttpTranslationServiceTermTranslator(
        base_url="http://translation-service.test",
        provider="deepseek",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(TranslationServiceError) as exc_info:
        translator.translate_terms(
            category="mixed",
            items=[item],
            source_lang="no",
            target_lang="en",
            model="deepseek-v4-flash",
            prompt_version="v2",
        )

    assert str(exc_info.value) == "LLM returned malformed JSON"
    assert exc_info.value.error_category == "invalid_llm_output"
    assert exc_info.value.error_code == "malformed_llm_response"
    assert exc_info.value.retry_strategy == "change_model_or_prompt"


def json_from_request(request: httpx.Request) -> dict:
    import json

    return json.loads(request.content.decode("utf-8"))
