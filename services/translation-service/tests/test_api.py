from __future__ import annotations

from fastapi.testclient import TestClient

from corpscout_translation_service.api import create_app
from corpscout_translation_service.service import TranslationService

from tests.fakes import FakeLLMClient
from tests.fixtures import brreg_record_payload


def test_api_translates_brreg_records_and_accepts_llm_query_selection() -> None:
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)

    response = client.post(
        "/v1/translate/brreg-records?provider=fake&model=fake-fast",
        json={"records": [brreg_record_payload(1)]},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["provider"] == "fake"
    assert body["model"] == "fake-fast"
    assert body["results"][0]["status"] == "succeeded"
    assert body["results"][0]["translated_payload"]["terms"]


def test_api_translates_term_batch_and_accepts_llm_query_selection() -> None:
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)

    response = client.post(
        "/v1/translate/terms?provider=fake&model=fake-fast",
        json={
            "provider": "default",
            "model": "default",
            "prompt_version": "v1",
            "source_lang": "no",
            "target_lang": "en",
            "items": [
                {"id": "activity:one", "category": "activity", "text": "Drive utleie av fast eiendom"},
            ],
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["schema_version"] == "translation-service.terms.v1"
    assert body["status"] == "succeeded"
    assert body["provider"] == "fake"
    assert body["model"] == "fake-fast"
    assert body["items_seen"] == 1
    assert body["items_completed"] == 1
    assert body["items_failed"] == 0
    assert body["translations"] == [
        {"id": "activity:one", "translation": "Drive utleie av fast eiendom EN"},
    ]
    assert body["missing_ids"] == []
    assert body["error"] is None


def test_health_endpoint_reports_service_status() -> None:
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
