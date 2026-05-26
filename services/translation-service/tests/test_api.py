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


def test_health_endpoint_reports_service_status() -> None:
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
