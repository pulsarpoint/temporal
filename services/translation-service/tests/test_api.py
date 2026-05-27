from __future__ import annotations

from fastapi.testclient import TestClient

from corpscout_translation_service.api import create_app
from corpscout_translation_service.service import TranslationService

from tests.fakes import FakeLLMClient
from tests.fixtures import brreg_record_payload


def test_mock_endpoints_are_hidden_when_mock_mode_is_disabled(monkeypatch) -> None:
    monkeypatch.delenv("TRANSLATION_MOCK_ENABLED", raising=False)
    monkeypatch.setenv("TRANSLATION_DEFAULT_PROVIDER", "local")
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)

    assert client.get("/__mock/state").status_code == 404
    assert client.post("/__mock/reset").status_code == 404


def test_mock_provider_returns_dummy_translations_and_stateful_fail_once(monkeypatch) -> None:
    monkeypatch.setenv("TRANSLATION_MOCK_ENABLED", "true")
    monkeypatch.setenv("MOCK_SEED", "brreg-e2e-v1")
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)
    fail_once_org = _org_number_for_bucket("translation", range(80, 90))
    payload = {
        "provider": "mock",
        "model": "mock-model",
        "prompt_version": "v1",
        "source_lang": "no",
        "target_lang": "en",
        "items": [
            {
                "id": f"activity:{fail_once_org}",
                "category": "activity",
                "text": f"Mock activity for organization {fail_once_org}",
            }
        ],
    }

    first = client.post("/v1/translate/terms", json=payload)
    second = client.post("/v1/translate/terms", json=payload)
    state = client.get("/__mock/state")
    reset = client.post("/__mock/reset")
    third = client.post("/v1/translate/terms", json=payload)

    assert first.status_code == 200
    assert first.json()["status"] == "failed"
    assert first.json()["error"]["category"] == "transient_external"
    assert second.status_code == 200
    assert second.json()["status"] == "succeeded"
    assert second.json()["translations"][0]["translation"].startswith("[mock-en]")
    assert state.status_code == 200
    assert state.json()["fail_once_keys"]
    assert reset.status_code == 200
    assert third.json()["status"] == "failed"


def test_mock_provider_buckets_by_organization_number_not_common_terms(monkeypatch) -> None:
    monkeypatch.setenv("TRANSLATION_MOCK_ENABLED", "true")
    monkeypatch.setenv("MOCK_SEED", "brreg-e2e-v1")
    app = create_app(translation_service=TranslationService(llm_client=FakeLLMClient()))
    client = TestClient(app)
    success_org = _org_number_for_bucket("translation", range(0, 80))
    payload = {
        "provider": "mock",
        "model": "mock-model",
        "prompt_version": "v1",
        "source_lang": "no",
        "target_lang": "en",
        "items": [
            {"id": "org_form:common", "category": "org_form", "text": "Aksjeselskap"},
            {
                "id": f"activity:{success_org}",
                "category": "activity",
                "text": f"Mock activity for organization {success_org}",
            },
        ],
    }

    response = client.post("/v1/translate/terms", json=payload)

    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"


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


def _org_number_for_bucket(task: str, bucket_range: range) -> str:
    import hashlib

    for index in range(1000, 9999):
        organization_number = f"81{index:07d}"[-9:]
        digest = hashlib.sha256(f"brreg-e2e-v1:{task}:{organization_number}".encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % 100
        if bucket in bucket_range:
            return organization_number
    raise AssertionError(f"could not find org number for {task} bucket {bucket_range}")
