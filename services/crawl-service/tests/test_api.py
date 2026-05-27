from __future__ import annotations

from fastapi.testclient import TestClient
import pytest

from corpscout_crawl_service.api import create_app
from corpscout_crawl_service.crawl4ai_service import LlmConfigError
from corpscout_crawl_service.crawl4ai_service import Crawl4AiResponse
from corpscout_crawl_service.service import CrawlService

from tests.fakes import FakeCrawl4AiService


def test_mock_endpoints_are_hidden_when_mock_mode_is_disabled(monkeypatch) -> None:
    monkeypatch.delenv("CRAWL_SERVICE_MOCK_ENABLED", raising=False)
    monkeypatch.delenv("CRAWL_SERVICE_MODE", raising=False)
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=FakeCrawl4AiService({}))))

    assert client.get("/__mock/state").status_code == 404
    assert client.post("/__mock/reset").status_code == 404


def test_mock_crawl_service_returns_stateful_fail_once_domain_payload(monkeypatch) -> None:
    monkeypatch.setenv("CRAWL_SERVICE_MOCK_ENABLED", "true")
    monkeypatch.setenv("CRAWL_SERVICE_MODE", "mock")
    monkeypatch.setenv("MOCK_SEED", "brreg-e2e-v1")
    client = TestClient(create_app())
    fail_once_org = _org_number_for_bucket("domain", range(80, 90))
    payload = {
        "record_id": f"record-{fail_once_org}",
        "organization_number": fail_once_org,
        "organization_name": f"MOCK {fail_once_org} AS",
        "raw_payload": {"organisasjonsnummer": fail_once_org, "navn": f"MOCK {fail_once_org} AS"},
        "country": "NO",
    }

    first = client.post("/v1/brreg/domain-discovery", json=payload)
    second = client.post("/v1/brreg/domain-discovery", json=payload)
    state = client.get("/__mock/state")
    reset = client.post("/__mock/reset")
    third = client.post("/v1/brreg/domain-discovery", json=payload)

    assert first.status_code == 200
    assert first.json()["status"] == "failed"
    assert first.json()["errors"][0]["category"] == "transient_external"
    assert second.status_code == 200
    assert second.json()["status"] == "succeeded"
    assert second.json()["best_domain"].endswith(".example.test")
    assert state.status_code == 200
    assert state.json()["fail_once_keys"]
    assert reset.status_code == 200
    assert third.json()["status"] == "failed"


def test_health_endpoint_reports_service_status() -> None:
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=FakeCrawl4AiService({}))))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_default_app_fails_startup_without_llm_config(monkeypatch) -> None:
    monkeypatch.delenv("CRAWL_SERVICE_LLM_MODEL", raising=False)
    monkeypatch.delenv("CRAWL_SERVICE_LLM_BASE_URL", raising=False)

    with pytest.raises(LlmConfigError, match="CRAWL_SERVICE_LLM_MODEL"):
        create_app()


def test_domains_discover_endpoint_returns_domain_result() -> None:
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
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=fake)))

    response = client.post("/v1/domains/discover", json={"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "succeeded"
    assert body["best_domain"] == "bortigard.no"
    assert body["search"]["markdown_hash"] == "search-hash"
    assert body["links"][0]["score"] == 88
    assert body["site_checks"][0]["crawl"]["markdown_hash"] == "site-hash"


def test_domains_discover_endpoint_rejects_invalid_search_engine() -> None:
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=FakeCrawl4AiService({}))))

    response = client.post(
        "/v1/domains/discover",
        json={"company_name": "BORTIGARD AS", "country": "NO", "search_engine": "google"},
    )

    assert response.status_code == 422


def test_domains_discover_endpoint_returns_structured_errors() -> None:
    client = TestClient(create_app(crawl_service=CrawlService(crawl4ai_service=FakeCrawl4AiService({}))))

    response = client.post("/v1/domains/discover", json={"company_name": "BORTIGARD AS", "country": "NO"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["errors"][0]["code"] == "domain_search_failed"
    assert body["search"]["error"]["code"] == "not_found"


def _org_number_for_bucket(task: str, bucket_range: range) -> str:
    import hashlib

    for index in range(1000, 9999):
        organization_number = f"81{index:07d}"[-9:]
        digest = hashlib.sha256(f"brreg-e2e-v1:{task}:{organization_number}".encode("utf-8")).hexdigest()
        bucket = int(digest[:8], 16) % 100
        if bucket in bucket_range:
            return organization_number
    raise AssertionError(f"could not find org number for {task} bucket {bucket_range}")
