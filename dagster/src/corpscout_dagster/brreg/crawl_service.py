from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from corpscout_dagster.brreg.working_store import RawTaskRecord


DEFAULT_CRAWL_SERVICE_URL = "http://crawl-service:8096"


class CrawlServiceClient(Protocol):
    def discover_brreg_domain(self, record: RawTaskRecord) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class HttpCrawlServiceClient:
    base_url: str
    timeout_seconds: float = 300

    @classmethod
    def from_env(cls) -> "HttpCrawlServiceClient":
        return cls(
            base_url=os.environ.get("CRAWL_SERVICE_URL", DEFAULT_CRAWL_SERVICE_URL).rstrip("/"),
            timeout_seconds=float(os.environ.get("CRAWL_SERVICE_TIMEOUT_SECONDS", "300")),
        )

    def discover_brreg_domain(self, record: RawTaskRecord) -> dict[str, Any]:
        response = httpx.post(
            f"{self.base_url}/v1/brreg/domain-discovery",
            json=crawl_service_request_payload(record),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("crawl service returned a non-object response")
        return payload


def crawl_service_request_payload(record: RawTaskRecord) -> dict[str, Any]:
    return {
        "record_id": record.id,
        "organization_number": record.organization_number,
        "organization_name": record.organization_name,
        "raw_payload": record.raw_payload,
        "existing_website": record.website or record.raw_payload.get("hjemmeside"),
        "country": "NO",
    }
