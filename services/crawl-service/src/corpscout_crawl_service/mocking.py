from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field

from corpscout_crawl_service.domain_utils import normalize_domain
from corpscout_crawl_service.models import (
    BrregDomainDiscoveryRequest,
    Crawl4AiResponse,
    DiscoveredDomain,
    DomainDiscoverRequest,
    DomainDiscoverResponse,
    RelatedSite,
    ScoredLink,
    ServiceError,
    SiteCheckResult,
)


DEFAULT_MOCK_SEED = "brreg-e2e-v1"


@dataclass
class MockCrawlService:
    seed: str = DEFAULT_MOCK_SEED
    profile: str = "mixed"
    fail_once_keys: set[str] = field(default_factory=set)

    @classmethod
    def from_env(cls) -> "MockCrawlService":
        return cls(
            seed=os.environ.get("MOCK_SEED") or DEFAULT_MOCK_SEED,
            profile=os.environ.get("MOCK_PROFILE") or "mixed",
        )

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def reset(self) -> None:
        self.fail_once_keys.clear()

    def state(self) -> dict[str, object]:
        return {
            "seed": self.seed,
            "profile": self.profile,
            "fail_once_keys": sorted(self.fail_once_keys),
        }

    async def discover_brreg_domain(self, request: BrregDomainDiscoveryRequest | dict) -> DomainDiscoverResponse:
        request = BrregDomainDiscoveryRequest.model_validate(request)
        return self._response(
            record_id=request.record_id,
            organization_number=request.organization_number,
            company_name=request.organization_name or str(request.raw_payload.get("navn") or request.organization_number),
            country=request.country,
            existing_website=request.existing_website,
        )

    async def discover_domains(self, request: DomainDiscoverRequest | dict) -> DomainDiscoverResponse:
        request = DomainDiscoverRequest.model_validate(request)
        organization_number = request.organization_number or _stable_number_from_name(request.company_name)
        return self._response(
            record_id=organization_number,
            organization_number=organization_number,
            company_name=request.company_name,
            country=request.country,
            existing_website=request.existing_website,
        )

    def _response(
        self,
        *,
        record_id: str,
        organization_number: str,
        company_name: str,
        country: str,
        existing_website: str | None,
    ) -> DomainDiscoverResponse:
        started = time.monotonic()
        bucket = _bucket(self.seed, "domain", organization_number)
        outcome = _outcome_for_bucket(bucket)
        key = f"domain:{organization_number}"
        if outcome == "fail_once" and key not in self.fail_once_keys:
            self.fail_once_keys.add(key)
            return _failed_response(
                organization_number=organization_number,
                company_name=company_name,
                code="mock_fail_once",
                message="Mock crawl transient failure.",
                category="transient_external",
                retry_strategy="automatic",
                started=started,
            )
        if outcome == "terminal":
            return _failed_response(
                organization_number=organization_number,
                company_name=company_name,
                code="mock_terminal_domain",
                message="Mock crawl terminal failure.",
                category="invalid_input",
                retry_strategy="manual_input",
                started=started,
            )
        if outcome == "not_found":
            return DomainDiscoverResponse(
                status="not_found",
                best_domain=None,
                search_engine="mock",
                search_term=f"{company_name} {country} website",
                search=_search_crawl(company_name),
                links=[],
                site_checks=[],
                related_sites=[],
                primary_web_presence=None,
                domains=[],
                owned_domains=[],
                candidates=[],
                errors=[],
                warnings=[],
                duration_ms=_elapsed_ms(started),
                service_version="mock",
            )

        domain = _mock_domain(organization_number, existing_website)
        discovered = DiscoveredDomain(
            domain=domain,
            normalized_domain=normalize_domain(domain),
            score=91,
            decision="accepted",
            source="mock_crawl_service",
            evidence={"organization_number": organization_number, "mock_bucket": bucket},
            metadata={"mock": True},
        )
        related = RelatedSite(
            url=f"https://{domain}/",
            domain=domain,
            normalized_domain=normalize_domain(domain),
            score=91,
            decision="accepted",
            site_type="company_website",
            relationship="primary_web_presence",
            owned_domain=True,
            reason="Deterministic mock domain discovery result.",
            evidence=[company_name],
            metadata={"mock": True},
        )
        return DomainDiscoverResponse(
            status="succeeded",
            best_domain=domain,
            search_engine="mock",
            search_term=f"{company_name} {country} website",
            search=_search_crawl(company_name),
            links=[
                ScoredLink(
                    url=f"https://{domain}/",
                    domain=domain,
                    normalized_domain=normalize_domain(domain),
                    score=88,
                    reason="Deterministic mock search result.",
                    evidence=[company_name],
                    source="mock_search",
                    metadata={"mock": True},
                )
            ],
            site_checks=[
                SiteCheckResult(
                    url=f"https://{domain}/",
                    domain=domain,
                    normalized_domain=normalize_domain(domain),
                    score=91,
                    decision="accepted",
                    site_type="company_website",
                    relationship="primary_web_presence",
                    owned_domain=True,
                    reason="Deterministic mock site verification.",
                    evidence=[company_name],
                    crawl=_site_crawl(domain, company_name),
                    metadata={"mock": True, "record_id": record_id},
                )
            ],
            related_sites=[related],
            primary_web_presence=related,
            domains=[discovered],
            owned_domains=[discovered],
            candidates=[discovered],
            errors=[],
            warnings=[],
            duration_ms=_elapsed_ms(started),
            service_version="mock",
        )


def mock_enabled_from_env() -> bool:
    return _truthy(os.environ.get("CRAWL_SERVICE_MOCK_ENABLED")) or os.environ.get("CRAWL_SERVICE_MODE") == "mock"


def _failed_response(
    *,
    organization_number: str,
    company_name: str,
    code: str,
    message: str,
    category: str,
    retry_strategy: str,
    started: float,
) -> DomainDiscoverResponse:
    return DomainDiscoverResponse(
        status="failed",
        best_domain=None,
        search_engine="mock",
        search_term=f"{company_name} NO website",
        search=_search_crawl(company_name),
        links=[],
        site_checks=[],
        related_sites=[],
        primary_web_presence=None,
        domains=[],
        owned_domains=[],
        candidates=[],
        errors=[
            ServiceError(
                code=code,
                message=message,
                category=category,
                retry_strategy=retry_strategy,
                detail={"mock": True, "organization_number": organization_number},
            )
        ],
        warnings=[],
        duration_ms=_elapsed_ms(started),
        service_version="mock",
    )


def _search_crawl(company_name: str) -> Crawl4AiResponse:
    return Crawl4AiResponse(
        url="https://mock-search.example.test/",
        final_url="https://mock-search.example.test/",
        status="succeeded",
        markdown=f"# Mock search results for {company_name}",
        markdown_hash=hashlib.sha256(company_name.encode("utf-8")).hexdigest(),
        links=[],
        llm_output=None,
        error=None,
        duration_ms=0,
        metadata={"mock": True},
    )


def _site_crawl(domain: str, company_name: str) -> Crawl4AiResponse:
    return Crawl4AiResponse(
        url=f"https://{domain}/",
        final_url=f"https://{domain}/",
        status="succeeded",
        markdown=f"# {company_name}\nOfficial mock website for {company_name}.",
        markdown_hash=hashlib.sha256(f"{domain}:{company_name}".encode("utf-8")).hexdigest(),
        links=[],
        llm_output=None,
        error=None,
        duration_ms=0,
        metadata={"mock": True},
    )


def _mock_domain(organization_number: str, existing_website: str | None) -> str:
    existing = normalize_domain(existing_website)
    if existing:
        return existing
    return f"mock-{organization_number}.example.test"


def _bucket(seed: str, task: str, organization_number: str) -> int:
    digest = hashlib.sha256(f"{seed}:{task}:{organization_number}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % 100


def _outcome_for_bucket(bucket: int) -> str:
    if 80 <= bucket < 90:
        return "fail_once"
    if 90 <= bucket < 95:
        return "terminal"
    if 95 <= bucket < 100:
        return "not_found"
    return "success"


def _stable_number_from_name(company_name: str) -> str:
    value = int(hashlib.sha256(company_name.encode("utf-8")).hexdigest()[:8], 16)
    return f"8{value % 100000000:08d}"


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
