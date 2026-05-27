from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


DomainStatus = Literal["succeeded", "not_found", "failed"]
CrawlStatus = Literal["succeeded", "failed"]
Decision = Literal["accepted", "rejected", "uncertain"]
SiteType = Literal[
    "company_website",
    "social_profile",
    "directory_profile",
    "registry_profile",
    "reference_page",
    "marketplace_profile",
    "unrelated",
]
SiteRelationship = Literal["primary_web_presence", "evidence_profile", "supporting_reference", "unrelated"]


class Crawl4AiRequest(BaseModel):
    url: str = Field(min_length=1)
    llm_enabled: bool = False
    llm_query: str | None = Field(default=None, min_length=1)
    llm_schema: dict[str, Any] | None = None
    timeout_seconds: int = Field(default=60, ge=1, le=300)
    purpose: str | None = Field(default=None, min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class Crawl4AiResponse(BaseModel):
    url: str
    final_url: str
    status: CrawlStatus
    markdown: str | None = None
    markdown_hash: str | None = None
    links: list[str] = Field(default_factory=list)
    llm_output: Any | None = None
    error: dict[str, Any] | None = None
    duration_ms: int
    metadata: dict[str, Any] = Field(default_factory=dict)


class DomainDiscoverLimits(BaseModel):
    max_search_candidates: int = Field(default=5, ge=1, le=20)
    max_site_checks: int = Field(default=3, ge=1, le=10)
    search_candidate_threshold: int = Field(default=50, ge=0, le=100)
    domain_threshold: int = Field(default=70, ge=0, le=100)
    timeout_seconds: int = Field(default=60, ge=1, le=300)


class BrregDomainDiscoveryRequest(BaseModel):
    record_id: str = Field(min_length=1)
    organization_number: str = Field(min_length=1)
    organization_name: str | None = None
    raw_payload: dict[str, Any]
    existing_website: str | None = None
    country: str = Field(default="NO", min_length=2)
    search_provider: str | None = Field(default=None, min_length=1)
    prompt_version: str = Field(default="v1", min_length=1)
    limits: DomainDiscoverLimits = Field(default_factory=DomainDiscoverLimits)


class DomainDiscoverRequest(BaseModel):
    company_name: str = Field(min_length=1)
    organization_number: str | None = Field(default=None, min_length=1)
    country: str = Field(default="NO", min_length=2)
    address_lines: list[str] = Field(default_factory=list)
    city: str | None = None
    postal_code: str | None = None
    business_activity: list[str] = Field(default_factory=list)
    statutory_purpose: list[str] = Field(default_factory=list)
    industry_codes: list[str] = Field(default_factory=list)
    existing_website: str | None = None
    search_engine: str = Field(default="duckduckgo", min_length=1)
    search_term: str | None = Field(default=None, min_length=1)
    prompt_version: str = Field(default="v1", min_length=1)
    limits: DomainDiscoverLimits = Field(default_factory=DomainDiscoverLimits)

    @field_validator("search_engine")
    @classmethod
    def validate_search_engine(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"duckduckgo", "yandex"}:
            raise ValueError(f"Unsupported search engine: {value}")
        return normalized


class ScoredLink(BaseModel):
    url: str
    domain: str
    normalized_domain: str
    score: int = Field(ge=0, le=100)
    reason: str = ""
    evidence: list[str] = Field(default_factory=list)
    source: str = "search_page_llm"
    metadata: dict[str, Any] = Field(default_factory=dict)


class SiteCheckResult(BaseModel):
    url: str
    domain: str
    normalized_domain: str
    score: int = Field(ge=0, le=100)
    decision: Decision
    site_type: SiteType = "unrelated"
    relationship: SiteRelationship = "unrelated"
    owned_domain: bool = False
    reason: str = ""
    evidence: list[str] = Field(default_factory=list)
    crawl: Crawl4AiResponse
    metadata: dict[str, Any] = Field(default_factory=dict)


class DiscoveredDomain(BaseModel):
    domain: str
    normalized_domain: str
    score: int = Field(ge=0, le=100)
    decision: Decision = "accepted"
    source: str = "domain_site_llm"
    evidence: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RelatedSite(BaseModel):
    url: str
    domain: str
    normalized_domain: str
    score: int = Field(ge=0, le=100)
    decision: Decision = "accepted"
    site_type: SiteType
    relationship: SiteRelationship
    owned_domain: bool = False
    reason: str = ""
    evidence: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ServiceError(BaseModel):
    code: str
    message: str
    category: str | None = None
    retry_strategy: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class DomainDiscoverResponse(BaseModel):
    schema_version: str = "crawl-service.domains.v1"
    status: DomainStatus
    best_domain: str | None = None
    search_engine: str | None = None
    search_term: str | None = None
    search: Crawl4AiResponse | None = None
    links: list[ScoredLink] = Field(default_factory=list)
    site_checks: list[SiteCheckResult] = Field(default_factory=list)
    related_sites: list[RelatedSite] = Field(default_factory=list)
    primary_web_presence: RelatedSite | None = None
    domains: list[DiscoveredDomain] = Field(default_factory=list)
    owned_domains: list[DiscoveredDomain] = Field(default_factory=list)
    candidates: list[DiscoveredDomain] = Field(default_factory=list)
    errors: list[ServiceError] = Field(default_factory=list)
    warnings: list[ServiceError] = Field(default_factory=list)
    duration_ms: int
    service_version: str = "0.1.0"
