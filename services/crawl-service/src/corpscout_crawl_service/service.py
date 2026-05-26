from __future__ import annotations

import time
import re
from typing import Any

from corpscout_crawl_service.candidate_policy import known_site_classification, search_result_exclusion_reason
from corpscout_crawl_service.crawl4ai_service import Crawl4AiService, search_url_for_engine
from corpscout_crawl_service.domain_utils import domain_from_url, normalize_domain, normalize_url, unwrap_search_result_url
from corpscout_crawl_service.models import (
    BrregDomainDiscoveryRequest,
    Crawl4AiRequest,
    Crawl4AiResponse,
    DiscoveredDomain,
    DomainDiscoverRequest,
    DomainDiscoverResponse,
    RelatedSite,
    ScoredLink,
    ServiceError,
    SiteCheckResult,
)
from corpscout_crawl_service.site_analyzer import DirectLlmAnalyzer, SearchAnalyzer, SiteAnalyzer


class CrawlService:
    def __init__(
        self,
        *,
        crawl4ai_service: Crawl4AiService | None = None,
        search_analyzer: SearchAnalyzer | None = None,
        site_analyzer: SiteAnalyzer | None = None,
    ) -> None:
        self._crawl4ai = crawl4ai_service or Crawl4AiService.from_env()
        analyzer = DirectLlmAnalyzer() if search_analyzer is None or site_analyzer is None else None
        self._search_analyzer = search_analyzer or analyzer
        self._site_analyzer = site_analyzer or analyzer

    async def start(self) -> None:
        await self._crawl4ai.start()

    async def close(self) -> None:
        await self._crawl4ai.close()

    async def discover_brreg_domain(self, request: BrregDomainDiscoveryRequest | dict) -> DomainDiscoverResponse:
        brreg_request = BrregDomainDiscoveryRequest.model_validate(request)
        facts = _brreg_domain_request_facts(brreg_request)
        return await self.discover_domains(
            DomainDiscoverRequest(
                company_name=facts["company_name"],
                organization_number=brreg_request.organization_number,
                country=brreg_request.country,
                address_lines=facts["address_lines"],
                city=facts["city"],
                postal_code=facts["postal_code"],
                business_activity=facts["business_activity"],
                statutory_purpose=facts["statutory_purpose"],
                industry_codes=facts["industry_codes"],
                existing_website=brreg_request.existing_website or _raw_website(brreg_request.raw_payload),
                search_engine=brreg_request.search_provider or "duckduckgo",
                prompt_version=brreg_request.prompt_version,
                limits=brreg_request.limits,
            )
        )

    async def discover_domains(self, request: DomainDiscoverRequest | dict) -> DomainDiscoverResponse:
        request = DomainDiscoverRequest.model_validate(request)
        started = time.monotonic()
        errors: list[ServiceError] = []
        warnings: list[ServiceError] = []
        search_response: Crawl4AiResponse | None = None
        links: list[ScoredLink] = []
        site_checks: list[SiteCheckResult] = []
        domains: list[DiscoveredDomain] = []
        related_sites: list[RelatedSite] = []

        existing_website_url = normalize_url(request.existing_website) if request.existing_website else ""
        if existing_website_url:
            existing_link = _scored_link_from_url(
                url=existing_website_url,
                score=100,
                reason="Existing website was provided by the source record.",
                source="existing_website",
            )
            site_checks = [await self._check_site(request=request, link=existing_link)]
            domains = _accepted_domains(site_checks, threshold=request.limits.domain_threshold)
            related_sites = _related_sites(site_checks, threshold=request.limits.domain_threshold)
            return _domain_response(
                request=request,
                started=started,
                search=None,
                links=[existing_link],
                site_checks=site_checks,
                domains=domains,
                related_sites=related_sites,
                errors=errors,
                warnings=warnings,
            )

        search_term = request.search_term or _default_search_term(request)
        search_url = search_url_for_engine(search_term, search_engine=request.search_engine)
        search_response = await self._crawl4ai.crawl(
            Crawl4AiRequest(
                url=search_url,
                llm_enabled=False,
                timeout_seconds=request.limits.timeout_seconds,
                purpose="domain_search",
                metadata={"search_engine": request.search_engine, "search_term": search_term},
            )
        )
        if search_response.status != "succeeded":
            errors.append(
                ServiceError(
                    code="domain_search_failed",
                    message="Domain search crawl failed.",
                    detail=search_response.error or {},
                )
            )
            return _domain_response(
                request=request,
                started=started,
                search=search_response,
                links=[],
                site_checks=[],
                domains=[],
                related_sites=[],
                errors=errors,
                warnings=warnings,
                search_term=search_term,
            )

        links = await self._candidate_links_from_search_response(
            request=request,
            search_response=search_response,
            warnings=warnings,
        )
        if not links:
            warnings.append(
                ServiceError(
                    code="search_candidate_links_empty",
                    message="Search-page crawl returned no candidate links.",
                    detail={"search_engine": request.search_engine},
                )
            )
        for link in links:
            if link.score < request.limits.search_candidate_threshold:
                continue
            if link.metadata.get("exclusion_reason") == "search_provider":
                continue
            if len(site_checks) >= request.limits.max_site_checks:
                break
            site_checks.append(await self._check_site(request=request, link=link))

        domains = _accepted_domains(site_checks, threshold=request.limits.domain_threshold)
        related_sites = _related_sites(site_checks, threshold=request.limits.domain_threshold)
        return _domain_response(
            request=request,
            started=started,
            search=search_response,
            links=links,
            site_checks=site_checks,
            domains=domains,
            related_sites=related_sites,
            errors=errors,
            warnings=warnings,
            search_term=search_term,
        )

    async def _candidate_links_from_search_response(
        self,
        *,
        request: DomainDiscoverRequest,
        search_response: Crawl4AiResponse,
        warnings: list[ServiceError],
    ) -> list[ScoredLink]:
        output = search_response.llm_output
        if output is None:
            try:
                output = await self._search_analyzer.analyze_search(
                    _search_analysis_payload(request=request, response=search_response)
                )
                search_response.llm_output = output
            except Exception as exc:
                warnings.append(
                    ServiceError(
                        code="search_candidate_llm_failed",
                        message="Search candidate analysis failed; falling back to crawled search-page links.",
                        detail={"error": str(exc)},
                    )
                )

        links = _scored_links_from_llm_output(
            output,
            source=f"{request.search_engine}_search_llm",
            limit=request.limits.max_search_candidates,
        )
        if links:
            return links

        warnings.append(
            ServiceError(
                code="search_candidate_llm_empty",
                message="Search candidate analysis returned no candidates; falling back to crawled search-page links.",
                detail={"search_engine": request.search_engine},
            )
        )
        return _scored_links_from_search_response(
            search_response,
            source=f"{request.search_engine}_search_links",
            limit=request.limits.max_search_candidates,
            score=request.limits.search_candidate_threshold,
        )

    async def _check_site(self, *, request: DomainDiscoverRequest, link: ScoredLink) -> SiteCheckResult:
        response = await self._crawl4ai.crawl(
            Crawl4AiRequest(
                url=link.url,
                llm_enabled=False,
                timeout_seconds=request.limits.timeout_seconds,
                purpose="domain_site_check",
                metadata={"candidate_score": link.score, "candidate_source": link.source},
            )
        )
        if response.status == "succeeded":
            try:
                output = response.llm_output
                if not isinstance(output, dict):
                    output = await self._site_analyzer.analyze_site(
                        _site_analysis_payload(request=request, link=link, response=response)
                    )
                response.llm_output = _apply_known_site_overrides(
                    _apply_context_scoring_rules(output),
                    normalized_domain=domain_from_url(response.final_url) or link.normalized_domain,
                )
            except Exception as exc:
                response.llm_output = {
                    "decision": "rejected",
                    "score": 0,
                    "site_type": "unrelated",
                    "relationship": "unrelated",
                    "owned_domain": False,
                    "reason": f"Site analysis failed: {exc}",
                    "evidence": [],
                }
        return _site_check_from_response(link=link, response=response)


def _domain_response(
    *,
    request: DomainDiscoverRequest,
    started: float,
    search: Crawl4AiResponse | None,
    links: list[ScoredLink],
    site_checks: list[SiteCheckResult],
    domains: list[DiscoveredDomain],
    related_sites: list[RelatedSite],
    errors: list[ServiceError],
    warnings: list[ServiceError],
    search_term: str | None = None,
) -> DomainDiscoverResponse:
    status = "failed" if errors else "succeeded" if domains or related_sites else "not_found"
    primary_web_presence = _primary_web_presence(related_sites)
    return DomainDiscoverResponse(
        status=status,
        best_domain=domains[0].normalized_domain if domains else None,
        search_engine=request.search_engine,
        search_term=search_term or request.search_term,
        search=search,
        links=links,
        site_checks=site_checks,
        related_sites=related_sites,
        primary_web_presence=primary_web_presence,
        domains=domains,
        owned_domains=domains,
        candidates=domains,
        errors=errors,
        warnings=warnings,
        duration_ms=int((time.monotonic() - started) * 1000),
    )


def _default_search_term(request: DomainDiscoverRequest) -> str:
    return f"{request.company_name} {request.country} website"


def _search_candidate_query(request: DomainDiscoverRequest) -> str:
    return (
        "Analyze this search result page and score URLs by how likely they are to be related to "
        f"company {request.company_name}. "
        "Return JSON matching the provided schema. Score each URL from 0 to 100. "
        "Prefer official company websites. Penalize registries, directories, maps, search engines, and unrelated public pages."
    )


def _site_verification_query(*, request: DomainDiscoverRequest, link: ScoredLink) -> str:
    return (
        f"Analyze this crawled website and decide whether it belongs to company {request.company_name}. "
        f"The candidate URL is {link.url}. "
        "Return JSON matching the provided schema with score 0-100. "
        "Use a high score only when the site itself contains evidence connecting it to the company."
    )


def _search_candidate_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "candidates": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string"},
                        "domain": {"type": "string"},
                        "score": {"type": "integer"},
                        "reason": {"type": "string"},
                        "evidence": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["url", "score"],
                },
            }
        },
        "required": ["candidates"],
    }


def _site_verification_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "decision": {"type": "string", "enum": ["accepted", "rejected", "uncertain"]},
            "score": {"type": "integer"},
            "reason": {"type": "string"},
            "evidence": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["score", "reason"],
    }


def _scored_links_from_llm_output(output: Any, *, source: str, limit: int) -> list[ScoredLink]:
    candidates = _candidate_items(output)
    rows: list[ScoredLink] = []
    seen: set[str] = set()
    for candidate in candidates:
        url = normalize_url(str(candidate.get("url") or candidate.get("website") or candidate.get("href") or ""))
        if not url:
            continue
        domain = normalize_domain(candidate.get("domain")) or domain_from_url(url)
        normalized = normalize_domain(domain)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        score = _score(candidate.get("score", candidate.get("confidence")))
        exclusion = search_result_exclusion_reason(normalized)
        metadata = {}
        if exclusion is not None:
            metadata = {
                "excluded": True,
                "exclusion_reason": exclusion["reason"],
                "excluded_domain_match": exclusion["matched_domain"],
            }
        rows.append(
            ScoredLink(
                url=url,
                domain=domain,
                normalized_domain=normalized,
                score=score,
                reason=str(candidate.get("reason") or ""),
                evidence=_string_list(candidate.get("evidence")),
                source=source,
                metadata=metadata,
            )
        )
        if len(rows) >= limit:
            break
    return rows


def _scored_links_from_search_response(
    response: Crawl4AiResponse,
    *,
    source: str,
    limit: int,
    score: int,
) -> list[ScoredLink]:
    rows: list[ScoredLink] = []
    seen: set[str] = set()
    candidate_count = 0
    for raw_url in response.links:
        url = unwrap_search_result_url(raw_url)
        if not url:
            continue
        domain = domain_from_url(url)
        normalized = normalize_domain(domain)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        exclusion = search_result_exclusion_reason(normalized)
        metadata: dict[str, Any] = {"fallback": "search_response_links", "raw_url": raw_url}
        if exclusion is not None:
            metadata.update(
                {
                    "excluded": True,
                    "exclusion_reason": exclusion["reason"],
                    "excluded_domain_match": exclusion["matched_domain"],
                }
            )
            if exclusion["reason"] == "search_provider":
                continue
        candidate_count += 1
        rows.append(
            ScoredLink(
                url=url,
                domain=domain,
                normalized_domain=normalized,
                score=score,
                reason="Candidate URL extracted from crawled search page links because LLM candidate extraction was empty.",
                source=source,
                metadata=metadata,
            )
        )
        if candidate_count >= limit:
            break
    return rows


def _site_check_from_response(*, link: ScoredLink, response: Crawl4AiResponse) -> SiteCheckResult:
    output = response.llm_output if isinstance(response.llm_output, dict) else {}
    final_domain = domain_from_url(response.final_url) or link.normalized_domain
    score = _score(output.get("score", output.get("confidence")))
    decision = _decision(output.get("decision"))
    raw_site_type = output.get("site_type")
    raw_relationship = output.get("relationship")
    raw_owned_domain = output.get("owned_domain")
    site_type = str(raw_site_type or "unrelated").strip().lower()
    relationship = str(raw_relationship or "unrelated").strip().lower()
    owned_domain = bool(raw_owned_domain)
    if response.status != "succeeded":
        decision = "rejected"
    if decision == "accepted" and raw_site_type is None and search_result_exclusion_reason(final_domain) is None:
        site_type = "company_website"
    if decision == "accepted" and raw_relationship is None and site_type == "company_website":
        relationship = "primary_web_presence"
    if decision == "accepted" and raw_owned_domain is None and site_type == "company_website":
        owned_domain = True
    if site_type not in {
        "company_website",
        "social_profile",
        "directory_profile",
        "registry_profile",
        "reference_page",
        "marketplace_profile",
        "unrelated",
    }:
        site_type = "unrelated"
    if relationship not in {"primary_web_presence", "evidence_profile", "supporting_reference", "unrelated"}:
        relationship = "unrelated"
    return SiteCheckResult(
        url=link.url,
        domain=final_domain,
        normalized_domain=normalize_domain(final_domain),
        score=score,
        decision=decision,  # type: ignore[arg-type]
        site_type=site_type,  # type: ignore[arg-type]
        relationship=relationship,  # type: ignore[arg-type]
        owned_domain=owned_domain,
        reason=str(output.get("reason") or ""),
        evidence=_string_list(output.get("evidence") or output.get("matched_evidence")),
        crawl=response,
        metadata={"candidate": link.model_dump()},
    )


def _accepted_domains(site_checks: list[SiteCheckResult], *, threshold: int) -> list[DiscoveredDomain]:
    rows: list[DiscoveredDomain] = []
    seen: set[str] = set()
    for check in sorted(site_checks, key=lambda item: item.score, reverse=True):
        if check.score < threshold or check.decision == "rejected":
            continue
        if check.site_type != "company_website" or not check.owned_domain:
            continue
        if not check.normalized_domain or check.normalized_domain in seen:
            continue
        seen.add(check.normalized_domain)
        rows.append(
            DiscoveredDomain(
                domain=check.domain,
                normalized_domain=check.normalized_domain,
                score=check.score,
                evidence={"reason": check.reason, "evidence": check.evidence},
                metadata={"url": check.url, "markdown_hash": check.crawl.markdown_hash},
            )
        )
    return rows


def _related_sites(site_checks: list[SiteCheckResult], *, threshold: int) -> list[RelatedSite]:
    rows: list[RelatedSite] = []
    seen: set[tuple[str, str]] = set()
    for check in sorted(site_checks, key=lambda item: item.score, reverse=True):
        if check.score < threshold or check.decision == "rejected" or check.relationship == "unrelated":
            continue
        key = (check.url, check.normalized_domain)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            RelatedSite(
                url=check.url,
                domain=check.domain,
                normalized_domain=check.normalized_domain,
                score=check.score,
                decision=check.decision,
                site_type=check.site_type,
                relationship=check.relationship,
                owned_domain=check.owned_domain,
                reason=check.reason,
                evidence=check.evidence,
                metadata={"markdown_hash": check.crawl.markdown_hash},
            )
        )
    return rows


def _primary_web_presence(related_sites: list[RelatedSite]) -> RelatedSite | None:
    primary_sites = [site for site in related_sites if site.relationship == "primary_web_presence"]
    if not primary_sites:
        return None
    return sorted(primary_sites, key=lambda item: (item.owned_domain, item.score), reverse=True)[0]


def _candidate_items(output: Any) -> list[dict[str, Any]]:
    if isinstance(output, dict):
        candidates = output.get("candidates")
        if isinstance(candidates, list):
            return [item for item in candidates if isinstance(item, dict)]
        if output.get("url") or output.get("domain"):
            return [output]
    if isinstance(output, list):
        return [item for item in output if isinstance(item, dict)]
    return []


def _scored_link_from_url(*, url: str, score: int, reason: str, source: str) -> ScoredLink:
    domain = domain_from_url(url)
    return ScoredLink(
        url=url,
        domain=domain,
        normalized_domain=normalize_domain(domain),
        score=score,
        reason=reason,
        source=source,
    )


def _score(value: Any) -> int:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if match := re.search(r"\b(\d{1,3})\b", normalized):
            return min(max(int(match.group(1)), 0), 100)
        if "very high" in normalized or "strong" in normalized:
            return 95
        if "high" in normalized:
            return 90
        if "medium" in normalized or "moderate" in normalized:
            return 60
        if "low" in normalized or "weak" in normalized:
            return 30
        if "none" in normalized or "no match" in normalized:
            return 0
    try:
        score = int(value or 0)
    except (TypeError, ValueError):
        score = 0
    return min(max(score, 0), 100)


def _decision(value: Any) -> str:
    normalized = str(value or "uncertain").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"accepted", "accept", "related", "matched", "match", "likely_related"}:
        return "accepted"
    if normalized in {"rejected", "reject", "unrelated", "not_related", "no_match"}:
        return "rejected"
    return "uncertain"


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _search_analysis_payload(
    *,
    request: DomainDiscoverRequest,
    response: Crawl4AiResponse,
) -> dict[str, Any]:
    return {
        "company_name": request.company_name,
        "organization_number": request.organization_number,
        "country": request.country,
        "address_lines": request.address_lines,
        "city": request.city,
        "postal_code": request.postal_code,
        "business_activity": request.business_activity,
        "statutory_purpose": request.statutory_purpose,
        "industry_codes": request.industry_codes,
        "search_engine": request.search_engine,
        "search_term": request.search_term or _default_search_term(request),
        "candidate_threshold": request.limits.search_candidate_threshold,
        "links": response.links[:30],
        "compact_markdown": _compact_markdown(response.markdown or ""),
        "timeout_seconds": request.limits.timeout_seconds,
    }


def _site_analysis_payload(
    *,
    request: DomainDiscoverRequest,
    link: ScoredLink,
    response: Crawl4AiResponse,
) -> dict[str, Any]:
    normalized_domain = domain_from_url(response.final_url) or link.normalized_domain
    classification = known_site_classification(normalized_domain) or {}
    return {
        "company_name": request.company_name,
        "organization_number": request.organization_number,
        "country": request.country,
        "address_lines": request.address_lines,
        "city": request.city,
        "postal_code": request.postal_code,
        "business_activity": request.business_activity,
        "statutory_purpose": request.statutory_purpose,
        "industry_codes": request.industry_codes,
        "url": link.url,
        "final_url": response.final_url,
        "normalized_domain": normalized_domain,
        "candidate_score": link.score,
        "candidate_reason": link.reason,
        "site_type_hint": classification.get("site_type"),
        "relationship_hint": classification.get("relationship"),
        "compact_markdown": _compact_markdown(response.markdown or ""),
        "timeout_seconds": request.limits.timeout_seconds,
    }


def _compact_markdown(markdown: str) -> str:
    lines: list[str] = []
    for raw_line in markdown.splitlines():
        line = " ".join(raw_line.strip().split())
        if not line:
            continue
        lines.append(line)
        if sum(len(item) + 1 for item in lines) >= 2500:
            break
    return "\n".join(lines)[:2500]


def _apply_known_site_overrides(output: dict[str, Any], *, normalized_domain: str) -> dict[str, Any]:
    result = dict(output)
    classification = known_site_classification(normalized_domain)
    if classification is None:
        return result
    if classification.get("site_type"):
        result["site_type"] = classification["site_type"]
    if classification.get("relationship"):
        result["relationship"] = classification["relationship"]
    result["owned_domain"] = False
    if result.get("relationship") in (None, "", "unrelated") and result.get("site_type") == "social_profile":
        result["relationship"] = "primary_web_presence"
    return result


def _apply_context_scoring_rules(output: dict[str, Any]) -> dict[str, Any]:
    result = dict(output)
    if str(result.get("site_type") or "").strip().lower() != "company_website":
        return result
    identity_signals = result.get("identity_signals")
    if not isinstance(identity_signals, dict):
        identity_signals = {}
    has_legal_identity = any(
        bool(identity_signals.get(key))
        for key in ("legal_name", "organization_number", "address_or_city")
    )
    activity_alignment = str(result.get("activity_alignment") or "").strip().lower()
    entity_scope = str(result.get("entity_scope") or "").strip().lower()

    if activity_alignment == "conflicting" and not has_legal_identity:
        result["decision"] = "uncertain"
        result["score"] = min(_score(result.get("score")), 60)
        result["relationship"] = "unrelated"
        result["owned_domain"] = False
        result["reason"] = _append_reason(
            result.get("reason"),
            "Downgraded because the page conflicts with BRREG activity context and lacks legal identity evidence.",
        )
        return result

    if entity_scope in {"parent_group", "affiliate", "umbrella_organization"} and not has_legal_identity:
        result["owned_domain"] = False
        result["relationship"] = "supporting_reference"
        result["score"] = min(_score(result.get("score")), 80)
        result["reason"] = _append_reason(
            result.get("reason"),
            "Downgraded from owned domain because the page appears to describe a parent, affiliate, or umbrella organization rather than the exact legal entity.",
        )
    return result


def _append_reason(existing: Any, suffix: str) -> str:
    text = str(existing or "").strip()
    if not text:
        return suffix
    return f"{text} {suffix}"


def _raw_website(raw_payload: dict) -> str | None:
    website = raw_payload.get("hjemmeside") or raw_payload.get("website")
    return str(website) if website else None


def _brreg_domain_request_facts(request: BrregDomainDiscoveryRequest) -> dict[str, Any]:
    raw = request.raw_payload
    address = raw.get("forretningsadresse") if isinstance(raw.get("forretningsadresse"), dict) else {}
    address_lines = address.get("adresse") if isinstance(address.get("adresse"), list) else []
    return {
        "company_name": request.organization_name or str(raw.get("navn") or request.organization_number),
        "address_lines": [str(item) for item in address_lines],
        "city": str(address.get("poststed")) if address.get("poststed") else None,
        "postal_code": str(address.get("postnummer")) if address.get("postnummer") else None,
        "business_activity": _string_values(raw.get("aktivitet")),
        "statutory_purpose": _string_values(raw.get("vedtektsfestetFormaal")),
        "industry_codes": _brreg_industry_codes(raw),
    }


def _string_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _brreg_industry_codes(raw: dict[str, Any]) -> list[str]:
    rows: list[str] = []
    for key in ("naeringskode1", "naeringskode2", "naeringskode3", "hjelpeenhetskode"):
        value = raw.get(key)
        if not isinstance(value, dict):
            continue
        code = str(value.get("kode") or "").strip()
        description = str(value.get("beskrivelse") or "").strip()
        if code and description:
            rows.append(f"{code} {description}")
        elif code or description:
            rows.append(code or description)
    return rows
