from __future__ import annotations

import json
import logging
import os
import urllib.parse
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

import httpx

from corpscout_dagster.brreg.domain_enrichment import DomainCandidate, normalize_domain
from corpscout_dagster.brreg.translation import _clean_json_content, _openai_api_base


logger = logging.getLogger(__name__)

DEFAULT_DOMAIN_LLM_BASE_URL = "https://api.deepseek.com"
DEFAULT_DOMAIN_LLM_MODEL = "deepseek-v4-flash"
DEFAULT_DOMAIN_LLM_PROMPT_VERSION = "v1"
DEFAULT_TRIAGE_THRESHOLD = 50
DEFAULT_VERIFICATION_THRESHOLD = 60
DEFAULT_MAX_VERIFIED_CANDIDATES = 3
DEFAULT_MAX_MARKDOWN_CHARS = 6000
DOMAIN_WEB_SEARCH_SIGNAL = "web_search_llm"
EXCLUDED_SEARCH_RESULT_DOMAINS = {
    "1881.no",
    "brreg.no",
    "eniro.no",
    "gulesider.no",
    "kompass.com",
    "nor47business.com",
    "proff.no",
    "purehelp.no",
    "regnskapstall.no",
    "virk.dk",
    "yra.no",
}


@dataclass(frozen=True)
class DomainSearchCompanyFacts:
    organization_number: str
    organization_name: str
    country: str
    address: str | None = None


@dataclass(frozen=True)
class SearchResult:
    query: str
    rank: int
    url: str
    domain: str
    normalized_domain: str
    title: str
    description: str


@dataclass(frozen=True)
class TriageDecision:
    normalized_domain: str
    confidence: int
    reason: str


@dataclass(frozen=True)
class VerificationDecision:
    normalized_domain: str
    confidence: int
    reason: str
    matched_evidence: list[str]
    reject_reason: str


class DomainSearchLLM(Protocol):
    def triage_search_results(
        self,
        *,
        company: DomainSearchCompanyFacts,
        results: list[SearchResult],
        prompt_version: str,
    ) -> list[TriageDecision]:
        ...

    def verify_candidate(
        self,
        *,
        company: DomainSearchCompanyFacts,
        result: SearchResult,
        markdown: str,
        prompt_version: str,
    ) -> VerificationDecision | None:
        ...


class MissingDomainLLMConfig(RuntimeError):
    pass


class DirectDomainSearchLLM:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 90,
    ) -> None:
        self.model = model
        self._base_url = _openai_api_base(base_url)
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> DirectDomainSearchLLM:
        api_key = os.environ.get("DOMAIN_LLM_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise MissingDomainLLMConfig("DOMAIN_LLM_API_KEY or DEEPSEEK_API_KEY is required")
        return cls(
            base_url=os.environ.get("DOMAIN_LLM_BASE_URL") or os.environ.get("DEEPSEEK_BASE_URL") or DEFAULT_DOMAIN_LLM_BASE_URL,
            api_key=api_key,
            model=os.environ.get("DOMAIN_LLM_MODEL") or os.environ.get("DEEPSEEK_MODEL") or DEFAULT_DOMAIN_LLM_MODEL,
        )

    def triage_search_results(
        self,
        *,
        company: DomainSearchCompanyFacts,
        results: list[SearchResult],
        prompt_version: str,
    ) -> list[TriageDecision]:
        if not results:
            return []
        content = self._chat_completion(
            messages=build_search_triage_messages(
                company=company,
                results=results,
                prompt_version=prompt_version,
            ),
            max_tokens=1200,
        )
        return parse_search_triage_response(
            content,
            allowed_domains={result.normalized_domain for result in results},
        )

    def verify_candidate(
        self,
        *,
        company: DomainSearchCompanyFacts,
        result: SearchResult,
        markdown: str,
        prompt_version: str,
    ) -> VerificationDecision | None:
        content = self._chat_completion(
            messages=build_domain_verification_messages(
                company=company,
                result=result,
                markdown=markdown,
                prompt_version=prompt_version,
            ),
            max_tokens=1200,
        )
        return parse_domain_verification_response(content, expected_domain=result.normalized_domain)

    def _chat_completion(self, *, messages: list[dict[str, str]], max_tokens: int) -> str:
        response = httpx.post(
            f"{self._base_url}/chat/completions",
            headers={"Authorization": f"Bearer {self._api_key}"},
            json={
                "model": self.model,
                "messages": messages,
                "temperature": 0,
                "max_tokens": max_tokens,
            },
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        return str(response.json()["choices"][0]["message"]["content"])


async def discover_web_search_llm_domain_candidates(
    *,
    raw_payload: dict[str, Any],
    organization_number: str,
    organization_name: str | None,
    country: str = "NO",
    classifier: DomainSearchLLM | None = None,
    crawler_factory=None,
    triage_threshold: int = DEFAULT_TRIAGE_THRESHOLD,
    verification_threshold: int = DEFAULT_VERIFICATION_THRESHOLD,
    max_verified_candidates: int = DEFAULT_MAX_VERIFIED_CANDIDATES,
    prompt_version: str = DEFAULT_DOMAIN_LLM_PROMPT_VERSION,
) -> list[DomainCandidate]:
    prompt_version = os.environ.get("DOMAIN_LLM_PROMPT_VERSION") or prompt_version
    company = build_domain_search_company_facts(
        raw_payload=raw_payload,
        organization_number=organization_number,
        organization_name=organization_name,
        country=country,
    )
    if not company.organization_name:
        return []
    if classifier is None:
        try:
            classifier = DirectDomainSearchLLM.from_env()
        except MissingDomainLLMConfig as exc:
            logger.warning("web search LLM domain signal skipped: %s", exc)
            return []
    if crawler_factory is None:
        try:
            from crawl4ai import AsyncWebCrawler  # type: ignore[import]
        except ModuleNotFoundError:
            logger.warning("web search LLM domain signal skipped because crawl4ai is not installed")
            return []
        crawler_factory = lambda: AsyncWebCrawler(config=domain_crawler_browser_config_from_env())

    candidates: list[DomainCandidate] = []
    async with crawler_factory() as crawler:
        for query in build_domain_search_queries(company):
            search_results = await crawl_duckduckgo_first_page(crawler=crawler, query=query)
            if not search_results:
                continue

            triage = classifier.triage_search_results(
                company=company,
                results=search_results,
                prompt_version=prompt_version,
            )
            triage_by_domain = {
                decision.normalized_domain: decision
                for decision in triage
                if decision.confidence > triage_threshold
            }
            if not triage_by_domain:
                continue

            for result in _selected_results_for_verification(search_results, triage_by_domain)[:max_verified_candidates]:
                markdown = await crawl_candidate_markdown(crawler=crawler, url=result.url)
                verification = classifier.verify_candidate(
                    company=company,
                    result=result,
                    markdown=markdown,
                    prompt_version=prompt_version,
                )
                if verification is None or verification.confidence < verification_threshold:
                    continue
                candidates.append(
                    _candidate_from_verified_search_result(
                        company=company,
                        result=result,
                        triage=triage_by_domain[result.normalized_domain],
                        verification=verification,
                        markdown=markdown,
                        prompt_version=prompt_version,
                        model=getattr(classifier, "model", None),
                    )
                )
            if candidates:
                break

    return _deduplicate_candidates(candidates)


def domain_crawler_browser_config_from_env():
    from crawl4ai import BrowserConfig  # type: ignore[import]

    chrome_channel = os.environ.get("DOMAIN_CRAWLER_CHROME_CHANNEL", "chromium").strip() or "chromium"
    return BrowserConfig(
        browser_type=os.environ.get("DOMAIN_CRAWLER_BROWSER_TYPE", "chromium").strip() or "chromium",
        headless=_env_bool("DOMAIN_CRAWLER_HEADLESS", default=True),
        chrome_channel=chrome_channel,
        channel=chrome_channel,
        ignore_https_errors=True,
        light_mode=_env_bool("DOMAIN_CRAWLER_LIGHT_MODE", default=True),
        use_managed_browser=_env_bool("DOMAIN_CRAWLER_USE_MANAGED_BROWSER", default=False),
        enable_stealth=_env_bool("DOMAIN_CRAWLER_ENABLE_STEALTH", default=True),
        verbose=False,
    )


def build_domain_search_company_facts(
    *,
    raw_payload: dict[str, Any],
    organization_number: str,
    organization_name: str | None,
    country: str,
) -> DomainSearchCompanyFacts:
    return DomainSearchCompanyFacts(
        organization_number=organization_number,
        organization_name=(organization_name or _string_or_none(raw_payload.get("navn")) or "").strip(),
        country=country,
        address=_business_address_text(raw_payload),
    )


def build_domain_search_queries(company: DomainSearchCompanyFacts) -> list[str]:
    name = company.organization_name.strip()
    if not name:
        return []
    country_text = "Norway" if company.country.upper() == "NO" else company.country.upper()
    values = [
        f'"{name}" {country_text} official website',
        f'"{name}" "{company.organization_number}"' if company.organization_number else "",
        f'"{name}" "{company.address}"' if company.address else "",
    ]
    queries: list[str] = []
    seen: set[str] = set()
    for value in values:
        stripped = value.strip()
        if stripped and stripped not in seen:
            seen.add(stripped)
            queries.append(stripped)
    return queries


async def crawl_duckduckgo_first_page(*, crawler, query: str) -> list[SearchResult]:
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    result = await crawler.arun(url=url)
    return search_results_from_crawl_result(query=query, crawl_result=result)


async def crawl_candidate_markdown(*, crawler, url: str) -> str:
    result = await crawler.arun(url=url)
    return _truncate(_markdown_from_result(result), DEFAULT_MAX_MARKDOWN_CHARS)


def search_results_from_crawl_result(*, query: str, crawl_result) -> list[SearchResult]:
    rows: list[SearchResult] = []
    seen: set[str] = set()
    links = getattr(crawl_result, "links", None) or {}
    for link in [*list(links.get("external") or []), *list(links.get("internal") or [])]:
        if not isinstance(link, dict):
            continue
        href = str(link.get("href") or "")
        real_url = _extract_duckduckgo_url(href) or href
        normalized = normalize_domain(real_url)
        if (
            normalized is None
            or "duckduckgo.com" in normalized
            or _is_excluded_search_result_domain(normalized)
            or normalized in seen
        ):
            continue
        seen.add(normalized)
        rows.append(
            SearchResult(
                query=query,
                rank=len(rows) + 1,
                url=real_url,
                domain=_domain_from_value(real_url),
                normalized_domain=normalized,
                title=_link_text(link, "title") or _link_text(link, "text"),
                description=_link_text(link, "description") or _link_text(link, "snippet"),
            )
        )
        if len(rows) >= 10:
            break
    return rows


def build_search_triage_messages(
    *,
    company: DomainSearchCompanyFacts,
    results: list[SearchResult],
    prompt_version: str,
) -> list[dict[str, str]]:
    payload = {
        "prompt_version": prompt_version,
        "company": company.__dict__,
        "search_results": [_search_result_payload(result) for result in results],
    }
    return [
        {
            "role": "system",
            "content": (
                "You identify whether DuckDuckGo search result domains are probably the official website "
                "for a Norwegian registry company. Judge only domains present in the input. Return JSON only."
            ),
        },
        {
            "role": "user",
            "content": (
                'Return JSON: {"candidates":[{"normalized_domain":"...","confidence":0-100,"reason":"..."}]}.\n'
                "Use confidence above 50 only when the result title, description, or domain plausibly match the company.\n"
                f"Input: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
            ),
        },
    ]


def build_domain_verification_messages(
    *,
    company: DomainSearchCompanyFacts,
    result: SearchResult,
    markdown: str,
    prompt_version: str,
) -> list[dict[str, str]]:
    payload = {
        "prompt_version": prompt_version,
        "company": company.__dict__,
        "candidate": _search_result_payload(result),
        "markdown": _truncate(markdown, DEFAULT_MAX_MARKDOWN_CHARS),
    }
    return [
        {
            "role": "system",
            "content": (
                "You verify whether a crawled website belongs to a specific Norwegian registry company. "
                "Require page evidence such as legal name, organization number, address, contact details, "
                "or strongly matching business description. Return JSON only."
            ),
        },
        {
            "role": "user",
            "content": (
                '{"normalized_domain":"...","confidence":0-100,"reason":"...",'
                '"matched_evidence":["..."],"reject_reason":"..."}\n'
                "Use low confidence when the page only has a similar domain name but no company evidence.\n"
                f"Input: {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
            ),
        },
    ]


def parse_search_triage_response(content: str, *, allowed_domains: set[str]) -> list[TriageDecision]:
    parsed = json.loads(_clean_json_content(content))
    values = parsed.get("candidates") if isinstance(parsed, dict) else parsed
    if not isinstance(values, list):
        return []
    decisions: list[TriageDecision] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        normalized = normalize_domain(str(value.get("normalized_domain") or value.get("domain") or ""))
        confidence = _int_or_none(value.get("confidence"))
        if normalized not in allowed_domains or confidence is None:
            continue
        decisions.append(
            TriageDecision(
                normalized_domain=normalized,
                confidence=_clamp_confidence(confidence),
                reason=str(value.get("reason") or "").strip(),
            )
        )
    return decisions


def parse_domain_verification_response(content: str, *, expected_domain: str) -> VerificationDecision | None:
    parsed = json.loads(_clean_json_content(content))
    if not isinstance(parsed, dict):
        return None
    normalized = normalize_domain(str(parsed.get("normalized_domain") or parsed.get("domain") or expected_domain))
    confidence = _int_or_none(parsed.get("confidence"))
    if normalized != expected_domain or confidence is None:
        return None
    evidence = parsed.get("matched_evidence") or []
    return VerificationDecision(
        normalized_domain=normalized,
        confidence=_clamp_confidence(confidence),
        reason=str(parsed.get("reason") or "").strip(),
        matched_evidence=[str(item).strip() for item in evidence if str(item).strip()] if isinstance(evidence, list) else [],
        reject_reason=str(parsed.get("reject_reason") or "").strip(),
    )


def _selected_results_for_verification(
    search_results: list[SearchResult],
    triage_by_domain: dict[str, TriageDecision],
) -> list[SearchResult]:
    selected = [result for result in search_results if result.normalized_domain in triage_by_domain]
    return sorted(selected, key=lambda result: (-triage_by_domain[result.normalized_domain].confidence, result.rank))


def _candidate_from_verified_search_result(
    *,
    company: DomainSearchCompanyFacts,
    result: SearchResult,
    triage: TriageDecision,
    verification: VerificationDecision,
    markdown: str,
    prompt_version: str,
    model: str | None,
) -> DomainCandidate:
    return DomainCandidate(
        domain=result.domain,
        normalized_domain=result.normalized_domain,
        signal=DOMAIN_WEB_SEARCH_SIGNAL,
        confidence=verification.confidence,
        evidence={
            "organization_number": company.organization_number,
            "organization_name": company.organization_name,
            "search_result": _search_result_payload(result),
            "triage": triage.__dict__,
            "verification": verification.__dict__,
            "markdown_excerpt": _truncate(markdown, 1200),
        },
        metadata={
            "country": company.country,
            "source": "dagster",
            "signal": DOMAIN_WEB_SEARCH_SIGNAL,
            "model": model or "",
            "prompt_version": prompt_version,
        },
    )


def _search_result_payload(result: SearchResult) -> dict[str, Any]:
    return {
        "query": result.query,
        "rank": result.rank,
        "url": result.url,
        "domain": result.domain,
        "normalized_domain": result.normalized_domain,
        "title": result.title,
        "description": result.description,
    }


def _business_address_text(raw_payload: dict[str, Any]) -> str | None:
    address = raw_payload.get("forretningsadresse")
    if not isinstance(address, dict):
        return None
    parts: list[str] = []
    street = address.get("adresse")
    if isinstance(street, list):
        parts.extend(str(item).strip() for item in street if str(item).strip())
    elif isinstance(street, str) and street.strip():
        parts.append(street.strip())
    for key in ("poststed", "postnummer", "kommune"):
        value = address.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    if not parts:
        return None
    return " ".join(dict.fromkeys(parts))


def _markdown_from_result(result) -> str:
    markdown = getattr(result, "markdown", "")
    if isinstance(markdown, str):
        return markdown
    for attr in ("raw_markdown", "fit_markdown", "markdown"):
        value = getattr(markdown, attr, None)
        if isinstance(value, str):
            return value
    return str(markdown or "")


def _extract_duckduckgo_url(href: str) -> str | None:
    parsed = urllib.parse.urlparse(href)
    if parsed.netloc in ("", "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com"):
        parsed_qs = urllib.parse.parse_qs(parsed.query)
        return parsed_qs.get("uddg", [None])[0]
    return href


def _domain_from_value(value: str) -> str:
    trimmed = value.strip()
    parsed = urlparse(trimmed if "://" in trimmed else f"https://{trimmed}")
    return (parsed.hostname or trimmed).strip().lower()


def _link_text(link: dict[str, Any], key: str) -> str:
    value = link.get(key)
    return str(value).strip() if value is not None else ""


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clamp_confidence(value: int) -> int:
    return max(1, min(100, value))


def _truncate(value: str, max_chars: int) -> str:
    stripped = value.strip()
    if len(stripped) <= max_chars:
        return stripped
    return stripped[:max_chars].rsplit(" ", 1)[0].strip()


def _deduplicate_candidates(candidates: list[DomainCandidate]) -> list[DomainCandidate]:
    seen: set[str] = set()
    unique: list[DomainCandidate] = []
    for candidate in sorted(candidates, key=lambda item: -item.confidence):
        if candidate.normalized_domain in seen:
            continue
        seen.add(candidate.normalized_domain)
        unique.append(candidate)
    return unique


def _is_excluded_search_result_domain(normalized_domain: str) -> bool:
    return any(
        normalized_domain == excluded or normalized_domain.endswith(f".{excluded}")
        for excluded in EXCLUDED_SEARCH_RESULT_DOMAINS
    )


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
