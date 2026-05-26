from __future__ import annotations

import re
from typing import Any, Protocol

import httpx
from json_repair import repair_json

from corpscout_crawl_service.crawl4ai_service import LlmConfig, llm_config_from_env


class SiteAnalyzer(Protocol):
    async def analyze_site(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class SearchAnalyzer(Protocol):
    async def analyze_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class DirectLlmAnalyzer:
    def __init__(self, *, llm_config: LlmConfig | None = None) -> None:
        self._llm_config = llm_config or llm_config_from_env()

    async def analyze_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._completion_json(
            _search_analysis_prompt(payload),
            timeout_seconds=payload.get("timeout_seconds", 60),
            list_key="candidates",
        )

    async def analyze_site(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._completion_json(
            _site_analysis_prompt(payload),
            timeout_seconds=payload.get("timeout_seconds", 60),
            list_key=None,
        )

    async def _completion_json(self, prompt: str, *, timeout_seconds: int, list_key: str | None) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                f"{self._llm_config.base_url}/chat/completions",
                headers={
                    "authorization": f"Bearer {self._llm_config.api_key or 'local-no-key-required'}",
                    "content-type": "application/json",
                },
                json={
                    "model": self._llm_config.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a strict JSON API. Respond with one valid JSON object only. "
                                "Do not use markdown, comments, prose, or schema explanations."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0,
                    "max_tokens": 700,
                },
            )
            response.raise_for_status()
        data = response.json()
        content = str(data["choices"][0]["message"].get("content") or "")
        return _parse_llm_json(content, list_key=list_key)


DirectLlmSiteAnalyzer = DirectLlmAnalyzer
DirectLlmSearchAnalyzer = DirectLlmAnalyzer


def _parse_llm_json(content: str, *, list_key: str | None) -> dict[str, Any]:
    text = _strip_reasoning_blocks(content)
    for candidate in _json_candidates(text):
        try:
            repaired = repair_json(candidate, return_objects=True)
        except Exception:
            continue
        if isinstance(repaired, list) and list_key is not None:
            return {list_key: repaired}
        if isinstance(repaired, dict):
            return repaired
    excerpt = " ".join(text.split())[:300]
    raise ValueError(f"LLM analysis output was not parseable JSON: {excerpt}")


def _strip_reasoning_blocks(content: str) -> str:
    return re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL | re.IGNORECASE).strip()


def _json_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    candidates.extend(
        match.group(1).strip()
        for match in re.finditer(r"```(?:json)?\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
        if match.group(1).strip()
    )
    candidates.append(text)
    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        end = text.rfind(closer)
        if start >= 0 and end > start:
            candidates.append(text[start : end + 1])
    unique: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def _search_analysis_prompt(payload: dict[str, Any]) -> str:
    return (
        "Score search-result links for whether they are likely web presences for a company.\n"
        "Return only a JSON object with field candidates.\n"
        "candidates must be an array of objects with: url, domain, score, reason, evidence.\n"
        "score must be an integer from 0 to 100, never a word such as high/medium/low. "
        "Score 50+ only when the link is plausibly related to the company. "
        "Directory, registry, social, reference, and marketplace pages may be related, but they are not owned domains.\n"
        "Prefer candidate official company websites, but do not invent URLs not present in the supplied links. "
        "Use the registered address, business activity, statutory purpose, and industry codes as context when titles/snippets are ambiguous.\n\n"
        f"Company: {payload.get('company_name')}\n"
        f"Organization number: {payload.get('organization_number') or ''}\n"
        f"Country: {payload.get('country') or ''}\n"
        f"City: {payload.get('city') or ''}\n"
        f"Postal code: {payload.get('postal_code') or ''}\n"
        f"Address lines: {payload.get('address_lines') or []}\n"
        f"Business activity: {payload.get('business_activity') or []}\n"
        f"Statutory purpose: {payload.get('statutory_purpose') or []}\n"
        f"Industry codes: {payload.get('industry_codes') or []}\n"
        f"Search engine: {payload.get('search_engine')}\n"
        f"Search term: {payload.get('search_term')}\n"
        f"Candidate threshold: {payload.get('candidate_threshold')}\n\n"
        "Links extracted from the search page:\n"
        f"{payload.get('links') or []}\n\n"
        "Compact search-page markdown:\n"
        f"{payload.get('compact_markdown') or ''}\n"
    )


def _site_analysis_prompt(payload: dict[str, Any]) -> str:
    return (
        "Classify whether a crawled web page is related to a company.\n"
        "Do not output schema, markdown, or explanations. Output exactly one JSON object using this shape:\n"
        '{"decision":"accepted|rejected|uncertain","score":0,'
        '"site_type":"company_website|social_profile|directory_profile|registry_profile|reference_page|marketplace_profile|unrelated",'
        '"relationship":"primary_web_presence|evidence_profile|supporting_reference|unrelated",'
        '"owned_domain":false,"activity_alignment":"aligned|conflicting|unknown",'
        '"entity_scope":"target_entity|parent_group|affiliate|umbrella_organization|unrelated_entity|unknown",'
        '"identity_signals":{"legal_name":false,"organization_number":false,"address_or_city":false,"activity_or_industry":false,"brand_or_domain":false},'
        '"reason":"short reason","evidence":["short quoted evidence"]}\n'
        "Rules: score is integer 0-100, not high/medium/low. "
        "decision is accepted, rejected, or uncertain only. "
        "owned_domain is true only when the domain appears controlled by the exact company. "
        "Social, directory, registry, and reference pages are not owned domains. "
        "If the page topic conflicts with registered activity, purpose, or industry and lacks legal name, organization number, or address/city evidence, owned_domain must be false. "
        "If the page is a parent group, franchise, lodge network, marketplace, or umbrella organization rather than the exact legal entity, use relationship=supporting_reference and owned_domain=false unless the exact legal entity is identified.\n\n"
        f"Company: {payload.get('company_name')}\n"
        f"Organization number: {payload.get('organization_number') or ''}\n"
        f"Country: {payload.get('country') or ''}\n"
        f"City: {payload.get('city') or ''}\n"
        f"Postal code: {payload.get('postal_code') or ''}\n"
        f"Address lines: {payload.get('address_lines') or []}\n"
        f"Business activity: {payload.get('business_activity') or []}\n"
        f"Statutory purpose: {payload.get('statutory_purpose') or []}\n"
        f"Industry codes: {payload.get('industry_codes') or []}\n"
        f"Candidate URL: {payload.get('url')}\n"
        f"Candidate domain: {payload.get('normalized_domain')}\n"
        f"Known site type hint: {payload.get('site_type_hint') or ''}\n"
        f"Known relationship hint: {payload.get('relationship_hint') or ''}\n\n"
        "Crawled page compact content:\n"
        f"{payload.get('compact_markdown') or ''}\n"
    )
