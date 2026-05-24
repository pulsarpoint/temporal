from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import socket
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx


logger = logging.getLogger(__name__)

_USER_AGENT = "corpscout-dagster/1.0"

_WIKIDATA_QUERY = """
SELECT ?company ?website WHERE {{
  ?company wdt:P856 ?website .
  ?company rdfs:label "{name}"@en .
}}
LIMIT 5
""".strip()

_WIKIDATA_BACKOFF_UNTIL: float = 0.0

_certsh_lock: asyncio.Lock | None = None
_wikidata_lock: asyncio.Lock | None = None

_LEGAL_RE = re.compile(
    "|".join(
        [
            r"\bprivate limited company\b",
            r"\bpublic limited company\b",
            r"\baksjeselskap\b",
            r"\bannpartselskab\b",
            r"\banpartsselskab\b",
            r"\baktieselskab\b",
            r"\baksjonærselskap\b",
            r"\bincorporated\b",
            r"\bcorporation\b",
            r"\blimited liability company\b",
            r"\blimited liability partnership\b",
            r"\bllc\b",
            r"\bllp\b",
            r"\binc\b",
            r"\bltd\b",
            r"\bplc\b",
            r"\bcorp\b",
            r"\bco\b",
            r"\b(as|asa|ans|da|ba|sa|nuf|ks|sf)\b",
            r"\b(gmbh|ag|kg|ohg|kgaa|eg|gbr|ug)\b",
            r"\b(srl|spa|sas|snc|sapa|scarl)\b",
            r"\b(sarl|sas|sc|snc|sca)\b",
            r"\b(sl|sa|cb|scp)\b",
        ]
    ),
    re.IGNORECASE,
)

_COUNTRY_TLD_EXCEPTIONS: dict[str, str] = {
    "GB": ".co.uk",
    "US": ".com",
    "AU": ".com.au",
    "NZ": ".co.nz",
    "JP": ".co.jp",
    "KR": ".co.kr",
    "BR": ".com.br",
    "MX": ".com.mx",
    "AR": ".com.ar",
    "ZA": ".co.za",
    "IN": ".co.in",
}


@dataclass(frozen=True)
class DomainCandidate:
    domain: str
    normalized_domain: str
    signal: str
    confidence: int
    evidence: dict[str, Any]
    metadata: dict[str, Any]


async def discover_domain_candidates(
    *,
    raw_payload: dict[str, Any],
    organization_number: str,
    organization_name: str | None,
    website: str | None,
    country: str = "NO",
) -> list[DomainCandidate]:
    candidates = extract_domain_candidates(raw_payload=raw_payload, website=website)
    if candidates:
        return _deduplicate_candidates(candidates)
    name = _company_name(raw_payload=raw_payload, organization_name=organization_name)
    if name:
        signal_batches = await asyncio.gather(
            _duckduckgo_signal(name, country),
            _wikidata_signal(name),
            _certsh_signal(name),
            _heuristic_signal(name, country),
            return_exceptions=True,
        )
        for batch in signal_batches:
            if isinstance(batch, BaseException):
                logger.warning("domain signal error for %r: %s", name, batch)
                continue
            candidates.extend(
                _candidate_from_signal(
                    domain=domain,
                    signal=signal,
                    confidence=confidence,
                    organization_number=organization_number,
                    organization_name=name,
                    country=country,
                )
                for domain, signal, confidence in batch
            )
    return _deduplicate_candidates(candidates)


def extract_domain_candidates(*, raw_payload: dict[str, Any], website: str | None) -> list[DomainCandidate]:
    source_value = website or _string_or_none(raw_payload.get("hjemmeside"))
    if not source_value:
        return []
    normalized = normalize_domain(source_value)
    if normalized is None:
        return []
    parsed_domain = _domain_from_value(source_value)
    return [
        DomainCandidate(
            domain=parsed_domain,
            normalized_domain=normalized,
            signal="website_field",
            confidence=95,
            evidence={"website": source_value},
            metadata={"source_field": "website" if website else "hjemmeside"},
        )
    ]


def normalize_domain(value: str) -> str | None:
    domain = _safe_domain(value) or _domain_from_value(value).lower().strip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    if "." not in domain or " " in domain:
        return None
    return domain


def _certsh_lock_() -> asyncio.Lock:
    global _certsh_lock
    if _certsh_lock is None:
        _certsh_lock = asyncio.Lock()
    return _certsh_lock


def _wikidata_lock_() -> asyncio.Lock:
    global _wikidata_lock
    if _wikidata_lock is None:
        _wikidata_lock = asyncio.Lock()
    return _wikidata_lock


def _country_tld(country: str) -> str:
    code = country.upper()
    return _COUNTRY_TLD_EXCEPTIONS.get(code, f".{code.lower()}")


def _safe_domain(url: str) -> str | None:
    if not url:
        return None
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc or parsed.path
    if not host:
        return None
    host = host.split("@")[-1].split(":")[0].lower().strip().rstrip(".")
    while host.startswith("*."):
        host = host[2:]
    if not host or host == "localhost":
        return None
    try:
        ipaddress.ip_address(host)
        return None
    except ValueError:
        pass
    if "." not in host:
        return None
    return host


def _company_slug(name: str) -> str:
    value = _LEGAL_RE.sub("", name).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def _candidate_domains(name: str, country: str) -> list[str]:
    slug = _company_slug(name)
    if not slug or len(slug) < 2:
        return []
    slug_clean = slug.lstrip("0123456789-") or slug
    if len(slug_clean) < 2:
        slug_clean = slug
    tlds = [".com"]
    country_tld = _country_tld(country)
    if country_tld != ".com":
        tlds = [country_tld, ".com"]
    seen: set[str] = set()
    candidates: list[str] = []
    for tld in tlds:
        domain = slug_clean + tld
        if domain not in seen:
            seen.add(domain)
            candidates.append(domain)
        slug_nohyphen = slug_clean.replace("-", "")
        if slug_nohyphen != slug_clean and len(slug_nohyphen) >= 3:
            domain2 = slug_nohyphen + tld
            if domain2 not in seen:
                seen.add(domain2)
                candidates.append(domain2)
    return candidates


def _dns_resolve(domain: str) -> bool:
    try:
        socket.getaddrinfo(domain, None, proto=socket.IPPROTO_TCP)
        return True
    except OSError:
        return False


def _sparql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _extract_ddg_url(href: str) -> str | None:
    parsed = urllib.parse.urlparse(href)
    if parsed.netloc in ("", "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com"):
        parsed_qs = urllib.parse.parse_qs(parsed.query)
        return parsed_qs.get("uddg", [None])[0]
    return href


async def _duckduckgo_signal(company_name: str, country: str) -> list[tuple[str, str, int]]:
    try:
        from crawl4ai import AsyncWebCrawler  # type: ignore[import]

        query = urllib.parse.quote(f'"{company_name}" official website')
        url = f"https://html.duckduckgo.com/html/?q={query}"

        async with AsyncWebCrawler(verbose=False) as crawler:
            result = await crawler.arun(url=url)

        results: list[tuple[str, str, int]] = []
        seen: set[str] = set()
        for link in list((result.links or {}).get("external", []))[:15]:
            href = link.get("href", "")
            real_url = _extract_ddg_url(href) or href
            domain = _safe_domain(real_url)
            if domain and "duckduckgo.com" not in domain and domain not in seen:
                seen.add(domain)
                results.append((domain, "duckduckgo", 70))
                if len(results) >= 5:
                    break
        return results
    except ModuleNotFoundError:
        logger.debug("duckduckgo signal skipped because crawl4ai is not installed")
        return []
    except Exception as exc:
        logger.warning("duckduckgo signal failed for %r: %s", company_name, exc)
        return []


async def _wikidata_signal(company_name: str) -> list[tuple[str, str, int]]:
    global _WIKIDATA_BACKOFF_UNTIL

    if time.monotonic() < _WIKIDATA_BACKOFF_UNTIL:
        return []

    lock = _wikidata_lock_()
    if lock.locked():
        return []

    query = _WIKIDATA_QUERY.format(name=_sparql_literal(company_name))
    results: list[tuple[str, str, int]] = []
    seen: set[str] = set()

    async with lock:
        if time.monotonic() < _WIKIDATA_BACKOFF_UNTIL:
            return []
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    "https://query.wikidata.org/sparql",
                    params={"query": query, "format": "json"},
                    headers={"User-Agent": _USER_AGENT, "Accept": "application/sparql-results+json"},
                )
                if response.status_code == 429:
                    retry_after = int(response.headers.get("retry-after", "60"))
                    _WIKIDATA_BACKOFF_UNTIL = time.monotonic() + min(retry_after, 60)
                    logger.warning("wikidata 429, backing off for %ds", min(retry_after, 60))
                    return []
                response.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("wikidata signal failed: %s", exc)
                return []

            data = response.json()
            for binding in (data.get("results") or {}).get("bindings") or []:
                website = (binding.get("website") or {}).get("value", "")
                domain = _safe_domain(website)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append((domain, "wikidata", 85))

        await asyncio.sleep(1.0)
    return results


async def _certsh_signal(company_name: str) -> list[tuple[str, str, int]]:
    lock = _certsh_lock_()
    if lock.locked():
        return []

    results: list[tuple[str, str, int]] = []
    seen: set[str] = set()

    async with lock:
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                response = await client.get(
                    "https://crt.sh/",
                    params={"q": company_name, "output": "json"},
                    headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                )
                response.raise_for_status()
                entries = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                logger.warning("crt.sh signal failed: %s", exc)
                return []

        if isinstance(entries, list):
            for entry in entries:
                for raw in str(entry.get("name_value") or "").splitlines():
                    domain = _safe_domain(raw.strip())
                    if domain and domain not in seen:
                        seen.add(domain)
                        results.append((domain, "certsh", 60))

    await asyncio.sleep(0.5)
    return results


async def _heuristic_signal(company_name: str, country: str) -> list[tuple[str, str, int]]:
    candidates = _candidate_domains(company_name, country)
    if not candidates:
        return []
    results: list[tuple[str, str, int]] = []
    loop = asyncio.get_event_loop()
    for domain in candidates:
        try:
            resolves = await loop.run_in_executor(None, _dns_resolve, domain)
        except Exception:
            resolves = False
        if resolves:
            results.append((domain, "heuristic", 40))
    return results


def _candidate_from_signal(
    *,
    domain: str,
    signal: str,
    confidence: int,
    organization_number: str,
    organization_name: str,
    country: str,
) -> DomainCandidate:
    normalized = normalize_domain(domain)
    if normalized is None:
        raise ValueError(f"invalid domain candidate {domain!r}")
    return DomainCandidate(
        domain=_domain_from_value(domain),
        normalized_domain=normalized,
        signal=signal,
        confidence=max(1, min(100, confidence)),
        evidence={"organization_number": organization_number, "organization_name": organization_name},
        metadata={"country": country, "source": "dagster", "port": "temporal.discover_company_domains"},
    )


def _deduplicate_candidates(candidates: list[DomainCandidate]) -> list[DomainCandidate]:
    seen: set[str] = set()
    unique: list[DomainCandidate] = []
    for candidate in candidates:
        if candidate.normalized_domain in seen:
            continue
        seen.add(candidate.normalized_domain)
        unique.append(candidate)
    return unique


def _company_name(*, raw_payload: dict[str, Any], organization_name: str | None) -> str:
    if organization_name and organization_name.strip():
        return organization_name.strip()
    return _string_or_none(raw_payload.get("navn")) or ""


def _domain_from_value(value: str) -> str:
    trimmed = value.strip()
    parsed = urlparse(trimmed if "://" in trimmed else f"https://{trimmed}")
    return (parsed.hostname or trimmed).strip().lower()


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
