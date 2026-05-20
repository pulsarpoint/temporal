from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
import socket
import time
import urllib.parse

import httpx
from temporalio import activity

from contracts import DiscoverDomainsInput, DiscoverDomainsResult, DomainDiscovery, CompanyLookup

logger = logging.getLogger(__name__)

_USER_AGENT = "corpscout-data-pipelines/1.0"

_WIKIDATA_QUERY = """
SELECT ?company ?website WHERE {{
  ?company wdt:P856 ?website .
  ?company rdfs:label "{name}"@en .
}}
LIMIT 5
""".strip()

# Global Wikidata backoff — set on 429 to skip all calls until this monotonic time.
_WIKIDATA_BACKOFF_UNTIL: float = 0.0

_certsh_lock: asyncio.Lock | None = None
_wikidata_lock: asyncio.Lock | None = None


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


_LEGAL_RE = re.compile(
    "|".join([
        r"\bprivate limited company\b", r"\bpublic limited company\b",
        r"\baksjeselskap\b", r"\bannpartselskab\b", r"\banpartsselskab\b",
        r"\baktieselskab\b", r"\baksjonærselskap\b",
        r"\bincorporated\b", r"\bcorporation\b",
        r"\blimited liability company\b", r"\blimited liability partnership\b",
        r"\bllc\b", r"\bllp\b", r"\binc\b", r"\bltd\b", r"\bplc\b", r"\bcorp\b",
        r"\bco\b",
        r"\b(as|asa|ans|da|ba|sa|nuf|ks|sf)\b",
        r"\b(gmbh|ag|kg|ohg|kgaa|eg|gbr|ug)\b",
        r"\b(srl|spa|sas|snc|sapa|scarl)\b",
        r"\b(sarl|sas|sc|snc|sca)\b",
        r"\b(sl|sa|cb|scp)\b",
    ]),
    re.IGNORECASE,
)

_COUNTRY_TLD: dict[str, str] = {
    "NO": ".no", "DK": ".dk", "SE": ".se", "FI": ".fi",
    "GB": ".co.uk", "DE": ".de", "FR": ".fr", "NL": ".nl",
    "IT": ".it", "ES": ".es", "PT": ".pt", "PL": ".pl",
    "AT": ".at", "CH": ".ch", "BE": ".be", "CZ": ".cz",
    "HU": ".hu", "RO": ".ro", "SK": ".sk", "BG": ".bg",
    "HR": ".hr", "SI": ".si", "EE": ".ee", "LV": ".lv",
    "LT": ".lt", "US": ".com", "CA": ".ca", "AU": ".com.au",
}


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
    s = _LEGAL_RE.sub("", name).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def _candidate_domains(name: str, country: str) -> list[str]:
    slug = _company_slug(name)
    if not slug or len(slug) < 2:
        return []
    slug_clean = slug.lstrip("0123456789-") or slug
    if len(slug_clean) < 2:
        slug_clean = slug
    tlds = [".com"]
    country_tld = _COUNTRY_TLD.get(country.upper())
    if country_tld and country_tld != ".com":
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
    """Unwrap a DuckDuckGo redirect href to the real destination URL."""
    parsed = urllib.parse.urlparse(href)
    if parsed.netloc in ("", "duckduckgo.com", "www.duckduckgo.com", "html.duckduckgo.com"):
        qs = urllib.parse.parse_qs(parsed.query)
        uddg = qs.get("uddg", [None])[0]
        return uddg  # may be None if no uddg param
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
    except Exception as exc:
        logger.warning("duckduckgo signal failed for %r: %s", company_name, exc)
        return []


async def _wikidata_signal(company_name: str) -> list[tuple[str, str, int]]:
    global _WIKIDATA_BACKOFF_UNTIL  # noqa: PLW0603

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
                resp = await client.get(
                    "https://query.wikidata.org/sparql",
                    params={"query": query, "format": "json"},
                    headers={"User-Agent": _USER_AGENT, "Accept": "application/sparql-results+json"},
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("retry-after", "60"))
                    _WIKIDATA_BACKOFF_UNTIL = time.monotonic() + min(retry_after, 60)
                    logger.warning("wikidata 429 — backoff %ds", min(retry_after, 60))
                    return []
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                logger.warning("wikidata signal failed: %s", exc)
                return []

            data = resp.json()
            for binding in (data.get("results") or {}).get("bindings") or []:
                website = (binding.get("website") or {}).get("value", "")
                domain = _safe_domain(website)
                if domain and domain not in seen:
                    seen.add(domain)
                    results.append((domain, "wikidata", 85))

        await asyncio.sleep(1.0)  # respect Wikidata ~1 req/s
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
                resp = await client.get(
                    "https://crt.sh/",
                    params={"q": company_name, "output": "json"},
                    headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
                )
                resp.raise_for_status()
                entries = resp.json()
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


async def _discover_for_company(company: CompanyLookup, country: str) -> list[DomainDiscovery]:
    signal_batches = await asyncio.gather(
        _duckduckgo_signal(company.name, country),
        _wikidata_signal(company.name),
        _certsh_signal(company.name),
        _heuristic_signal(company.name, country),
        return_exceptions=True,
    )
    discoveries: list[DomainDiscovery] = []
    seen_domains: set[str] = set()
    for batch in signal_batches:
        if isinstance(batch, BaseException):
            logger.warning("signal error for %r: %s", company.name, batch)
            continue
        for domain, signal, confidence in batch:
            if domain not in seen_domains:
                seen_domains.add(domain)
                discoveries.append(DomainDiscovery(
                    native_id=company.native_id,
                    domain=domain,
                    signal=signal,
                    confidence=confidence,
                ))
    return discoveries


@activity.defn(name="discover_company_domains")
async def discover_company_domains(input: DiscoverDomainsInput) -> DiscoverDomainsResult:
    all_discoveries: list[DomainDiscovery] = []
    for i, company in enumerate(input.companies):
        if i > 0:
            await asyncio.sleep(2.0)
        try:
            discoveries = await _discover_for_company(company, input.country)
            all_discoveries.extend(discoveries)
            logger.info(
                "domain discovery: company=%r found=%d",
                company.name,
                len(discoveries),
            )
        except Exception as exc:
            logger.warning("domain discovery failed for company %r: %s", company.name, exc)
    return DiscoverDomainsResult(discoveries=all_discoveries)
