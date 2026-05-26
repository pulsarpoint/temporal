from __future__ import annotations

import os

from corpscout_crawl_service.domain_utils import normalize_domain


DEFAULT_EXCLUDED_SEARCH_RESULT_DOMAINS = {
    "1881.no",
    "21st.ai",
    "brreg.no",
    "creditsafe.com",
    "dnb.com",
    "eniro.no",
    "europages.co.uk",
    "firmadatabasen.no",
    "gulesider.no",
    "infobel.com",
    "kompass.com",
    "largestcompanies.com",
    "nor47business.com",
    "norwayregistry.org",
    "norsktakst.no",
    "proff.no",
    "purehelp.no",
    "regnskapstall.no",
    "takserer.no",
    "virk.dk",
    "webagent.no",
    "yra.no",
}

DIRECTORY_PROFILE_DOMAINS = {
    "1881.no",
    "21st.ai",
    "creditsafe.com",
    "dnb.com",
    "eniro.no",
    "europages.co.uk",
    "firmadatabasen.no",
    "gulesider.no",
    "infobel.com",
    "kompass.com",
    "largestcompanies.com",
    "nor47business.com",
    "norwayregistry.org",
    "norsktakst.no",
    "proff.no",
    "purehelp.no",
    "regnskapstall.no",
    "takserer.no",
    "webagent.no",
    "yra.no",
}
REGISTRY_PROFILE_DOMAINS = {"brreg.no", "virk.dk"}
SOCIAL_PROFILE_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
REFERENCE_PAGE_DOMAINS = {"wikipedia.org", "wikidata.org"}


def search_result_exclusion_reason(normalized_domain: str) -> dict[str, str] | None:
    normalized_domain = normalize_domain(normalized_domain)
    if not normalized_domain:
        return None
    if _matching_configured_domain(normalized_domain, _configured_allowed_domains()) is not None:
        return None
    matched_search_provider_domain = _matching_configured_domain(
        normalized_domain,
        {"duckduckgo.com", "duckduckgo.io", "yandex.com", "yandex.ru", "ya.ru"},
    )
    if matched_search_provider_domain is not None:
        return {"reason": "search_provider", "matched_domain": matched_search_provider_domain}
    matched_domain = _matching_configured_domain(normalized_domain, _configured_excluded_domains())
    if matched_domain is None:
        return None
    return {"reason": "directory_or_registry", "matched_domain": matched_domain}


def known_site_classification(normalized_domain: str) -> dict[str, object] | None:
    normalized_domain = normalize_domain(normalized_domain)
    if not normalized_domain:
        return None
    if _matching_configured_domain(normalized_domain, SOCIAL_PROFILE_DOMAINS):
        return {"site_type": "social_profile", "owned_domain": False}
    if _matching_configured_domain(normalized_domain, REFERENCE_PAGE_DOMAINS):
        return {"site_type": "reference_page", "relationship": "supporting_reference", "owned_domain": False}
    if _matching_configured_domain(normalized_domain, REGISTRY_PROFILE_DOMAINS):
        return {"site_type": "registry_profile", "relationship": "evidence_profile", "owned_domain": False}
    if _matching_configured_domain(normalized_domain, DIRECTORY_PROFILE_DOMAINS):
        return {"site_type": "directory_profile", "relationship": "evidence_profile", "owned_domain": False}
    return None


def _configured_excluded_domains() -> set[str]:
    if "DOMAIN_SEARCH_EXCLUDED_DOMAINS" in os.environ:
        domains = _domain_set_from_env("DOMAIN_SEARCH_EXCLUDED_DOMAINS")
    else:
        domains = set(DEFAULT_EXCLUDED_SEARCH_RESULT_DOMAINS)
    domains.update(_domain_set_from_env("DOMAIN_SEARCH_EXTRA_EXCLUDED_DOMAINS"))
    return domains


def _configured_allowed_domains() -> set[str]:
    return _domain_set_from_env("DOMAIN_SEARCH_ALLOWED_DOMAINS")


def _domain_set_from_env(name: str) -> set[str]:
    value = os.environ.get(name)
    if value is None:
        return set()
    return {
        normalized
        for item in value.split(",")
        if (normalized := normalize_domain(item.strip()))
    }


def _matching_configured_domain(normalized_domain: str, configured_domains: set[str]) -> str | None:
    return next(
        (
            configured_domain
            for configured_domain in sorted(configured_domains, key=len, reverse=True)
            if normalized_domain == configured_domain or normalized_domain.endswith(f".{configured_domain}")
        ),
        None,
    )
