from __future__ import annotations

from corpscout_crawl_service.candidate_policy import search_result_exclusion_reason
from corpscout_crawl_service.domain_utils import domain_from_url, normalize_domain, normalize_url, unwrap_search_result_url


def test_normalize_domain_removes_scheme_www_path_and_case() -> None:
    assert normalize_domain("HTTPS://www.Example.NO/path?q=1") == "example.no"


def test_domain_from_url_returns_empty_string_for_invalid_url() -> None:
    assert domain_from_url("not a url") == ""


def test_normalize_url_adds_https_to_plain_domains() -> None:
    assert normalize_url("www.example.no/contact") == "https://www.example.no/contact"


def test_normalize_url_rejects_values_without_valid_hosts() -> None:
    assert normalize_url("not a url") == ""
    assert normalize_url("https:///missing-host") == ""
    assert normalize_url("mailto:hello@example.no") == ""


def test_unwrap_search_result_url_decodes_duckduckgo_target() -> None:
    assert (
        unwrap_search_result_url("https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.example.no%2Fcontact")
        == "https://www.example.no/contact"
    )


def test_search_result_exclusion_marks_directory_domains() -> None:
    assert search_result_exclusion_reason("www.proff.no") == {
        "reason": "directory_or_registry",
        "matched_domain": "proff.no",
    }


def test_search_result_exclusion_allows_configured_domains(monkeypatch) -> None:
    monkeypatch.setenv("DOMAIN_SEARCH_ALLOWED_DOMAINS", "proff.no")

    assert search_result_exclusion_reason("www.proff.no") is None
