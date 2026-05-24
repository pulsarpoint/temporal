from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class DomainCandidate:
    domain: str
    normalized_domain: str
    signal: str
    confidence: int
    evidence: dict[str, Any]
    metadata: dict[str, Any]


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
    domain = _domain_from_value(value).lower().strip(".")
    if domain.startswith("www."):
        domain = domain[4:]
    if "." not in domain or " " in domain:
        return None
    return domain


def _domain_from_value(value: str) -> str:
    trimmed = value.strip()
    parsed = urlparse(trimmed if "://" in trimmed else f"https://{trimmed}")
    return (parsed.hostname or trimmed).strip().lower()


def _string_or_none(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
