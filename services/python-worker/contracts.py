from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FetchPageInput:
    source: str
    country: str
    page: int
    ids: list[str] = field(default_factory=list)
    cursor: str = ""


@dataclass
class RawRecord:
    native_id: str
    name: str
    status: str
    raw_json: dict[str, Any]
    hash: str
    company_type: str = ""


@dataclass
class FetchResult:
    records: list[RawRecord]
    has_more: bool
    next_cursor: str = ""


@dataclass
class CompanyLookup:
    native_id: str
    name: str


@dataclass
class DiscoverDomainsInput:
    source: str
    country: str
    companies: list[CompanyLookup]


@dataclass
class DomainDiscovery:
    native_id: str
    domain: str
    signal: str
    confidence: int


@dataclass
class DiscoverDomainsResult:
    discoveries: list[DomainDiscovery] = field(default_factory=list)
