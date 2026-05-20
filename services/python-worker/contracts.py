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
class FetchCompanyDetailInput:
    source: str
    native_id: str


@dataclass
class CompanyDetailResult:
    native_id: str
    name: str
    status: str
    type: str = ""
    date_of_creation: str = ""
    address_line_1: str | None = None
    address_line_2: str | None = None
    locality: str | None = None
    postal_code: str | None = None
    country: str | None = None
    region: str | None = None
    sic_codes: list[str] = field(default_factory=list)
