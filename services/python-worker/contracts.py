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


@dataclass
class TranslationItem:
    id: str
    text: str


@dataclass
class TranslateTermsInput:
    category: str
    items: list[TranslationItem]
    model: str = ""
    prompt_version: str = "v1"


@dataclass
class TranslatedTerm:
    id: str
    translation: str


@dataclass
class TranslationFailure:
    id: str
    error: str


@dataclass
class TranslateTermsResult:
    translations: list[TranslatedTerm] = field(default_factory=list)
    failures: list[TranslationFailure] = field(default_factory=list)
    model: str = ""


@dataclass
class DownloadSourceFilesInput:
    source: str
    mode: str
    output_dir: str
    datasets: list[str] = field(default_factory=list)
    snapshot_id: str = ""
    delta_window: str = ""


@dataclass
class DownloadedSourceFile:
    source: str
    dataset: str
    file_path: str
    snapshot_id: str
    sha256: str
    format: str


@dataclass
class DownloadSourceFilesResult:
    source: str
    snapshot_id: str
    files: list[DownloadedSourceFile] = field(default_factory=list)
