from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CorpscoutBrregRawInputRow:
    source_native_id: str
    organization_number: str
    organization_name: str
    registration_status: str
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    run_id: str


@dataclass(frozen=True)
class BrregWorkingRawRecordRow:
    source_native_id: str
    organization_number: str
    organization_name: str
    registration_status: str
    website: str | None
    country_iso2: str
    raw_payload: dict[str, Any]
    payload_hash: str
    metadata: dict[str, Any]
