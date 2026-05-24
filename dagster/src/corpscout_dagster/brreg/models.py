from __future__ import annotations

import hashlib
import json
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
class BrregRawRecord:
    payload: dict[str, Any]
    organization_number: str
    organization_name: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "BrregRawRecord | None":
        organization_number = str(payload.get("organisasjonsnummer") or "").strip()
        if not organization_number:
            return None
        organization_name = str(payload.get("navn") or "").strip()
        return cls(
            payload=payload,
            organization_number=organization_number,
            organization_name=organization_name,
        )

    def to_corpscout_row(self, *, run_id: str) -> CorpscoutBrregRawInputRow:
        raw_bytes = json.dumps(self.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return CorpscoutBrregRawInputRow(
            source_native_id=self.organization_number,
            organization_number=self.organization_number,
            organization_name=self.organization_name,
            registration_status=self._registration_status(),
            website=_blank_to_none(self.payload.get("hjemmeside")),
            country_iso2="NO",
            raw_payload=self.payload,
            payload_hash=hashlib.sha256(raw_bytes).hexdigest(),
            run_id=run_id,
        )

    def _registration_status(self) -> str:
        if bool(self.payload.get("konkurs")) or bool(self.payload.get("underAvvikling")):
            return "dissolved"
        return "active"


def _blank_to_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
