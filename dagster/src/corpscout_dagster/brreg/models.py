from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow, CorpscoutBrregRawInputRow


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
        return CorpscoutBrregRawInputRow(
            source_native_id=self.organization_number,
            organization_number=self.organization_number,
            organization_name=self.organization_name,
            registration_status=self._registration_status(),
            website=_blank_to_none(self.payload.get("hjemmeside")),
            country_iso2="NO",
            raw_payload=self.payload,
            payload_hash=_payload_hash(self.payload),
            run_id=run_id,
        )

    def to_working_row(self) -> BrregWorkingRawRecordRow:
        return BrregWorkingRawRecordRow(
            source_native_id=self.organization_number,
            organization_number=self.organization_number,
            organization_name=self.organization_name,
            registration_status=self._registration_status(),
            website=_blank_to_none(self.payload.get("hjemmeside")),
            country_iso2="NO",
            raw_payload=self.payload,
            payload_hash=_payload_hash(self.payload),
            metadata={},
        )

    def _registration_status(self) -> str:
        if bool(self.payload.get("konkurs")) or bool(self.payload.get("underAvvikling")):
            return "dissolved"
        return "active"


def _blank_to_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _payload_hash(payload: dict[str, Any]) -> str:
    raw_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw_bytes).hexdigest()
