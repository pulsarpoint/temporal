from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from corpscout_dagster.db_brreg.models import CorpscoutBrregRawInputRow


class Cursor(Protocol):
    def execute(self, sql: str, params: dict) -> object:
        ...


@dataclass(frozen=True)
class UpsertResult:
    rows_seen: int
    rows_written: int


class BrregRawInputWriter:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def upsert_many(self, rows: list[CorpscoutBrregRawInputRow]) -> UpsertResult:
        for row in rows:
            self._cursor.execute(
                BRREG_RAW_INPUT_UPSERT_SQL,
                {
                    "source_native_id": row.source_native_id,
                    "organization_number": row.organization_number,
                    "organization_name": row.organization_name,
                    "registration_status": row.registration_status,
                    "website": row.website,
                    "country_iso2": row.country_iso2,
                    "raw_payload": json.dumps(row.raw_payload, sort_keys=True, separators=(",", ":")),
                    "payload_hash": row.payload_hash,
                    "run_id": row.run_id,
                },
            )
        return UpsertResult(rows_seen=len(rows), rows_written=len(rows))


BRREG_RAW_INPUT_UPSERT_SQL = """
INSERT INTO brreg_company_raw_inputs (
    source_native_id,
    organization_number,
    organization_name,
    registration_status,
    website,
    country_iso2,
    raw_payload,
    payload_hash,
    run_id
) VALUES (
    %(source_native_id)s,
    %(organization_number)s,
    %(organization_name)s,
    %(registration_status)s,
    %(website)s,
    %(country_iso2)s,
    %(raw_payload)s::jsonb,
    %(payload_hash)s,
    %(run_id)s
)
ON CONFLICT (organization_number, payload_hash) DO UPDATE
SET
    last_seen_at = now(),
    organization_name = EXCLUDED.organization_name,
    registration_status = EXCLUDED.registration_status,
    website = EXCLUDED.website,
    country_iso2 = EXCLUDED.country_iso2,
    run_id = EXCLUDED.run_id,
    updated_at = now()
"""
