from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

import psycopg

from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.writer import BrregRawInputWriter

SMOKE_ORG_NUMBER = "999999991"
SMOKE_RUN_ID = "dagster-smoke"
SMOKE_NAME = "CORPSCOUT DAGSTER SMOKE AS"


@dataclass(frozen=True)
class SmokeResult:
    organization_number: str
    payload_hash: str
    rolled_back: bool


def build_smoke_row(*, run_id: str = SMOKE_RUN_ID) -> CorpscoutBrregRawInputRow:
    record = BrregRawRecord.from_payload(
        {
            "organisasjonsnummer": SMOKE_ORG_NUMBER,
            "navn": SMOKE_NAME,
            "konkurs": False,
            "underAvvikling": False,
            "corpscout_smoke": True,
        }
    )
    if record is None:
        raise RuntimeError("invalid BRREG smoke payload")
    return record.to_corpscout_row(run_id=run_id)


def run_smoke(
    database_url: str,
    *,
    connection_factory: Callable = psycopg.connect,
) -> SmokeResult:
    row = build_smoke_row()
    with connection_factory(database_url) as conn:
        with conn.cursor() as cursor:
            BrregRawInputWriter(cursor).upsert_many([row])
            cursor.execute(
                """
                SELECT organization_name, run_id
                FROM brreg_company_raw_inputs
                WHERE organization_number = %s
                  AND payload_hash = %s
                """,
                (row.organization_number, row.payload_hash),
            )
            found = cursor.fetchone()
            if found != (SMOKE_NAME, SMOKE_RUN_ID):
                raise RuntimeError("BRREG smoke row was not readable after upsert")
        conn.rollback()
    return SmokeResult(
        organization_number=row.organization_number,
        payload_hash=row.payload_hash,
        rolled_back=True,
    )


def main() -> None:
    database_url = (
        os.environ.get("CORPSCOUT_DATABASE_URL")
        or os.environ.get("CORPSCOUT_DB_URL")
        or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        raise SystemExit("CORPSCOUT_DATABASE_URL, CORPSCOUT_DB_URL, or DATABASE_URL is required")
    result = run_smoke(database_url)
    print(
        "BRREG raw input DB smoke verified "
        f"organization_number={result.organization_number} rolled_back={str(result.rolled_back).lower()}"
    )


if __name__ == "__main__":
    main()
