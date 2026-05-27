from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

import psycopg

from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.db_brreg.models import BrregWorkingRawRecordRow
from corpscout_dagster.brreg.source import BRREG_API_BASE_URL, BRREG_BULK_PATH
from corpscout_dagster.db_brreg import BrregAssetGateway, SmokeIngestRawRecordCommand

SMOKE_ORG_NUMBER = "999999991"
SMOKE_RUN_ID = "dagster-smoke"
SMOKE_NAME = "CORPSCOUT DAGSTER SMOKE AS"
BRREG_BULK_URL = f"{BRREG_API_BASE_URL}{BRREG_BULK_PATH}"


@dataclass(frozen=True)
class SmokeResult:
    organization_number: str
    payload_hash: str
    rolled_back: bool


def build_smoke_row() -> BrregWorkingRawRecordRow:
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
    return record.to_working_row()


def run_smoke(
    database_url: str,
    *,
    connection_factory: Callable = psycopg.connect,
) -> SmokeResult:
    row = build_smoke_row()
    with connection_factory(database_url) as conn:
        BrregAssetGateway(conn).smoke_ingest_raw_record(
            SmokeIngestRawRecordCommand(
                dagster_run_id=SMOKE_RUN_ID,
                source_url=BRREG_BULK_URL,
                row=row,
                metadata={"smoke": True},
            )
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT organization_name
                FROM dagster_brreg.raw_records
                WHERE organization_number = %(organization_number)s
                  AND payload_hash = %(payload_hash)s
                """,
                {
                    "organization_number": row.organization_number,
                    "payload_hash": row.payload_hash,
                },
            )
            found = cursor.fetchone()
            if found != (SMOKE_NAME,):
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
