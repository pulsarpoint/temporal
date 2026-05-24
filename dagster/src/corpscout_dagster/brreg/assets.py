from __future__ import annotations

import os
from collections.abc import Iterable

import psycopg
from dagster import asset

from corpscout_dagster.brreg.models import BrregRawRecord, CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.source import iter_brreg_bulk_records
from corpscout_dagster.brreg.writer import BrregRawInputWriter


def build_brreg_raw_input_rows(
    *,
    records: Iterable[BrregRawRecord | None],
    run_id: str,
) -> list[CorpscoutBrregRawInputRow]:
    return [record.to_corpscout_row(run_id=run_id) for record in records if record is not None]


@asset(name="brreg_raw_inputs")
def brreg_raw_inputs(context) -> dict[str, int]:
    connection_url = _corpscout_database_url()
    rows = build_brreg_raw_input_rows(
        records=iter_brreg_bulk_records(),
        run_id=context.run_id,
    )
    with psycopg.connect(connection_url) as conn:
        with conn.cursor() as cursor:
            result = BrregRawInputWriter(cursor).upsert_many(rows)
        conn.commit()
    context.add_output_metadata(
        {
            "rows_seen": result.rows_seen,
            "rows_written": result.rows_written,
            "dagster_run_id": context.run_id,
        }
    )
    return {"rows_seen": result.rows_seen, "rows_written": result.rows_written}


def _corpscout_database_url() -> str:
    value = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("CORPSCOUT_DB_URL")
    if not value:
        raise RuntimeError("CORPSCOUT_DATABASE_URL or CORPSCOUT_DB_URL is required")
    return value
