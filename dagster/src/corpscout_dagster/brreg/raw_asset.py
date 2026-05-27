from __future__ import annotations

from dagster import Field, Int, asset

from corpscout_dagster.brreg.asset_config import env_int
from corpscout_dagster.brreg.materializations import (
    DEFAULT_RAW_RECORD_BATCH_SIZE,
    DEFAULT_RAW_RECORD_LIMIT,
    materialize_brreg_raw_records,
)


@asset(
    name="brreg_raw_records",
    config_schema={
        "batch_size": Field(
            Int,
            default_value=env_int("BRREG_RAW_RECORD_BATCH_SIZE", DEFAULT_RAW_RECORD_BATCH_SIZE),
            description="Number of BRREG raw records inserted into dagster_brreg.raw_records per database batch.",
        ),
        "limit": Field(
            Int,
            default_value=env_int("BRREG_RAW_RECORD_LIMIT", DEFAULT_RAW_RECORD_LIMIT),
            description="Maximum valid BRREG bulk records to ingest. Use 0 to ingest all records.",
        ),
    },
    required_resource_keys={"postgres", "brreg_bulk"},
)
def brreg_raw_records(context) -> dict[str, int]:
    op_config = getattr(context, "op_config", None) or {}
    batch_size = int(op_config.get("batch_size", env_int("BRREG_RAW_RECORD_BATCH_SIZE", DEFAULT_RAW_RECORD_BATCH_SIZE)))
    limit = int(op_config.get("limit", env_int("BRREG_RAW_RECORD_LIMIT", DEFAULT_RAW_RECORD_LIMIT)))
    postgres = context.resources.postgres
    brreg_bulk = context.resources.brreg_bulk
    return materialize_brreg_raw_records(
        context,
        connection_factory=postgres.connection_factory,
        database_url=postgres.database_url,
        bulk_client=brreg_bulk.client,
        batch_size=batch_size,
        limit=limit,
    )
