from __future__ import annotations

from dagster import Field, Int, asset

from corpscout_dagster.brreg.asset_config import env_int
from corpscout_dagster.brreg.materializations import (
    DEFAULT_RAW_RECORD_BATCH_SIZE,
    materialize_brreg_raw_records,
)


@asset(
    name="brreg_raw_records",
    config_schema={
        "batch_size": Field(
            Int,
            default_value=env_int("BRREG_RAW_RECORD_BATCH_SIZE", DEFAULT_RAW_RECORD_BATCH_SIZE),
            description="Number of BRREG raw records inserted into dagster_brreg.raw_records per database batch.",
        )
    },
    required_resource_keys={"postgres", "brreg_bulk"},
)
def brreg_raw_records(context) -> dict[str, int]:
    op_config = getattr(context, "op_config", None) or {}
    batch_size = int(op_config.get("batch_size", env_int("BRREG_RAW_RECORD_BATCH_SIZE", DEFAULT_RAW_RECORD_BATCH_SIZE)))
    postgres = context.resources.postgres
    brreg_bulk = context.resources.brreg_bulk
    return materialize_brreg_raw_records(
        context,
        connection_factory=postgres.connection_factory,
        database_url=postgres.database_url,
        bulk_client=brreg_bulk.client,
        batch_size=batch_size,
    )
