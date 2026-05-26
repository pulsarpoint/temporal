from __future__ import annotations

import psycopg
from dagster import AssetKey, Field, Int, asset

from corpscout_dagster.brreg.asset_config import corpscout_database_url, env_int
from corpscout_dagster.brreg.materializations import (
    DEFAULT_ENHANCED_RECORD_BATCH_SIZE,
    materialize_brreg_enhanced_records,
)


@asset(
    name="brreg_enhanced_records",
    deps=[
        AssetKey("brreg_translation_results"),
        AssetKey("brreg_domain_results"),
        AssetKey("brreg_currency_results"),
    ],
    config_schema={
        "batch_size": Field(
            Int,
            default_value=env_int("BRREG_ENHANCED_RECORD_BATCH_SIZE", DEFAULT_ENHANCED_RECORD_BATCH_SIZE),
            description="Number of BRREG rows assembled into enhanced records in this run.",
        )
    },
)
def brreg_enhanced_records(context) -> dict[str, int]:
    op_config = getattr(context, "op_config", None) or {}
    batch_size = int(
        op_config.get(
            "batch_size",
            env_int("BRREG_ENHANCED_RECORD_BATCH_SIZE", DEFAULT_ENHANCED_RECORD_BATCH_SIZE),
        )
    )
    return materialize_brreg_enhanced_records(
        context,
        connection_factory=psycopg.connect,
        database_url=corpscout_database_url(),
        batch_size=batch_size,
    )
