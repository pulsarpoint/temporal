from __future__ import annotations

from dagster import AssetKey, asset

from corpscout_dagster.brreg.materializations import materialize_brreg_enhanced_records


@asset(
    name="brreg_enhanced_records",
    deps=[
        AssetKey("brreg_translation_results"),
        AssetKey("brreg_domain_results"),
        AssetKey("brreg_currency_results"),
    ],
    required_resource_keys={"postgres"},
)
def brreg_enhanced_records(context) -> dict[str, int]:
    postgres = context.resources.postgres
    return materialize_brreg_enhanced_records(
        context,
        connection_factory=postgres.connection_factory,
        database_url=postgres.database_url,
    )
