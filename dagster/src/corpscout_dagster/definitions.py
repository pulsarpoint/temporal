from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.assets import (
    brreg_currency_results,
    brreg_domain_results,
    brreg_enhanced_records,
    brreg_raw_records,
    brreg_translation_results,
)
from corpscout_dagster.brreg.resources import (
    brreg_bulk_resource,
    crawl_service_resource,
    fx_resource,
    postgres_resource,
    translation_service_resource,
)

defs = Definitions(
    assets=[
        brreg_raw_records,
        brreg_translation_results,
        brreg_domain_results,
        brreg_currency_results,
        brreg_enhanced_records,
    ],
    jobs=[
        define_asset_job("brreg_ingest_raw_job", selection=AssetSelection.assets(brreg_raw_records)),
        define_asset_job("brreg_translate_job", selection=AssetSelection.assets(brreg_translation_results)),
        define_asset_job("brreg_domain_job", selection=AssetSelection.assets(brreg_domain_results)),
        define_asset_job("brreg_currency_job", selection=AssetSelection.assets(brreg_currency_results)),
        define_asset_job("brreg_build_enhanced_job", selection=AssetSelection.assets(brreg_enhanced_records)),
        define_asset_job(
            "brreg_full_enrichment_job",
            selection=AssetSelection.assets(
                brreg_raw_records,
                brreg_translation_results,
                brreg_domain_results,
                brreg_currency_results,
                brreg_enhanced_records,
            ),
        ),
    ],
    resources={
        "postgres": postgres_resource,
        "translation_service": translation_service_resource,
        "crawl_service": crawl_service_resource,
        "fx": fx_resource,
        "brreg_bulk": brreg_bulk_resource,
    },
)
