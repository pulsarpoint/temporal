from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.assets import (
    brreg_domain_results,
    brreg_enhanced_records,
    brreg_publish_enhanced_records,
    brreg_translation_results,
    brreg_working_raw_records,
)

BRREG_DOMAIN_ASSETS = [
    brreg_domain_results,
]

defs = Definitions(
    assets=[
        brreg_working_raw_records,
        brreg_translation_results,
        *BRREG_DOMAIN_ASSETS,
        brreg_enhanced_records,
        brreg_publish_enhanced_records,
    ],
    jobs=[
        define_asset_job("brreg_ingest_job", selection=AssetSelection.assets(brreg_working_raw_records)),
        define_asset_job("brreg_translate_job", selection=AssetSelection.assets(brreg_translation_results)),
        define_asset_job("brreg_domain_results_job", selection=AssetSelection.assets(brreg_domain_results)),
        define_asset_job("brreg_domain_enrichment_job", selection=AssetSelection.assets(brreg_domain_results)),
        define_asset_job("brreg_enhanced_records_job", selection=AssetSelection.assets(brreg_enhanced_records)),
        define_asset_job("brreg_publish_enhanced_records_job", selection=AssetSelection.assets(brreg_publish_enhanced_records)),
    ],
)
