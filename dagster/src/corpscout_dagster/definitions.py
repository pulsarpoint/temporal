from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.assets import (
    brreg_domain_candidates,
    brreg_translation_results,
    brreg_working_raw_records,
)

defs = Definitions(
    assets=[
        brreg_working_raw_records,
        brreg_translation_results,
        brreg_domain_candidates,
    ],
    jobs=[
        define_asset_job("brreg_ingest_job", selection=AssetSelection.assets(brreg_working_raw_records)),
        define_asset_job("brreg_translate_job", selection=AssetSelection.assets(brreg_translation_results)),
        define_asset_job("brreg_domain_enrichment_job", selection=AssetSelection.assets(brreg_domain_candidates)),
    ],
)
