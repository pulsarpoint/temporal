from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.assets import (
    brreg_currency_results,
    brreg_domain_results,
    brreg_enhanced_records,
    brreg_translation_results,
)

defs = Definitions(
    assets=[
        brreg_translation_results,
        brreg_domain_results,
        brreg_currency_results,
        brreg_enhanced_records,
    ],
    jobs=[
        define_asset_job("brreg_translate_job", selection=AssetSelection.assets(brreg_translation_results)),
        define_asset_job("brreg_domain_job", selection=AssetSelection.assets(brreg_domain_results)),
        define_asset_job("brreg_currency_job", selection=AssetSelection.assets(brreg_currency_results)),
        define_asset_job("brreg_build_enhanced_job", selection=AssetSelection.assets(brreg_enhanced_records)),
        define_asset_job(
            "brreg_full_enrichment_job",
            selection=AssetSelection.assets(
                brreg_translation_results,
                brreg_domain_results,
                brreg_currency_results,
                brreg_enhanced_records,
            ),
        ),
    ],
)
