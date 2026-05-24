from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.assets import (
    brreg_domain_crtsh_candidates,
    brreg_domain_duckduckgo_candidates,
    brreg_domain_proposals,
    brreg_domain_web_search_llm_candidates,
    brreg_domain_website_field_candidates,
    brreg_domain_wikidata_candidates,
    brreg_enhanced_records,
    brreg_publish_enhanced_records,
    brreg_translation_results,
    brreg_working_raw_records,
)

BRREG_DOMAIN_ASSETS = [
    brreg_domain_website_field_candidates,
    brreg_domain_duckduckgo_candidates,
    brreg_domain_crtsh_candidates,
    brreg_domain_wikidata_candidates,
    brreg_domain_web_search_llm_candidates,
    brreg_domain_proposals,
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
        define_asset_job(
            "brreg_domain_website_field_job",
            selection=AssetSelection.assets(brreg_domain_website_field_candidates),
        ),
        define_asset_job("brreg_domain_duckduckgo_job", selection=AssetSelection.assets(brreg_domain_duckduckgo_candidates)),
        define_asset_job("brreg_domain_crtsh_job", selection=AssetSelection.assets(brreg_domain_crtsh_candidates)),
        define_asset_job("brreg_domain_wikidata_job", selection=AssetSelection.assets(brreg_domain_wikidata_candidates)),
        define_asset_job(
            "brreg_domain_web_search_llm_job",
            selection=AssetSelection.assets(brreg_domain_web_search_llm_candidates),
        ),
        define_asset_job("brreg_domain_proposals_job", selection=AssetSelection.assets(brreg_domain_proposals)),
        define_asset_job("brreg_domain_enrichment_job", selection=AssetSelection.assets(*BRREG_DOMAIN_ASSETS)),
        define_asset_job("brreg_enhanced_records_job", selection=AssetSelection.assets(brreg_enhanced_records)),
        define_asset_job("brreg_publish_enhanced_records_job", selection=AssetSelection.assets(brreg_publish_enhanced_records)),
    ],
)
