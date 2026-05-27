from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from corpscout_dagster.brreg.asset_checks import (
    brreg_currency_results_live_table_state,
    brreg_domain_results_live_table_state,
    brreg_enhanced_records_live_table_state,
    brreg_raw_records_live_table_state,
    brreg_translation_results_live_table_state,
)
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
from corpscout_dagster.brreg.retry_jobs import (
    brreg_retry_currency_transient_external_job,
    brreg_retry_domain_rate_limited_job,
    brreg_retry_domain_transient_external_job,
    brreg_retry_interrupted_failures_job,
    brreg_retry_translation_invalid_llm_output_job,
    brreg_retry_translation_rate_limited_job,
    brreg_retry_translation_transient_external_job,
)


def _assets_without_checks(*assets):
    return AssetSelection.assets(*assets).without_checks()


defs = Definitions(
    assets=[
        brreg_raw_records,
        brreg_translation_results,
        brreg_domain_results,
        brreg_currency_results,
        brreg_enhanced_records,
    ],
    asset_checks=[
        brreg_raw_records_live_table_state,
        brreg_translation_results_live_table_state,
        brreg_domain_results_live_table_state,
        brreg_currency_results_live_table_state,
        brreg_enhanced_records_live_table_state,
    ],
    jobs=[
        define_asset_job("brreg_ingest_raw_job", selection=_assets_without_checks(brreg_raw_records)),
        define_asset_job("brreg_translate_job", selection=_assets_without_checks(brreg_translation_results)),
        define_asset_job("brreg_domain_job", selection=_assets_without_checks(brreg_domain_results)),
        define_asset_job("brreg_currency_job", selection=_assets_without_checks(brreg_currency_results)),
        define_asset_job("brreg_build_enhanced_job", selection=_assets_without_checks(brreg_enhanced_records)),
        define_asset_job(
            "brreg_full_enrichment_job",
            selection=_assets_without_checks(
                brreg_raw_records,
                brreg_translation_results,
                brreg_domain_results,
                brreg_currency_results,
                brreg_enhanced_records,
            ),
        ),
        define_asset_job("brreg_live_table_checks_job", selection=AssetSelection.all_asset_checks()),
        brreg_retry_translation_invalid_llm_output_job,
        brreg_retry_translation_transient_external_job,
        brreg_retry_translation_rate_limited_job,
        brreg_retry_domain_rate_limited_job,
        brreg_retry_domain_transient_external_job,
        brreg_retry_currency_transient_external_job,
        brreg_retry_interrupted_failures_job,
    ],
    resources={
        "postgres": postgres_resource,
        "translation_service": translation_service_resource,
        "crawl_service": crawl_service_resource,
        "fx": fx_resource,
        "brreg_bulk": brreg_bulk_resource,
    },
)
