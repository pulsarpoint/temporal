from __future__ import annotations

from dagster import AssetCheckResult, asset_check

from corpscout_dagster.brreg.working_store import BrregWorkingStore


def _postgres_resource(context):
    return context.resources.postgres


@asset_check(
    asset="brreg_raw_records",
    name="live_table_state",
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.raw_records table, not historical Dagster materialization metadata.",
)
def brreg_raw_records_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_raw_records_live_table_state(context)


@asset_check(
    asset="brreg_translation_results",
    name="live_table_state",
    required_resource_keys={"postgres", "translation_service"},
    description="Checks the live dagster_brreg.translation_results table for the configured model and prompt.",
)
def brreg_translation_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_translation_results_live_table_state(context)


@asset_check(
    asset="brreg_domain_results",
    name="live_table_state",
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.domain_results table, not historical Dagster materialization metadata.",
)
def brreg_domain_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_domain_results_live_table_state(context)


@asset_check(
    asset="brreg_currency_results",
    name="live_table_state",
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.currency_results table, not historical Dagster materialization metadata.",
)
def brreg_currency_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_currency_results_live_table_state(context)


@asset_check(
    asset="brreg_enhanced_records",
    name="live_table_state",
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.enhanced_records table, not historical Dagster materialization metadata.",
)
def brreg_enhanced_records_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_enhanced_records_live_table_state(context)


def evaluate_brreg_raw_records_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            summary = BrregWorkingStore(cursor).fetch_raw_task_state_summary(task_type="translate")
    current_rows = summary["raw_records_current"]
    return AssetCheckResult(
        passed=current_rows > 0,
        metadata={
            "live_raw_records_total": summary["raw_records_total"],
            "live_raw_records_current": current_rows,
            "live_raw_records_not_current": summary["raw_records_not_current"],
        },
        description=(
            "dagster_brreg.raw_records has current rows."
            if current_rows > 0
            else "dagster_brreg.raw_records has no current rows."
        ),
    )


def evaluate_brreg_translation_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    translation_service = context.resources.translation_service
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            raw_summary = store.fetch_raw_task_state_summary(task_type="translate")
            artifact_summary = store.fetch_translation_artifact_summary(
                model=translation_service.model,
                prompt_version=translation_service.prompt_version,
            )
    missing = artifact_summary["translation_result_missing"]
    failed = artifact_summary["translation_result_failed"]
    passed = raw_summary["raw_records_current"] > 0 and missing == 0 and failed == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": raw_summary["raw_records_current"],
            "live_translation_model": translation_service.model,
            "live_translation_prompt_version": translation_service.prompt_version,
            "live_translation_results_succeeded": artifact_summary["translation_result_succeeded"],
            "live_translation_results_skipped": artifact_summary["translation_result_skipped"],
            "live_translation_results_failed": failed,
            "live_translation_results_missing": missing,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.translation_results",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_domain_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            raw_summary = store.fetch_raw_task_state_summary(task_type="domain_results")
            artifact_summary = store.fetch_domain_result_summary()
    missing = artifact_summary["domain_result_missing"]
    failed = artifact_summary["domain_result_failed"]
    passed = raw_summary["raw_records_current"] > 0 and missing == 0 and failed == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": raw_summary["raw_records_current"],
            "live_domain_results_succeeded": artifact_summary["domain_result_succeeded"],
            "live_domain_results_partial": artifact_summary["domain_result_partial"],
            "live_domain_results_not_found": artifact_summary["domain_result_not_found"],
            "live_domain_results_failed": failed,
            "live_domain_results_missing": missing,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.domain_results",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_currency_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            raw_summary = store.fetch_raw_task_state_summary(task_type="currency_conversion")
            artifact_summary = store.fetch_currency_result_summary()
    missing = artifact_summary["currency_result_missing"]
    failed = artifact_summary["currency_result_failed"]
    passed = raw_summary["raw_records_current"] > 0 and missing == 0 and failed == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": raw_summary["raw_records_current"],
            "live_currency_results_succeeded": artifact_summary["currency_result_succeeded"],
            "live_currency_results_skipped": artifact_summary["currency_result_skipped"],
            "live_currency_results_not_available": artifact_summary["currency_result_not_available"],
            "live_currency_results_failed": failed,
            "live_currency_results_missing": missing,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.currency_results",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_enhanced_records_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            store = BrregWorkingStore(cursor)
            raw_summary = store.fetch_raw_task_state_summary(task_type="build_enhanced")
            artifact_summary = store.fetch_enhanced_record_summary()
    missing = artifact_summary["enhanced_record_missing"]
    failed = artifact_summary["enhanced_record_publish_failed"]
    passed = raw_summary["raw_records_current"] > 0 and missing == 0 and failed == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": raw_summary["raw_records_current"],
            "live_enhanced_records_built": artifact_summary["enhanced_record_built"],
            "live_enhanced_records_published": artifact_summary["enhanced_record_published"],
            "live_enhanced_records_publish_failed": failed,
            "live_enhanced_records_superseded": artifact_summary["enhanced_record_superseded"],
            "live_enhanced_records_missing": missing,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.enhanced_records",
            missing=missing,
            failed=failed,
        ),
    )


def _artifact_check_description(*, table_name: str, missing: int, failed: int) -> str:
    if missing == 0 and failed == 0:
        return f"{table_name} has a non-failed latest artifact for every current BRREG raw row."
    return f"{table_name} live state is incomplete: missing={missing}, failed={failed}."
