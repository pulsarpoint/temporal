from __future__ import annotations

from dagster import AssetCheckResult, asset_check

from corpscout_dagster.db_brreg.views import BrregAssetStateView, BrregAssetStateViewReader


def _postgres_resource(context):
    return context.resources.postgres


@asset_check(
    asset="brreg_raw_records",
    name="live_table_state",
    blocking=True,
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.raw_records table, not historical Dagster materialization metadata.",
)
def brreg_raw_records_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_raw_records_live_table_state(context)


@asset_check(
    asset="brreg_translation_results",
    name="live_table_state",
    blocking=True,
    required_resource_keys={"postgres", "translation_service"},
    description="Checks the live dagster_brreg.translation_results table for the configured model and prompt.",
)
def brreg_translation_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_translation_results_live_table_state(context)


@asset_check(
    asset="brreg_domain_results",
    name="live_table_state",
    blocking=True,
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.domain_results table, not historical Dagster materialization metadata.",
)
def brreg_domain_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_domain_results_live_table_state(context)


@asset_check(
    asset="brreg_currency_results",
    name="live_table_state",
    blocking=True,
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.currency_results table, not historical Dagster materialization metadata.",
)
def brreg_currency_results_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_currency_results_live_table_state(context)


@asset_check(
    asset="brreg_enhanced_records",
    name="live_table_state",
    blocking=True,
    required_resource_keys={"postgres"},
    description="Checks the live dagster_brreg.enhanced_records table, not historical Dagster materialization metadata.",
)
def brreg_enhanced_records_live_table_state(context) -> AssetCheckResult:
    return evaluate_brreg_enhanced_records_live_table_state(context)


def evaluate_brreg_raw_records_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            state = BrregAssetStateViewReader(cursor).fetch_raw_records_state()
    current_rows = state.current_rows
    return AssetCheckResult(
        passed=current_rows > 0,
        metadata={
            "live_raw_records_total": state.total_rows,
            "live_raw_records_current": current_rows,
            "live_raw_records_not_current": state.not_current_rows,
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
            reader = BrregAssetStateViewReader(cursor)
            raw_state = reader.fetch_raw_records_state()
            state = reader.fetch_translation_state(
                model=translation_service.model,
                prompt_version=translation_service.prompt_version,
                raw_total_rows=raw_state.current_rows,
            )
    missing = state.missing_artifact_rows
    failed = _failed_rows(state)
    passed = state.total_rows > 0 and state.is_complete and not state.is_blocked
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": state.total_rows,
            "live_translation_model": translation_service.model,
            "live_translation_prompt_version": translation_service.prompt_version,
            "live_translation_results_succeeded": state.succeeded_rows,
            "live_translation_results_skipped": state.skipped_rows,
            "live_translation_results_failed": failed,
            "live_translation_results_missing": missing,
            "live_translation_results_eligible": state.eligible_rows,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.v_translation_asset_state",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_domain_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            state = BrregAssetStateViewReader(cursor).fetch_domain_state()
    missing = state.missing_artifact_rows
    failed = _failed_rows(state)
    passed = state.total_rows > 0 and state.is_complete and not state.is_blocked
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": state.total_rows,
            "live_domain_results_succeeded": state.succeeded_rows,
            "live_domain_results_skipped": state.skipped_rows,
            "live_domain_results_failed": failed,
            "live_domain_results_missing": missing,
            "live_domain_results_eligible": state.eligible_rows,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.v_domain_asset_state",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_currency_results_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            state = BrregAssetStateViewReader(cursor).fetch_financial_state()
    missing = state.missing_artifact_rows
    failed = _failed_rows(state)
    passed = state.total_rows > 0 and state.is_complete and not state.is_blocked
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": state.total_rows,
            "live_currency_results_succeeded": state.succeeded_rows,
            "live_currency_results_skipped": state.skipped_rows,
            "live_currency_results_failed": failed,
            "live_currency_results_missing": missing,
            "live_currency_results_eligible": state.eligible_rows,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.v_financial_asset_state",
            missing=missing,
            failed=failed,
        ),
    )


def evaluate_brreg_enhanced_records_live_table_state(context) -> AssetCheckResult:
    postgres = _postgres_resource(context)
    with postgres.connection_factory(postgres.database_url) as conn:
        with conn.cursor() as cursor:
            state = BrregAssetStateViewReader(cursor).fetch_enhanced_state()
    missing = state.missing_artifact_rows
    failed = _failed_rows(state)
    passed = state.total_rows > 0 and state.is_complete and not state.is_blocked
    return AssetCheckResult(
        passed=passed,
        metadata={
            "live_raw_records_current": state.total_rows,
            "live_enhanced_records_built": state.succeeded_rows,
            "live_enhanced_records_published": 0,
            "live_enhanced_records_publish_failed": failed,
            "live_enhanced_records_superseded": state.skipped_rows,
            "live_enhanced_records_missing": missing,
            "live_enhanced_records_eligible": state.eligible_rows,
        },
        description=_artifact_check_description(
            table_name="dagster_brreg.v_enhanced_asset_state",
            missing=missing,
            failed=failed,
        ),
    )


def _failed_rows(state: BrregAssetStateView) -> int:
    return state.failed_retryable_rows + state.failed_terminal_rows


def _artifact_check_description(*, table_name: str, missing: int, failed: int) -> str:
    if missing == 0 and failed == 0:
        return f"{table_name} has a completed live asset state for every current BRREG raw row."
    return f"{table_name} live state is incomplete: missing={missing}, failed={failed}."
