from pathlib import Path


MIGRATIONS_DIR = Path(__file__).parents[2] / "db" / "migrations"
UP_SQL = MIGRATIONS_DIR / "000001_brreg_working_store.up.sql"
DOWN_SQL = MIGRATIONS_DIR / "000001_brreg_working_store.down.sql"
ALL_UP_SQL = "\n".join(path.read_text() for path in sorted(MIGRATIONS_DIR.glob("*.up.sql")))


def test_working_store_migration_creates_required_tables() -> None:
    sql = UP_SQL.read_text()

    assert "CREATE SCHEMA IF NOT EXISTS dagster_brreg" in sql
    for table_name in [
        "enrichment_runs",
        "bulk_snapshots",
        "raw_records",
        "task_attempts",
        "translation_results",
        "financial_results",
        "enhanced_records",
    ]:
        assert f"CREATE TABLE dagster_brreg.{table_name}" in sql


def test_working_store_migration_has_idempotency_and_queue_indexes() -> None:
    sql = UP_SQL.read_text()

    assert "UNIQUE (organization_number, payload_hash)" in sql
    assert "CREATE UNIQUE INDEX idx_dagster_brreg_raw_records_current_org" in sql
    assert "CREATE INDEX idx_dagster_brreg_raw_records_org" in sql
    assert "CREATE INDEX idx_dagster_brreg_task_attempts_queue" in sql


def test_working_store_migration_tracks_task_outputs() -> None:
    sql = ALL_UP_SQL

    assert "task_type IN (" in sql
    assert "'translate'" in sql
    assert "'discover_domains'" in sql
    assert "'domain_results'" in sql
    assert "'currency_conversion'" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.translation_cache" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_results" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.currency_results" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_states" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_cursors" in sql
    assert "ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ" in sql
    assert "idx_dagster_brreg_task_states_running_lease" in sql
    assert "DELETE FROM dagster_brreg.raw_record_task_states" in sql
    assert "DROP TABLE IF EXISTS dagster_brreg.raw_record_task_run_leases" in sql
    assert "DROP TABLE IF EXISTS dagster_brreg.raw_record_task_cursors" in sql
    assert "last_raw_record_id UUID" in sql
    assert "'failed_retryable'" in sql
    assert "'failed_terminal'" in sql
    assert "UNIQUE (category, source_lang, target_lang, original_hash, model, prompt_version)" in sql
    assert "idx_dagster_brreg_translation_cache_lookup" in sql
    assert "idx_dagster_brreg_translation_success" in sql
    assert "idx_dagster_brreg_translation_results_model_prompt_raw_latest" in sql
    assert "idx_dagster_brreg_domain_results_raw_created" in sql
    assert "idx_dagster_brreg_currency_results_raw_created" in sql
    assert "idx_dagster_brreg_task_states_queue" in sql
    assert "idx_dagster_brreg_raw_records_current_last_seen_id" in sql
    assert "ON dagster_brreg.raw_records (last_seen_at, id)" in sql
    assert "idx_dagster_brreg_task_states_pending_retry_queue" in sql
    assert "idx_dagster_brreg_task_states_running_stale_queue" in sql
    assert "error_category TEXT" in sql
    assert "error_code TEXT" in sql
    assert "retry_strategy TEXT" in sql
    assert "chk_dagster_brreg_task_attempts_error_category" in sql
    assert "chk_dagster_brreg_task_states_error_category" in sql
    assert "idx_dagster_brreg_task_states_failure_retry" in sql


def test_working_store_migration_has_independent_brreg_run_types() -> None:
    sql = ALL_UP_SQL

    assert "'bulk_ingest'" in sql
    assert "'translate'" in sql
    assert "'discover_domains'" in sql
    assert "'domain_results'" in sql
    assert "'currency_conversion'" in sql
    assert "'build_enhanced'" in sql


def test_domain_results_migration_adds_single_business_result_artifact() -> None:
    sql = (MIGRATIONS_DIR / "000013_brreg_domain_results.up.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_results" in sql
    assert "status IN ('succeeded', 'not_found', 'partial', 'failed')" in sql
    assert "domain_payload JSONB NOT NULL DEFAULT '{}'::jsonb" in sql
    assert "idx_dagster_brreg_domain_results_raw_created" in sql
    assert "'domain_results'" in sql


def test_currency_results_migration_adds_currency_task_type_and_indexes() -> None:
    sql = (MIGRATIONS_DIR / "000014_brreg_currency_results.up.sql").read_text()

    assert "'currency_conversion'" in sql
    assert "idx_dagster_brreg_currency_results_raw_created" in sql
    assert "idx_dagster_brreg_currency_results_status" in sql


def test_enhanced_ready_records_migration_adds_materialized_view() -> None:
    sql = ALL_UP_SQL

    assert "CREATE MATERIALIZED VIEW IF NOT EXISTS dagster_brreg.mv_brreg_enhanced_ready_records" in sql
    assert "status IN ('succeeded', 'skipped')" in sql
    assert "status IN ('succeeded', 'not_found', 'partial')" in sql
    assert "status IN ('succeeded', 'skipped', 'not_available')" in sql
    assert "idx_dagster_brreg_mv_enhanced_ready_records_raw" in sql
    assert "idx_dagster_brreg_mv_enhanced_ready_records_queue" in sql


def test_working_store_migration_creates_observability_views() -> None:
    sql = ALL_UP_SQL

    for view_name in [
        "v_enrichment_run_summary",
        "v_task_state_summary",
        "v_task_failure_summary",
        "v_translation_result_detail",
        "v_failed_task_states",
        "v_raw_record_task_overview",
    ]:
        assert f"CREATE OR REPLACE VIEW dagster_brreg.{view_name}" in sql

    assert "jsonb_object_agg(" in sql
    assert "rts.task_type" in sql
    assert "best_domain" in sql
    assert "next_retry_at <= now()" in sql
    assert "'error_category', rts.error_category" in sql
    assert "'retry_strategy', rts.retry_strategy" in sql


def test_working_store_migration_down_drops_schema() -> None:
    sql = DOWN_SQL.read_text()

    assert "DROP SCHEMA IF EXISTS dagster_brreg CASCADE" in sql


def test_runtime_task_state_finished_sql_marks_manual_retry_strategies_terminal() -> None:
    from corpscout_dagster.brreg.working_store import UPDATE_TASK_STATE_FINISHED_SQL

    assert "retry_strategy)s IN (" in UPDATE_TASK_STATE_FINISHED_SQL
    assert "'change_model_or_prompt'" in UPDATE_TASK_STATE_FINISHED_SQL
    assert "'manual_config'" in UPDATE_TASK_STATE_FINISHED_SQL
    assert "'manual_input'" in UPDATE_TASK_STATE_FINISHED_SQL
    assert "'not_retryable'" in UPDATE_TASK_STATE_FINISHED_SQL


def test_runtime_retry_failures_sql_casts_optional_task_type_parameter() -> None:
    from corpscout_dagster.brreg.working_store import RETRY_TASK_FAILURES_SQL

    assert "%(task_type)s::text IS NULL" in RETRY_TASK_FAILURES_SQL
    assert "rts.task_type = %(task_type)s::text" in RETRY_TASK_FAILURES_SQL
