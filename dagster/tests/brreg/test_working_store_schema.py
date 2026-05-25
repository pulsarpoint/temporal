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
        "domain_candidates",
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
    assert "CREATE INDEX idx_dagster_brreg_enhanced_publish_queue" in sql


def test_working_store_migration_tracks_task_outputs() -> None:
    sql = ALL_UP_SQL

    assert "task_type IN (" in sql
    assert "'translate'" in sql
    assert "'discover_domains'" in sql
    assert "'domain_website_field'" in sql
    assert "'domain_duckduckgo'" in sql
    assert "'domain_crtsh'" in sql
    assert "'domain_wikidata'" in sql
    assert "'domain_web_search_llm'" in sql
    assert "'merge_domain_proposals'" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.translation_cache" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_proposals" in sql
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
    assert "idx_dagster_brreg_domain_task_success" in sql
    assert "idx_dagster_brreg_domain_proposals_raw_score" in sql
    assert "idx_dagster_brreg_task_states_queue" in sql
    assert "idx_dagster_brreg_raw_records_current_last_seen_id" in sql
    assert "ON dagster_brreg.raw_records (last_seen_at, id)" in sql
    assert "idx_dagster_brreg_task_states_pending_retry_queue" in sql
    assert "idx_dagster_brreg_task_states_running_stale_queue" in sql
    assert "idx_dagster_brreg_domain_candidates_raw_updated" in sql


def test_working_store_migration_has_independent_brreg_run_types() -> None:
    sql = ALL_UP_SQL

    assert "'bulk_ingest'" in sql
    assert "'translate'" in sql
    assert "'discover_domains'" in sql
    assert "'domain_website_field'" in sql
    assert "'domain_duckduckgo'" in sql
    assert "'domain_crtsh'" in sql
    assert "'domain_wikidata'" in sql
    assert "'domain_web_search_llm'" in sql
    assert "'merge_domain_proposals'" in sql
    assert "'build_enhanced'" in sql
    assert "'publish'" in sql


def test_domain_discovery_dependencies_migration_adds_search_and_crawl_artifacts() -> None:
    sql = (MIGRATIONS_DIR / "000012_brreg_domain_discovery_dependencies.up.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_search_results" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_crawl_results" in sql
    assert "provider IN ('duckduckgo')" in sql
    assert "UNIQUE (raw_record_id, provider, query, rank, url)" in sql
    assert "UNIQUE (raw_record_id, url)" in sql
    assert "idx_dagster_brreg_domain_search_results_raw" in sql
    assert "idx_dagster_brreg_domain_crawl_results_raw" in sql
    assert "idx_dagster_brreg_domain_crawl_results_decision" in sql


def test_domain_discovery_dependencies_migration_updates_task_types_and_views() -> None:
    sql = (MIGRATIONS_DIR / "000012_brreg_domain_discovery_dependencies.up.sql").read_text()

    assert "'domain_duckduckgo_search'" in sql
    assert "'domain_duckduckgo'" in sql
    assert "'domain_crtsh'" in sql
    assert "'domain_wikidata'" in sql
    assert "DROP VIEW IF EXISTS dagster_brreg.v_domain_enrichment_summary" in sql
    assert sql.index("DROP VIEW IF EXISTS dagster_brreg.v_domain_enrichment_summary") < sql.index(
        "CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary"
    )
    assert "CREATE OR REPLACE VIEW dagster_brreg.v_domain_enrichment_summary" in sql
    assert "domain_search_results" in sql
    assert "domain_crawl_results" in sql


def test_working_store_migration_creates_observability_views() -> None:
    sql = ALL_UP_SQL

    for view_name in [
        "v_enrichment_run_summary",
        "v_task_state_summary",
        "v_failed_task_states",
        "v_raw_record_task_overview",
        "v_domain_enrichment_summary",
    ]:
        assert f"CREATE OR REPLACE VIEW dagster_brreg.{view_name}" in sql

    assert "jsonb_object_agg(" in sql
    assert "rts.task_type" in sql
    assert "domain_candidates_by_signal" in sql
    assert "best_domain" in sql
    assert "next_retry_at <= now()" in sql


def test_working_store_migration_down_drops_schema() -> None:
    sql = DOWN_SQL.read_text()

    assert "DROP SCHEMA IF EXISTS dagster_brreg CASCADE" in sql
