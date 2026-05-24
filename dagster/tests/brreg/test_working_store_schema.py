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
    assert "'domain_dns_heuristic'" in sql
    assert "'merge_domain_proposals'" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.translation_cache" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.domain_proposals" in sql
    assert "CREATE TABLE IF NOT EXISTS dagster_brreg.raw_record_task_states" in sql
    assert "'failed_retryable'" in sql
    assert "'failed_terminal'" in sql
    assert "UNIQUE (category, source_lang, target_lang, original_hash, model, prompt_version)" in sql
    assert "idx_dagster_brreg_translation_cache_lookup" in sql
    assert "idx_dagster_brreg_translation_success" in sql
    assert "idx_dagster_brreg_domain_task_success" in sql
    assert "idx_dagster_brreg_domain_proposals_raw_score" in sql
    assert "idx_dagster_brreg_task_states_queue" in sql
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
    assert "'domain_dns_heuristic'" in sql
    assert "'merge_domain_proposals'" in sql
    assert "'build_enhanced'" in sql
    assert "'publish'" in sql


def test_working_store_migration_down_drops_schema() -> None:
    sql = DOWN_SQL.read_text()

    assert "DROP SCHEMA IF EXISTS dagster_brreg CASCADE" in sql
