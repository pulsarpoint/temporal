from pathlib import Path


MIGRATIONS_DIR = Path(__file__).parents[2] / "db" / "migrations"
UP_SQL = MIGRATIONS_DIR / "000001_brreg_working_store.up.sql"
DOWN_SQL = MIGRATIONS_DIR / "000001_brreg_working_store.down.sql"


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
    sql = UP_SQL.read_text()

    assert "task_type IN (" in sql
    assert "'translate'" in sql
    assert "'discover_domains'" in sql
    assert "'extract_financials'" in sql
    assert "'build_enhanced'" in sql
    assert "translated_payload JSONB" in sql
    assert "financial_payload JSONB NOT NULL DEFAULT '{}'::jsonb" in sql
    assert "usd_payload JSONB NOT NULL DEFAULT '{}'::jsonb" in sql
    assert "enhanced_payload JSONB NOT NULL" in sql


def test_working_store_migration_down_drops_schema() -> None:
    sql = DOWN_SQL.read_text()

    assert "DROP SCHEMA IF EXISTS dagster_brreg CASCADE" in sql
