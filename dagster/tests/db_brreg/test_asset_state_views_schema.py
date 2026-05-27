from __future__ import annotations

from pathlib import Path


MIGRATIONS_DIR = Path(__file__).parents[2] / "db" / "migrations"
UP_SQL = MIGRATIONS_DIR / "000019_brreg_asset_state_views.up.sql"
DOWN_SQL = MIGRATIONS_DIR / "000019_brreg_asset_state_views.down.sql"


def test_asset_state_migration_creates_required_views() -> None:
    sql = UP_SQL.read_text()

    for view_name in [
        "v_raw_records_asset_state",
        "v_translation_asset_state",
        "v_domain_asset_state",
        "v_financial_asset_state",
        "v_enhanced_asset_state",
        "v_translation_asset_rows",
        "v_domain_asset_rows",
        "v_financial_asset_rows",
        "v_enhanced_asset_rows",
    ]:
        assert f"CREATE OR REPLACE VIEW dagster_brreg.{view_name}" in sql


def test_asset_state_views_expose_common_state_columns() -> None:
    sql = UP_SQL.read_text()

    for column in [
        "total_rows",
        "pending_rows",
        "running_rows",
        "failed_retryable_rows",
        "failed_terminal_rows",
        "succeeded_rows",
        "skipped_rows",
        "missing_artifact_rows",
        "eligible_rows",
        "is_complete",
        "is_blocked",
    ]:
        assert column in sql


def test_enhanced_asset_state_exposes_eligible_build_count() -> None:
    sql = UP_SQL.read_text()

    for column in [
        "translation_ready_rows",
        "domain_ready_rows",
        "financial_ready_rows",
        "eligible_for_enhanced_rows",
        "enhanced_built_rows",
        "enhanced_missing_rows",
        "enhanced_failed_rows",
    ]:
        assert column in sql


def test_asset_state_migration_down_drops_views() -> None:
    sql = DOWN_SQL.read_text()

    for view_name in [
        "v_enhanced_asset_rows",
        "v_financial_asset_rows",
        "v_domain_asset_rows",
        "v_translation_asset_rows",
        "v_enhanced_asset_state",
        "v_financial_asset_state",
        "v_domain_asset_state",
        "v_translation_asset_state",
        "v_raw_records_asset_state",
    ]:
        assert f"DROP VIEW IF EXISTS dagster_brreg.{view_name}" in sql
