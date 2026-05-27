from __future__ import annotations

from types import SimpleNamespace

from dagster import AssetKey

from corpscout_dagster.brreg.asset_checks import (
    evaluate_brreg_currency_results_live_table_state,
    evaluate_brreg_domain_results_live_table_state,
    evaluate_brreg_enhanced_records_live_table_state,
    evaluate_brreg_raw_records_live_table_state,
    evaluate_brreg_translation_results_live_table_state,
)
from corpscout_dagster.definitions import defs


RAW_SUMMARY_READY = (1000, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 1000, 0, 0, 0)


class FakeCursor:
    def __init__(self, rows_by_marker: dict[str, tuple[int, ...]]) -> None:
        self.rows_by_marker = rows_by_marker
        self.last_sql = ""
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict) -> None:
        self.last_sql = sql
        self.calls.append((sql, params))

    def fetchone(self):
        for marker, row in self.rows_by_marker.items():
            if marker in self.last_sql:
                return row
        raise AssertionError(f"unexpected SQL: {self.last_sql}")


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_instance = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def _context(rows_by_marker: dict[str, tuple[int, ...]]):
    cursor = FakeCursor(rows_by_marker)
    connection = FakeConnection(cursor)
    return SimpleNamespace(
        resources=SimpleNamespace(
            postgres=SimpleNamespace(
                database_url="postgresql://example.invalid/corpscout",
                connection_factory=lambda _: connection,
            ),
            translation_service=SimpleNamespace(model="qwen3:6b", prompt_version="v1"),
        )
    )


def test_definitions_expose_live_table_asset_checks() -> None:
    check_specs = [
        spec
        for checks_definition in defs.asset_checks or []
        for spec in checks_definition.check_specs
    ]

    assert {
        (spec.asset_key, spec.name)
        for spec in check_specs
    } == {
        (AssetKey("brreg_raw_records"), "live_table_state"),
        (AssetKey("brreg_translation_results"), "live_table_state"),
        (AssetKey("brreg_domain_results"), "live_table_state"),
        (AssetKey("brreg_currency_results"), "live_table_state"),
        (AssetKey("brreg_enhanced_records"), "live_table_state"),
    }
    assert all(spec.blocking for spec in check_specs)


def test_operational_asset_jobs_do_not_run_live_table_checks() -> None:
    assert set(defs.resolve_job_def("brreg_translate_job").graph.node_dict) == {"brreg_translation_results"}
    assert set(defs.resolve_job_def("brreg_domain_job").graph.node_dict) == {"brreg_domain_results"}
    assert set(defs.resolve_job_def("brreg_currency_job").graph.node_dict) == {"brreg_currency_results"}
    assert set(defs.resolve_job_def("brreg_build_enhanced_job").graph.node_dict) == {"brreg_enhanced_records"}


def test_live_table_checks_job_runs_only_checks() -> None:
    assert set(defs.resolve_job_def("brreg_live_table_checks_job").graph.node_dict) == {
        "brreg_raw_records_live_table_state",
        "brreg_translation_results_live_table_state",
        "brreg_domain_results_live_table_state",
        "brreg_currency_results_live_table_state",
        "brreg_enhanced_records_live_table_state",
    }


def test_raw_records_live_table_check_fails_without_current_rows() -> None:
    context = _context({"fetch_raw_task_state_summary": (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)})

    result = evaluate_brreg_raw_records_live_table_state(context)

    assert result.passed is False
    assert result.metadata["live_raw_records_current"].value == 0


def test_translation_results_live_table_check_fails_when_table_rows_are_deleted() -> None:
    context = _context(
        {
            "fetch_raw_task_state_summary": RAW_SUMMARY_READY,
            "fetch_translation_artifact_summary": (0, 0, 0, 1000, 1000),
        }
    )

    result = evaluate_brreg_translation_results_live_table_state(context)

    assert result.passed is False
    assert result.metadata["live_translation_results_missing"].value == 1000
    assert result.metadata["live_translation_results_succeeded"].value == 0


def test_translation_results_live_table_check_passes_when_every_current_row_has_success_or_skip() -> None:
    context = _context(
        {
            "fetch_raw_task_state_summary": RAW_SUMMARY_READY,
            "fetch_translation_artifact_summary": (950, 50, 0, 0, 0),
        }
    )

    result = evaluate_brreg_translation_results_live_table_state(context)

    assert result.passed is True
    assert result.metadata["live_translation_results_succeeded"].value == 950
    assert result.metadata["live_translation_prompt_version"].text == "v1"


def test_artifact_live_table_checks_fail_on_missing_or_failed_latest_rows() -> None:
    domain = evaluate_brreg_domain_results_live_table_state(
        _context(
            {
                "fetch_raw_task_state_summary": RAW_SUMMARY_READY,
                "fetch_domain_result_summary": (900, 0, 0, 10, 90),
            }
        )
    )
    currency = evaluate_brreg_currency_results_live_table_state(
        _context(
            {
                "fetch_raw_task_state_summary": RAW_SUMMARY_READY,
                "fetch_currency_result_summary": (900, 0, 0, 10, 90),
            }
        )
    )
    enhanced = evaluate_brreg_enhanced_records_live_table_state(
        _context(
            {
                "fetch_raw_task_state_summary": RAW_SUMMARY_READY,
                "fetch_enhanced_record_summary": (900, 0, 10, 0, 90),
            }
        )
    )

    assert domain.passed is False
    assert currency.passed is False
    assert enhanced.passed is False
    assert domain.metadata["live_domain_results_missing"].value == 90
    assert currency.metadata["live_currency_results_failed"].value == 10
    assert enhanced.metadata["live_enhanced_records_publish_failed"].value == 10
