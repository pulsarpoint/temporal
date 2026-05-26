from __future__ import annotations

import json

from corpscout_dagster.brreg.assets import (
    build_brreg_working_raw_record_rows,
    brreg_domain_enhanced_records,
    brreg_translation_results,
    materialize_brreg_enhanced_records,
    materialize_brreg_publish_enhanced_records,
    materialize_brreg_domain_results,
    materialize_brreg_domain_proposals,
    materialize_brreg_duckduckgo_search_results,
    materialize_brreg_domain_signal_candidates,
    materialize_brreg_web_search_llm_candidates,
    materialize_brreg_translation_results,
    materialize_brreg_working_raw_records,
    resolve_brreg_batch_run_config,
)
from corpscout_dagster.brreg.fx_rates import FxRateSet
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.domain_enrichment import DomainCandidate
from corpscout_dagster.brreg.domain_search_llm import DomainCrawlArtifact, SearchResult, VerifiedDomainSearchResults
from corpscout_dagster.brreg.translation import TranslationItem, translation_item_id
from corpscout_dagster.definitions import defs


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)


class FakeContext:
    def __init__(self, op_config: dict | None = None) -> None:
        self.run_id = "dagster-run-1"
        self.log = FakeLogger()
        self.metadata: list[dict] = []
        self.op_config = op_config or {}

    def add_output_metadata(self, metadata: dict) -> None:
        self.metadata.append(metadata)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.many_calls: list[tuple[str, list[dict]]] = []
        self.last_sql = ""
        self.seed_pending_count = 0
        self.fetchone_values = [
            ("00000000-0000-0000-0000-000000000001",),
            ("00000000-0000-0000-0000-000000000002",),
        ]
        self.fetchall_values: list[list[tuple]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict) -> None:
        self.last_sql = sql
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
        if "seeded_raw_records" in self.last_sql:
            return (self.seed_pending_count,)
        return self.fetchone_values.pop(0)

    def fetchall(self):
        if not self.fetchall_values:
            return []
        return self.fetchall_values.pop(0)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeTranslator:
    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        return {
            translation_item_id(item): f"translated {item.text}"
            for item in items
        }


class RecordingTranslator:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        self.calls.append(
            {
                "category": category,
                "items": items,
                "source_lang": source_lang,
                "target_lang": target_lang,
                "model": model,
                "prompt_version": prompt_version,
            }
        )
        return {
            translation_item_id(item): f"translated {item.text}"
            for item in items
        }


class MissingTranslator:
    def translate_terms(
        self,
        *,
        category: str,
        items: list[TranslationItem],
        source_lang: str,
        target_lang: str,
        model: str,
        prompt_version: str,
    ) -> dict[str, str]:
        return {}


class FakeCrawlServiceClient:
    def __init__(self, *, payload: dict | None = None, error: Exception | None = None) -> None:
        self.payload = payload or {
            "schema_version": "crawl-service.brreg.v1",
            "status": "succeeded",
            "record_id": "00000000-0000-0000-0000-000000000010",
            "organization_number": "810202572",
            "best_domain": "bortigard.no",
            "candidates": [
                {
                    "domain": "bortigard.no",
                    "normalized_domain": "bortigard.no",
                    "confidence": 91,
                    "source": "existing_website",
                    "evidence": {"url": "https://www.bortigard.no"},
                }
            ],
            "search_artifacts": [],
            "crawl_artifacts": [],
            "errors": [],
            "warnings": [],
            "provider": "fake",
            "model": "fake-model",
            "service_version": "0.1.0",
        }
        self.error = error
        self.calls: list = []

    def discover_brreg_domain(self, record):
        self.calls.append(record)
        if self.error is not None:
            raise self.error
        return self.payload


def test_build_brreg_working_raw_record_rows_maps_valid_records() -> None:
    records = [
        BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
        None,
        BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
    ]

    rows = build_brreg_working_raw_record_rows(records=records)

    assert [row.organization_number for row in rows] == ["810202572", "910202572"]


def test_definitions_expose_only_operational_brreg_assets() -> None:
    asset_keys = {
        key.to_user_string()
        for definition in defs.assets or []
        for key in definition.keys
    }

    assert asset_keys == {
        "brreg_translation_results",
        "brreg_domain_enhanced_records",
    }
    assert "brreg_working_raw_records" not in asset_keys
    assert "brreg_domain_results" not in asset_keys
    assert "brreg_domain_website_field_candidates" not in asset_keys
    assert "brreg_domain_duckduckgo_search_results" not in asset_keys
    assert "brreg_domain_duckduckgo_candidates" not in asset_keys
    assert "brreg_domain_crtsh_candidates" not in asset_keys
    assert "brreg_domain_wikidata_candidates" not in asset_keys
    assert "brreg_domain_web_search_llm_candidates" not in asset_keys
    assert "brreg_domain_dns_heuristic_candidates" not in asset_keys
    assert "brreg_domain_proposals" not in asset_keys
    assert "brreg_enhanced_records" not in asset_keys
    assert "brreg_publish_enhanced_records" not in asset_keys


def test_definitions_expose_only_translation_and_domain_enhanced_jobs() -> None:
    job_names = {job.name for job in defs.jobs or []}

    assert job_names == {
        "brreg_translate_job",
        "brreg_domain_enhanced_job",
    }
    assert "brreg_ingest_job" not in job_names
    assert "brreg_domain_enrichment_job" not in job_names
    assert "brreg_domain_results_job" not in job_names
    assert "brreg_domain_website_field_job" not in job_names
    assert "brreg_domain_duckduckgo_search_job" not in job_names
    assert "brreg_domain_duckduckgo_job" not in job_names
    assert "brreg_domain_crtsh_job" not in job_names
    assert "brreg_domain_wikidata_job" not in job_names
    assert "brreg_domain_web_search_llm_job" not in job_names
    assert "brreg_domain_dns_heuristic_job" not in job_names
    assert "brreg_domain_proposals_job" not in job_names
    assert "brreg_enhanced_records_job" not in job_names
    assert "brreg_publish_enhanced_records_job" not in job_names


def test_brreg_task_assets_expose_batch_controls_in_launchpad() -> None:
    configurable_assets = [
        brreg_translation_results,
        brreg_domain_enhanced_records,
    ]

    for asset_def in configurable_assets:
        fields = asset_def.node_def.config_schema.config_type.fields
        assert set(fields) == {"batch_size", "max_batches_per_run", "max_parallel_tasks"}
        assert fields["batch_size"].default_provided
        assert fields["max_batches_per_run"].default_provided
        assert fields["max_parallel_tasks"].default_provided

def test_resolve_brreg_batch_run_config_prefers_launchpad_config_over_env(monkeypatch) -> None:
    monkeypatch.setenv("BRREG_TEST_BATCH_SIZE", "100")
    monkeypatch.setenv("BRREG_TEST_MAX_BATCHES", "20")
    context = FakeContext(op_config={"batch_size": 7, "max_batches_per_run": 3})

    config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_TEST_BATCH_SIZE",
        batch_size_default=50,
        max_batches_env="BRREG_TEST_MAX_BATCHES",
        max_batches_default=0,
    )

    assert config.batch_size == 7
    assert config.max_batches_per_run == 3
    assert config.max_parallel_tasks == 50


def test_resolve_brreg_batch_run_config_accepts_launchpad_parallel_task_override(monkeypatch) -> None:
    monkeypatch.setenv("BRREG_TEST_MAX_PARALLEL_TASKS", "2")
    context = FakeContext(op_config={"batch_size": 7, "max_batches_per_run": 3, "max_parallel_tasks": 4})

    config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_TEST_BATCH_SIZE",
        batch_size_default=50,
        max_batches_env="BRREG_TEST_MAX_BATCHES",
        max_batches_default=0,
        max_parallel_tasks_env="BRREG_TEST_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=1,
    )

    assert config.batch_size == 7
    assert config.max_batches_per_run == 3
    assert config.max_parallel_tasks == 4


def test_materialize_brreg_working_raw_records_writes_batches_and_progress() -> None:
    context = FakeContext()
    connection = FakeConnection()
    records = [
        BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
        BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
        BrregRawRecord.from_payload({"organisasjonsnummer": "710202572", "navn": "THIRD AS"}),
    ]

    result = materialize_brreg_working_raw_records(
        context,
        records=records,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=2,
    )

    assert result == {"rows_seen": 3, "rows_written": 3}
    assert connection.commits == 4
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert sum("INSERT INTO dagster_brreg.raw_records" in sql for sql in many_sql_calls) == 2
    assert sum("records_seen = records_seen + %(records_seen)s" in sql for sql in sql_calls) == 2
    assert any("finished_at = now()" in sql for sql in sql_calls)
    assert context.metadata[-1]["rows_seen"] == 3
    assert context.metadata[-1]["rows_written"] == 3
    assert any("BRREG raw ingest batch committed" in message for message in context.log.messages)


def test_materialize_brreg_translation_results_writes_task_cache_and_result() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {
                    "organisasjonsnummer": "810202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            )
        ],
        [],
    ]

    result = materialize_brreg_translation_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        translator=FakeTranslator(),
        batch_size=50,
        model="qwen3:6b",
        prompt_version="v1",
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    assert all(isinstance(value, int) for value in result.values())
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert any("run_type" in sql and "INSERT INTO dagster_brreg.enrichment_runs" in sql for sql in sql_calls)
    assert any("INSERT INTO dagster_brreg.translation_results" in sql for sql in sql_calls)
    assert any("INSERT INTO dagster_brreg.translation_cache" in sql for sql in many_sql_calls)
    assert context.metadata[-1]["rows_completed"] == 1


def test_materialize_brreg_translation_results_translates_unique_batch_misses_in_one_mixed_call() -> None:
    context = FakeContext()
    connection = FakeConnection()
    translator = RecordingTranslator()
    first_raw_record_id = "00000000-0000-0000-0000-000000000010"
    second_raw_record_id = "00000000-0000-0000-0000-000000000020"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", first_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000021", second_raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                first_raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {
                    "organisasjonsnummer": "810202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            ),
            (
                second_raw_record_id,
                "910202572",
                "NEXT AS",
                None,
                {
                    "organisasjonsnummer": "910202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                    "naeringskode1": {"kode": "62.010", "beskrivelse": "Programmeringstjenester"},
                },
            ),
        ],
        [],
    ]

    result = materialize_brreg_translation_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        translator=translator,
        batch_size=50,
        max_batches_per_run=1,
        model="qwen3:6b",
        prompt_version="v1",
    )

    assert result["rows_seen"] == 2
    assert result["rows_completed"] == 2
    assert result["rows_failed"] == 0
    assert len(translator.calls) == 1
    assert translator.calls[0]["category"] == "mixed"
    assert {
        (item.category, item.text)
        for item in translator.calls[0]["items"]
    } == {
        ("org_form", "Aksjeselskap"),
        ("industry_code", "Programmeringstjenester"),
    }
    assert sum("INSERT INTO dagster_brreg.translation_results" in sql for sql, _ in connection.cursor_instance.calls) == 2
    cache_rows = connection.cursor_instance.many_calls[-1][1]
    assert {(row["category"], row["original_text"]) for row in cache_rows} == {
        ("org_form", "Aksjeselskap"),
        ("industry_code", "Programmeringstjenester"),
    }


def test_materialize_brreg_translation_results_drains_multiple_batches_until_empty() -> None:
    context = FakeContext()
    connection = FakeConnection()
    first_raw_record_id = "00000000-0000-0000-0000-000000000010"
    second_raw_record_id = "00000000-0000-0000-0000-000000000020"
    third_raw_record_id = "00000000-0000-0000-0000-000000000030"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", first_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000021", second_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000031", third_raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                first_raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {
                    "organisasjonsnummer": "810202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            ),
            (
                second_raw_record_id,
                "910202572",
                "NEXT AS",
                None,
                {
                    "organisasjonsnummer": "910202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            ),
        ],
        [],
        [
            (
                third_raw_record_id,
                "710202572",
                "THIRD AS",
                None,
                {
                    "organisasjonsnummer": "710202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            )
        ],
        [],
        [],
    ]

    result = materialize_brreg_translation_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        translator=FakeTranslator(),
        batch_size=2,
        max_batches_per_run=0,
        model="qwen3:6b",
        prompt_version="v1",
    )

    assert result["rows_seen"] == 3
    assert result["rows_completed"] == 3
    assert result["rows_failed"] == 0
    assert result["batches_processed"] == 2
    assert context.metadata[-1]["stopped_reason"] == "no_pending_records"
    fetch_calls = [
        params
        for sql, params in connection.cursor_instance.calls
        if "pending_task_ids AS" in sql
    ]
    assert fetch_calls == [
        {
            "task_type": "translate",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 50,
            "lease_seconds": 1800,
        },
        {
            "task_type": "translate",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 50,
            "lease_seconds": 1800,
        },
        {
            "task_type": "translate",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 50,
            "lease_seconds": 1800,
        },
    ]
    assert not any("seeded_raw_records" in sql for sql, _ in connection.cursor_instance.calls)


def test_materialize_brreg_translation_results_marks_existing_attempt_failed() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {
                    "organisasjonsnummer": "810202572",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                },
            )
        ],
        [],
        [],
    ]

    result = materialize_brreg_translation_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        translator=MissingTranslator(),
        batch_size=50,
        model="qwen3:6b",
        prompt_version="v1",
    )

    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    assert result["rows_completed"] == 0
    assert result["rows_failed"] == 1
    assert all(isinstance(value, int) for value in result.values())
    assert sum("INSERT INTO dagster_brreg.task_attempts" in sql for sql in sql_calls) == 1
    assert any("INSERT INTO dagster_brreg.translation_results" in sql for sql in sql_calls)


def test_materialize_brreg_domain_signal_candidates_writes_independent_task_result() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572", "hjemmeside": "https://www.bortigard.no"},
            )
        ],
    ]

    result = materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        signal="website_field",
        task_type="domain_website_field",
        batch_size=500,
        max_parallel_tasks=1,
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    assert result["domains_written"] == 1
    assert all(isinstance(value, int) for value in result.values())
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert any("INSERT INTO dagster_brreg.domain_candidates" in sql for sql in many_sql_calls)
    assert any("UPDATE dagster_brreg.task_attempts" in sql for sql in sql_calls)


def test_materialize_brreg_domain_results_writes_single_service_artifact() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                "https://www.bortigard.no",
                {"organisasjonsnummer": "810202572", "hjemmeside": "https://www.bortigard.no"},
            )
        ],
        [],
    ]
    domain_client = FakeCrawlServiceClient()

    result = materialize_brreg_domain_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        crawl_service_client=domain_client,
        batch_size=500,
        max_parallel_tasks=1,
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    assert result["domain_results_written"] == 1
    assert all(isinstance(value, int) for value in result.values())
    assert len(domain_client.calls) == 1
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    params_by_sql = connection.cursor_instance.calls
    assert any("INSERT INTO dagster_brreg.domain_results" in sql for sql in sql_calls)
    assert any(
        "INSERT INTO dagster_brreg.task_attempts" in sql and params.get("task_type") == "domain_results"
        for sql, params in params_by_sql
    )
    assert any(
        "INSERT INTO dagster_brreg.domain_results" in sql
        and params.get("status") == "succeeded"
        and params.get("best_domain") == "bortigard.no"
        for sql, params in params_by_sql
    )


def test_materialize_brreg_website_field_candidates_skips_records_without_website() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572"},
            )
        ],
    ]

    result = materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        signal="website_field",
        task_type="domain_website_field",
        batch_size=500,
        max_parallel_tasks=1,
    )

    assert result["rows_completed"] == 1
    assert result["domains_written"] == 0
    assert any(
        params.get("status") == "skipped"
        for _, params in connection.cursor_instance.calls
    )


def test_materialize_brreg_duckduckgo_search_results_writes_search_artifacts() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572"},
            )
        ],
    ]

    result = materialize_brreg_duckduckgo_search_results(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=10,
        max_parallel_tasks=1,
        search_collector=lambda **_: [
            SearchResult(
                query='"BORTIGARD AS" Norway official website',
                rank=1,
                url="https://www.bortigard.no/",
                domain="www.bortigard.no",
                normalized_domain="bortigard.no",
                title="Bortigard AS",
                description="Norwegian property company.",
            )
        ],
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    assert result["search_results_written"] == 1
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    sql_params = [params for _, params in connection.cursor_instance.calls]
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert any(params.get("task_type") == "domain_duckduckgo_search" for params in sql_params)
    assert any("INSERT INTO dagster_brreg.domain_search_results" in sql for sql in many_sql_calls)


def test_materialize_brreg_web_search_llm_candidates_writes_crawl_and_candidate_artifacts() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    search_result_id = "00000000-0000-0000-0000-000000000099"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572"},
            )
        ],
        [
            (
                search_result_id,
                raw_record_id,
                '"BORTIGARD AS" Norway official website',
                1,
                "https://www.bortigard.no/",
                "www.bortigard.no",
                "bortigard.no",
                "Bortigard AS",
                "Norwegian property company.",
                {},
            )
        ],
    ]
    search_result = SearchResult(
        query='"BORTIGARD AS" Norway official website',
        rank=1,
        url="https://www.bortigard.no/",
        domain="www.bortigard.no",
        normalized_domain="bortigard.no",
        title="Bortigard AS",
        description="Norwegian property company.",
    )

    result = materialize_brreg_web_search_llm_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=10,
        max_parallel_tasks=1,
        verifier=lambda **_: VerifiedDomainSearchResults(
            candidates=[
                DomainCandidate(
                    domain="www.bortigard.no",
                    normalized_domain="bortigard.no",
                    signal="web_search_llm",
                    confidence=91,
                    evidence={"source": "test"},
                    metadata={"source": "dagster"},
                )
            ],
            crawl_results=[
                DomainCrawlArtifact(
                    search_result=search_result,
                    status="succeeded",
                    markdown="# Bortigard AS",
                    markdown_hash="markdown-hash",
                    llm_confidence=91,
                    llm_decision="accepted",
                    llm_reason="Exact company evidence.",
                    llm_evidence={"matched_evidence": ["Bortigard AS"]},
                    metadata={"source": "test"},
                )
            ],
        ),
    )

    assert result["rows_completed"] == 1
    assert result["domains_written"] == 1
    assert result["crawl_results_written"] == 1
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert any("INSERT INTO dagster_brreg.domain_crawl_results" in sql for sql in many_sql_calls)
    assert any("INSERT INTO dagster_brreg.domain_candidates" in sql for sql in many_sql_calls)


def test_materialize_brreg_domain_signal_candidates_drains_multiple_batches_for_one_signal() -> None:
    context = FakeContext()
    connection = FakeConnection()
    first_raw_record_id = "00000000-0000-0000-0000-000000000010"
    second_raw_record_id = "00000000-0000-0000-0000-000000000020"
    third_raw_record_id = "00000000-0000-0000-0000-000000000030"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", first_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000021", second_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000031", third_raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                first_raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572", "hjemmeside": "https://www.bortigard.no"},
            ),
            (
                second_raw_record_id,
                "910202572",
                "NEXT AS",
                None,
                {"organisasjonsnummer": "910202572", "hjemmeside": "https://www.next.no"},
            ),
        ],
        [
            (
                third_raw_record_id,
                "710202572",
                "THIRD AS",
                None,
                {"organisasjonsnummer": "710202572", "hjemmeside": "https://www.third.no"},
            )
        ],
        [],
    ]

    result = materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        signal="website_field",
        task_type="domain_website_field",
        batch_size=2,
        max_batches_per_run=3,
        max_parallel_tasks=1,
    )

    assert result["rows_seen"] == 3
    assert result["rows_completed"] == 3
    assert result["rows_failed"] == 0
    assert result["domains_written"] == 3
    assert result["batches_processed"] == 2
    assert context.metadata[-1]["max_batches_per_run"] == 3
    assert context.metadata[-1]["signal"] == "website_field"
    assert context.metadata[-1]["stopped_reason"] == "no_pending_records"
    fetch_calls = [
        params
        for sql, params in connection.cursor_instance.calls
        if "pending_task_ids AS" in sql
    ]
    assert fetch_calls == [
        {
            "task_type": "domain_website_field",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 1,
            "lease_seconds": 1800,
        },
        {
            "task_type": "domain_website_field",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 1,
            "lease_seconds": 1800,
        },
        {
            "task_type": "domain_website_field",
            "limit": 2,
            "include_new_records": True,
            "max_parallel_tasks": 1,
            "lease_seconds": 1800,
        },
    ]
    assert not any("seeded_raw_records" in sql for sql, _ in connection.cursor_instance.calls)


def test_materialize_brreg_domain_signal_candidates_zero_max_batches_drains_until_empty() -> None:
    context = FakeContext()
    connection = FakeConnection()
    first_raw_record_id = "00000000-0000-0000-0000-000000000010"
    second_raw_record_id = "00000000-0000-0000-0000-000000000020"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", first_raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000021", second_raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                first_raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572", "hjemmeside": "https://www.bortigard.no"},
            )
        ],
        [
            (
                second_raw_record_id,
                "910202572",
                "NEXT AS",
                None,
                {"organisasjonsnummer": "910202572", "hjemmeside": "https://www.next.no"},
            )
        ],
        [],
    ]

    result = materialize_brreg_domain_signal_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        signal="website_field",
        task_type="domain_website_field",
        batch_size=1,
        max_batches_per_run=0,
        max_parallel_tasks=1,
    )

    assert result["rows_seen"] == 2
    assert result["rows_completed"] == 2
    assert result["domains_written"] == 2
    assert result["batches_processed"] == 2
    assert context.metadata[-1]["max_batches_per_run"] == 0
    assert context.metadata[-1]["stopped_reason"] == "no_pending_records"


def test_materialize_brreg_domain_proposals_scores_candidates_for_pending_records() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                None,
                {"organisasjonsnummer": "810202572"},
            )
        ],
        [
            ("bortigard.no", "www.bortigard.no", "website_field", 95, {"website": "https://www.bortigard.no"}, {}),
            ("bortigard.no", "bortigard.no", "wikidata", 85, {"url": "https://www.bortigard.no"}, {}),
            ("wrong.no", "wrong.no", "duckduckgo", 70, {"url": "https://wrong.no"}, {}),
        ],
    ]

    result = materialize_brreg_domain_proposals(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=50,
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    assert result["proposals_written"] == 2
    assert all(isinstance(value, int) for value in result.values())
    many_sql_calls = [sql for sql, _ in connection.cursor_instance.many_calls]
    assert any("INSERT INTO dagster_brreg.domain_proposals" in sql for sql in many_sql_calls)
    proposal_params = connection.cursor_instance.many_calls[-1][1]
    assert proposal_params[0]["normalized_domain"] == "bortigard.no"
    assert proposal_params[0]["score"] == 100
    assert proposal_params[0]["signals"] == ["website_field", "wikidata"]


def test_materialize_brreg_enhanced_records_builds_payloads_for_ready_records() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000101",),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                "active",
                None,
                "NO",
                {
                    "organisasjonsnummer": "810202572",
                    "navn": "BORTIGARD AS",
                    "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
                    "kapital": {
                        "type": "Aksjekapital",
                        "belop": 81870.00,
                        "valuta": "NOK",
                        "innfortDato": "2012-07-09",
                        "antallAksjer": 8187,
                    },
                },
                "payload-hash",
                "succeeded",
                {
                    "terms": [
                        {
                            "category": "org_form",
                            "original_text": "Aksjeselskap",
                            "translated_text": "Limited Liability Company",
                        },
                        {
                            "category": "capital_type",
                            "original_text": "Aksjekapital",
                            "translated_text": "Share capital",
                        }
                    ]
                },
                "not_found",
                [],
                {"translate": "succeeded", "domain_results": "succeeded"},
            )
        ],
    ]

    result = materialize_brreg_enhanced_records(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=50,
        fx_rate_loader=lambda _: FxRateSet(
            source="ECB",
            rate_date="2026-05-21",
            eur_per={
                "EUR": 1.0,
                "NOK": 10.7075,
                "USD": 1.1599,
            },
        ),
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    assert any("INSERT INTO dagster_brreg.enhanced_records" in sql for sql in sql_calls)
    insert_params = next(
        params
        for sql, params in connection.cursor_instance.calls
        if "INSERT INTO dagster_brreg.enhanced_records" in sql
    )
    enhanced_payload = json.loads(insert_params["enhanced_payload"])
    assert enhanced_payload["schema_version"] == "brreg.enhanced.v1"
    assert enhanced_payload["enhancement"]["section_statuses"]["domains"] == "not_available"
    assert enhanced_payload["capital"]["amount_usd_cents"] == 886864
    assert context.metadata[-1]["rows_completed"] == 1


def test_materialize_brreg_publish_enhanced_records_writes_handoff_tables() -> None:
    context = FakeContext()
    connection = FakeConnection()
    raw_record_id = "00000000-0000-0000-0000-000000000010"
    enhanced_record_id = "00000000-0000-0000-0000-000000000020"
    connection.cursor_instance.fetchone_values = [
        ("00000000-0000-0000-0000-000000000001",),
        ("00000000-0000-0000-0000-000000000011", raw_record_id, 1),
        ("00000000-0000-0000-0000-000000000101",),
        ("00000000-0000-0000-0000-000000000201",),
    ]
    connection.cursor_instance.fetchall_values = [
        [
            (
                enhanced_record_id,
                raw_record_id,
                "810202572",
                "BORTIGARD AS",
                "active",
                None,
                "NO",
                {"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
                "raw-payload-hash",
                "brreg.enhanced.v1",
                {
                    "schema_version": "brreg.enhanced.v1",
                    "enhancement": {
                        "status": "partial",
                        "section_statuses": {"financials": "not_available"},
                    },
                },
                "enhanced-hash",
            )
        ],
    ]

    result = materialize_brreg_publish_enhanced_records(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=50,
    )

    assert result["rows_seen"] == 1
    assert result["rows_completed"] == 1
    assert result["rows_failed"] == 0
    sql_calls = [sql for sql, _ in connection.cursor_instance.calls]
    assert any("INSERT INTO brreg_company_raw_inputs" in sql for sql in sql_calls)
    assert any("INSERT INTO brreg_enhanced_raw_inputs" in sql for sql in sql_calls)
    assert any("status = 'published'" in sql for sql in sql_calls)
