from __future__ import annotations

from corpscout_dagster.brreg.assets import (
    build_brreg_working_raw_record_rows,
    brreg_domain_candidates,
    brreg_translation_results,
    materialize_brreg_domain_candidates,
    materialize_brreg_translation_results,
    materialize_brreg_working_raw_records,
)
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.brreg.translation import TranslationItem, translation_item_id
from corpscout_dagster.definitions import defs


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)


class FakeContext:
    def __init__(self) -> None:
        self.run_id = "dagster-run-1"
        self.log = FakeLogger()
        self.metadata: list[dict] = []

    def add_output_metadata(self, metadata: dict) -> None:
        self.metadata.append(metadata)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.many_calls: list[tuple[str, list[dict]]] = []
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
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
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


def test_build_brreg_working_raw_record_rows_maps_valid_records() -> None:
    records = [
        BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
        None,
        BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
    ]

    rows = build_brreg_working_raw_record_rows(records=records)

    assert [row.organization_number for row in rows] == ["810202572", "910202572"]


def test_definitions_include_brreg_working_raw_records_asset() -> None:
    asset_keys = {
        key.to_user_string()
        for definition in defs.assets or []
        for key in definition.keys
    }

    assert "brreg_working_raw_records" in asset_keys
    assert "brreg_translation_results" in asset_keys
    assert "brreg_domain_candidates" in asset_keys


def test_definitions_include_independent_brreg_jobs() -> None:
    job_names = {job.name for job in defs.jobs or []}

    assert "brreg_ingest_job" in job_names
    assert "brreg_translate_job" in job_names
    assert "brreg_domain_enrichment_job" in job_names
    assert brreg_translation_results is not brreg_domain_candidates


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


def test_materialize_brreg_domain_candidates_writes_independent_task_result() -> None:
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

    result = materialize_brreg_domain_candidates(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        batch_size=500,
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
