from __future__ import annotations

from corpscout_dagster.brreg.assets import (
    build_brreg_working_raw_record_rows,
    materialize_brreg_working_raw_records,
)
from corpscout_dagster.brreg.models import BrregRawRecord
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
