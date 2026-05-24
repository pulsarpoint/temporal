from __future__ import annotations

from corpscout_dagster.brreg.models import CorpscoutBrregRawInputRow
from corpscout_dagster.brreg.writer import BrregRawInputWriter, UpsertResult


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))


def test_writer_upserts_raw_rows_with_conflict_preserving_existing_state() -> None:
    cursor = FakeCursor()
    writer = BrregRawInputWriter(cursor)
    row = CorpscoutBrregRawInputRow(
        source_native_id="810202572",
        organization_number="810202572",
        organization_name="BORTIGARD AS",
        registration_status="active",
        website="https://bortigard.no",
        country_iso2="NO",
        raw_payload={"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"},
        payload_hash="a" * 64,
        run_id="dagster-run-1",
    )

    result = writer.upsert_many([row])

    assert result == UpsertResult(rows_seen=1, rows_written=1)
    sql, params = cursor.calls[0]
    assert "INSERT INTO brreg_company_raw_inputs" in sql
    assert "ON CONFLICT (organization_number, payload_hash) DO UPDATE" in sql
    assert "last_seen_at = now()" in sql
    assert "run_id = EXCLUDED.run_id" in sql
    assert "state" not in sql
    assert params["organization_number"] == "810202572"
    assert params["raw_payload"] == '{"navn":"BORTIGARD AS","organisasjonsnummer":"810202572"}'


def test_writer_ignores_empty_batches() -> None:
    cursor = FakeCursor()
    writer = BrregRawInputWriter(cursor)

    result = writer.upsert_many([])

    assert result == UpsertResult(rows_seen=0, rows_written=0)
    assert cursor.calls == []
