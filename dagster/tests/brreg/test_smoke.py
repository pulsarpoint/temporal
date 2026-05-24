from __future__ import annotations

from corpscout_dagster.brreg.smoke import SMOKE_ORG_NUMBER, build_smoke_row, run_smoke


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.many_calls: list[tuple[str, list[dict]]] = []
        self.fetchone_values = [
            ("00000000-0000-0000-0000-000000000001",),
            ("00000000-0000-0000-0000-000000000002",),
            ("CORPSCOUT DAGSTER SMOKE AS",),
        ]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params) -> None:
        self.calls.append((sql, params))

    def executemany(self, sql: str, params_seq: list[dict]) -> None:
        self.many_calls.append((sql, params_seq))

    def fetchone(self):
        return self.fetchone_values.pop(0)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def rollback(self) -> None:
        self.rolled_back = True


def test_build_smoke_row_uses_stable_payload_and_run_id() -> None:
    row = build_smoke_row()

    assert row.organization_number == SMOKE_ORG_NUMBER
    assert row.source_native_id == SMOKE_ORG_NUMBER
    assert row.organization_name == "CORPSCOUT DAGSTER SMOKE AS"
    assert row.raw_payload["organisasjonsnummer"] == SMOKE_ORG_NUMBER


def test_run_smoke_upserts_verifies_and_rolls_back() -> None:
    connection = FakeConnection()

    result = run_smoke(
        "postgresql://example.invalid/corpscout",
        connection_factory=lambda _: connection,
    )

    assert result.organization_number == SMOKE_ORG_NUMBER
    assert result.rolled_back is True
    assert connection.rolled_back is True
    assert len(connection.cursor_instance.calls) == 3
    assert len(connection.cursor_instance.many_calls) == 2
    assert "INSERT INTO dagster_brreg.enrichment_runs" in connection.cursor_instance.calls[0][0]
    assert "INSERT INTO dagster_brreg.bulk_snapshots" in connection.cursor_instance.calls[1][0]
    assert "UPDATE dagster_brreg.raw_records" in connection.cursor_instance.many_calls[0][0]
    assert "INSERT INTO dagster_brreg.raw_records" in connection.cursor_instance.many_calls[1][0]
    assert "SELECT organization_name" in connection.cursor_instance.calls[2][0]
