from __future__ import annotations

from corpscout_dagster.brreg.smoke import SMOKE_ORG_NUMBER, build_smoke_row, run_smoke


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return ("CORPSCOUT DAGSTER SMOKE AS", "dagster-smoke")


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
    row = build_smoke_row(run_id="dagster-smoke")

    assert row.organization_number == SMOKE_ORG_NUMBER
    assert row.source_native_id == SMOKE_ORG_NUMBER
    assert row.organization_name == "CORPSCOUT DAGSTER SMOKE AS"
    assert row.run_id == "dagster-smoke"
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
    assert len(connection.cursor_instance.calls) == 2
    assert "INSERT INTO brreg_company_raw_inputs" in connection.cursor_instance.calls[0][0]
    assert "SELECT organization_name, run_id" in connection.cursor_instance.calls[1][0]
