from __future__ import annotations

from corpscout_dagster.db_brreg.views import BrregAssetStateViewReader


class FakeCursor:
    def __init__(self, row: tuple | None) -> None:
        self.row = row
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return self.row


def test_view_reader_fetches_translation_state_for_model_and_prompt() -> None:
    cursor = FakeCursor((1000, 0, 0, 0, 0, 950, 50, 0, 0, True, False))
    reader = BrregAssetStateViewReader(cursor)

    state = reader.fetch_translation_state(model="qwen3:6b", prompt_version="v1")

    assert state.total_rows == 1000
    assert state.succeeded_rows == 950
    assert state.skipped_rows == 50
    assert state.is_complete is True
    sql, params = cursor.calls[0]
    assert "dagster_brreg.v_translation_asset_state" in sql
    assert params == {"model": "qwen3:6b", "prompt_version": "v1"}


def test_view_reader_fetches_domain_state() -> None:
    cursor = FakeCursor((1000, 0, 0, 0, 0, 900, 90, 10, 0, False, False))
    reader = BrregAssetStateViewReader(cursor)

    state = reader.fetch_domain_state()

    assert state.total_rows == 1000
    assert state.missing_artifact_rows == 10
    assert state.is_complete is False
    assert "dagster_brreg.v_domain_asset_state" in cursor.calls[0][0]


def test_view_reader_missing_translation_model_uses_raw_total_as_missing() -> None:
    cursor = FakeCursor(None)
    reader = BrregAssetStateViewReader(cursor)

    state = reader.fetch_translation_state(model="missing-model", prompt_version="v1", raw_total_rows=1000)

    assert state.total_rows == 1000
    assert state.missing_artifact_rows == 1000
    assert state.is_complete is False
