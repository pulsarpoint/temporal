from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class Cursor(Protocol):
    def execute(self, sql: str, params: dict[str, Any]) -> object:
        ...

    def fetchone(self):
        ...


@dataclass(frozen=True)
class BrregRawRecordsStateView:
    total_rows: int
    current_rows: int
    not_current_rows: int
    is_complete: bool


@dataclass(frozen=True)
class BrregAssetStateView:
    total_rows: int
    pending_rows: int
    running_rows: int
    failed_retryable_rows: int
    failed_terminal_rows: int
    succeeded_rows: int
    skipped_rows: int
    missing_artifact_rows: int
    eligible_rows: int
    is_complete: bool
    is_blocked: bool


class BrregAssetStateViewReader:
    def __init__(self, cursor: Cursor) -> None:
        self._cursor = cursor

    def fetch_raw_records_state(self) -> BrregRawRecordsStateView:
        self._cursor.execute(FETCH_RAW_RECORDS_ASSET_STATE_SQL, {})
        return _raw_records_state_from_row(self._cursor.fetchone())

    def fetch_translation_state(
        self,
        *,
        model: str,
        prompt_version: str,
        raw_total_rows: int | None = None,
    ) -> BrregAssetStateView:
        self._cursor.execute(
            FETCH_TRANSLATION_ASSET_STATE_SQL,
            {"model": model, "prompt_version": prompt_version},
        )
        return _asset_state_from_row(self._cursor.fetchone(), missing_total=raw_total_rows)

    def fetch_domain_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_DOMAIN_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())

    def fetch_financial_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_FINANCIAL_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())

    def fetch_enhanced_state(self) -> BrregAssetStateView:
        self._cursor.execute(FETCH_ENHANCED_ASSET_STATE_SQL, {})
        return _asset_state_from_row(self._cursor.fetchone())


def _raw_records_state_from_row(row) -> BrregRawRecordsStateView:
    if row is None:
        return BrregRawRecordsStateView(0, 0, 0, False)
    return BrregRawRecordsStateView(
        total_rows=int(row[0] or 0),
        current_rows=int(row[1] or 0),
        not_current_rows=int(row[2] or 0),
        is_complete=bool(row[3]),
    )


def _asset_state_from_row(row, *, missing_total: int | None = None) -> BrregAssetStateView:
    if row is None:
        total_rows = int(missing_total or 0)
        return BrregAssetStateView(
            total_rows,
            0,
            0,
            0,
            0,
            0,
            0,
            total_rows,
            total_rows,
            False,
            False,
        )
    return BrregAssetStateView(
        total_rows=int(row[0] or 0),
        pending_rows=int(row[1] or 0),
        running_rows=int(row[2] or 0),
        failed_retryable_rows=int(row[3] or 0),
        failed_terminal_rows=int(row[4] or 0),
        succeeded_rows=int(row[5] or 0),
        skipped_rows=int(row[6] or 0),
        missing_artifact_rows=int(row[7] or 0),
        eligible_rows=int(row[8] or 0),
        is_complete=bool(row[9]),
        is_blocked=bool(row[10]),
    )


FETCH_RAW_RECORDS_ASSET_STATE_SQL = """
SELECT
  total_rows,
  current_rows,
  not_current_rows,
  is_complete
FROM dagster_brreg.v_raw_records_asset_state
"""

FETCH_TRANSLATION_ASSET_STATE_SQL = """
SELECT
  total_rows,
  pending_rows,
  running_rows,
  failed_retryable_rows,
  failed_terminal_rows,
  succeeded_rows,
  skipped_rows,
  missing_artifact_rows,
  eligible_rows,
  is_complete,
  is_blocked
FROM dagster_brreg.v_translation_asset_state
WHERE model = %(model)s
  AND prompt_version = %(prompt_version)s
"""

FETCH_DOMAIN_ASSET_STATE_SQL = """
SELECT
  total_rows,
  pending_rows,
  running_rows,
  failed_retryable_rows,
  failed_terminal_rows,
  succeeded_rows,
  skipped_rows,
  missing_artifact_rows,
  eligible_rows,
  is_complete,
  is_blocked
FROM dagster_brreg.v_domain_asset_state
"""

FETCH_FINANCIAL_ASSET_STATE_SQL = FETCH_DOMAIN_ASSET_STATE_SQL.replace(
    "v_domain_asset_state",
    "v_financial_asset_state",
)
FETCH_ENHANCED_ASSET_STATE_SQL = FETCH_DOMAIN_ASSET_STATE_SQL.replace(
    "v_domain_asset_state",
    "v_enhanced_asset_state",
)
