from __future__ import annotations

from corpscout_dagster.brreg.assets import build_brreg_raw_input_rows
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.definitions import defs


def test_build_brreg_raw_input_rows_maps_records_with_run_id() -> None:
    records = [
        BrregRawRecord.from_payload({"organisasjonsnummer": "810202572", "navn": "BORTIGARD AS"}),
        None,
        BrregRawRecord.from_payload({"organisasjonsnummer": "910202572", "navn": "NEXT AS"}),
    ]

    rows = build_brreg_raw_input_rows(records=records, run_id="dagster-run-1")

    assert [row.organization_number for row in rows] == ["810202572", "910202572"]
    assert {row.run_id for row in rows} == {"dagster-run-1"}


def test_definitions_include_brreg_raw_inputs_asset() -> None:
    asset_keys = {
        key.to_user_string()
        for definition in defs.assets or []
        for key in definition.keys
    }

    assert "brreg_raw_inputs" in asset_keys
