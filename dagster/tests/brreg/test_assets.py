from __future__ import annotations

from corpscout_dagster.brreg.assets import build_brreg_working_raw_record_rows
from corpscout_dagster.brreg.models import BrregRawRecord
from corpscout_dagster.definitions import defs


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
