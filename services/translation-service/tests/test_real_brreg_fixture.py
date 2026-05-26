from __future__ import annotations

from corpscout_translation_service.brreg import extract_translation_items
from corpscout_translation_service.models import BrregRecord

from tests.real_brreg_records import load_real_brreg_records


def test_real_brreg_fixture_contains_300_database_records() -> None:
    records = load_real_brreg_records()

    assert len(records) == 300
    assert all(isinstance(record, BrregRecord) for record in records)
    assert len({record.organization_number for record in records}) == 300


def test_real_brreg_fixture_records_have_translation_material() -> None:
    records = load_real_brreg_records()

    assert all(extract_translation_items(record.raw_payload) for record in records)
