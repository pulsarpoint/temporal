from __future__ import annotations

import json
from pathlib import Path

from corpscout_translation_service.models import BrregRecord


REAL_BRREG_RECORDS_PATH = Path(__file__).parent / "data" / "brreg_raw_records_300.json"


def load_real_brreg_records(*, limit: int | None = None) -> list[BrregRecord]:
    values = json.loads(REAL_BRREG_RECORDS_PATH.read_text())
    records = [BrregRecord.model_validate(value) for value in values]
    if limit is None:
        return records
    return records[:limit]
