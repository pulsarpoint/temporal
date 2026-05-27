from __future__ import annotations

from pathlib import Path


BRREG_SRC = Path(__file__).parents[2] / "src" / "corpscout_dagster" / "brreg"


def test_brreg_production_modules_do_not_import_db_brreg_store() -> None:
    offenders = []
    for path in sorted(BRREG_SRC.glob("*.py")):
        text = path.read_text()
        if "corpscout_dagster.db_brreg.store" in text:
            offenders.append(path.name)

    assert offenders == []
