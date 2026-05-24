from __future__ import annotations

from dagster import Definitions

from corpscout_dagster.brreg.assets import brreg_working_raw_records

defs = Definitions(assets=[brreg_working_raw_records])
