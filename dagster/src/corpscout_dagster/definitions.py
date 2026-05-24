from __future__ import annotations

from dagster import Definitions

from corpscout_dagster.brreg.assets import brreg_raw_inputs

defs = Definitions(assets=[brreg_raw_inputs])
