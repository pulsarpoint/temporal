from dagster import Definitions

from corpscout_dagster import defs


def test_definitions_exported() -> None:
    assert isinstance(defs, Definitions)
