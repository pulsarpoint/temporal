from __future__ import annotations


def test_db_brreg_public_facade_exports_database_gateway_types() -> None:
    from corpscout_dagster.db_brreg import (
        AssetBlockedByActiveTasksError,
        AssetIncompleteError,
        BrregAssetGateway,
        BrregAssetName,
        BrregAssetState,
        BrregTaskStatus,
    )

    assert BrregAssetName.TRANSLATION_RESULTS.value == "translation_results"
    assert BrregTaskStatus.FAILED_TERMINAL.value == "failed_terminal"
    assert BrregAssetGateway.__name__ == "BrregAssetGateway"
    assert BrregAssetState.__name__ == "BrregAssetState"
    assert issubclass(AssetIncompleteError, RuntimeError)
    assert issubclass(AssetBlockedByActiveTasksError, RuntimeError)
