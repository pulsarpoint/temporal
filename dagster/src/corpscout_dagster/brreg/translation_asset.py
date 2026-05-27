from __future__ import annotations

from dagster import AssetKey, asset

from corpscout_dagster.brreg.asset_config import (
    brreg_batch_run_config_schema,
    env_int,
    resolve_brreg_batch_run_config,
)
from corpscout_dagster.brreg.materializations import (
    DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
    DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    DEFAULT_TRANSLATION_RECORD_BATCH_SIZE,
    materialize_brreg_translation_results,
)


@asset(
    name="brreg_translation_results",
    deps=[AssetKey("brreg_raw_records")],
    config_schema=brreg_batch_run_config_schema(
        batch_size_default=env_int("BRREG_TRANSLATION_BATCH_SIZE", DEFAULT_TRANSLATION_RECORD_BATCH_SIZE),
        max_batches_default=env_int(
            "BRREG_TRANSLATION_MAX_BATCHES_PER_RUN",
            DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
        ),
        max_parallel_tasks_default=env_int(
            "BRREG_TRANSLATION_MAX_PARALLEL_TASKS",
            DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
        ),
    ),
    required_resource_keys={"postgres", "translation_service"},
)
def brreg_translation_results(context) -> dict[str, int]:
    run_config = resolve_brreg_batch_run_config(
        context,
        batch_size_env="BRREG_TRANSLATION_BATCH_SIZE",
        batch_size_default=DEFAULT_TRANSLATION_RECORD_BATCH_SIZE,
        max_batches_env="BRREG_TRANSLATION_MAX_BATCHES_PER_RUN",
        max_batches_default=DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
        max_parallel_tasks_env="BRREG_TRANSLATION_MAX_PARALLEL_TASKS",
        max_parallel_tasks_default=DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    )
    postgres = context.resources.postgres
    translation_service = context.resources.translation_service
    return materialize_brreg_translation_results(
        context,
        connection_factory=postgres.connection_factory,
        database_url=postgres.database_url,
        translator=translation_service.translator,
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
        model=translation_service.model,
        prompt_version=translation_service.prompt_version,
    )
