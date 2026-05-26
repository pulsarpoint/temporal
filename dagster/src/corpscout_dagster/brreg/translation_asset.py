from __future__ import annotations

import os

import psycopg
from dagster import asset

from corpscout_dagster.brreg.asset_config import (
    brreg_batch_run_config_schema,
    corpscout_database_url,
    env_int,
    resolve_brreg_batch_run_config,
)
from corpscout_dagster.brreg.materializations import (
    DEFAULT_TRANSLATION_MAX_BATCHES_PER_RUN,
    DEFAULT_TRANSLATION_MAX_PARALLEL_TASKS,
    DEFAULT_TRANSLATION_RECORD_BATCH_SIZE,
    materialize_brreg_translation_results,
)
from corpscout_dagster.brreg.translation_terms import (
    DEFAULT_LLM_MODEL,
    DEFAULT_PROMPT_VERSION,
    HttpTranslationServiceTermTranslator,
)


@asset(
    name="brreg_translation_results",
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
    return materialize_brreg_translation_results(
        context,
        connection_factory=psycopg.connect,
        database_url=corpscout_database_url(),
        translator=HttpTranslationServiceTermTranslator.from_env(),
        batch_size=run_config.batch_size,
        max_batches_per_run=run_config.max_batches_per_run,
        max_parallel_tasks=run_config.max_parallel_tasks,
        model=(
            os.environ.get("BRREG_TRANSLATION_MODEL")
            or os.environ.get("TRANSLATION_DEFAULT_MODEL")
            or DEFAULT_LLM_MODEL
        ),
        prompt_version=os.environ.get("BRREG_TRANSLATION_PROMPT_VERSION") or DEFAULT_PROMPT_VERSION,
    )
