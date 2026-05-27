from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from typing import Any

import psycopg
from dagster import DagsterInstance


DEFAULT_STALE_RUN_CLEANUP_MINUTES = 30
DEFAULT_STALE_RUN_CLEANUP_REASON = "Cancelled stale BRREG work after Dagster worker disappeared."


@dataclass(frozen=True)
class StaleBrregRunCleanupResult:
    enrichment_runs_cancelled: int
    task_attempts_cancelled: int
    task_states_reset: int
    dagster_run_ids: list[str]


def cleanup_stale_brreg_runs(
    *,
    connection: Any,
    older_than_minutes: int,
    reason: str,
    run_type: str | None = None,
    dagster_run_ids: list[str] | None = None,
) -> StaleBrregRunCleanupResult:
    if older_than_minutes <= 0:
        raise ValueError("older_than_minutes must be greater than 0")

    normalized_dagster_run_ids = dagster_run_ids or None
    cursor = connection.cursor()
    cursor.execute(
        """
WITH stale_runs AS (
    SELECT
        er.id,
        coalesce(er.metadata->>'dagster_run_id', er.dagster_run_id) AS dagster_run_id
    FROM dagster_brreg.enrichment_runs er
    WHERE er.status = 'running'
      AND er.started_at < now() - (%(older_than_minutes)s * interval '1 minute')
      AND (%(run_type)s::text IS NULL OR er.run_type = %(run_type)s)
      AND (
          %(dagster_run_ids)s::text[] IS NULL
          OR coalesce(er.metadata->>'dagster_run_id', er.dagster_run_id) = ANY(%(dagster_run_ids)s::text[])
      )
      AND NOT EXISTS (
          SELECT 1
          FROM dagster_brreg.task_attempts active_ta
          WHERE active_ta.enrichment_run_id = er.id
            AND active_ta.status = 'running'
            AND active_ta.started_at >= now() - (%(older_than_minutes)s * interval '1 minute')
      )
),
cancelled_attempts AS (
    UPDATE dagster_brreg.task_attempts ta
    SET
        status = 'cancelled',
        finished_at = now(),
        error = %(reason)s,
        error_category = 'interrupted',
        error_code = 'stale_enrichment_run',
        retry_strategy = 'automatic'
    FROM stale_runs sr
    WHERE ta.enrichment_run_id = sr.id
      AND ta.status = 'running'
    RETURNING ta.id
),
reset_states AS (
    UPDATE dagster_brreg.raw_record_task_states rts
    SET
        status = 'failed_retryable',
        last_finished_at = now(),
        next_retry_at = now(),
        last_error = %(reason)s,
        error_category = 'interrupted',
        error_code = 'stale_enrichment_run',
        retry_strategy = 'automatic',
        updated_at = now()
    FROM cancelled_attempts ca
    WHERE rts.last_attempt_id = ca.id
    RETURNING rts.raw_record_id, rts.task_type
),
cancelled_runs AS (
    UPDATE dagster_brreg.enrichment_runs er
    SET
        status = 'cancelled',
        finished_at = now(),
        error = %(reason)s
    FROM stale_runs sr
    WHERE er.id = sr.id
    RETURNING er.id, sr.dagster_run_id
)
SELECT
    (SELECT count(*) FROM cancelled_runs)::int AS enrichment_runs_cancelled,
    (SELECT count(*) FROM cancelled_attempts)::int AS task_attempts_cancelled,
    (SELECT count(*) FROM reset_states)::int AS task_states_reset,
    coalesce(array_remove(array_agg(dagster_run_id), NULL), '{}'::text[]) AS dagster_run_ids
FROM cancelled_runs
""",
        {
            "older_than_minutes": older_than_minutes,
            "reason": reason,
            "run_type": run_type,
            "dagster_run_ids": normalized_dagster_run_ids,
        },
    )
    rows = cursor.fetchall()
    connection.commit()

    if not rows:
        return StaleBrregRunCleanupResult(
            enrichment_runs_cancelled=0,
            task_attempts_cancelled=0,
            task_states_reset=0,
            dagster_run_ids=[],
        )

    row = rows[0]
    return StaleBrregRunCleanupResult(
        enrichment_runs_cancelled=int(row[0] or 0),
        task_attempts_cancelled=int(row[1] or 0),
        task_states_reset=int(row[2] or 0),
        dagster_run_ids=[str(run_id) for run_id in (row[3] or [])],
    )


def cancel_dagster_runs(*, instance: Any, dagster_run_ids: list[str], reason: str) -> int:
    cancelled = 0
    seen: set[str] = set()
    for dagster_run_id in dagster_run_ids:
        if dagster_run_id in seen:
            continue
        seen.add(dagster_run_id)
        dagster_run = instance.get_run_by_id(dagster_run_id)
        if dagster_run is None or dagster_run.is_finished:
            continue
        instance.report_run_canceled(dagster_run, reason)
        cancelled += 1
    return cancelled


def database_url_from_environment() -> str:
    database_url = (
        os.environ.get("CORPSCOUT_DATABASE_URL")
        or os.environ.get("CORPSCOUT_DB_URL")
        or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        raise RuntimeError("CORPSCOUT_DATABASE_URL, CORPSCOUT_DB_URL, or DATABASE_URL is required")
    return database_url


def cleanup_stale_runs_from_environment(
    *,
    older_than_minutes: int,
    reason: str,
    run_type: str | None,
    dagster_run_ids: list[str] | None,
    cancel_runs_in_dagster: bool,
) -> dict[str, Any]:
    with psycopg.connect(database_url_from_environment()) as connection:
        cleanup_result = cleanup_stale_brreg_runs(
            connection=connection,
            older_than_minutes=older_than_minutes,
            reason=reason,
            run_type=run_type,
            dagster_run_ids=dagster_run_ids,
        )

    dagster_runs_cancelled = 0
    if cancel_runs_in_dagster and cleanup_result.dagster_run_ids:
        dagster_runs_cancelled = cancel_dagster_runs(
            instance=DagsterInstance.get(),
            dagster_run_ids=cleanup_result.dagster_run_ids,
            reason=reason,
        )

    return {
        **asdict(cleanup_result),
        "dagster_runs_cancelled": dagster_runs_cancelled,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BRREG Dagster maintenance helpers.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    cleanup = subcommands.add_parser(
        "cleanup-stale-runs",
        help="Cancel stale BRREG enrichment runs and make their running task rows retryable.",
    )
    cleanup.add_argument(
        "--older-than-minutes",
        type=int,
        default=int(os.environ.get("BRREG_STALE_RUN_CLEANUP_MINUTES", DEFAULT_STALE_RUN_CLEANUP_MINUTES)),
    )
    cleanup.add_argument("--run-type")
    cleanup.add_argument("--dagster-run-id", action="append", dest="dagster_run_ids")
    cleanup.add_argument("--reason", default=DEFAULT_STALE_RUN_CLEANUP_REASON)
    cleanup.add_argument(
        "--skip-dagster-run-cancel",
        action="store_true",
        help="Only update dagster_brreg tables; do not mark matching Dagster runs canceled.",
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.command == "cleanup-stale-runs":
        result = cleanup_stale_runs_from_environment(
            older_than_minutes=args.older_than_minutes,
            reason=args.reason,
            run_type=args.run_type,
            dagster_run_ids=args.dagster_run_ids,
            cancel_runs_in_dagster=not args.skip_dagster_run_cancel,
        )
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
