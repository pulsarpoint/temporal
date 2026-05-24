from __future__ import annotations

from corpscout_dagster.brreg.maintenance import (
    StaleBrregRunCleanupResult,
    cancel_dagster_runs,
    cleanup_stale_brreg_runs,
)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fetchall_values = [
            (
                2,
                3,
                3,
                ["dagster-run-1", "dagster-run-2"],
            )
        ]

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def fetchall(self):
        return self.fetchall_values


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1


class FakeRun:
    def __init__(self, run_id: str, *, is_finished: bool) -> None:
        self.run_id = run_id
        self.is_finished = is_finished


class FakeDagsterInstance:
    def __init__(self) -> None:
        self.runs = {
            "dagster-run-1": FakeRun("dagster-run-1", is_finished=False),
            "dagster-run-2": FakeRun("dagster-run-2", is_finished=True),
            "missing-run": None,
        }
        self.cancelled: list[tuple[str, str]] = []

    def get_run_by_id(self, run_id: str):
        return self.runs.get(run_id)

    def report_run_canceled(self, run, message: str) -> None:
        self.cancelled.append((run.run_id, message))


def test_cleanup_stale_brreg_runs_cancels_attempts_and_resets_task_state() -> None:
    connection = FakeConnection()

    result = cleanup_stale_brreg_runs(
        connection=connection,
        older_than_minutes=45,
        reason="Cancelled stale BRREG work after Dagster worker disappeared.",
        run_type="translate",
        dagster_run_ids=["dagster-run-1"],
    )

    assert result == StaleBrregRunCleanupResult(
        enrichment_runs_cancelled=2,
        task_attempts_cancelled=3,
        task_states_reset=3,
        dagster_run_ids=["dagster-run-1", "dagster-run-2"],
    )
    assert connection.commits == 1
    sql, params = connection.cursor_instance.calls[0]
    assert "WITH stale_runs AS" in sql
    assert "NOT EXISTS" in sql
    assert "active_ta.status = 'running'" in sql
    assert "coalesce(er.metadata->>'dagster_run_id', er.dagster_run_id)" in sql
    assert "UPDATE dagster_brreg.task_attempts ta" in sql
    assert "status = 'cancelled'" in sql
    assert "UPDATE dagster_brreg.raw_record_task_states rts" in sql
    assert "status = 'failed_retryable'" in sql
    assert "next_retry_at = now()" in sql
    assert "UPDATE dagster_brreg.enrichment_runs er" in sql
    assert "coalesce(er.metadata->>'dagster_run_id', er.dagster_run_id) = ANY(%(dagster_run_ids)s::text[])" in sql
    assert params["older_than_minutes"] == 45
    assert params["reason"] == "Cancelled stale BRREG work after Dagster worker disappeared."
    assert params["run_type"] == "translate"
    assert params["dagster_run_ids"] == ["dagster-run-1"]


def test_cancel_dagster_runs_only_cancels_unfinished_known_runs() -> None:
    instance = FakeDagsterInstance()

    cancelled = cancel_dagster_runs(
        instance=instance,
        dagster_run_ids=["dagster-run-1", "dagster-run-2", "missing-run"],
        reason="BRREG cleanup marked stale work cancelled.",
    )

    assert cancelled == 1
    assert instance.cancelled == [
        ("dagster-run-1", "BRREG cleanup marked stale work cancelled."),
    ]
