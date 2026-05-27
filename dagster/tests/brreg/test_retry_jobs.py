from __future__ import annotations

from corpscout_dagster.brreg.retry_jobs import retry_brreg_task_failures


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def info(self, message: str, *args) -> None:
        self.messages.append(message % args if args else message)


class FakeContext:
    def __init__(self) -> None:
        self.log = FakeLogger()
        self.metadata: list[dict] = []

    def add_output_metadata(self, metadata: dict) -> None:
        self.metadata.append(metadata)


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: dict) -> None:
        self.calls.append((sql, params))

    def fetchone(self):
        return (7,)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commits += 1


def test_retry_brreg_task_failures_logs_start_and_completion() -> None:
    context = FakeContext()
    connection = FakeConnection()

    result = retry_brreg_task_failures(
        context,
        connection_factory=lambda _: connection,
        database_url="postgresql://example.invalid/corpscout",
        task_type="translate",
        error_category="invalid_llm_output",
        limit=100,
    )

    assert result["retried_rows"] == 7
    assert context.metadata[-1]["retried_rows"] == 7
    assert connection.commits == 1
    assert any(
        "BRREG retry task failures started task_type=translate error_category=invalid_llm_output limit=100"
        in message
        for message in context.log.messages
    )
    assert any(
        "BRREG retry task failures completed task_type=translate error_category=invalid_llm_output limit=100 retried_rows=7"
        in message
        for message in context.log.messages
    )
