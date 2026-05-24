FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY workspace.yaml ./
COPY src ./src

RUN uv sync --frozen --no-dev && mkdir -p "${DAGSTER_HOME}"

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/workspace.yaml"]

