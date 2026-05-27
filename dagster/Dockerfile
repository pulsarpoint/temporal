# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN mkdir -p "${DAGSTER_HOME}"

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY README.md workspace.yaml dagster.yaml ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/uv \
    cp dagster.yaml "${DAGSTER_HOME}/dagster.yaml" \
    && uv sync --frozen --no-dev

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/workspace.yaml"]
