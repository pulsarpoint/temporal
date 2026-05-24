# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.12-bookworm

WORKDIR /app

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libnspr4 \
        libnss3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libdbus-1-3 \
        libcups2 \
        libxkbcommon0 \
        libasound2 \
        libgbm1 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p "${DAGSTER_HOME}"

COPY pyproject.toml uv.lock README.md ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project \
    && CRAWL4AI_MODE=api crawl4ai-setup \
    && python -m playwright install --no-shell chromium

COPY workspace.yaml dagster.yaml ./
COPY src ./src

RUN --mount=type=cache,target=/root/.cache/uv \
    cp dagster.yaml "${DAGSTER_HOME}/dagster.yaml" \
    && uv sync --frozen --no-dev

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/workspace.yaml"]
