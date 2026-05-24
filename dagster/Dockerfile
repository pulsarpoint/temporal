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
    && uv sync --frozen --no-dev \
    && CRAWL4AI_MODE=api crawl4ai-setup \
    && python -m playwright install --no-shell chromium \
    && mkdir -p "${DAGSTER_HOME}"

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/app/workspace.yaml"]
