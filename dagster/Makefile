COMPOSE ?= docker compose
MOCK_ENV_FILE ?= .env.mock
MOCK_COMPOSE_FILE ?= docker-compose.mock.yml
DAGSTER_PORT ?= 3000
DAGSTER_IMAGE ?= ghcr.io/pulsarpoint/corpscout-dagster:latest
TRANSLATION_SERVICE_IMAGE ?= ghcr.io/pulsarpoint/corpscout-translation-service:latest
CRAWL_SERVICE_IMAGE ?= ghcr.io/pulsarpoint/corpscout-crawl-service:latest
BRREG_STALE_RUN_CLEANUP_MINUTES ?= 30

.PHONY: sync test validate smoke-brreg-db cleanup-stale-brreg-runs webserver daemon build pull up run down migrate-up migrate-down migrate-version logs logs-webserver logs-daemon logs-translation logs-crawl shell mock-env mock-up mock-down mock-reset mock-e2e mock-logs

sync:
	uv sync

test:
	uv run pytest -q

validate:
	tmp=$$(mktemp -d); cp dagster.yaml "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster definitions validate -w workspace.yaml

smoke-brreg-db:
	uv run python -m corpscout_dagster.brreg.smoke

cleanup-stale-brreg-runs:
	$(COMPOSE) run --rm dagster-webserver python -m corpscout_dagster.brreg.maintenance cleanup-stale-runs --older-than-minutes $(BRREG_STALE_RUN_CLEANUP_MINUTES)

webserver:
	tmp=$$(mktemp -d); cp dagster.yaml "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster-webserver -h 0.0.0.0 -p $(DAGSTER_PORT) -w workspace.yaml

daemon:
	tmp=$$(mktemp -d); cp dagster.yaml "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster-daemon run -w workspace.yaml

build:
	docker build -t $(DAGSTER_IMAGE) .

pull:
	$(COMPOSE) pull dagster-webserver dagster-daemon translation-service crawl-service

up run:
	$(COMPOSE) pull dagster-webserver dagster-daemon translation-service crawl-service
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

migrate-up:
	$(COMPOSE) run --rm dagster-migrate-up

migrate-down:
	$(COMPOSE) run --rm dagster-migrate-down

migrate-version:
	$(COMPOSE) run --rm dagster-migrate-version

logs:
	$(COMPOSE) logs -f

logs-webserver:
	$(COMPOSE) logs -f dagster-webserver

logs-daemon:
	$(COMPOSE) logs -f dagster-daemon

logs-translation:
	$(COMPOSE) logs -f translation-service

logs-crawl:
	$(COMPOSE) logs -f crawl-service

shell:
	$(COMPOSE) run --rm dagster-webserver bash

mock-env:
	@test -f $(MOCK_ENV_FILE) || cp .env.mock.example $(MOCK_ENV_FILE)

mock-up: mock-env
	$(COMPOSE) --env-file $(MOCK_ENV_FILE) -f $(MOCK_COMPOSE_FILE) up -d --build

mock-down: mock-env
	$(COMPOSE) --env-file $(MOCK_ENV_FILE) -f $(MOCK_COMPOSE_FILE) down

mock-reset: mock-env
	$(COMPOSE) --env-file $(MOCK_ENV_FILE) -f $(MOCK_COMPOSE_FILE) down -v --remove-orphans
	rm -rf .dagster_home_mock

mock-e2e: mock-env
	uv run python scripts/run_mock_e2e.py

mock-logs: mock-env
	$(COMPOSE) --env-file $(MOCK_ENV_FILE) -f $(MOCK_COMPOSE_FILE) logs -f
