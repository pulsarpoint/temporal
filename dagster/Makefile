COMPOSE ?= docker compose
DAGSTER_PORT ?= 3000
DAGSTER_IMAGE ?= ghcr.io/pulsarpoint/corpscout-dagster:latest

.PHONY: sync test validate smoke-brreg-db webserver daemon build pull up run down migrate-up migrate-down migrate-version logs logs-webserver logs-daemon shell

sync:
	uv sync

test:
	uv run pytest -q

validate:
	tmp=$$(mktemp -d); touch "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster definitions validate -w workspace.yaml

smoke-brreg-db:
	uv run python -m corpscout_dagster.brreg.smoke

webserver:
	tmp=$$(mktemp -d); touch "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster-webserver -h 0.0.0.0 -p $(DAGSTER_PORT) -w workspace.yaml

daemon:
	tmp=$$(mktemp -d); touch "$$tmp/dagster.yaml"; DAGSTER_HOME="$$tmp" uv run dagster-daemon run -w workspace.yaml

build:
	docker build -t $(DAGSTER_IMAGE) .

pull:
	$(COMPOSE) pull dagster-webserver dagster-daemon

up run:
	$(COMPOSE) pull dagster-webserver dagster-daemon
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

shell:
	$(COMPOSE) run --rm dagster-webserver bash
