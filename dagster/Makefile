COMPOSE ?= docker compose
DAGSTER_PORT ?= 3000

.PHONY: sync test validate smoke-brreg-db webserver daemon build up run down logs logs-webserver logs-daemon shell

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
	$(COMPOSE) build

up run:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

logs-webserver:
	$(COMPOSE) logs -f dagster-webserver

logs-daemon:
	$(COMPOSE) logs -f dagster-daemon

shell:
	$(COMPOSE) run --rm dagster-webserver bash
