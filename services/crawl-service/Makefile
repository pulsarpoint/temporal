IMAGE ?= ghcr.io/pulsarpoint/corpscout-crawl-service:latest
PORT ?= 8096

.PHONY: sync test run build

sync:
	uv sync

test:
	uv run pytest -q

run:
	uv run uvicorn corpscout_crawl_service.api:app --env-file .env --host 0.0.0.0 --port $(PORT)

build:
	docker build -t $(IMAGE) .
