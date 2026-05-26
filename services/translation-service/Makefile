IMAGE ?= ghcr.io/pulsarpoint/corpscout-translation-service:latest
PORT ?= 8095

.PHONY: sync test test-real run build

sync:
	uv sync

test:
	uv run pytest -q

test-real:
	set -a; [ ! -f .env ] || . ./.env; set +a; TRANSLATION_SERVICE_RUN_REAL_LLM_TESTS=1 uv run pytest -q -m real_llm

run:
	uv run uvicorn corpscout_translation_service.api:app --env-file .env --host 0.0.0.0 --port $(PORT)

build:
	docker build -t $(IMAGE) .
