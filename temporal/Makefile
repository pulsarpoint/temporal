TEMPORAL_NAMESPACE ?= corpscout
TEMPORAL_NAMESPACE_RETENTION ?= 168h

.PHONY: up down logs create-namespace describe-namespace

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

create-namespace:
	docker compose exec -T \
		-e TEMPORAL_ADDRESS=temporal:7233 \
		-e TEMPORAL_NAMESPACE=$(TEMPORAL_NAMESPACE) \
		-e TEMPORAL_NAMESPACE_RETENTION=$(TEMPORAL_NAMESPACE_RETENTION) \
		temporal sh -lc 'temporal operator namespace describe -n "$$TEMPORAL_NAMESPACE" >/dev/null 2>&1 || temporal operator namespace create --namespace "$$TEMPORAL_NAMESPACE" --retention "$$TEMPORAL_NAMESPACE_RETENTION"'

describe-namespace:
	docker compose exec -T -e TEMPORAL_ADDRESS=temporal:7233 temporal \
		temporal operator namespace describe -n "$(TEMPORAL_NAMESPACE)"
