.PHONY: up down logs build test create-namespace

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

build:
	GOWORK=off go build -o bin/worker ./cmd/worker

test:
	GOWORK=off go test ./...
	cd python && python -m pytest -v

create-namespace:
	docker compose exec -e TEMPORAL_ADDRESS=temporal:7233 temporal temporal operator namespace create --namespace corpscout --retention 7d

py-install:
	cd python && pip install -r requirements.txt
