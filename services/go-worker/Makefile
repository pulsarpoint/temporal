.PHONY: build test run down logs

build:
	GOWORK=off go build -o bin/worker ./cmd/worker

test:
	GOWORK=off go test ./...

run:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f
