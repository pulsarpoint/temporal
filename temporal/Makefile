.PHONY: up down logs create-namespace

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

create-namespace:
	docker compose exec -e TEMPORAL_ADDRESS=temporal:7233 temporal \
		temporal operator namespace create --namespace corpscout --retention 7d
