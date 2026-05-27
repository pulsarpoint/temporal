from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]


def read_project_file(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text()


def test_makefile_exposes_compose_migration_targets() -> None:
    makefile = read_project_file("Makefile")

    assert "migrate-up" in makefile
    assert "migrate-down" in makefile
    assert "migrate-version" in makefile
    assert "$(COMPOSE) run --rm dagster-migrate-up" in makefile
    assert "$(COMPOSE) run --rm dagster-migrate-down" in makefile
    assert "$(COMPOSE) run --rm dagster-migrate-version" in makefile


def test_compose_defines_dockerized_golang_migrate_services() -> None:
    compose = read_project_file("docker-compose.yml")

    assert "migrate/migrate" in compose
    assert "dagster-migrate-up:" in compose
    assert "dagster-migrate-down:" in compose
    assert "dagster-migrate-version:" in compose
    assert "./db/migrations:/migrations:ro" in compose
    assert "${DAGSTER_MIGRATIONS_DATABASE_URL:-postgresql://corpscout:password123@companycollect:5432/corpscout?sslmode=disable&x-migrations-table=dagster_schema_migrations}" in compose
    assert "- up" in compose
    assert "- down" in compose
    assert "- \"1\"" in compose
    assert "- version" in compose


def test_compose_runs_migrations_before_dagster_containers() -> None:
    compose = read_project_file("docker-compose.yml")

    assert "dagster-migrate:" in compose
    assert "dagster-migrate:\n        condition: service_completed_successfully" in compose
    assert "CORPSCOUT_DATABASE_URL: ${CORPSCOUT_DATABASE_URL:-postgresql://corpscout:password123@companycollect:5432/corpscout?sslmode=disable}" in compose


def test_env_example_uses_separate_dagster_migration_table() -> None:
    env_example = read_project_file(".env.example")

    assert "CORPSCOUT_DATABASE_URL=" in env_example
    assert "DAGSTER_MIGRATIONS_DATABASE_URL=" in env_example
    assert "x-migrations-table=dagster_schema_migrations" in env_example


def test_compose_exposes_dagster_webserver_port() -> None:
    compose = read_project_file("docker-compose.yml")

    assert "ports:" in compose
    assert "${DAGSTER_PORT:-3000}:${DAGSTER_WEBSERVER_PORT:-3000}" in compose
    assert "companycollect:100.85.212.113" in compose
    assert "host.docker.internal" not in compose
    assert "network_mode: host" not in compose


def test_compose_uses_published_dagster_image_without_remote_build() -> None:
    compose = read_project_file("docker-compose.yml")
    makefile = read_project_file("Makefile")

    assert "build: ." not in compose
    assert "image: ${DAGSTER_IMAGE:-ghcr.io/pulsarpoint/corpscout-dagster:latest}" in compose
    assert "image: ${TRANSLATION_SERVICE_IMAGE:-ghcr.io/pulsarpoint/corpscout-translation-service:latest}" in compose
    assert "image: ${CRAWL_SERVICE_IMAGE:-ghcr.io/pulsarpoint/corpscout-crawl-service:latest}" in compose
    assert "$(COMPOSE) pull dagster-webserver dagster-daemon translation-service crawl-service" in makefile
    assert "$(COMPOSE) up -d" in makefile
    assert "$(COMPOSE) up -d --build" not in makefile


def test_migrations_directory_is_committed() -> None:
    assert (PROJECT_ROOT / "db" / "migrations").is_dir()


def test_mock_compose_is_fully_local_and_exposes_mock_ports() -> None:
    compose = read_project_file("docker-compose.mock.yml")
    env_example = read_project_file(".env.mock.example")
    makefile = read_project_file("Makefile")

    assert "postgres:16" in compose
    assert "companycollect" not in compose
    assert "BRREG_BULK_FIXTURE_PATH" in compose
    assert "CRAWL_SERVICE_MODE" in compose
    assert "TRANSLATION_MOCK_ENABLED" in compose
    assert "15432" in env_example
    assert "18095" in env_example
    assert "18096" in env_example
    assert "DAGSTER_PORT=3001" in env_example
    assert "mock-e2e" in makefile
    assert "scripts/run_mock_e2e.py" in makefile
