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
    assert "${CORPSCOUT_DATABASE_URL:?" in compose
    assert "- up" in compose
    assert "- down" in compose
    assert "- \"1\"" in compose
    assert "- version" in compose


def test_migrations_directory_is_committed() -> None:
    assert (PROJECT_ROOT / "db" / "migrations").is_dir()
