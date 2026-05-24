from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_dagster_instance_config_enables_run_monitoring_without_resume() -> None:
    config = yaml.safe_load((ROOT / "dagster.yaml").read_text())

    assert config["run_monitoring"]["enabled"] is True
    assert config["run_monitoring"]["max_resume_run_attempts"] == 0
    assert config["run_monitoring"]["start_timeout_seconds"] == 180
    assert config["run_monitoring"]["cancel_timeout_seconds"] == 180


def test_compose_uses_tracked_dagster_instance_config() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()

    assert "./dagster.yaml:/opt/dagster/dagster_home/dagster.yaml:ro" in compose
    assert 'touch "$$DAGSTER_HOME/dagster.yaml"' not in compose
