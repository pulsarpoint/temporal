from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import UTC, datetime

import httpx
from temporalio import activity

from activities.source_downloads import safe_output_file_path
from contracts import DownloadedSourceFile, DownloadSourceFilesInput, DownloadSourceFilesResult

_logger = logging.getLogger(__name__)
_DEFAULT_DATASETS = [
    {
        "dataset": "simple-data",
        "url": "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip",
        "format": "csv.zip",
    }
]


def _snapshot_id(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


def _configured_datasets() -> list[dict[str, str]]:
    raw_config = os.environ.get("ARIREGISTER_DATASETS_JSON", "")
    if not raw_config:
        return _DEFAULT_DATASETS

    try:
        parsed = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise RuntimeError("ARIREGISTER_DATASETS_JSON must be valid JSON") from exc
    if not isinstance(parsed, list):
        raise RuntimeError("ARIREGISTER_DATASETS_JSON must be a JSON array")

    datasets: list[dict[str, str]] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise RuntimeError("ARIREGISTER_DATASETS_JSON entries must be objects")
        raw_dataset = item.get("dataset")
        raw_url = item.get("url")
        raw_format = item.get("format")
        if not isinstance(raw_dataset, str) or not isinstance(raw_url, str) or not isinstance(raw_format, str):
            raise RuntimeError("ARIREGISTER_DATASETS_JSON entries require dataset, url, and format")
        dataset = raw_dataset.strip()
        url = raw_url.strip()
        file_format = raw_format.strip()
        if not dataset or not url or not file_format:
            raise RuntimeError("ARIREGISTER_DATASETS_JSON entries require dataset, url, and format")
        datasets.append({"dataset": dataset, "url": url, "format": file_format})
    return datasets


@activity.defn(name="download_ariregister_dataset")
async def download_ariregister_dataset(input: DownloadSourceFilesInput) -> DownloadSourceFilesResult:
    source = input.source or "ariregister"
    snapshot_id = _snapshot_id(input.snapshot_id)
    datasets = _configured_datasets()

    files: list[DownloadedSourceFile] = []
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        for config in datasets:
            dataset = config["dataset"]
            url = config["url"]
            file_format = config["format"]
            file_path = safe_output_file_path(input.output_dir, source, dataset, snapshot_id, file_format)

            _logger.info("downloading Ariregister %s file to %s", dataset, file_path)
            response = await client.get(url, headers={"Accept": "*/*", "User-Agent": "corpscout-data-pipelines/1.0"})
            response.raise_for_status()
            content = response.content

            with open(file_path, "wb") as fh:
                fh.write(content)

            files.append(
                DownloadedSourceFile(
                    source=source,
                    dataset=dataset,
                    file_path=file_path,
                    snapshot_id=snapshot_id,
                    sha256=hashlib.sha256(content).hexdigest(),
                    format=file_format,
                )
            )

    return DownloadSourceFilesResult(source=source, snapshot_id=snapshot_id, files=files)
