from __future__ import annotations

import hashlib
import logging
import os
from datetime import UTC, datetime

import httpx
from temporalio import activity

from activities.source_downloads import safe_output_file_path
from contracts import DownloadedSourceFile, DownloadSourceFilesInput, DownloadSourceFilesResult

_logger = logging.getLogger(__name__)
_DEFAULT_BASE_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"
_DEFAULT_DATASET = "lei2"
_DEFAULT_DELTA_WINDOW = "PT24H"


def _snapshot_id(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%dT%H")


@activity.defn(name="download_gleif_golden_copy")
async def download_gleif_golden_copy(input: DownloadSourceFilesInput) -> DownloadSourceFilesResult:
    source = input.source or "gleif"
    dataset = input.datasets[0] if input.datasets else _DEFAULT_DATASET
    snapshot_id = _snapshot_id(input.snapshot_id)
    base_url = os.environ.get("GLEIF_GOLDEN_COPY_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
    file_format = "json"
    mode = input.mode or "full"

    if mode == "delta":
        delta_window = input.delta_window or _DEFAULT_DELTA_WINDOW
        url = f"{base_url}/{dataset}/delta/{delta_window}.{file_format}"
    elif mode == "full":
        url = f"{base_url}/{dataset}/latest.{file_format}"
    else:
        raise RuntimeError(f"Unsupported GLEIF download mode: {mode}")

    file_path = safe_output_file_path(input.output_dir, source, dataset, snapshot_id, file_format)

    _logger.info("downloading GLEIF %s file to %s", dataset, file_path)
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        response = await client.get(url, headers={"Accept": "application/json", "User-Agent": "corpscout-data-pipelines/1.0"})
        response.raise_for_status()
        content = response.content

    with open(file_path, "wb") as fh:
        fh.write(content)

    return DownloadSourceFilesResult(
        source=source,
        snapshot_id=snapshot_id,
        files=[
            DownloadedSourceFile(
                source=source,
                dataset=dataset,
                file_path=file_path,
                snapshot_id=snapshot_id,
                sha256=hashlib.sha256(content).hexdigest(),
                format=file_format,
            )
        ],
    )
