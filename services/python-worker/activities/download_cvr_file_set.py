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


def _snapshot_id(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


def _datasets(input: DownloadSourceFilesInput) -> list[str]:
    if input.datasets:
        return input.datasets
    raw_datasets = os.environ.get("CVR_FILEDOWNLOAD_DATASETS", "")
    return [dataset.strip() for dataset in raw_datasets.split(",") if dataset.strip()]


def _credential_headers() -> dict[str, str]:
    bearer_token = os.environ.get("CVR_FILEDOWNLOAD_BEARER_TOKEN", "") or os.environ.get(
        "DATAFORDELER_CVR_TOKEN", ""
    )
    api_key = os.environ.get("CVR_FILEDOWNLOAD_API_KEY", "")
    if bearer_token:
        return {"Authorization": f"Bearer {bearer_token}"}
    if api_key:
        return {"X-API-Key": api_key}
    raise RuntimeError("CVR file download credentials are not configured")


@activity.defn(name="download_cvr_file_set")
async def download_cvr_file_set(input: DownloadSourceFilesInput) -> DownloadSourceFilesResult:
    source = input.source or "cvr"
    snapshot_id = _snapshot_id(input.snapshot_id)
    base_url = os.environ.get("CVR_FILEDOWNLOAD_BASE_URL", "").rstrip("/")
    if not base_url:
        raise RuntimeError("CVR file download configuration error: CVR_FILEDOWNLOAD_BASE_URL is required")
    if "cvrapi.dk" in base_url.lower():
        raise RuntimeError("CVR file download configuration error: CVR_FILEDOWNLOAD_BASE_URL must use Datafordeler")

    datasets = _datasets(input)
    if not datasets:
        raise RuntimeError("CVR file download configuration error: no datasets are configured")

    headers = {
        "Accept": "application/json",
        "User-Agent": "corpscout-data-pipelines/1.0",
        **_credential_headers(),
    }

    files: list[DownloadedSourceFile] = []
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        for dataset in datasets:
            url = f"{base_url}/{dataset}"
            file_format = "json"
            file_path = safe_output_file_path(input.output_dir, source, dataset, snapshot_id, file_format)

            _logger.info("downloading CVR %s file to %s", dataset, file_path)
            response = await client.get(url, headers=headers)
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
