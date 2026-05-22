from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime

import httpx
from temporalio import activity

from activities.source_downloads import safe_output_file_path
from contracts import DownloadedSourceFile, DownloadSourceFilesInput, DownloadSourceFilesResult

_DEFAULT_SIC_CODES_URL = (
    "https://assets.publishing.service.gov.uk/media/5a7f8639e5274a2e87db65e1/"
    "SIC07_CH_condensed_list_en.csv"
)


def _snapshot_id(value: str) -> str:
    return value or datetime.now(UTC).strftime("%Y-%m-%d")


@activity.defn(name="download_companies_house_sic_codes")
async def download_companies_house_sic_codes(input: DownloadSourceFilesInput) -> DownloadSourceFilesResult:
    source = input.source or "companies_house_sic"
    snapshot_id = _snapshot_id(input.snapshot_id)
    source_url = os.environ.get("COMPANIES_HOUSE_SIC_CODES_URL", _DEFAULT_SIC_CODES_URL)
    file_path = safe_output_file_path(input.output_dir, source, "sic_codes", snapshot_id, "csv")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(
            source_url,
            headers={"Accept": "text/csv,application/vnd.ms-excel,*/*", "User-Agent": "corpscout-data-pipelines/1.0"},
        )
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
                dataset="sic_codes",
                file_path=file_path,
                snapshot_id=snapshot_id,
                sha256=hashlib.sha256(content).hexdigest(),
                format="csv",
                source_url=source_url,
            )
        ],
    )
