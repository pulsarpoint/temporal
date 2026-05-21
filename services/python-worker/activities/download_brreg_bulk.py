from __future__ import annotations

import logging
import os
from datetime import date

import httpx
from temporalio import activity

_logger = logging.getLogger(__name__)
_BULK_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"


@activity.defn(name="download_brreg_bulk")
async def download_brreg_bulk(output_dir: str) -> dict:
    """Download the Brreg bulk zip to output_dir and return {"file_path": ..., "date": ...}."""
    bulk_date = date.today().isoformat()
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"bulk_{bulk_date}.zip")

    _logger.info("downloading Brreg bulk zip to %s", file_path)
    activity.heartbeat("starting download...")

    downloaded = 0
    with open(file_path, "wb") as fh:
        async with httpx.AsyncClient(timeout=600.0) as client:
            async with client.stream(
                "GET",
                _BULK_URL,
                headers={"Accept": "application/zip", "User-Agent": "corpscout-data-pipelines/1.0"},
                follow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                    fh.write(chunk)
                    downloaded += len(chunk)
                    activity.heartbeat(f"downloaded {downloaded // (1024*1024)} MB")

    _logger.info("bulk zip saved: %s (%d bytes)", file_path, downloaded)
    return {"file_path": file_path, "date": bulk_date}
