from __future__ import annotations

import asyncio
import logging
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from activities.download_brreg_bulk import download_brreg_bulk
from activities.fetch_brreg_list import fetch_brreg_list
from activities.fetch_companies_house_list import fetch_companies_house_list
from activities.discover_company_domains import discover_company_domains

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    logging.info("connecting to Temporal at %s", temporal_host)

    client = await Client.connect(temporal_host, namespace="corpscout")

    worker = Worker(
        client,
        task_queue="corpscout-pipelines-python",
        activities=[download_brreg_bulk, fetch_brreg_list, fetch_companies_house_list, discover_company_domains],
    )

    logging.info("Python activity worker started on queue: corpscout-pipelines-python")
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Python worker shut down")
        sys.exit(0)
