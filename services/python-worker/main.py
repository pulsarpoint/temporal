from __future__ import annotations

import asyncio
import logging
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from activities.download_ariregister_dataset import download_ariregister_dataset
from activities.download_brreg_bulk import download_brreg_bulk
from activities.download_companies_house_sic_codes import download_companies_house_sic_codes
from activities.download_cvr_file_set import download_cvr_file_set
from activities.download_gleif_golden_copy import download_gleif_golden_copy
from activities.fetch_brreg_list import fetch_brreg_list
from activities.fetch_companies_house_list import fetch_companies_house_list
from activities.discover_company_domains import discover_company_domains
from activities.llm_translation import translate_terms_with_dspy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    temporal_host = os.environ.get("TEMPORAL_HOST", "localhost:7233")
    logging.info("connecting to Temporal at %s", temporal_host)

    client = await Client.connect(temporal_host, namespace="corpscout")

    worker = Worker(
        client,
        task_queue="corpscout-pipelines-python",
        activities=[
            download_brreg_bulk,
            download_companies_house_sic_codes,
            download_gleif_golden_copy,
            download_ariregister_dataset,
            download_cvr_file_set,
            fetch_brreg_list,
            fetch_companies_house_list,
            discover_company_domains,
            translate_terms_with_dspy,
        ],
    )

    logging.info("Python activity worker started on queue: corpscout-pipelines-python")
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Python worker shut down")
        sys.exit(0)
