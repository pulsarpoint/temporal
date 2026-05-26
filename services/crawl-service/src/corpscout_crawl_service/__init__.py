from __future__ import annotations


def main() -> None:
    import uvicorn

    uvicorn.run(
        "corpscout_crawl_service.api:app",
        host="0.0.0.0",
        port=8096,
        reload=False,
    )
