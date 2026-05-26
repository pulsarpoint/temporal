from __future__ import annotations


def main() -> None:
    import uvicorn

    uvicorn.run(
        "corpscout_translation_service.api:app",
        host="0.0.0.0",
        port=8095,
        reload=False,
    )
