# /// script
# requires-python = ">=3.12"
# dependencies = ["psycopg[binary]>=3.3.4"]
# ///
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import psycopg


def main() -> None:
    parser = argparse.ArgumentParser(description="Export current BRREG raw records into a translation test fixture.")
    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--output", type=Path, default=Path("tests/data/brreg_raw_records_300.json"))
    args = parser.parse_args()

    database_url = os.environ.get("CORPSCOUT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("CORPSCOUT_DATABASE_URL or DATABASE_URL is required")

    rows = _fetch_records(database_url=database_url, limit=args.limit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    print(f"wrote {len(rows)} BRREG records to {args.output}")


def _fetch_records(*, database_url: str, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id::text AS record_id,
                    organization_number,
                    raw_payload
                FROM brreg_workflow.raw_records
                WHERE is_current = true
                ORDER BY organization_number ASC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "record_id": str(record_id),
                    "organization_number": str(organization_number),
                    "raw_payload": raw_payload,
                }
                for record_id, organization_number, raw_payload in cursor.fetchall()
            ]


if __name__ == "__main__":
    main()
