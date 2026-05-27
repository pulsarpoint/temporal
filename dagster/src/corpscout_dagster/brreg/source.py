from __future__ import annotations

import gzip
import io
from collections.abc import Iterable, Iterator
from typing import Protocol

import httpx
import ijson

from corpscout_dagster.brreg.models import BrregRawRecord

BRREG_API_BASE_URL = "https://data.brreg.no"
BRREG_BULK_PATH = "/enhetsregisteret/api/enheter/lastned"
USER_AGENT = "corpscout-dagster/0.1"


class BrregBulkRecordClient(Protocol):
    def iter_records(self) -> Iterator[BrregRawRecord | None]:
        ...


class BrregBulkClient:
    def __init__(self, *, http_client: httpx.Client | None = None) -> None:
        self._http_client = http_client

    def iter_records(self) -> Iterator[BrregRawRecord]:
        if self._http_client is None:
            with httpx.Client(base_url=BRREG_API_BASE_URL, timeout=600.0) as client:
                yield from self._iter_records(client)
            return
        yield from self._iter_records(self._http_client)

    def _iter_records(self, client: httpx.Client) -> Iterator[BrregRawRecord]:
        with client.stream(
            "GET",
            BRREG_BULK_PATH,
            headers={"Accept": "*/*", "User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as response:
            response.raise_for_status()
            yield from iter_brreg_bulk_payload(response.iter_bytes())


def parse_brreg_bulk_payload(content: bytes) -> list[BrregRawRecord]:
    return list(iter_brreg_bulk_payload([content]))


def iter_brreg_bulk_payload(chunks: Iterable[bytes]) -> Iterator[BrregRawRecord]:
    reader = io.BufferedReader(_BytesIteratorReader(chunks))
    with reader, gzip.GzipFile(fileobj=reader) as gzip_file:
        first_byte = _first_non_whitespace_byte(gzip_file.peek(4096))
        prefix = "_embedded.enheter.item" if first_byte == b"{" else "item"
        for item in ijson.items(gzip_file, prefix, use_float=True):
            if isinstance(item, dict) and (record := BrregRawRecord.from_payload(item)) is not None:
                yield record


def iter_brreg_bulk_records(
    *,
    client: BrregBulkRecordClient | None = None,
) -> Iterator[BrregRawRecord]:
    for record in (client or BrregBulkClient()).iter_records():
        if record is not None:
            yield record


class _BytesIteratorReader(io.RawIOBase):
    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        if self._eof and not self._buffer:
            return 0
        while not self._buffer:
            try:
                self._buffer.extend(next(self._chunks))
            except StopIteration:
                self._eof = True
                return 0
        size = min(len(target), len(self._buffer))
        target[:size] = self._buffer[:size]
        del self._buffer[:size]
        return size


def _first_non_whitespace_byte(data: bytes) -> bytes:
    for byte in data:
        if chr(byte).isspace():
            continue
        return bytes([byte])
    return b""
