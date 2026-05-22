from __future__ import annotations

import hashlib
import json
import logging

import httpx
import pytest
import respx

from activities.download_ariregister_dataset import download_ariregister_dataset
from activities.download_cvr_file_set import download_cvr_file_set
from activities.download_gleif_golden_copy import download_gleif_golden_copy
from contracts import DownloadSourceFilesInput, DownloadSourceFilesResult


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@respx.mock
@pytest.mark.asyncio
async def test_gleif_downloads_golden_copy_file(tmp_path, monkeypatch):
    content = b'{"leiRecords":[]}'
    base_url = "https://gleif.example.test/publishes"
    monkeypatch.setenv("GLEIF_GOLDEN_COPY_BASE_URL", base_url)
    respx.get(f"{base_url}/lei2/latest.json").mock(return_value=httpx.Response(200, content=content))

    result = await download_gleif_golden_copy(
        DownloadSourceFilesInput(
            source="gleif",
            mode="full",
            output_dir=str(tmp_path),
            snapshot_id="2026-05-21T10",
        )
    )

    assert isinstance(result, DownloadSourceFilesResult)
    assert result.source == "gleif"
    assert result.snapshot_id == "2026-05-21T10"
    assert len(result.files) == 1
    downloaded = result.files[0]
    assert downloaded.source == "gleif"
    assert downloaded.dataset == "lei2"
    assert downloaded.snapshot_id == "2026-05-21T10"
    assert downloaded.sha256 == _sha256(content)
    assert downloaded.format == "json"
    assert downloaded.file_path == str(tmp_path / "gleif-lei2-2026-05-21T10.json")
    assert (tmp_path / "gleif-lei2-2026-05-21T10.json").read_bytes() == content


@respx.mock
@pytest.mark.asyncio
async def test_gleif_delta_mode_uses_delta_url(tmp_path, monkeypatch):
    content = b'{"delta":[]}'
    base_url = "https://gleif.example.test/publishes"
    monkeypatch.setenv("GLEIF_GOLDEN_COPY_BASE_URL", base_url)
    route = respx.get(f"{base_url}/lei2/latest.json?delta=LastDay").mock(
        return_value=httpx.Response(200, content=content)
    )

    result = await download_gleif_golden_copy(
        DownloadSourceFilesInput(
            source="gleif",
            mode="delta",
            output_dir=str(tmp_path),
            snapshot_id="delta-window",
            delta_window="PT24H",
        )
    )

    assert route.called
    assert result.files[0].file_path == str(tmp_path / "gleif-lei2-delta-window.json")
    assert result.files[0].sha256 == _sha256(content)


@pytest.mark.asyncio
async def test_gleif_rejects_unsafe_filename_components(tmp_path, monkeypatch):
    monkeypatch.setenv("GLEIF_GOLDEN_COPY_BASE_URL", "https://gleif.example.test/publishes")
    outside_path = tmp_path.parent / "escape-lei2-safe.json"

    with pytest.raises(RuntimeError, match="unsafe filename component"):
        await download_gleif_golden_copy(
            DownloadSourceFilesInput(
                source="../escape",
                mode="full",
                output_dir=str(tmp_path),
                snapshot_id="safe",
            )
        )

    assert not outside_path.exists()


@pytest.mark.asyncio
async def test_gleif_rejects_unsupported_mode(tmp_path):
    with pytest.raises(RuntimeError, match="Unsupported GLEIF download mode"):
        await download_gleif_golden_copy(
            DownloadSourceFilesInput(source="gleif", mode="hourly", output_dir=str(tmp_path), snapshot_id="safe")
        )


@respx.mock
@pytest.mark.asyncio
async def test_ariregister_downloads_configured_datasets_with_stable_snapshot_id(tmp_path, monkeypatch):
    datasets = [
        {"dataset": "simple", "url": "https://ariregister.example.test/simple.csv.zip", "format": "csv.zip"},
        {"dataset": "owners", "url": "https://ariregister.example.test/owners.csv.zip", "format": "csv.zip"},
    ]
    monkeypatch.setenv("ARIREGISTER_DATASETS_JSON", json.dumps(datasets))
    respx.get(datasets[0]["url"]).mock(return_value=httpx.Response(200, content=b"simple"))
    respx.get(datasets[1]["url"]).mock(return_value=httpx.Response(200, content=b"owners"))

    result = await download_ariregister_dataset(
        DownloadSourceFilesInput(
            source="ariregister",
            mode="full",
            output_dir=str(tmp_path),
            snapshot_id="ari-snapshot",
        )
    )

    assert result.source == "ariregister"
    assert result.snapshot_id == "ari-snapshot"
    assert [file.dataset for file in result.files] == ["simple", "owners"]
    assert [file.file_path for file in result.files] == [
        str(tmp_path / "ariregister-simple-ari-snapshot.csv.zip"),
        str(tmp_path / "ariregister-owners-ari-snapshot.csv.zip"),
    ]
    assert result.files[0].sha256 == _sha256(b"simple")
    assert result.files[1].sha256 == _sha256(b"owners")
    assert (tmp_path / "ariregister-simple-ari-snapshot.csv.zip").read_bytes() == b"simple"
    assert (tmp_path / "ariregister-owners-ari-snapshot.csv.zip").read_bytes() == b"owners"


@pytest.mark.asyncio
async def test_ariregister_rejects_unsafe_dataset_filename_component(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "ARIREGISTER_DATASETS_JSON",
        json.dumps(
            [
                {
                    "dataset": "../escape",
                    "url": "https://ariregister.example.test/escape.csv.zip",
                    "format": "csv.zip",
                }
            ]
        ),
    )
    outside_path = tmp_path.parent / "ariregister-escape-safe.csv.zip"

    with pytest.raises(RuntimeError, match="unsafe filename component"):
        await download_ariregister_dataset(
            DownloadSourceFilesInput(
                source="ariregister",
                mode="full",
                output_dir=str(tmp_path),
                snapshot_id="safe",
            )
        )

    assert not outside_path.exists()


@pytest.mark.asyncio
async def test_ariregister_malformed_datasets_json_raises_configuration_error(tmp_path, monkeypatch):
    monkeypatch.setenv("ARIREGISTER_DATASETS_JSON", "{not json")

    with pytest.raises(RuntimeError, match="ARIREGISTER_DATASETS_JSON"):
        await download_ariregister_dataset(
            DownloadSourceFilesInput(
                source="ariregister",
                mode="full",
                output_dir=str(tmp_path),
                snapshot_id="safe",
            )
        )


@pytest.mark.asyncio
async def test_ariregister_invalid_dataset_field_types_raise_configuration_error(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "ARIREGISTER_DATASETS_JSON",
        json.dumps(
            [
                {"dataset": 123, "url": "https://ariregister.example.test/simple.csv.zip", "format": "csv.zip"},
                {"dataset": "owners", "url": None, "format": "csv.zip"},
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="ARIREGISTER_DATASETS_JSON"):
        await download_ariregister_dataset(
            DownloadSourceFilesInput(
                source="ariregister",
                mode="full",
                output_dir=str(tmp_path),
                snapshot_id="safe",
            )
        )


@pytest.mark.asyncio
async def test_cvr_errors_when_no_credentials_are_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("CVR_FILEDOWNLOAD_BASE_URL", "https://datafordeler.example.test/cvr")
    monkeypatch.setenv("CVR_FILEDOWNLOAD_DATASETS", "companies")
    monkeypatch.delenv("CVR_FILEDOWNLOAD_API_KEY", raising=False)
    monkeypatch.delenv("CVR_FILEDOWNLOAD_BEARER_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="CVR file download credentials are not configured"):
        await download_cvr_file_set(
            DownloadSourceFilesInput(source="cvr", mode="full", output_dir=str(tmp_path), snapshot_id="cvr-snapshot")
        )


@pytest.mark.asyncio
async def test_cvr_rejects_unsafe_snapshot_filename_component(tmp_path, monkeypatch):
    monkeypatch.setenv("CVR_FILEDOWNLOAD_BASE_URL", "https://datafordeler.example.test/cvr")
    monkeypatch.setenv("CVR_FILEDOWNLOAD_DATASETS", "companies")
    monkeypatch.setenv("CVR_FILEDOWNLOAD_API_KEY", "secret")
    monkeypatch.delenv("CVR_FILEDOWNLOAD_BEARER_TOKEN", raising=False)
    outside_path = tmp_path.parent / "escape.json"

    with pytest.raises(RuntimeError, match="unsafe filename component"):
        await download_cvr_file_set(
            DownloadSourceFilesInput(
                source="cvr",
                mode="full",
                output_dir=str(tmp_path),
                snapshot_id="../escape",
            )
        )

    assert not outside_path.exists()


@respx.mock
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("env_name", "secret", "expected_header", "expected_value"),
    [
        ("CVR_FILEDOWNLOAD_API_KEY", "api-secret", "X-API-Key", "api-secret"),
        ("CVR_FILEDOWNLOAD_BEARER_TOKEN", "bearer-secret", "Authorization", "Bearer bearer-secret"),
    ],
)
async def test_cvr_sends_credentials_only_in_headers_and_writes_files(
    tmp_path,
    monkeypatch,
    caplog,
    env_name,
    secret,
    expected_header,
    expected_value,
):
    base_url = "https://datafordeler.example.test/cvr"
    monkeypatch.setenv("CVR_FILEDOWNLOAD_BASE_URL", base_url)
    monkeypatch.setenv("CVR_FILEDOWNLOAD_DATASETS", "companies")
    monkeypatch.delenv("CVR_FILEDOWNLOAD_API_KEY", raising=False)
    monkeypatch.delenv("CVR_FILEDOWNLOAD_BEARER_TOKEN", raising=False)
    monkeypatch.setenv(env_name, secret)
    route = respx.get(f"{base_url}/companies").mock(return_value=httpx.Response(200, content=b"cvr-data"))

    caplog.set_level(logging.INFO)
    result = await download_cvr_file_set(
        DownloadSourceFilesInput(source="cvr", mode="full", output_dir=str(tmp_path), snapshot_id="cvr-snapshot")
    )

    request = route.calls.last.request
    assert request.headers[expected_header] == expected_value
    assert secret not in str(request.url)
    assert request.content == b""
    assert secret not in caplog.text
    assert result.files[0].file_path == str(tmp_path / "cvr-companies-cvr-snapshot.json")
    assert result.files[0].sha256 == _sha256(b"cvr-data")
    assert (tmp_path / "cvr-companies-cvr-snapshot.json").read_bytes() == b"cvr-data"
