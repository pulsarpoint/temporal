from __future__ import annotations

import re
from pathlib import Path

_SAFE_FILENAME_COMPONENT = re.compile(r"[A-Za-z0-9._-]+")


def safe_output_file_path(
    output_dir: str,
    source: str,
    dataset: str,
    snapshot_id: str,
    file_format: str,
) -> str:
    for label, value in (
        ("source", source),
        ("dataset", dataset),
        ("snapshot_id", snapshot_id),
        ("format", file_format),
    ):
        _validate_filename_component(label, value)

    output_base = Path(output_dir).resolve()
    output_base.mkdir(parents=True, exist_ok=True)
    file_path = (output_base / f"{source}-{dataset}-{snapshot_id}.{file_format}").resolve()

    try:
        file_path.relative_to(output_base)
    except ValueError as exc:
        raise RuntimeError("download output path escapes output_dir") from exc

    return str(file_path)


def _validate_filename_component(label: str, value: str) -> None:
    if value in {"", ".", ".."}:
        raise RuntimeError(f"unsafe filename component for {label}")
    if "/" in value or "\\" in value:
        raise RuntimeError(f"unsafe filename component for {label}")
    if not _SAFE_FILENAME_COMPONENT.fullmatch(value):
        raise RuntimeError(f"unsafe filename component for {label}")
