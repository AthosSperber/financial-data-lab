"""Artifact storage helpers."""

from __future__ import annotations

import shutil
from pathlib import Path

from financial_data_lab.core.hashing import sha256_bytes, sha256_file
from financial_data_lab.store import layout


def store_object(source_path: Path, store: Path) -> tuple[str, Path, bool]:
    sha256_hex = sha256_file(source_path)
    object_path = layout.object_path(store, sha256_hex)
    existed = object_path.exists()
    if not existed:
        object_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, object_path)
    return sha256_hex, object_path, existed


def store_object_bytes(data: bytes, store: Path, suffix: str | None = None) -> tuple[str, Path, bool]:
    sha256_hex = sha256_bytes(data)
    object_path = layout.object_path(store, sha256_hex)
    existed = object_path.exists()
    if not existed:
        object_path.parent.mkdir(parents=True, exist_ok=True)
        object_path.write_bytes(data)
    return sha256_hex, object_path, existed
