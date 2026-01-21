"""Manifest helpers."""

from __future__ import annotations

import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from financial_data_lab.core.jsoncanon import write_canonical_json
from financial_data_lab.core.hashing import receipt_id_from_sha256
from financial_data_lab.store import layout

MANIFEST_SCHEMA = "financial-data-lab/manifest.v1"


def build_manifest(
    *,
    receipt_id: str,
    sha256_hex: str,
    source_path: Path,
    path_hint: str,
    original_filename: str,
    object_path: Path,
    ingested_at: str | None = None,
) -> dict[str, Any]:
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    media_type, _ = mimetypes.guess_type(source_path.name)
    byte_size = source_path.stat().st_size
    return {
        "schema": MANIFEST_SCHEMA,
        "receipt_id": receipt_id,
        "ingested_at": ingested_at,
        "source": {
            "path_hint": path_hint,
            "original_filename": original_filename,
            "media_type": media_type,
            "byte_size": byte_size,
        },
        "content": {
            "sha256": sha256_hex,
            "object_path": str(object_path),
        },
        "observed": {},
        "inferred": {},
    }


def write_manifest(
    *,
    store: Path,
    sha256_hex: str,
    source_path: Path,
    path_hint: str,
    original_filename: str,
    object_path: Path,
    ingested_at: str | None = None,
) -> Path:
    receipt_id = receipt_id_from_sha256(sha256_hex)
    manifest_path = layout.manifest_path(store, receipt_id)
    if manifest_path.exists():
        return manifest_path
    object_ref = layout.relative_to_store(store, object_path)
    manifest = build_manifest(
        receipt_id=receipt_id,
        sha256_hex=sha256_hex,
        source_path=source_path,
        path_hint=path_hint,
        original_filename=original_filename,
        object_path=object_ref,
        ingested_at=ingested_at,
    )
    write_canonical_json(manifest_path, manifest)
    return manifest_path
