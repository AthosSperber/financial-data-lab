"""Export helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from financial_data_lab.core.jsoncanon import append_canonical_json_line
from financial_data_lab.store import layout


def export_receipts(store: Path, out_path: Path | None = None) -> Path:
    receipts_root = layout.receipts_root(store)
    if out_path is None:
        out_path = layout.receipts_export_path(store)
    if not receipts_root.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("", encoding="utf-8")
        return out_path
    manifests = sorted(receipts_root.glob("*/manifest.v1.json"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    for manifest_path in manifests:
        record = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_ref = layout.relative_to_store(store, manifest_path)
        export_line: dict[str, Any] = {
            "receipt_id": record.get("receipt_id"),
            "sha256": record.get("content", {}).get("sha256"),
            "media_type": record.get("source", {}).get("media_type"),
            "byte_size": record.get("source", {}).get("byte_size"),
            "ingested_at": record.get("ingested_at"),
            "object_path": record.get("content", {}).get("object_path"),
            "manifest_path": str(manifest_ref),
        }
        append_canonical_json_line(out_path, export_line)
    return out_path
