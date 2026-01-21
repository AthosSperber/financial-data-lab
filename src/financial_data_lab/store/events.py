"""Event log helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from financial_data_lab.core.jsoncanon import append_canonical_json_line
from financial_data_lab.store import layout

EVENT_SCHEMA = "financial-data-lab/event.v1"


def _event_exists(path: Path, receipt_id: str, event_type: str) -> bool:
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("receipt_id") == receipt_id and record.get("type") == event_type:
                return True
    return False


def append_receipt_ingested(
    *,
    store: Path,
    receipt_id: str,
    manifest_path: Path,
    object_path: Path,
    ingested_at: str | None = None,
) -> bool:
    events_path = layout.events_path(store)
    event_type = "receipt.ingested"
    if _event_exists(events_path, receipt_id, event_type):
        return False
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    manifest_ref = layout.relative_to_store(store, manifest_path)
    object_ref = layout.relative_to_store(store, object_path)
    payload: dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "ts": ingested_at,
        "type": event_type,
        "receipt_id": receipt_id,
        "refs": {
            "manifest_path": str(manifest_ref),
            "object_path": str(object_ref),
        },
    }
    append_canonical_json_line(events_path, payload)
    return True


def append_receipt_ocr_observed(
    *,
    store: Path,
    receipt_id: str,
    ocr_path: Path,
    ingested_at: str | None = None,
) -> bool:
    events_path = layout.events_path(store)
    event_type = "receipt.ocr_observed"
    if _event_exists(events_path, receipt_id, event_type):
        return False
    if ingested_at is None:
        ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    ocr_ref = layout.relative_to_store(store, ocr_path)
    payload: dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "ts": ingested_at,
        "type": event_type,
        "receipt_id": receipt_id,
        "refs": {
            "ocr_path": str(ocr_ref),
        },
    }
    append_canonical_json_line(events_path, payload)
    return True
