"""Store layout helpers."""

from __future__ import annotations

from pathlib import Path

DEFAULT_STORE = Path("./data")


def objects_root(store: Path) -> Path:
    return store / "objects" / "sha256"


def object_path(store: Path, sha256_hex: str) -> Path:
    return objects_root(store) / sha256_hex[:2] / sha256_hex[2:4] / sha256_hex


def receipts_root(store: Path) -> Path:
    return store / "receipts"


def manifest_path(store: Path, receipt_id: str) -> Path:
    return receipts_root(store) / receipt_id / "manifest.v1.json"


def events_path(store: Path) -> Path:
    return store / "events" / "events.v1.jsonl"


def exports_root(store: Path) -> Path:
    return store / "exports"


def receipts_export_path(store: Path) -> Path:
    return exports_root(store) / "receipts.v1.jsonl"


def relative_to_store(store: Path, path: Path) -> Path:
    try:
        return path.relative_to(store)
    except ValueError:
        return path
