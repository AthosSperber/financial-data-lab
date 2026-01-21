from __future__ import annotations

from pathlib import Path

from financial_data_lab.core.hashing import receipt_id_from_sha256
from financial_data_lab.store import artifacts, events, layout, manifests


def _ingest(path: Path, store: Path) -> None:
    sha256_hex, object_path, _ = artifacts.store_object(path, store)
    receipt_id = receipt_id_from_sha256(sha256_hex)
    manifest_path = manifests.write_manifest(
        store=store,
        sha256_hex=sha256_hex,
        source_path=path,
        path_hint=str(path),
        original_filename=path.name,
        object_path=object_path,
        ingested_at="2024-01-01T00:00:00Z",
    )
    manifest_ref = layout.relative_to_store(store, manifest_path)
    object_ref = layout.relative_to_store(store, object_path)
    events.append_receipt_ingested(
        store=store,
        receipt_id=receipt_id,
        manifest_path=manifest_ref,
        object_path=object_ref,
        ingested_at="2024-01-01T00:00:00Z",
    )


def test_events_append_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    source = tmp_path / "receipt.txt"
    source.write_text("hello", encoding="utf-8")

    _ingest(source, store)
    _ingest(source, store)

    events_path = layout.events_path(store)
    lines = events_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
