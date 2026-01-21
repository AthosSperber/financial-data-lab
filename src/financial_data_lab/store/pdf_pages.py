"""PDF page rendering helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from financial_data_lab.core.jsoncanon import write_canonical_json
from financial_data_lab.store import artifacts, layout

PDF_PAGES_SCHEMA = "financial-data-lab/pdf_pages.v1"


def _render_pdf_pages(pdf_path: Path) -> tuple[str, list[bytes]]:
    import fitz

    doc = fitz.open(pdf_path)
    pages: list[bytes] = []
    for page in doc:
        pixmap = page.get_pixmap()
        pages.append(pixmap.tobytes("png"))
    return str(getattr(fitz, "__version__", "unknown")), pages


def write_pdf_pages_observed(
    *,
    store: Path,
    receipt_id: str,
    pdf_object_path: Path,
    created_at: str | None = None,
) -> Path:
    pages_path = layout.pdf_pages_path(store, receipt_id)
    if pages_path.exists():
        return pages_path
    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    engine_version, page_images = _render_pdf_pages(pdf_object_path)
    pages: list[dict[str, Any]] = []
    for page_index, image_bytes in enumerate(page_images, start=1):
        sha256_hex, object_path, _ = artifacts.store_object_bytes(image_bytes, store, suffix=".png")
        object_ref = layout.relative_to_store(store, object_path)
        pages.append(
            {
                "page": page_index,
                "image": {
                    "sha256": sha256_hex,
                    "object_path": str(object_ref),
                    "media_type": "image/png",
                    "byte_size": len(image_bytes),
                },
            }
        )
    pdf_ref = layout.relative_to_store(store, pdf_object_path)
    payload = {
        "schema": PDF_PAGES_SCHEMA,
        "receipt_id": receipt_id,
        "created_at": created_at,
        "input": {
            "object_path": str(pdf_ref),
        },
        "engine": {
            "name": "pymupdf",
            "version": engine_version,
        },
        "observed": {
            "page_count": len(pages),
            "pages": pages,
        },
    }
    write_canonical_json(pages_path, payload)
    return pages_path
