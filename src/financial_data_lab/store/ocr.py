"""OCR helpers for observed artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from financial_data_lab.core.jsoncanon import write_canonical_json
from financial_data_lab.store import layout

OCR_SCHEMA = "financial-data-lab/ocr.v1"
SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


def build_ocr_observed(
    *,
    receipt_id: str,
    object_ref: Path,
    text: str,
    engine_name: str,
    engine_version: str,
    lang: str,
    created_at: str,
    pages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    observed: dict[str, Any] = {"text": text}
    if pages is not None:
        observed["pages"] = pages
    return {
        "schema": OCR_SCHEMA,
        "receipt_id": receipt_id,
        "created_at": created_at,
        "input": {
            "object_path": str(object_ref),
        },
        "engine": {
            "name": engine_name,
            "version": engine_version,
            "lang": lang,
        },
        "observed": observed,
    }


def write_ocr_observed(
    *,
    store: Path,
    receipt_id: str,
    object_path: Path,
    lang: str = "por",
    created_at: str | None = None,
    text: str | None = None,
    pages: list[dict[str, Any]] | None = None,
    engine_version: str | None = None,
) -> Path:
    ocr_path = layout.ocr_path(store, receipt_id)
    if ocr_path.exists():
        return ocr_path
    if created_at is None:
        created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if text is None:
        from PIL import Image
        import pytesseract

        with Image.open(object_path) as image:
            text = pytesseract.image_to_string(image, lang=lang)
        engine_version = str(pytesseract.get_tesseract_version())
    if engine_version is None:
        raise ValueError("engine_version is required when text is provided.")
    object_ref = layout.relative_to_store(store, object_path)
    payload = build_ocr_observed(
        receipt_id=receipt_id,
        object_ref=object_ref,
        text=text,
        engine_name="tesseract",
        engine_version=engine_version,
        lang=lang,
        created_at=created_at,
        pages=pages,
    )
    write_canonical_json(ocr_path, payload)
    return ocr_path
