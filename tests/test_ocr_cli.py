from __future__ import annotations

import json
from pathlib import Path

import pytest

from financial_data_lab import cli
from financial_data_lab.core.hashing import receipt_id_from_sha256
from financial_data_lab.store import artifacts, layout, manifests


def _ingest(path: Path, store: Path) -> str:
    sha256_hex, object_path, _ = artifacts.store_object(path, store)
    receipt_id = receipt_id_from_sha256(sha256_hex)
    manifests.write_manifest(
        store=store,
        sha256_hex=sha256_hex,
        source_path=path,
        path_hint=str(path),
        original_filename=path.name,
        object_path=object_path,
        ingested_at="2024-01-01T00:00:00Z",
    )
    return receipt_id


def test_ocr_missing_receipt_returns_nonzero(tmp_path: Path, capsys: object) -> None:
    store = tmp_path / "store"
    exit_code = cli.main(["ocr", "rcpt_missing", "--store", str(store)])
    assert exit_code != 0
    captured = capsys.readouterr()
    assert "Manifest not found" in captured.err


def test_ocr_pdf_returns_nonzero(tmp_path: Path, capsys: object) -> None:
    store = tmp_path / "store"
    pdf_path = tmp_path / "receipt.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\\n%EOF\\n")
    receipt_id = _ingest(pdf_path, store)

    exit_code = cli.main(["ocr", receipt_id, "--store", str(store)])
    assert exit_code != 0
    captured = capsys.readouterr()
    assert "PDF OCR is not supported yet." in captured.err


def test_ocr_image_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    Image = pytest.importorskip("PIL.Image")
    pytesseract = pytest.importorskip("pytesseract")

    store = tmp_path / "store"
    image_path = tmp_path / "receipt.png"
    image = Image.new("RGB", (4, 4), color=(255, 255, 255))
    image.save(image_path)
    receipt_id = _ingest(image_path, store)

    monkeypatch.setattr(pytesseract, "image_to_string", lambda *_args, **_kwargs: "mocked text")
    monkeypatch.setattr(pytesseract, "get_tesseract_version", lambda: "9.9.9")

    exit_code = cli.main(["ocr", receipt_id, "--store", str(store)])
    assert exit_code == 0

    ocr_path = layout.ocr_path(store, receipt_id)
    assert ocr_path.exists()
    payload = json.loads(ocr_path.read_text(encoding="utf-8"))
    assert payload["observed"]["text"] == "mocked text"

    events_path = layout.events_path(store)
    lines = events_path.read_text(encoding="utf-8").strip().splitlines()
    event_types = [json.loads(line)["type"] for line in lines]
    assert "receipt.ocr_observed" in event_types
