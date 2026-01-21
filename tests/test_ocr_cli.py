from __future__ import annotations

import io
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


def test_ocr_pdf_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    Image = pytest.importorskip("PIL.Image")
    pytesseract = pytest.importorskip("pytesseract")

    store = tmp_path / "store"
    pdf_path = tmp_path / "receipt.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\\n%EOF\\n")
    receipt_id = _ingest(pdf_path, store)

    def fake_render(_path: Path) -> tuple[str, list[bytes]]:
        first = Image.new("RGB", (2, 2), color=(255, 255, 255))
        second = Image.new("RGB", (2, 2), color=(0, 0, 0))
        first_bytes = io.BytesIO()
        second_bytes = io.BytesIO()
        first.save(first_bytes, format="PNG")
        second.save(second_bytes, format="PNG")
        return "1.2.3", [first_bytes.getvalue(), second_bytes.getvalue()]

    monkeypatch.setattr(
        "financial_data_lab.store.pdf_pages._render_pdf_pages", fake_render
    )
    monkeypatch.setattr(pytesseract, "image_to_string", lambda *_args, **_kwargs: "page text")
    monkeypatch.setattr(pytesseract, "get_tesseract_version", lambda: "9.9.9")

    exit_code = cli.main(["ocr", receipt_id, "--store", str(store)])
    assert exit_code == 0

    pdf_pages_path = layout.pdf_pages_path(store, receipt_id)
    assert pdf_pages_path.exists()
    pdf_payload = json.loads(pdf_pages_path.read_text(encoding="utf-8"))
    assert pdf_payload["observed"]["page_count"] == 2
    for page in pdf_payload["observed"]["pages"]:
        assert not Path(page["image"]["object_path"]).is_absolute()

    ocr_path = layout.ocr_path(store, receipt_id)
    assert ocr_path.exists()
    payload = json.loads(ocr_path.read_text(encoding="utf-8"))
    assert payload["observed"]["text"] == "page text\n\n---\n\npage text"
    assert len(payload["observed"]["pages"]) == 2

    events_path = layout.events_path(store)
    lines = events_path.read_text(encoding="utf-8").strip().splitlines()
    event_types = [json.loads(line)["type"] for line in lines]
    assert "receipt.pdf_pages_observed" in event_types
    assert "receipt.ocr_observed" in event_types


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
