from __future__ import annotations

import json
from pathlib import Path

from financial_data_lab.core.hashing import receipt_id_from_sha256, sha256_bytes
from financial_data_lab.core.jsoncanon import canonical_json_dumps, write_canonical_json


def test_receipt_id_deterministic() -> None:
    payload = b"same bytes"
    sha_a = sha256_bytes(payload)
    sha_b = sha256_bytes(payload)
    assert sha_a == sha_b
    assert receipt_id_from_sha256(sha_a) == receipt_id_from_sha256(sha_b)


def test_manifest_canonical(tmp_path: Path) -> None:
    manifest = {
        "schema": "fdl.receipt.manifest.v1",
        "receipt_id": "rcpt_1234",
        "ingested_at": "2024-01-01T00:00:00Z",
        "source": {
            "path_hint": "source.txt",
            "original_filename": "source.txt",
            "media_type": "text/plain",
            "byte_size": 3,
        },
        "content": {"sha256": "abc", "object_path": "objects/sha256/ab/cd/abc"},
        "observed": {},
        "inferred": {},
    }
    path = tmp_path / "manifest.json"
    write_canonical_json(path, manifest)
    payload = path.read_text(encoding="utf-8")
    assert payload.endswith("\n")
    assert payload == canonical_json_dumps(manifest) + "\n"

    manifest["ingested_at"] = "2024-01-02T00:00:00Z"
    path_two = tmp_path / "manifest_two.json"
    write_canonical_json(path_two, manifest)
    payload_two = path_two.read_text(encoding="utf-8")
    assert payload_two.endswith("\n")
    parsed = json.loads(payload_two)
    for key in ["schema", "receipt_id", "ingested_at", "source", "content", "observed", "inferred"]:
        assert key in parsed
