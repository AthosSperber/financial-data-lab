"""Canonical JSON helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def canonical_json_dumps(obj: Any) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def write_canonical_json(path: Path, obj: Any) -> None:
    payload = canonical_json_dumps(obj) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


def append_canonical_json_line(path: Path, obj: Any) -> None:
    payload = canonical_json_dumps(obj) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(payload)
