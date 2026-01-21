"""CLI entrypoints for Financial Data Lab."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from financial_data_lab.core.hashing import receipt_id_from_sha256, sha256_file
from financial_data_lab.store import artifacts, events, export, layout, manifests


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fdl")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest a file")
    ingest_parser.add_argument("path", type=Path)
    ingest_parser.add_argument("--store", type=Path, default=layout.DEFAULT_STORE)

    export_parser = subparsers.add_parser("export", help="Export data")
    export_subparsers = export_parser.add_subparsers(dest="export_command", required=True)
    export_receipts = export_subparsers.add_parser("receipts", help="Export receipts")
    export_receipts.add_argument("--store", type=Path, default=layout.DEFAULT_STORE)
    export_receipts.add_argument("--out", type=Path)

    verify_parser = subparsers.add_parser("verify", help="Verify store integrity")
    verify_parser.add_argument("--store", type=Path, default=layout.DEFAULT_STORE)

    return parser.parse_args(argv)


def _cmd_ingest(path: Path, store: Path) -> int:
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        return 1
    sha256_hex, object_path, _ = artifacts.store_object(path, store)
    receipt_id = receipt_id_from_sha256(sha256_hex)
    manifest_path = manifests.write_manifest(
        store=store,
        sha256_hex=sha256_hex,
        source_path=path,
        object_path=object_path,
    )
    manifest_ref = layout.relative_to_store(store, manifest_path)
    object_ref = layout.relative_to_store(store, object_path)
    events.append_receipt_ingested(
        store=store,
        receipt_id=receipt_id,
        manifest_path=manifest_ref,
        object_path=object_ref,
    )
    print(f"receipt_id: {receipt_id}")
    print(f"object_path: {object_path}")
    print(f"manifest_path: {manifest_path}")
    return 0


def _cmd_export_receipts(store: Path, out_path: Path | None) -> int:
    output_path = export.export_receipts(store, out_path)
    print(f"export_path: {output_path}")
    return 0


def _cmd_verify(store: Path) -> int:
    receipts_root = layout.receipts_root(store)
    errors = 0
    if not receipts_root.exists():
        print("No receipts found.")
        return 0
    for manifest_path in receipts_root.glob("*/manifest.v1.json"):
        try:
            manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"Invalid JSON in {manifest_path}: {exc}", file=sys.stderr)
            errors += 1
            continue
        content = manifest_data.get("content", {})
        sha256_hex = content.get("sha256")
        object_path = Path(content.get("object_path", ""))
        if not object_path.is_absolute():
            object_path = store / object_path
        if not sha256_hex:
            print(f"Missing sha256 in {manifest_path}", file=sys.stderr)
            errors += 1
            continue
        if not object_path.exists():
            print(f"Missing object: {object_path}", file=sys.stderr)
            errors += 1
            continue
        actual_hash = sha256_file(object_path)
        if sha256_hex != actual_hash:
            print(
                f"Hash mismatch for {manifest_path}: expected {sha256_hex}, got {actual_hash}",
                file=sys.stderr,
            )
            errors += 1
    if errors:
        return 1
    print("Store verification passed.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "ingest":
        return _cmd_ingest(args.path, args.store)
    if args.command == "export" and args.export_command == "receipts":
        return _cmd_export_receipts(args.store, args.out)
    if args.command == "verify":
        return _cmd_verify(args.store)
    raise SystemExit("Unknown command")


if __name__ == "__main__":
    raise SystemExit(main())
