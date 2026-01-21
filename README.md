# Financial Data Lab

Phase 1 provides a local-only, CLI-only pipeline for ingesting receipt files into an append-only store.
Manifests are written in canonical JSON and events are de-duplicated by `(receipt_id, type)` so
re-ingesting the same file does not create duplicate `receipt.ingested` events. Source metadata stores
the CLI input path as `source.path_hint` and the original basename as `source.original_filename` instead
of absolute paths.

## Setup

```bash
pip install -e .
```

## Ingest

```bash
fdl ingest path/to/receipt.pdf --store ./data
```

## Verify

```bash
fdl verify --store ./data
```

## Show a receipt

```bash
fdl show rcpt_1234abcd5678ef00 --store ./data
```

## Export receipts

```bash
fdl export receipts --store ./data --out ./data/exports/receipts.v1.jsonl
```
