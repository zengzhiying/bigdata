#!/usr/bin/env python3
"""Generate deterministic JSONL CDC batches for lab 06 without external services."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


BATCHES = {
    "001": [
        {
            "event_id": "evt-1001-v1",
            "operation": "INSERT",
            "order_id": 1001,
            "event_version": 1,
            "event_time": "2026-06-01 09:00:00.000",
            "customer_id": 501,
            "order_status": "CREATED",
            "order_total": 20.00,
            "event_day": "2026-06-01",
        },
        {
            "event_id": "evt-1002-v1",
            "operation": "INSERT",
            "order_id": 1002,
            "event_version": 1,
            "event_time": "2026-06-01 09:05:00.000",
            "customer_id": 502,
            "order_status": "CREATED",
            "order_total": 80.25,
            "event_day": "2026-06-01",
        },
    ],
    "002": [
        {
            "event_id": "evt-1001-v2",
            "operation": "UPDATE",
            "order_id": 1001,
            "event_version": 2,
            "event_time": "2026-06-01 09:10:00.000",
            "customer_id": 501,
            "order_status": "PAID",
            "order_total": 20.00,
            "event_day": "2026-06-01",
        },
        {
            "event_id": "evt-1001-v2",
            "operation": "UPDATE",
            "order_id": 1001,
            "event_version": 2,
            "event_time": "2026-06-01 09:10:00.000",
            "customer_id": 501,
            "order_status": "PAID",
            "order_total": 20.00,
            "event_day": "2026-06-01",
        },
        {
            "event_id": "evt-1002-v3",
            "operation": "UPDATE",
            "order_id": 1002,
            "event_version": 3,
            "event_time": "2026-06-01 09:20:00.000",
            "customer_id": 502,
            "order_status": "SHIPPED",
            "order_total": 80.25,
            "event_day": "2026-06-01",
        },
        {
            "event_id": "evt-1002-v2-late",
            "operation": "UPDATE",
            "order_id": 1002,
            "event_version": 2,
            "event_time": "2026-06-01 09:15:00.000",
            "customer_id": 502,
            "order_status": "PAID",
            "order_total": 80.25,
            "event_day": "2026-06-01",
        },
    ],
    "003": [
        {
            "event_id": "evt-1001-v3-delete",
            "operation": "DELETE",
            "order_id": 1001,
            "event_version": 3,
            "event_time": "2026-06-01 09:30:00.000",
            "customer_id": 501,
            "order_status": "CANCELLED",
            "order_total": 20.00,
            "event_day": "2026-06-01",
        },
        {
            "event_id": "evt-1003-v1",
            "operation": "INSERT",
            "order_id": 1003,
            "event_version": 1,
            "event_time": "2026-06-01 09:35:00.000",
            "customer_id": 503,
            "order_status": "CREATED",
            "order_total": 15.50,
            "event_day": "2026-06-01",
        },
    ],
}


def write_batch(output_dir: Path, batch_id: str, overwrite: bool) -> dict[str, object]:
    path = output_dir / f"batch-{batch_id}.json"
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite {path}; pass --overwrite to replace it.")

    with path.open("w", encoding="utf-8") as stream:
        for event in BATCHES[batch_id]:
            stream.write(json.dumps(event, separators=(",", ":")) + "\n")

    return {"file": path.name, "event_count": len(BATCHES[batch_id])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("generated-data/cdc-orders"),
        help="directory for generated JSONL batches (default: generated-data/cdc-orders)",
    )
    parser.add_argument(
        "--batch",
        choices=["all", *BATCHES],
        default="all",
        help="write one batch or all batches in source order",
    )
    parser.add_argument("--overwrite", action="store_true", help="replace already generated files")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    batch_ids = list(BATCHES) if args.batch == "all" else [args.batch]
    manifest = [write_batch(args.output_dir, batch_id, args.overwrite) for batch_id in batch_ids]
    print(json.dumps({"output_dir": str(args.output_dir), "batches": manifest}, indent=2))


if __name__ == "__main__":
    main()
