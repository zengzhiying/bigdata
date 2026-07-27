#!/usr/bin/env python3
"""Read every document in an Elasticsearch index with PIT + search_after."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import suppress
from typing import Any, Iterator

from elasticsearch import Elasticsearch


def read_batches(
    client: Elasticsearch,
    index: str,
    batch_size: int,
    keep_alive: str,
) -> Iterator[list[dict[str, Any]]]:
    """Yield batches from one consistent point-in-time snapshot."""
    pit_id = client.open_point_in_time(
        index=index,
        keep_alive=keep_alive,
    )["id"]
    search_after: list[Any] | None = None

    try:
        while True:
            params: dict[str, Any] = {
                "pit": {"id": pit_id, "keep_alive": keep_alive},
                "size": batch_size,
                "query": {"match_all": {}},
                # _shard_doc is the efficient order for a complete scan.
                "sort": [{"_shard_doc": "asc"}],
                "track_total_hits": False,
                # Do not silently return an incomplete batch if a shard fails.
                "allow_partial_search_results": False,
            }
            if search_after is not None:
                params["search_after"] = search_after

            response = client.search(**params)
            # Elasticsearch can rotate the PIT ID; always carry the latest one.
            pit_id = response.get("pit_id", pit_id)
            hits = response["hits"]["hits"]
            if not hits:
                return

            yield hits
            search_after = hits[-1]["sort"]
    finally:
        # Closing is best effort because the PIT may already have expired.
        with suppress(Exception):
            client.close_point_in_time(id=pit_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read an Elasticsearch index with PIT + search_after."
    )
    parser.add_argument("index", help="Index (or alias) to read")
    parser.add_argument(
        "--url",
        default=os.getenv("ELASTIC_URL", "http://localhost:9200"),
        help="Elasticsearch URL (default: ELASTIC_URL or http://localhost:9200)",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("ELASTIC_USERNAME"),
        help="Basic-auth username (default: ELASTIC_USERNAME)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("ELASTIC_PASSWORD"),
        help="Basic-auth password (default: ELASTIC_PASSWORD)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Documents per search request (default: 100)",
    )
    parser.add_argument(
        "--keep-alive",
        default="5m",
        help="PIT keep-alive duration (default: 5m)",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=120,
        help="Request timeout in seconds (default: 120)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size < 1:
        print("--batch-size must be greater than zero", file=sys.stderr)
        return 2
    if (args.username is None) != (args.password is None):
        print("--username and --password must be supplied together", file=sys.stderr)
        return 2

    client_kwargs: dict[str, Any] = {
        # Requested for development/self-signed HTTPS endpoints.
        "verify_certs": False,
        "ssl_show_warn": False,
        "request_timeout": args.request_timeout,
    }
    if args.username is not None:
        client_kwargs["basic_auth"] = (args.username, args.password)

    client = Elasticsearch(args.url, **client_kwargs)

    try:
        total = 0
        for batch_number, batch in enumerate(
            read_batches(client, args.index, args.batch_size, args.keep_alive),
            start=1,
        ):
            print(f"batch={batch_number} count={len(batch)}")
            for hit in batch:
                print(json.dumps(hit, ensure_ascii=False, default=str))
            total += len(batch)
        print(f"completed total={total}")
        return 0
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"read failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
