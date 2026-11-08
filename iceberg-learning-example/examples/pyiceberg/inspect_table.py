#!/usr/bin/env python3
"""Inspect an existing Iceberg table directly through PyIceberg and a REST Catalog."""

import os

from pyiceberg.catalog import load_catalog


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def main() -> None:
    catalog_name = os.environ.get("CATALOG_NAME", "ice")
    table_identifier = os.environ.get("PYICEBERG_TABLE", "learning_api.orders_pyspark")

    # These names are shared with Spark's REST Catalog configuration.
    catalog = load_catalog(
        catalog_name,
        **{
            "type": "rest",
            "uri": required_env("CATALOG_URI"),
            "warehouse": required_env("CATALOG_WAREHOUSE"),
            "s3.endpoint": required_env("S3_ENDPOINT"),
            "s3.path-style-access": os.environ.get("S3_PATH_STYLE_ACCESS", "true"),
            "s3.access-key-id": required_env("MINIO_ACCESS_KEY_ID"),
            "s3.secret-access-key": required_env("MINIO_SECRET_ACCESS_KEY"),
            "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
        },
    )
    table = catalog.load_table(table_identifier)
    current_snapshot = table.current_snapshot()

    print(f"table: {table_identifier}")
    print(f"schema: {table.schema()}")
    print(f"metadata location: {getattr(table, 'metadata_location', 'unavailable')}")
    if current_snapshot is None:
        print("current snapshot: none")
    else:
        print(f"current snapshot id: {current_snapshot.snapshot_id}")
        operation = getattr(current_snapshot.summary, "operation", "unknown")
        print(f"current snapshot operation: {operation}")


if __name__ == "__main__":
    main()
