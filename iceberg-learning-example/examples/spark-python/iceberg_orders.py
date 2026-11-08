#!/usr/bin/env python3
"""Create, merge into, inspect, or clean up an Iceberg table through PySpark."""

import os
import re
import sys

from pyspark.sql import SparkSession


NAMESPACE = "learning_api"
TABLE = "orders_pyspark"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def optional_env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def create_spark(catalog: str) -> SparkSession:
    return (
        SparkSession.builder.appName("iceberg-learning-pyspark")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", required_env("CATALOG_URI"))
        .config(f"spark.sql.catalog.{catalog}.warehouse", required_env("CATALOG_WAREHOUSE"))
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", required_env("S3_ENDPOINT"))
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", optional_env("S3_PATH_STYLE_ACCESS", "true"))
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", required_env("MINIO_ACCESS_KEY_ID"))
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", required_env("MINIO_SECRET_ACCESS_KEY"))
        .config(f"spark.sql.catalog.{catalog}.client.region", optional_env("AWS_REGION", "us-east-1"))
        .getOrCreate()
    )


def main() -> None:
    catalog = optional_env("CATALOG_NAME", "ice")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
        raise RuntimeError("CATALOG_NAME must be a Spark identifier")

    table_identifier = f"{catalog}.{NAMESPACE}.{TABLE}"
    spark = create_spark(catalog)
    try:
        if sys.argv[1:] == ["--cleanup"]:
            spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")
            return
        if len(sys.argv) != 1:
            raise RuntimeError("Only --cleanup is supported")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{NAMESPACE}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_identifier} ("
            "order_id BIGINT, customer_id BIGINT, order_status STRING, "
            "order_total DECIMAL(12, 2), event_time TIMESTAMP) USING iceberg "
            "PARTITIONED BY (days(event_time)) TBLPROPERTIES ('format-version' = '2')"
        )
        spark.sql(
            f"MERGE INTO {table_identifier} target USING (SELECT "
            "CAST(9401 AS BIGINT) AS order_id, CAST(902 AS BIGINT) AS customer_id, "
            "'SHIPPED' AS order_status, CAST(61.50 AS DECIMAL(12, 2)) AS order_total, "
            "TIMESTAMP '2026-04-03 10:00:00' AS event_time) source "
            "ON target.order_id = source.order_id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
        spark.sql(
            f"SELECT order_id, customer_id, order_status, order_total, event_time "
            f"FROM {table_identifier} ORDER BY order_id"
        ).show(truncate=False)
        spark.sql(
            f"SELECT snapshot_id, committed_at, operation FROM {table_identifier}.snapshots "
            "ORDER BY committed_at"
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
