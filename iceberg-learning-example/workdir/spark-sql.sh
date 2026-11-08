#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ICEBERG_ENV_FILE:-$SCRIPT_DIR/.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing credentials file: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${MINIO_ACCESS_KEY_ID:?MINIO_ACCESS_KEY_ID is not set in $ENV_FILE}"
: "${MINIO_SECRET_ACCESS_KEY:?MINIO_SECRET_ACCESS_KEY is not set in $ENV_FILE}"
: "${CATALOG_NAME:=ice}"
: "${CATALOG_URI:?CATALOG_URI is not set in $ENV_FILE}"
: "${CATALOG_WAREHOUSE:?CATALOG_WAREHOUSE is not set in $ENV_FILE}"
: "${S3_ENDPOINT:?S3_ENDPOINT is not set in $ENV_FILE}"
: "${AWS_REGION:=us-east-1}"
: "${S3_PATH_STYLE_ACCESS:=true}"
: "${SPARK_HOME:=/opt/spark-3.5.9-bin-hadoop3}"
: "${ICEBERG_LIB_DIR:=$PROJECT_ROOT/iceberg-libs}"

SPARK_SQL="$SPARK_HOME/bin/spark-sql"
RUNTIME_JAR="$ICEBERG_LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar"
AWS_BUNDLE_JAR="$ICEBERG_LIB_DIR/iceberg-aws-bundle-1.11.0.jar"

for required_file in "$SPARK_SQL" "$RUNTIME_JAR" "$AWS_BUNDLE_JAR"; do
  if [[ ! -f "$required_file" ]]; then
    echo "Required file is missing: $required_file" >&2
    exit 1
  fi
done

exec "$SPARK_SQL" \
  --jars "$RUNTIME_JAR,$AWS_BUNDLE_JAR" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.rest.RESTCatalog" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.uri=${CATALOG_URI}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.warehouse=${CATALOG_WAREHOUSE}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.endpoint=${S3_ENDPOINT}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.path-style-access=${S3_PATH_STYLE_ACCESS}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.access-key-id=${MINIO_ACCESS_KEY_ID}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.secret-access-key=${MINIO_SECRET_ACCESS_KEY}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.client.region=${AWS_REGION}" \
  "$@"
