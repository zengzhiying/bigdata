#!/usr/bin/env bash
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

# Prefer a complete Hadoop client distribution over copying Hadoop dependencies into Flink lib.
if [[ -n "${HADOOP_HOME:-}" ]]; then
  HADOOP_COMMAND="$HADOOP_HOME/bin/hadoop"
  if [[ ! -x "$HADOOP_COMMAND" ]]; then
    echo "Hadoop command is missing or not executable: $HADOOP_COMMAND" >&2
    exit 1
  fi
  : "${HADOOP_CONF_DIR:=$HADOOP_HOME/etc/hadoop}"
  if ! hadoop_classpath="$("$HADOOP_COMMAND" classpath --glob)"; then
    echo "Unable to resolve Hadoop classpath with: $HADOOP_COMMAND classpath --glob" >&2
    exit 1
  fi
  export HADOOP_HOME HADOOP_CONF_DIR
  export HADOOP_CLASSPATH="${hadoop_classpath}${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
fi

: "${MINIO_ACCESS_KEY_ID:?MINIO_ACCESS_KEY_ID is not set in $ENV_FILE}"
: "${MINIO_SECRET_ACCESS_KEY:?MINIO_SECRET_ACCESS_KEY is not set in $ENV_FILE}"
: "${CATALOG_NAME:=ice}"
: "${CATALOG_URI:?CATALOG_URI is not set in $ENV_FILE}"
: "${CATALOG_WAREHOUSE:?CATALOG_WAREHOUSE is not set in $ENV_FILE}"
: "${S3_ENDPOINT:?S3_ENDPOINT is not set in $ENV_FILE}"
: "${AWS_REGION:=us-east-1}"
: "${S3_PATH_STYLE_ACCESS:=true}"
: "${FLINK_HOME:=/opt/flink-1.20.5}"
: "${FLINK_RUNTIME_MODE:=batch}"

if [[ "$FLINK_RUNTIME_MODE" != "batch" && "$FLINK_RUNTIME_MODE" != "streaming" && "$FLINK_RUNTIME_MODE" != "automatic" ]]; then
  echo "FLINK_RUNTIME_MODE must be batch, streaming, or automatic: $FLINK_RUNTIME_MODE" >&2
  exit 1
fi

if [[ ! "$CATALOG_NAME" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "CATALOG_NAME must be a Flink SQL identifier: $CATALOG_NAME" >&2
  exit 1
fi

FLINK_SQL_CLIENT="$FLINK_HOME/bin/sql-client.sh"
FLINK_ICEBERG_RUNTIME_JAR="$FLINK_HOME/lib/iceberg-flink-runtime-1.20-1.11.0.jar"
FLINK_ICEBERG_AWS_BUNDLE_JAR="$FLINK_HOME/lib/iceberg-aws-bundle-1.11.0.jar"
for required_file in "$FLINK_SQL_CLIENT" "$FLINK_ICEBERG_RUNTIME_JAR" "$FLINK_ICEBERG_AWS_BUNDLE_JAR"; do
  if [[ ! -f "$required_file" ]]; then
    echo "Required global Flink dependency is missing: $required_file" >&2
    exit 1
  fi
done

sql_literal() {
  local value="${1//$'\n'/ }"
  value="${value//\'/\'\'}"
  printf "'%s'" "$value"
}

# Keep generated catalog SQL out of the repository. Flink may still log it on errors.
TEMP_SQL_FILE="$(mktemp "${TMPDIR:-/tmp}/iceberg-flink-sql.XXXXXX")"
trap 'rm -f "$TEMP_SQL_FILE"' EXIT

cat > "$TEMP_SQL_FILE" <<SQL
SET 'execution.runtime-mode' = '$FLINK_RUNTIME_MODE';

CREATE CATALOG $CATALOG_NAME WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = $(sql_literal "$CATALOG_URI"),
    'warehouse' = $(sql_literal "$CATALOG_WAREHOUSE"),
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = $(sql_literal "$S3_ENDPOINT"),
    's3.path-style-access' = $(sql_literal "$S3_PATH_STYLE_ACCESS"),
    's3.access-key-id' = $(sql_literal "$MINIO_ACCESS_KEY_ID"),
    's3.secret-access-key' = $(sql_literal "$MINIO_SECRET_ACCESS_KEY"),
    'client.region' = $(sql_literal "$AWS_REGION")
);

USE CATALOG $CATALOG_NAME;
SQL

if [[ "$#" -eq 0 ]]; then
  echo "==> Starting Flink SQL Client with catalog $CATALOG_NAME"
  "$FLINK_SQL_CLIENT" embedded \
    -i "$TEMP_SQL_FILE"
  exit 0
fi

for sql_file in "$@"; do
  if [[ "$sql_file" != /* ]]; then
    sql_file="$PROJECT_ROOT/$sql_file"
  fi
  if [[ ! -f "$sql_file" ]]; then
    echo "SQL file not found: $sql_file" >&2
    exit 1
  fi

  echo "==> Running ${sql_file#"$PROJECT_ROOT"/}"
  "$FLINK_SQL_CLIENT" embedded \
    -i "$TEMP_SQL_FILE" \
    -f "$sql_file"
done
