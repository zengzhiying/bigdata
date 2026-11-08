#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ICEBERG_ENV_FILE:-$PROJECT_ROOT/workdir/.env}"
STRICT=false
if [[ "${1:-}" == "--strict" ]]; then
  STRICT=true
fi

echo "Checking local tools and project configuration..."

warn_or_fail() {
  if $STRICT; then
    echo "Error: $1" >&2
    exit 1
  fi
  echo "Warning: $1" >&2
}

command -v java >/dev/null 2>&1 || { echo "java is required (Java 17)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required (Python 3.11)." >&2; exit 1; }

java_major="$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)"
python_version="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"

if [ "${java_major}" != "17" ]; then
  warn_or_fail "Java ${java_major} detected; the project baseline is Java 17."
fi

if [ "${python_version}" != "3.11" ] && [ "${python_version}" != "3.12" ]; then
  warn_or_fail "Python ${python_version} detected; supported versions are Python 3.11 and 3.12."
fi

if [[ ! -f "$ENV_FILE" ]]; then
  warn_or_fail "configuration file not found: $ENV_FILE (copy workdir/.env.example to .env)"
else
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  for variable_name in CATALOG_NAME CATALOG_URI CATALOG_WAREHOUSE S3_ENDPOINT AWS_REGION \
      MINIO_ACCESS_KEY_ID MINIO_SECRET_ACCESS_KEY; do
    if [[ -z "${!variable_name:-}" ]]; then
      warn_or_fail "$variable_name is not set in $ENV_FILE"
    fi
  done

  spark_home="${SPARK_HOME:-/opt/spark-3.5.9-bin-hadoop3}"
  iceberg_lib_dir="${ICEBERG_LIB_DIR:-$PROJECT_ROOT/iceberg-libs}"
  [[ -x "$spark_home/bin/spark-sql" ]] || warn_or_fail "spark-sql not found under $spark_home"
  [[ -f "$iceberg_lib_dir/iceberg-spark-runtime-3.5_2.12-1.11.0.jar" ]] || \
    warn_or_fail "Iceberg Spark runtime JAR not found under $iceberg_lib_dir"
  [[ -f "$iceberg_lib_dir/iceberg-aws-bundle-1.11.0.jar" ]] || \
    warn_or_fail "Iceberg AWS bundle JAR not found under $iceberg_lib_dir"
fi

if command -v spark-submit >/dev/null 2>&1; then
  spark_version="$(spark-submit --version 2>&1 | awk '/version/ {print $NF; exit}')"
  echo "Spark detected: ${spark_version:-unknown}"
else
  echo "Spark CLI is not on PATH; an external Spark installation may still be used through SPARK_HOME."
fi

echo "Local check complete. Use --strict in CI or before a lab to turn configuration warnings into failures."
