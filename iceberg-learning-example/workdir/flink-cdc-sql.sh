#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="${ICEBERG_ENV_FILE:-$SCRIPT_DIR/.env}"
TEMPLATE_FILE="$PROJECT_ROOT/labs/06-streaming-cdc/flink-sql/01_stream_append.sql.template"

if [[ "${1:-}" == "--template" ]]; then
  if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 [--template SQL_TEMPLATE]" >&2
    exit 2
  fi
  TEMPLATE_FILE="$2"
  if [[ "$TEMPLATE_FILE" != /* ]]; then
    TEMPLATE_FILE="$PROJECT_ROOT/$TEMPLATE_FILE"
  fi
elif [[ "$#" -ne 0 ]]; then
  echo "Usage: $0 [--template SQL_TEMPLATE]" >&2
  exit 2
fi

if [[ ! -f "$TEMPLATE_FILE" ]]; then
  echo "CDC SQL template not found: $TEMPLATE_FILE" >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing credentials file: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${CDC_INPUT_PATH:?CDC_INPUT_PATH is not set in $ENV_FILE}"

sql_literal() {
  local value="${1//$'\n'/ }"
  value="${value//\'/\'\'}"
  printf "'%s'" "$value"
}

TEMP_SQL_FILE="$(mktemp "${TMPDIR:-/tmp}/iceberg-flink-cdc.XXXXXX.sql")"
trap 'rm -f "$TEMP_SQL_FILE"' EXIT

# The placeholder keeps a machine-specific source path and credentials out of SQL tracked by Git.
sql_path="$(sql_literal "$CDC_INPUT_PATH")"
while IFS= read -r line || [[ -n "$line" ]]; do
  printf '%s\n' "${line//__CDC_INPUT_PATH__/$sql_path}"
done < "$TEMPLATE_FILE" > "$TEMP_SQL_FILE"

"$SCRIPT_DIR/flink-sql.sh" "$TEMP_SQL_FILE"
