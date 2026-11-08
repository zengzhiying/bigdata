#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$#" -eq 0 ]]; then
  echo "Usage: $0 SQL_FILE [SQL_FILE ...]" >&2
  exit 2
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
  "$PROJECT_ROOT/workdir/spark-sql.sh" -f "$sql_file"
done
