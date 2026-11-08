#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
lab_root="$project_root/labs/10-order-lakehouse"

for required_file in \
  README.md \
  validation-record.md.template \
  flink-sql/01_stream_append.sql.template \
  spark-sql/00_reset_and_create.sql \
  spark-sql/01_materialize_current_state.sql \
  spark-sql/02_validate_current_state.sql \
  spark-sql/03_quality_and_evolve.sql \
  spark-sql/04_maintenance.sql \
  spark-sql/05_final_validate.sql \
  spark-sql/06_cleanup.sql \
  trino/01_validate_cross_engine.sql; do
  test -f "$lab_root/$required_file"
done

rg -q 'ice\.learning_capstone\.orders_cdc_raw' "$lab_root/flink-sql/01_stream_append.sql.template"
rg -q 'MERGE INTO ice\.learning_capstone\.orders_current' "$lab_root/spark-sql/01_materialize_current_state.sql"
rg -q 'ADD COLUMN fulfillment_region' "$lab_root/spark-sql/03_quality_and_evolve.sql"
rg -q 'rewrite-all' "$lab_root/spark-sql/04_maintenance.sql"
rg -q -- '--template SQL_TEMPLATE' "$project_root/workdir/flink-cdc-sql.sh"

echo 'Lab 10 asset validation passed.'
