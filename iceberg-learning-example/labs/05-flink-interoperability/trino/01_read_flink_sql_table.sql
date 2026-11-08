-- Run after flink-sql/02_write_flink_table.sql in Trino 476 with catalog `ice`.
SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning_flink.orders_from_flink
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 3 AND SUM(order_total) = DECIMAL '145.75'
         THEN 'PASS' ELSE 'FAIL' END AS trino_reads_flink_sql_check,
    COUNT(*) AS actual_row_count,
    SUM(order_total) AS actual_total_amount
FROM ice.learning_flink.orders_from_flink;

SELECT snapshot_id, committed_at, operation
FROM ice.learning_flink."orders_from_flink$snapshots"
ORDER BY committed_at;
