-- Run with scripts/run-spark-sql.sh after Flink has completed 02_write_flink_table.sql.
SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning_flink.orders_from_flink
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 3 AND SUM(order_total) = CAST(145.75 AS DECIMAL(12, 2))
         THEN 'PASS' ELSE 'FAIL' END AS spark_reads_flink_check,
    COUNT(*) AS actual_row_count,
    SUM(order_total) AS actual_total_amount
FROM ice.learning_flink.orders_from_flink;

SELECT snapshot_id, committed_at, operation
FROM ice.learning_flink.orders_from_flink.snapshots
ORDER BY committed_at;
