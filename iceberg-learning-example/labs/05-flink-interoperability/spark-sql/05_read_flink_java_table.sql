-- Run with scripts/run-spark-sql.sh after examples/flink-java has completed.
SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning_flink_java.orders_from_flink_java
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 3 AND SUM(order_total) = CAST(140.75 AS DECIMAL(12, 2))
         THEN 'PASS' ELSE 'FAIL' END AS spark_reads_flink_java_check,
    COUNT(*) AS actual_row_count,
    SUM(order_total) AS actual_total_amount
FROM ice.learning_flink_java.orders_from_flink_java;

SELECT snapshot_id, committed_at, operation
FROM ice.learning_flink_java.orders_from_flink_java.snapshots
ORDER BY committed_at;
