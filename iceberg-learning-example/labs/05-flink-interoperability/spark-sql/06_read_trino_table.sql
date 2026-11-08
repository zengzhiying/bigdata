-- Run with scripts/run-spark-sql.sh after trino/03_write_and_evolve.sql.
SELECT order_id, customer_id, fulfillment_status, order_total, event_time, source
FROM ice.learning_trino.orders_from_trino
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 3
              AND SUM(order_total) = CAST(160.25 AS DECIMAL(12, 2))
              AND COUNT(CASE WHEN source = 'trino' THEN 1 END) = 3
              AND COUNT(CASE WHEN fulfillment_status = 'SHIPPED' THEN 1 END) = 2
         THEN 'PASS' ELSE 'FAIL' END AS spark_reads_trino_check,
    COUNT(*) AS actual_row_count,
    SUM(order_total) AS actual_total_amount
FROM ice.learning_trino.orders_from_trino;

SELECT content, COUNT(*) AS file_count, SUM(record_count) AS records
FROM ice.learning_trino.orders_from_trino.files
GROUP BY content
ORDER BY content;
