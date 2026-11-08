-- Run after 03_write_and_evolve.sql in Trino 476.
SELECT order_id, customer_id, fulfillment_status, order_total, event_time, source
FROM ice.learning_trino.orders_from_trino
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 3
              AND SUM(order_total) = DECIMAL '160.25'
              AND COUNT_IF(source = 'trino') = 3
              AND COUNT_IF(fulfillment_status = 'SHIPPED') = 2
         THEN 'PASS' ELSE 'FAIL' END AS trino_write_and_evolution_check,
    COUNT(*) AS actual_row_count,
    SUM(order_total) AS actual_total_amount
FROM ice.learning_trino.orders_from_trino;

SELECT partition, record_count, file_count
FROM ice.learning_trino."orders_from_trino$partitions"
ORDER BY partition;

-- The actual content values reveal whether this environment materialized data or delete files.
SELECT content, COUNT(*) AS file_count, SUM(record_count) AS records
FROM ice.learning_trino."orders_from_trino$files"
GROUP BY content
ORDER BY content;

SELECT snapshot_id, committed_at, operation
FROM ice.learning_trino."orders_from_trino$snapshots"
ORDER BY committed_at;
