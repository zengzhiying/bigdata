-- Run with the Trino Iceberg catalog that maps to the same REST catalog and warehouse as Spark/Flink.
SELECT order_id, customer_id, fulfillment_region, order_status, order_total, event_version
FROM ice.learning_capstone.orders_current
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_total) = DECIMAL '95.75'
              AND COUNT_IF(fulfillment_region IN ('east', 'west')) = 2
              AND COUNT_IF(order_id = 1002 AND order_status = 'SHIPPED' AND event_version = 3) = 1
              AND COUNT_IF(order_id = 1003 AND order_status = 'CREATED' AND event_version = 1) = 1
         THEN 'PASS' ELSE 'CHECK' END AS trino_current_state_check,
    COUNT(*) AS row_count,
    SUM(order_total) AS total_amount
FROM ice.learning_capstone.orders_current;

SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_count) = 2
              AND SUM(total_amount) = DECIMAL '95.75'
         THEN 'PASS' ELSE 'CHECK' END AS trino_metrics_check,
    COUNT(*) AS metric_row_count,
    SUM(total_amount) AS metric_total_amount
FROM ice.learning_capstone.orders_daily_metrics;

SELECT operation, COUNT(*) AS snapshot_count
FROM "ice.learning_capstone.orders_current$snapshots"
GROUP BY operation
ORDER BY operation;
