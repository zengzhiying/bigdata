SELECT order_id, customer_id, order_status, order_total, event_version, source_event_id
FROM ice.learning_cdc.orders_current
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_total) = DECIMAL '95.75'
         THEN 'PASS' ELSE 'CHECK' END AS current_state_check,
    COUNT(*) AS current_row_count,
    SUM(order_total) AS current_total
FROM ice.learning_cdc.orders_current;

SELECT operation, COUNT(*) AS snapshot_count
FROM "ice.learning_cdc.orders_cdc_raw$snapshots"
GROUP BY operation
ORDER BY operation;
