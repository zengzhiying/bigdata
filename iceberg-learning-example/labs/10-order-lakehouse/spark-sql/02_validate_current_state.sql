SELECT
    CASE WHEN COUNT(*) = 8 AND COUNT(DISTINCT event_id) = 7
         THEN 'PASS' ELSE 'CHECK' END AS raw_event_check,
    COUNT(*) AS raw_event_count,
    COUNT(DISTINCT event_id) AS distinct_event_count
FROM ice.learning_capstone.orders_cdc_raw;

SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_total) = CAST(95.75 AS DECIMAL(12, 2))
              AND SUM(CASE WHEN order_id = 1001 THEN 1 ELSE 0 END) = 0
              AND SUM(CASE WHEN order_id = 1002
                                 AND order_status = 'SHIPPED'
                                 AND event_version = 3 THEN 1 ELSE 0 END) = 1
              AND SUM(CASE WHEN order_id = 1003
                                 AND order_status = 'CREATED'
                                 AND event_version = 1 THEN 1 ELSE 0 END) = 1
         THEN 'PASS' ELSE 'CHECK' END AS current_state_check,
    COUNT(*) AS current_row_count,
    SUM(order_total) AS current_total
FROM ice.learning_capstone.orders_current;

SELECT snapshot_id, parent_id, operation, committed_at
FROM ice.learning_capstone.orders_cdc_raw.snapshots
ORDER BY committed_at;
