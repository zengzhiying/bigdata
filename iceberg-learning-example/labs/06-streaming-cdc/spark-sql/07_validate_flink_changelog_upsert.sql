SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_total) = CAST(115.75 AS DECIMAL(12, 2))
              AND SUM(CASE WHEN order_id = 2001
                                 AND customer_id = 701
                                 AND order_status = 'PAID'
                                 AND order_total = CAST(42.50 AS DECIMAL(12, 2))
                                 AND event_version = 2 THEN 1 ELSE 0 END) = 1
              AND SUM(CASE WHEN order_id = 2002 THEN 1 ELSE 0 END) = 0
              AND SUM(CASE WHEN order_id = 2003
                                 AND customer_id = 703
                                 AND order_status = 'SHIPPED'
                                 AND order_total = CAST(73.25 AS DECIMAL(12, 2))
                                 AND event_version = 1 THEN 1 ELSE 0 END) = 1
         THEN 'PASS' ELSE 'CHECK' END AS flink_changelog_upsert_check,
    COUNT(*) AS current_row_count,
    SUM(order_total) AS current_total
FROM ice.learning_cdc.orders_changelog_upsert;

SELECT snapshot_id, parent_id, operation, committed_at, summary
FROM ice.learning_cdc.orders_changelog_upsert.snapshots
ORDER BY committed_at;
