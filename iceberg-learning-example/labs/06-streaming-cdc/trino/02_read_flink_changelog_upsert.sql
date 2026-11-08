SELECT order_id, customer_id, order_status, order_total, event_version, event_time
FROM ice.learning_cdc.orders_changelog_upsert
ORDER BY order_id;

SELECT
    CASE WHEN COUNT(*) = 2
              AND SUM(order_total) = DECIMAL '115.75'
         THEN 'PASS' ELSE 'CHECK' END AS flink_changelog_upsert_check,
    COUNT(*) AS current_row_count,
    SUM(order_total) AS current_total
FROM ice.learning_cdc.orders_changelog_upsert;

SELECT operation, COUNT(*) AS snapshot_count
FROM "ice.learning_cdc.orders_changelog_upsert$snapshots"
GROUP BY operation
ORDER BY operation;
