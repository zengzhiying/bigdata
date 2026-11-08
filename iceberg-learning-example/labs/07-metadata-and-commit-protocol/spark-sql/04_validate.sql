SELECT
    CASE WHEN COUNT(*) = 3
              AND SUM(CASE WHEN order_id = 7102
                                 AND order_status = 'PAID'
                                 AND order_total = CAST(26.00 AS DECIMAL(12, 2))
                           THEN 1 ELSE 0 END) = 1
         THEN 'PASS' ELSE 'CHECK' END AS current_state_check,
    COUNT(*) AS row_count
FROM ice.learning_metadata.orders;

SELECT
    CASE WHEN COUNT(*) >= 3 THEN 'PASS' ELSE 'CHECK' END AS snapshot_lineage_check,
    COUNT(*) AS snapshot_count
FROM ice.learning_metadata.orders.snapshots;

SELECT
    CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'CHECK' END AS manifest_check,
    COUNT(*) AS manifest_count
FROM ice.learning_metadata.orders.manifests;

