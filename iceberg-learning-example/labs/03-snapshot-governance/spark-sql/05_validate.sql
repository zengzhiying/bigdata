SELECT
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'CHECK' END AS current_row_count_check,
    COUNT(*) AS current_row_count
FROM ice.learning_governance.orders;

SELECT
    CASE WHEN SUM(CASE WHEN order_id = 2001 AND order_status = 'CREATED' THEN 1 ELSE 0 END) = 1
              AND SUM(CASE WHEN order_id = 2004 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'CHECK'
    END AS rollback_state_check
FROM ice.learning_governance.orders;

SELECT snapshot_id, committed_at, operation
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;
