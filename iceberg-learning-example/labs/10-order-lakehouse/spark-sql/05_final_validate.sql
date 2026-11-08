SELECT assert_true(
    COUNT(*) = 2
    AND SUM(order_total) = CAST(95.75 AS DECIMAL(12, 2))
    AND SUM(CASE WHEN fulfillment_region IN ('east', 'west') THEN 1 ELSE 0 END) = 2,
    'Final current-state contract failed'
) AS final_current_state_assertion
FROM ice.learning_capstone.orders_current;

SELECT assert_true(
    COUNT(*) = 2
    AND SUM(order_count) = 2
    AND SUM(total_amount) = CAST(95.75 AS DECIMAL(14, 2)),
    'Daily metrics contract failed'
) AS final_metrics_assertion
FROM ice.learning_capstone.orders_daily_metrics;

SELECT assert_true(
    COUNT(*) >= 1,
    'Current-state table has no retained snapshots'
) AS snapshot_retention_assertion
FROM ice.learning_capstone.orders_current.snapshots;

SELECT 'PASS' AS capstone_final_validation;
