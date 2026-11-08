SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning_recovery_restore.orders_backup_fixture ORDER BY order_id;

SELECT assert_true(
    COUNT(*) = 3
    AND COUNT(DISTINCT order_id) = 3
    AND SUM(order_total) = CAST(99.75 AS DECIMAL(12, 2)),
    'Restored table data does not match the captured fixture'
) FROM ice.learning_recovery_restore.orders_backup_fixture;

SELECT assert_true(COUNT(*) > 0, 'Restored table has no current data files')
FROM ice.learning_recovery_restore.orders_backup_fixture.files
WHERE content = 0;

SELECT 'PASS' AS restored_table_check;
