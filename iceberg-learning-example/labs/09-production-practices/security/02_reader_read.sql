-- Run with the reader identity. Both data and Iceberg metadata reads must work.
SELECT order_id, customer_id, order_status, order_total
FROM ice.learning_security.orders_access_fixture;

SELECT snapshot_id, operation, committed_at
FROM ice.learning_security.orders_access_fixture.snapshots;

SELECT COUNT(*) AS current_data_file_count
FROM ice.learning_security.orders_access_fixture.files
WHERE content = 0;
