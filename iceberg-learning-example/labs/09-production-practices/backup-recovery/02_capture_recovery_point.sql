-- Stop writers and maintenance before running this capture. DESCRIBE TABLE
-- EXTENDED prints the table Location required by the storage backup procedure.
DESCRIBE TABLE EXTENDED ice.learning_recovery.orders_backup_fixture;

SELECT r.name, r.type, r.snapshot_id, s.committed_at, s.operation,
       s.manifest_list, s.summary
FROM ice.learning_recovery.orders_backup_fixture.refs r
JOIN ice.learning_recovery.orders_backup_fixture.snapshots s ON r.snapshot_id = s.snapshot_id
ORDER BY r.name;

SELECT timestamp, file
FROM ice.learning_recovery.orders_backup_fixture.metadata_log_entries
ORDER BY timestamp;

SELECT content, COUNT(*) AS file_count, SUM(record_count) AS record_count,
       SUM(file_size_in_bytes) AS total_bytes
FROM ice.learning_recovery.orders_backup_fixture.files
GROUP BY content ORDER BY content;

SELECT COUNT(*) AS row_count, SUM(order_total) AS total_amount,
       COUNT(DISTINCT order_id) AS distinct_order_count
FROM ice.learning_recovery.orders_backup_fixture;

SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning_recovery.orders_backup_fixture ORDER BY order_id;
