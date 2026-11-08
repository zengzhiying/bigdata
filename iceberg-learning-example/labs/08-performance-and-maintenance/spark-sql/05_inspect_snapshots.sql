-- Save one non-current snapshot ID from each table before using the expiration template.
SELECT snapshot_id, parent_id, committed_at, operation, summary
FROM ice.learning_maintenance.events_binpack.snapshots
ORDER BY committed_at;

SELECT snapshot_id, parent_id, committed_at, operation, summary
FROM ice.learning_maintenance.events_sort.snapshots
ORDER BY committed_at;

SELECT timestamp, file, latest_snapshot_id
FROM ice.learning_maintenance.events_binpack.metadata_log_entries
ORDER BY timestamp;

