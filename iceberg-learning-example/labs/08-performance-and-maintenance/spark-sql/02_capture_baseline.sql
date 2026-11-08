-- Record these values before maintenance. Exact file and manifest counts vary with
-- the Spark runtime, but each table should contain twelve independent append commits.
SELECT 'events_binpack' AS table_name,
       COUNT(*) AS data_file_count,
       SUM(record_count) AS record_count,
       MIN(file_size_in_bytes) AS min_file_size_bytes,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes,
       MAX(file_size_in_bytes) AS max_file_size_bytes
FROM ice.learning_maintenance.events_binpack.files
WHERE content = 0
UNION ALL
SELECT 'events_sort' AS table_name,
       COUNT(*) AS data_file_count,
       SUM(record_count) AS record_count,
       MIN(file_size_in_bytes) AS min_file_size_bytes,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes,
       MAX(file_size_in_bytes) AS max_file_size_bytes
FROM ice.learning_maintenance.events_sort.files
WHERE content = 0
ORDER BY table_name;

SELECT 'events_binpack' AS table_name, COUNT(*) AS manifest_count
FROM ice.learning_maintenance.events_binpack.manifests
UNION ALL
SELECT 'events_sort' AS table_name, COUNT(*) AS manifest_count
FROM ice.learning_maintenance.events_sort.manifests
ORDER BY table_name;

SELECT snapshot_id, committed_at, operation, summary
FROM ice.learning_maintenance.events_binpack.snapshots
ORDER BY committed_at;

SELECT snapshot_id, committed_at, operation, summary
FROM ice.learning_maintenance.events_sort.snapshots
ORDER BY committed_at;

