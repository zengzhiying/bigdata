SELECT table_name,
       CASE WHEN record_count = 120 AND event_id_sum = 127260 THEN 'PASS' ELSE 'CHECK' END AS logical_data_check,
       record_count,
       event_id_sum
FROM (
    SELECT 'events_binpack' AS table_name, COUNT(*) AS record_count, SUM(event_id) AS event_id_sum
    FROM ice.learning_maintenance.events_binpack
    UNION ALL
    SELECT 'events_sort' AS table_name, COUNT(*) AS record_count, SUM(event_id) AS event_id_sum
    FROM ice.learning_maintenance.events_sort
)
ORDER BY table_name;

-- The fixture begins with twelve independent writes per table. A rewrite should
-- consolidate that set, but do not treat the resulting exact file count as portable.
SELECT 'events_binpack' AS table_name,
       CASE WHEN COUNT(*) < 12 THEN 'PASS' ELSE 'CHECK' END AS compaction_check,
       COUNT(*) AS current_data_file_count,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes
FROM ice.learning_maintenance.events_binpack.files
WHERE content = 0
UNION ALL
SELECT 'events_sort' AS table_name,
       CASE WHEN COUNT(*) < 12 THEN 'PASS' ELSE 'CHECK' END AS compaction_check,
       COUNT(*) AS current_data_file_count,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes
FROM ice.learning_maintenance.events_sort.files
WHERE content = 0
ORDER BY table_name;

SELECT COUNT(*) AS current_manifest_count
FROM ice.learning_maintenance.events_binpack.manifests;

