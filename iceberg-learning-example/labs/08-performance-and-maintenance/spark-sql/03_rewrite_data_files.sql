-- Bin-pack only groups small files. It does not establish a new record ordering.
CALL ice.system.rewrite_data_files(
    table => 'learning_maintenance.events_binpack',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '32768',
        'min-input-files', '2'
    )
);

-- Use an explicit sort order so the rewrite result can be compared with bin-pack.
CALL ice.system.rewrite_data_files(
    table => 'learning_maintenance.events_sort',
    strategy => 'sort',
    sort_order => 'customer_id ASC NULLS LAST, event_id ASC NULLS LAST',
    options => map(
        'target-file-size-bytes', '32768',
        'min-input-files', '2'
    )
);

SELECT 'events_binpack' AS table_name, COUNT(*) AS data_file_count,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes
FROM ice.learning_maintenance.events_binpack.files
WHERE content = 0
UNION ALL
SELECT 'events_sort' AS table_name, COUNT(*) AS data_file_count,
       ROUND(AVG(file_size_in_bytes), 0) AS avg_file_size_bytes
FROM ice.learning_maintenance.events_sort.files
WHERE content = 0
ORDER BY table_name;

-- Check file-level bounds after the sort rewrite. These bounds are the evidence
-- Iceberg uses during scan planning to exclude files that cannot match a predicate.
SELECT file_path,
       record_count,
       file_size_in_bytes,
       readable_metrics.customer_id.lower_bound AS customer_id_min,
       readable_metrics.customer_id.upper_bound AS customer_id_max,
       readable_metrics.event_id.lower_bound AS event_id_min,
       readable_metrics.event_id.upper_bound AS event_id_max
FROM ice.learning_maintenance.events_sort.files
WHERE content = 0
ORDER BY customer_id_min, customer_id_max, event_id_min, event_id_max, file_path;

-- This plan confirms predicate pushdown to Iceberg, but does not print the final
-- candidate file count. Use the bounds above and Spark SQL metrics together.
EXPLAIN FORMATTED
SELECT event_id, customer_id
FROM ice.learning_maintenance.events_sort
WHERE customer_id = 3;
