-- Run these defaults before the queries, including in an interactive SQL session.
-- Edit the values here to configure the report; a full rerun resets these settings.
SET lab09.small_file_bytes=1048576;
SET lab09.max_small_file_ratio=0.5;
SET lab09.max_snapshot_age_seconds=3600;

SELECT current_timestamp() AS observed_at, r.snapshot_id AS current_snapshot_id,
       s.committed_at AS last_snapshot_commit_at, s.operation,
       unix_timestamp(current_timestamp()) - unix_timestamp(s.committed_at) AS snapshot_age_seconds,
       CASE WHEN unix_timestamp(current_timestamp()) - unix_timestamp(s.committed_at)
                      > ${lab09.max_snapshot_age_seconds}
            THEN 'WARN' ELSE 'OK' END AS snapshot_freshness,
       s.summary
FROM ice.learning_production.orders.refs r
JOIN ice.learning_production.orders.snapshots s ON r.snapshot_id = s.snapshot_id
WHERE r.name = 'main';

SELECT COUNT(*) AS data_file_count,
       COALESCE(SUM(file_size_in_bytes), 0) AS data_bytes,
       MIN(file_size_in_bytes) AS min_data_file_bytes,
       AVG(file_size_in_bytes) AS avg_data_file_bytes,
       MAX(file_size_in_bytes) AS max_data_file_bytes,
       COALESCE(SUM(CASE WHEN file_size_in_bytes < ${lab09.small_file_bytes}
                         THEN 1 ELSE 0 END), 0) AS small_file_count,
       COALESCE(AVG(CASE WHEN file_size_in_bytes < ${lab09.small_file_bytes}
                        THEN 1.0 ELSE 0.0 END), 0) AS small_file_ratio,
       CASE WHEN COALESCE(AVG(CASE WHEN file_size_in_bytes < ${lab09.small_file_bytes}
                                  THEN 1.0 ELSE 0.0 END), 0) > ${lab09.max_small_file_ratio}
            THEN 'WARN' ELSE 'OK' END AS small_file_health
FROM ice.learning_production.orders.files WHERE content = 0;

SELECT COUNT(*) AS delete_file_count,
       COALESCE(SUM(file_size_in_bytes), 0) AS delete_bytes,
       COALESCE(SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END), 0) AS position_delete_files,
       COALESCE(SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END), 0) AS equality_delete_files
FROM ice.learning_production.orders.files WHERE content IN (1, 2);

SELECT COUNT(*) AS current_manifest_count FROM ice.learning_production.orders.manifests;
SELECT COUNT(*) AS retained_snapshot_count, MIN(committed_at) AS oldest_retained_commit
FROM ice.learning_production.orders.snapshots;
SELECT name, type, snapshot_id FROM ice.learning_production.orders.refs ORDER BY name;
