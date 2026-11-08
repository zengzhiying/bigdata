-- Preserve the logical result in this session so a rewrite is verified, not assumed.
CREATE OR REPLACE TEMP VIEW capstone_before_maintenance AS
SELECT order_id, customer_id, order_status, order_total, event_version,
       event_time, event_day, source_event_id, fulfillment_region
FROM ice.learning_capstone.orders_current;

SELECT COUNT(*) AS current_file_count,
       SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_file_count,
       SUM(CASE WHEN content IN (1, 2) THEN 1 ELSE 0 END) AS delete_file_count
FROM ice.learning_capstone.orders_current.files;

-- rewrite-all makes this small deterministic fixture exercise a replace commit.
CALL ice.system.rewrite_data_files(
    table => 'learning_capstone.orders_current',
    strategy => 'binpack',
    options => map(
        'rewrite-all', 'true',
        'target-file-size-bytes', '134217728'
    )
);

SELECT assert_true(
    NOT EXISTS (
        (SELECT * FROM capstone_before_maintenance
         EXCEPT ALL
         SELECT order_id, customer_id, order_status, order_total, event_version,
                event_time, event_day, source_event_id, fulfillment_region
         FROM ice.learning_capstone.orders_current)
        UNION ALL
        (SELECT order_id, customer_id, order_status, order_total, event_version,
                event_time, event_day, source_event_id, fulfillment_region
         FROM ice.learning_capstone.orders_current
         EXCEPT ALL
         SELECT * FROM capstone_before_maintenance)
    ),
    'Data-file rewrite changed the logical current-state result'
);

SELECT 'PASS' AS maintenance_logical_data_check;
SELECT snapshot_id, parent_id, operation, committed_at,
       summary['added-data-files'] AS added_data_files,
       summary['deleted-data-files'] AS deleted_data_files
FROM ice.learning_capstone.orders_current.snapshots
ORDER BY committed_at;
