SELECT
    CASE WHEN COUNT(*) = 6 THEN 'PASS' ELSE 'CHECK' END AS row_count_check,
    COUNT(*) AS row_count
FROM ice.learning_delete_files.events;

SELECT
    CASE WHEN SUM(CASE WHEN event_id = 3002 THEN 1 ELSE 0 END) = 0
              AND SUM(CASE WHEN event_id = 3004 AND event_value = 'account' THEN 1 ELSE 0 END) = 1
              AND SUM(CASE WHEN event_id = 3005 AND event_value = 'results' THEN 1 ELSE 0 END) = 1
              AND SUM(CASE WHEN event_id = 3007 THEN 1 ELSE 0 END) = 1
         THEN 'PASS' ELSE 'CHECK'
    END AS row_level_change_check
FROM ice.learning_delete_files.events;

SELECT
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'CHECK' END AS delete_file_check,
    COUNT(*) AS delete_file_count
FROM ice.learning_delete_files.events.delete_files;
