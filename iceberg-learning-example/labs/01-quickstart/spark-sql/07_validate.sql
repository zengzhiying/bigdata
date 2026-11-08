-- Lightweight assertions for the final state. A lab runner can check the PASS value.
SELECT
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'CHECK' END AS row_count_check,
    COUNT(*) AS row_count
FROM ice.learning.orders;

SELECT
    CASE
        WHEN SUM(CASE WHEN order_id = 1004 THEN 1 ELSE 0 END) = 0
         AND SUM(CASE WHEN order_id = 1006 THEN 1 ELSE 0 END) = 1
        THEN 'PASS' ELSE 'CHECK'
    END AS merge_delete_check
FROM ice.learning.orders;
