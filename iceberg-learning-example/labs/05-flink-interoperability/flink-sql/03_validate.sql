-- Run after 00_catalog.sql in the same Flink SQL Client session.
SELECT
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS row_count_check,
    COUNT(*) AS actual_row_count,
    CAST(SUM(order_total) AS DECIMAL(12, 2)) AS total_amount
FROM learning_flink.orders_from_flink;

SELECT snapshot_id, committed_at, operation
FROM learning_flink.`orders_from_flink$snapshots`
ORDER BY committed_at;
