-- Run after 00_catalog.sql in the same Flink SQL Client session.
SELECT order_id, customer_id, order_status, order_total, event_time
FROM learning.orders
ORDER BY order_id;

SELECT COUNT(*) AS spark_order_count
FROM learning.orders;

SELECT snapshot_id, committed_at, operation
FROM learning.`orders$snapshots`
ORDER BY committed_at;
