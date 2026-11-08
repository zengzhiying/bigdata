-- 将 <SNAPSHOT_ID> 替换为 04_snapshot_and_metadata.sql 返回的历史快照 ID。
SELECT order_id, order_status, order_total
FROM ice.learning.orders VERSION AS OF <SNAPSHOT_ID>
ORDER BY order_id;

-- 也可以按提交时间读取历史状态。将 <COMMIT_TIME> 替换为 committed_at 的实际值。
SELECT order_id, order_status, order_total
FROM ice.learning.orders TIMESTAMP AS OF '<COMMIT_TIME>'
ORDER BY order_id;
