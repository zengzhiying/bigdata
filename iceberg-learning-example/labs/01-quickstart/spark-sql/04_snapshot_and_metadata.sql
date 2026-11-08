-- Iceberg 元数据表用于观察表状态、快照和数据文件。

SELECT snapshot_id, parent_id, committed_at, operation, summary
FROM ice.learning.orders.snapshots
ORDER BY committed_at;

SELECT made_current_at, snapshot_id, is_current_ancestor
FROM ice.learning.orders.history
ORDER BY made_current_at;

SELECT content, file_path, file_size_in_bytes, record_count
FROM ice.learning.orders.files
ORDER BY file_path;

-- 记录当前表的快照 ID，下一步 Time Travel 实验会用到其中一个历史 ID。
SELECT snapshot_id, committed_at
FROM ice.learning.orders.snapshots
ORDER BY committed_at;
