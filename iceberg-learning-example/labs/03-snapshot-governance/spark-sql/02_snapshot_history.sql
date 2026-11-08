SELECT snapshot_id, parent_id, committed_at, operation, summary
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;

SELECT made_current_at, snapshot_id, is_current_ancestor
FROM ice.learning_governance.orders.history
ORDER BY made_current_at;

-- 记录最早和最新的 Snapshot ID，供后续脚本替换占位符。
SELECT snapshot_id, committed_at, operation
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;
