-- 先查询 Snapshot，再根据审计和恢复要求选择保留窗口。
SELECT snapshot_id, committed_at, operation
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;

-- 将时间替换为明确早于保留边界的值；retain_last 用于保留最近的 Snapshot。
-- 每次执行仅会触发一次清理
-- older_than 表示 Snapshot 的提交时间阈值，对应判断条件是 snapshots.committed_at
CALL ice.system.expire_snapshots(
    table => 'learning_governance.orders',
    older_than => TIMESTAMP '2026-08-14 13:32:30',
    retain_last => 2
);

SELECT snapshot_id, committed_at, operation
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;
