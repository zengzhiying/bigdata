-- 将 <BASE_SNAPSHOT_ID> 替换为 02_snapshot_history.sql 中的基础 Snapshot ID。
-- 将 <LATEST_SNAPSHOT_ID> 替换为回滚前的最新 Snapshot ID。
SELECT order_id, order_status, order_total
FROM ice.learning_governance.orders VERSION AS OF <BASE_SNAPSHOT_ID>
ORDER BY order_id;

SELECT order_id, order_status, order_total
FROM ice.learning_governance.orders VERSION AS OF <LATEST_SNAPSHOT_ID>
ORDER BY order_id;

-- 回滚只改变当前表引用，不删除目标 Snapshot。
CALL ice.system.rollback_to_snapshot(
    'learning_governance.orders',
    <BASE_SNAPSHOT_ID>
);

SELECT order_id, order_status, order_total
FROM ice.learning_governance.orders
ORDER BY order_id;

-- 这个 SQL 查询的始终是当前存在的实际快照情况
SELECT snapshot_id, committed_at, operation
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;

-- 然后查询 is_current_ancestor 可以区分“仍然存在的快照”和“当前有效链路上的快照”
-- 比如会滚到之前的快照之后，中间的比较新的快照状态会变为 false，然后会新增一条旧快照的记录
-- 这个 SQL 和上面不同，这个查询的是操作的快照操作记录，包括切换快照的操作
SELECT made_current_at, snapshot_id, is_current_ancestor
FROM ice.learning_governance.orders.history
ORDER BY made_current_at;

-- 如果想切换到当前最新的快照或者比较新的快照，不能使用 rollback_to_snapshot，需要使用 set_current_snapshot
-- rollback_to_snapshot 适合回到当前 Snapshot 的祖先快照，而 set_current_snapshot 可以把当前表引用直接切换到一个仍被表元数据保留的指定 Snapshot
CALL ice.system.set_current_snapshot(
    'learning_governance.orders',
    <LATEST_SNAPSHOT_ID>
);
