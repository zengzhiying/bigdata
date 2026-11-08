-- 本文件需要支持 Branch、Tag 和 WAP 的 Catalog（例如 Nessie）。
-- 将 <BASE_SNAPSHOT_ID> 替换为一个已存在的 Snapshot ID。

ALTER TABLE ice.learning_governance.orders
CREATE BRANCH audit AS OF VERSION <BASE_SNAPSHOT_ID>;

ALTER TABLE ice.learning_governance.orders
CREATE TAG release_2026_03 AS OF VERSION <BASE_SNAPSHOT_ID>;

-- 查看 Branch 和 Tag
SELECT
    name,
    type,
    snapshot_id,
    max_reference_age_in_ms,
    min_snapshots_to_keep,
    max_snapshot_age_in_ms
FROM ice.learning_governance.orders.refs
ORDER BY type, name;

-- 查看 Branch
SELECT name, snapshot_id
FROM ice.learning_governance.orders.refs
WHERE type = 'BRANCH'
ORDER BY name;

-- 查看 Tag
SELECT name, snapshot_id
FROM ice.learning_governance.orders.refs
WHERE type = 'TAG'
ORDER BY name;

-- 同时查看对应的快照和操作类型可以联表 snapshots
SELECT
    refs.name,
    refs.type,
    refs.snapshot_id,
    snapshots.committed_at,
    snapshots.operation
FROM ice.learning_governance.orders.refs AS refs
LEFT JOIN ice.learning_governance.orders.snapshots AS snapshots
    ON refs.snapshot_id = snapshots.snapshot_id
ORDER BY refs.type, refs.name;


-- 使用引用读取历史状态。不同 Spark/Iceberg Runtime 对引用读取语法的支持可能不同。
SELECT order_id, order_status
FROM ice.learning_governance.orders VERSION AS OF 'audit'
ORDER BY order_id;

SELECT order_id, order_status
FROM ice.learning_governance.orders VERSION AS OF 'release_2026_03'
ORDER BY order_id;

-- WAP 使用独立的 ID 暂存一次提交；默认表状态在发布前不应看到这条记录。
-- 写入前需要启用表级 WAP。WAP ID 与 spark.wap.branch 不能同时设置。
ALTER TABLE ice.learning_governance.orders
SET TBLPROPERTIES ('write.wap.enabled' = 'true');

-- Spark SQL 的 SET 会保留值中的单引号；这里必须使用未加引号的 WAP ID。
SET spark.wap.id = audit-202603;

INSERT INTO ice.learning_governance.orders VALUES
    (2005, 605, 'WAP_PENDING', CAST(50.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-03-03 09:00:00');

SELECT order_id, order_status
FROM ice.learning_governance.orders
WHERE order_id = 2005;

-- 查询确认 WAP ID 是存在的
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['wap.id'] AS wap_id,
    summary
FROM ice.learning_governance.orders.snapshots
ORDER BY committed_at;

-- 发布 WAP Snapshot；publish_changes 的第二个参数是 WAP ID，不是 Branch 名称。
CALL ice.system.publish_changes(
    table => 'learning_governance.orders',
    wap_id => 'audit-202603'
);

SELECT order_id, order_status
FROM ice.learning_governance.orders
WHERE order_id = 2005;

RESET spark.wap.id;

-- 实验结束前删除引用。若 Catalog 不支持该语法，应按其官方 API 清理。
ALTER TABLE ice.learning_governance.orders DROP BRANCH audit;
ALTER TABLE ice.learning_governance.orders DROP TAG release_2026_03;
