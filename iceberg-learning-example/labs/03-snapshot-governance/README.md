# 实验 03 - Snapshot 治理、Branch、Tag 和 WAP

## 实验目标

在一张 Iceberg Format Version 2 表上观察 Snapshot 生命周期，并练习历史读取、回滚、过期清理、Branch、Tag 和 Write-Audit-Publish（WAP）。

本实验分为两部分：

- `spark-sql/01` 至 `05` 使用普通 Iceberg Catalog 即可运行。
- `spark-sql/06_branch_tag_wap.sql` 中的 Branch/Tag 需要 Catalog 支持引用管理。推荐使用 Nessie 或其他明确支持 Branch/Tag 的实现；普通 REST Catalog 不一定支持这些语句。WAP ID 部分可以单独在支持 Spark WAP procedure 的 Catalog 上执行。

## 前置条件

- 外部 Spark `3.5.9`，加载 Iceberg `1.11.0` Runtime 和 Iceberg Spark Extensions。
- Catalog 名称默认为 `ice`；如果使用其他名称，需要替换 SQL 中的 `ice`。
- Warehouse、Spark Session 和 Catalog 时钟已同步。
- 已确认实验专用表 `ice.learning_governance.orders` 不会与其他实验或用户共享。
- Branch、Tag 部分额外需要支持引用管理的 Catalog；WAP ID 部分需要对应 Runtime 暴露 `publish_changes` procedure。

## 文件顺序

```text
spark-sql/00_reset.sql
spark-sql/01_create_and_insert.sql
spark-sql/02_snapshot_history.sql
spark-sql/03_time_travel_and_rollback.sql
spark-sql/04_expire_snapshots.sql
spark-sql/05_validate.sql
spark-sql/06_branch_tag_wap.sql   # 可选；Branch/Tag 需要支持引用管理的 Catalog
spark-sql/07_cleanup.sql
```

推荐使用项目脚本执行基础部分：

```bash
./scripts/run-spark-sql.sh \
  labs/03-snapshot-governance/spark-sql/00_reset.sql \
  labs/03-snapshot-governance/spark-sql/01_create_and_insert.sql \
  labs/03-snapshot-governance/spark-sql/02_snapshot_history.sql
```

`03_time_travel_and_rollback.sql` 和 `06_branch_tag_wap.sql` 中的占位符必须替换为前一步查询得到的真实 Snapshot ID。不要把 `<BASE_SNAPSHOT_ID>` 等占位符直接提交给 Spark。

WAP ID 的 `SET` 语法使用未加引号的值，例如 `SET spark.wap.id = audit-202603`；Spark SQL 会把 `SET spark.wap.id = 'audit-202603'` 中的单引号保留到 Snapshot 的 `wap.id` 值中。

## 实验步骤

1. 创建 Format Version 2 表并产生多个 Snapshot。
2. 查询 `snapshots` 和 `history` 元数据表，记录基础 Snapshot 和最新 Snapshot。
3. 使用 `VERSION AS OF` 读取历史数据。
4. 回滚到指定 Snapshot，确认当前表状态发生变化但历史 Snapshot 仍可查询。
5. 使用 `expire_snapshots` 只清理明确超出保留窗口的 Snapshot。
6. 在支持引用管理的 Catalog 中创建 Branch 和 Tag。
7. 使用 WAP ID 暂存一次写入，检查默认表状态在发布前不可见，发布后再检查默认状态可见。

## 关键观察点

- Rollback 只移动表的当前引用，不会把历史 Metadata JSON 改写成另一个 Snapshot。
- `expire_snapshots` 会删除不再需要的历史引用和未被保留 Snapshot 引用的元数据；过期前应先确认审计和恢复窗口。
- Branch 和 Tag 是 Catalog 的引用能力，不是 Warehouse 目录下的普通文件夹。
- Tag 通常用于固定一个不可变的发布点；Branch 允许继续提交新 Snapshot。
- WAP 写入成功并不等于默认分支立即可见，发布是独立的表级操作。

## 预期结果

- `02_snapshot_history.sql` 至少返回三次提交，并能区分 `operation` 和父子 Snapshot 关系。
- Time Travel 查询能读到回滚前的历史数据。
- 回滚后当前查询结果恢复到目标 Snapshot，`05_validate.sql` 输出 `PASS`。
- 过期操作不会删除保留窗口内的 Snapshot；实际删除数量以引擎输出为准。
- Branch/Tag/WAP 部分在支持引用管理的 Catalog 中可以创建引用、隔离写入并发布；在不支持的 Catalog 中应记录明确的“不支持”错误，而不是修改 SQL 规避。

## 失败路径和安全边界

- 在 `06_branch_tag_wap.sql` 中使用不存在的 Snapshot ID，预期提交失败且默认分支不变。
- 在共享 Warehouse 上执行 `00_reset.sql` 或 `07_cleanup.sql` 前，必须再次确认表名属于本实验。
- 不要为了让 Branch/Tag 语句通过而手工编辑 Metadata JSON，也不要在没有确认保留策略时使用极早的 `older_than` 时间。

## 清理

完成所有观察后执行 `spark-sql/07_cleanup.sql`。如果创建了 Branch 或 Tag，先在支持引用管理的 Catalog 中删除实验引用，再删除表。
