# 实验 04 - Delete File 与 Merge-on-Read

## 实验目标

在 Iceberg Format Version 2 表上观察行级删除、更新和 `MERGE` 产生的 Delete File，并使用元数据表确认当前 Snapshot 引用的文件类型。

本实验以 Spark SQL 为主。Spark 的具体写入策略、Iceberg Runtime 版本和表属性会影响最终生成 Position Delete 还是 Equality Delete，因此 SQL 中的结果检查使用元数据观察，不把某一种文件类型硬编码为跨引擎保证。

## 前置条件

- 外部 Spark `3.5.9`，加载 Iceberg `1.11.0` Runtime 和 Iceberg Spark Extensions。
- Catalog 名称默认为 `ice`；如果使用其他名称，需要替换 SQL 中的 `ice`。
- 使用支持 Format Version 2 的 Catalog 和 Warehouse。
- 确认实验专用表 `ice.learning_delete_files.events` 不会与其他任务并发写入。

## 文件顺序

```text
spark-sql/00_reset.sql
spark-sql/01_create_and_insert.sql
spark-sql/02_delete_and_update.sql
spark-sql/03_inspect_delete_files.sql
spark-sql/04_validate.sql
spark-sql/05_failure_case.sql   # 可选：预期失败
spark-sql/06_cleanup.sql
```

执行示例：

```bash
./scripts/run-spark-sql.sh \
  labs/04-delete-files/spark-sql/00_reset.sql \
  labs/04-delete-files/spark-sql/01_create_and_insert.sql \
  labs/04-delete-files/spark-sql/02_delete_and_update.sql \
  labs/04-delete-files/spark-sql/03_inspect_delete_files.sql \
  labs/04-delete-files/spark-sql/04_validate.sql
```

## 实验步骤

1. 创建 Format Version 2 表，并将删除、更新和合并模式设置为 `merge-on-read`。
2. 插入两批数据，产生至少两个 Data File。
3. 使用 `DELETE` 删除一条记录，使用 `UPDATE` 修改另一条记录，再使用 `MERGE` 更新和插入记录。
4. 查询 `files`、`delete_files`、`all_delete_files` 和 `entries` 元数据表。
5. 对照 `content` 字段观察文件内容：`0` 表示 Data File，`1` 表示 Position Delete，`2` 表示 Equality Delete。

## 预期结果

- 当前表只返回未删除的业务记录，`04_validate.sql` 输出 `PASS`。
- `delete_files` 或 `all_delete_files` 中能看到至少一条 Delete File，具体是 Position Delete 还是 Equality Delete 以实际引擎实现为准。
- `entries` 能把 Data File、Delete File 与 Snapshot 关联起来。
- 历史 Snapshot 仍可能包含删除前的数据；只有在快照过期并完成相关清理后，旧文件才可能被物理删除。

## 多次删除时的 Position Delete 合并

在当前项目的 Spark `3.5.9` 与 Iceberg `1.11.0` 组合中，MOR 不保证每次 `DELETE` 都在当前 Snapshot 中保留一个独立的新 Position Delete File。对于同一个被引用的 Data File，后一次删除可以读取已有的 Position Delete，合并旧、新删除位置后写出新的替代 Delete File。

例如，第一次删除后的当前 Snapshot 引用 `delete-v1.parquet`，第二次删除同一 Data File 后，新的当前 Snapshot 可能改为引用包含两次删除位置的 `delete-v2.parquet`。`delete-v1.parquet` 会从当前引用中移除，但仍由第一次删除对应的历史 Snapshot 引用，因此可以支持 Time Travel。

这仍然是 MOR：原 Data File 在当前 Snapshot 中仍被引用，查询时由最新 Delete File 应用逻辑删除。应通过 `content` 字段区分实际行为：

- `content = 1`，且 `referenced_data_file` 指向原 Data File：Position Delete 的合并或替换。
- `content = 0`，且原 Data File 不再被当前 Snapshot 引用：Data File 重写，属于 COW 行为或后续维护操作的物化结果。

删除命中不同 Data File、不同分区，或受到任务并行度和 Delete File 粒度影响时，当前 Snapshot 仍可能同时包含多个 Delete File。不要通过文件数量判断是否使用 MOR，应结合 `content`、`referenced_data_file` 和 Snapshot 引用关系确认。

## 重要说明

- `merge-on-read` 只声明写入策略，不保证每种 SQL 操作一定生成同一种 Delete File。
- Spark、Flink 和 Trino 对 Equality Delete、Position Delete 的读写支持可能不同，必须在各自引擎中单独验证。
- Delete File 的物理存在不等于查询会返回被删除记录；查询引擎需要在扫描时应用删除文件。
- 不要手工删除 Warehouse 中的 Delete File；应通过 Snapshot 过期、数据文件重写和孤儿文件清理等维护流程处理。

## 失败路径

`05_failure_case.sql` 尝试使用不兼容的 Delete File 表属性值，预期提交失败且当前表数据不变。失败后重新执行 `04_validate.sql`。

## 清理

确认完成元数据观察后执行 `spark-sql/06_cleanup.sql`。不要删除共享 Warehouse 或其他表的文件。
