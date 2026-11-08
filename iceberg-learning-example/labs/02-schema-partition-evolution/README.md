# 实验 02 - Schema 和 Partition Evolution

## 实验目标

在同一张 Iceberg 表上观察字段 ID、Schema Evolution、隐藏分区和 Partition Spec 演进，并验证旧数据和新数据可以共存。

本实验只使用 Spark 执行变更，再使用 Trino 读取结果。它建立在实验 01 的 REST Catalog、Warehouse 和 S3 配置之上。

## 前置条件

- 已完成实验 01 的环境准备，Spark 使用 Iceberg `1.11.0` Runtime。
- Spark Session 启用 `IcebergSparkSessionExtensions`。
- Catalog 名称默认为 `ice`；如果使用其他名称，需要替换 SQL 中的 `ice`。
- 使用 Iceberg Format Version 2，以便后续实验可以继续观察 Delete File 语义。

## 文件顺序

```text
spark-sql/00_reset.sql
spark-sql/01_create_and_insert.sql
spark-sql/02_schema_evolution.sql
spark-sql/03_partition_evolution.sql
spark-sql/04_validate.sql
spark-sql/07_query_pruning.sql
trino/01_read_and_inspect.sql
spark-sql/05_failure_case.sql   # 可选：预期失败，不属于正常流水线
spark-sql/06_cleanup.sql
```

推荐使用项目脚本执行：

```bash
./scripts/run-spark-sql.sh \
  labs/02-schema-partition-evolution/spark-sql/00_reset.sql \
  labs/02-schema-partition-evolution/spark-sql/01_create_and_insert.sql \
  labs/02-schema-partition-evolution/spark-sql/02_schema_evolution.sql \
  labs/02-schema-partition-evolution/spark-sql/03_partition_evolution.sql \
  labs/02-schema-partition-evolution/spark-sql/04_validate.sql \
  labs/02-schema-partition-evolution/spark-sql/07_query_pruning.sql
```

## 观察重点

1. `customer_name` 重命名为 `full_name` 后，数据仍然能按原字段读取；通过 `DESCRIBE TABLE EXTENDED` 观察字段 ID。
2. 新增的 `loyalty_tier` 对旧数据表现为 `NULL`，不会重写历史数据。
3. `customer_id` 从 `INT` 扩展为 `BIGINT` 属于安全的类型演进；不兼容类型变更放在失败案例中。
4. 新增 `bucket(8, customer_id)` 后，旧文件仍属于旧 Partition Spec，新写入文件使用新 Spec。
5. Spark 和 Trino 都能读取完整结果，但元数据表字段和语法可能不同。

## 查询裁剪案例

`07_query_pruning.sql` 在同一张同时包含旧、新 Partition Spec 的表上执行三组查询：

- 只过滤 `event_time`：新旧文件都可以使用日期分区裁剪。
- 只过滤 `customer_id`：新 bucket 文件可以进一步裁剪，旧文件没有 bucket 信息，只能依赖 Manifest 和列统计。
- 同时过滤时间和客户：新文件可以使用两级条件，通常扫描范围最小。

脚本同时输出 `EXPLAIN FORMATTED` 和按 `spec_id` 汇总的文件信息。不要只比较返回行数；应结合执行计划、文件数量和实际扫描指标判断性能差异。

## 预期结果

- Schema 中包含 `full_name` 和 `loyalty_tier`，不再包含 `customer_name`。
- 查询结果包含 5 条客户记录，最初的 3 条记录的 `loyalty_tier` 为 `NULL`。
- `04_validate.sql` 输出 `PASS`，并且 `partitions` 或 `files` 元数据能看到多个 `spec_id`。
- `07_query_pruning.sql` 的时间过滤、客户过滤和组合过滤分别返回预期记录；执行计划中应能观察到不同过滤条件的裁剪机会。
- Trino 查询出的记录数与 Spark 一致。

## 失败路径

`05_failure_case.sql` 尝试把字段重命名为已经存在的字段，预期收到字段冲突错误。失败后重新执行 `04_validate.sql`，确认表状态没有变化。

如果 `RENAME COLUMN` 报错 `Invalid update timestamp ... before the latest metadata log entry timestamp ...`，这不是字段重命名能力缺失。Iceberg 支持字段重命名；该错误表示本次提交使用的客户端时间早于表元数据中已有的更新时间。先停止其他写入者并重启 Spark Session，确认执行提交的机器时间已同步；临时环境可执行 `00_reset.sql` 后重新运行本实验。不要手工编辑 Metadata JSON。

## 清理

只执行 `06_cleanup.sql` 删除 `learning_evolution.customers`，不要删除共享 Warehouse 或其他 Namespace。
