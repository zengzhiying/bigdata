# 09 - Spark 写入模式与 Hudi 对比

## 先按写入语义选择 API

Iceberg 不会把任意列当作数据库意义上的主键。即使表声明了 identifier fields，普通追加写也不会查询旧数据、检查唯一性或自动更新同一键的记录。因此，Spark 写入应先按业务语义选择操作，而不是只按数据量选择：

| 业务语义 | 推荐 Spark / Iceberg 操作 | 结果 |
| --- | --- | --- |
| 新增数据，不需要与历史记录匹配 | `df.writeTo("catalog.namespace.table").append()` | 新 Data File 加入新的 Snapshot；不去重。 |
| 重算或回灌输入中涉及的完整分区 | `df.writeTo("catalog.namespace.table").overwritePartitions()` | 输入涉及的每个 Iceberg 分区被完整替换；其他分区不变。 |
| 用一批源数据更新或插入目标表记录 | `MERGE INTO target USING source ON ...` | 按 `ON` 条件执行更新/插入，可实现幂等 upsert。 |
| 只替换一个已知条件范围 | 带条件的 `overwrite(...)` 或 `DELETE` 后写入 | 替换/删除指定范围，不依赖输入中出现哪些分区。 |

`append` 通常是纯新增批量数据的首选，写入路径最直接。`MERGE INTO` 并非只能处理小数据量，但它需要寻找可能匹配的目标文件，并按 Copy-on-Write 或 Merge-on-Read 策略重写或生成 Delete File。因此大批量 upsert 应减少提交频率，并通过时间、分区键等条件缩小目标扫描范围。

## DataFrame Writer V2 示例

对于已存在的表，优先使用 DataFrame Writer V2。表名包含 Spark Catalog 名称；本项目默认是 `ice`：

```python
# df 的列和目标表 Schema 必须兼容。
df.writeTo("ice.learning_api.orders").append()
```

首次建表时可以一并定义表格式和分区：

```python
from pyspark.sql.functions import days

df.writeTo("ice.learning_api.orders") \
  .using("iceberg") \
  .partitionedBy(days("event_time")) \
  .tableProperty("format-version", "2") \
  .create()
```

传统的 DataFrame Writer V1 也支持 Iceberg：

```python
df.write.format("iceberg").mode("append").save("ice.learning_api.orders")
```

新代码优先采用 `writeTo`：它明确面向 Catalog 表，并提供 `overwritePartitions` 等 V2 表写入语义。

## `overwritePartitions()` 是分区替换，不是 upsert

`overwritePartitions()` 是动态分区覆盖。Spark 从本次 DataFrame 的分区转换值推导受影响的 Iceberg 分区，然后原子地以本次输入替换这些分区的所有旧数据。

例如表按 `days(event_time)` 分区，而输入只包含 `2026-04-03`：

```python
df.writeTo("ice.learning_api.orders").overwritePartitions()
```

提交成功后，`2026-04-03` 分区只包含 `df` 中的行；原分区内没有出现在 `df` 中的行也会被移除。写入过程不会按 `order_id` 判断新旧记录是否重复，`df` 自身存在重复行时也会原样写入。输入没有涉及的日期分区则维持不变。

空 DataFrame 不包含可推导的动态分区，不能借此可靠地清空某个旧分区。需要清空或精确替换一个已知范围时，使用带条件的覆盖或 SQL `DELETE FROM`，例如：

```sql
DELETE FROM ice.learning_api.orders
WHERE event_time >= TIMESTAMP '2026-04-03 00:00:00'
  AND event_time < TIMESTAMP '2026-04-04 00:00:00';
```

所有上述 Iceberg 写入都是 Snapshot 原子提交：读者只会读取提交前或提交后的完整表状态，不会看见删除旧分区和写入新分区之间的中间状态。

## 用 `MERGE INTO` 实现主键语义

当业务要求“相同 `order_id` 更新、未出现则插入”时，匹配规则必须显式写在 `MERGE INTO` 中：

```sql
MERGE INTO ice.learning_api.orders AS target
USING staged_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

在执行前应保证 source 在匹配键上至多一行。若多条 source 行匹配同一 target 行，Spark 无法确定应更新成哪一行，通常会拒绝该次 `MERGE`。CDC 场景可先按主键与版本时间在暂存 DataFrame 或暂存 Iceberg 表中去重，再执行一次集合式 merge。

常见的批量 CDC 流程如下：

```text
原始 CDC 批次 -> 去重 / 质量校验 -> append 到 staging Iceberg 表
  -> 一次 MERGE INTO target USING staging -> 按周期清理 staging
```

这避免了逐行提交或为每条变更单独执行 merge。若工作负载主要是整分区重算，则 `overwritePartitions()` 往往比逐记录 `MERGE` 更合适。

## 与 Hudi 的差异

Hudi 更偏向记录级变更模型。使用其 Spark DataSource 写入时，在配置 record key 和版本排序（常称 precombine）字段的典型场景中，默认操作是 `upsert`：同一 record key 的新记录会与已有记录匹配，并依版本字段决定保留的版本。Hudi 也提供 `insert`、`bulk_insert` 和分区覆盖等操作，但不应把其默认 upsert 语义套用到 Iceberg。

| 需求 | Hudi 常见操作 | Iceberg 常见操作 |
| --- | --- | --- |
| 大量纯新增 | `bulk_insert` / `insert` | `writeTo(...).append()` |
| 按键增量更新 | `upsert`（record key + precombine） | `MERGE INTO`（显式 `ON` 条件） |
| 重跑一个或多个分区 | `insert_overwrite` | `overwritePartitions()` |
| 版本去重 | `precombine` 规则 | source 侧去重，或在 `MERGE` 的 source 子查询中去重 |

因此，迁移 Hudi 作业时要特别检查普通写入的意图：Hudi 的 upsert 作业若直接替换成 Iceberg `append`，会产生重复记录；如果实际业务是纯新增或分区回灌，又不应机械地把每次写入改成 `MERGE`。选择操作应由数据变更语义、分区设计、批次大小和可接受的读取/写入成本共同决定。
