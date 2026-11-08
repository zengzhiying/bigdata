# 07 - Schema 和 Partition Evolution

## Schema Evolution 的核心

Iceberg Schema 的字段由稳定的 field ID 标识，而不是只依赖字段名称。新增字段、重命名字段和安全的类型扩展都可以通过更新表元数据完成，历史数据文件不需要立即重写。

| 操作 | 典型语义 | 风险 |
| --- | --- | --- |
| 新增字段 | 旧文件读取为 `NULL` | 需要确认默认值和下游兼容性 |
| 重命名字段 | 保留原 field ID | 不要用“删除再新增”替代 |
| 安全类型扩展 | 例如 `INT` 到 `BIGINT` | 必须确认所有引擎的类型映射 |
| 删除字段 | 从当前 Schema 隐藏字段 | 旧文件中的物理列可能仍存在 |

因此实验不能只比较查询结果，还应通过 `DESCRIBE TABLE EXTENDED`、Metadata JSON 或 API 观察 field ID 是否保持不变。

## Partition Spec Evolution

分区转换属于表元数据，而不是固定的目录约定。新 Partition Spec 可以在不重写旧文件的情况下生效：旧文件继续使用旧的 `spec_id`，新写入文件使用新的 `spec_id`。查询规划会根据每个文件所属的 Spec 推导分区过滤条件。

常见转换包括：

- `days(event_time)` 和 `months(event_time)`：按时间粒度分区。
- `bucket(N, customer_id)`：按哈希桶分区。
- `truncate(N, value)`：按截断值分区。
- `identity(column)`：按原始字段值分区。

隐藏分区意味着用户通常只需要过滤原始字段，例如 `event_time >= ...`，而不必在 SQL 中写物理分区列。

## 与物理重写的区别

Schema 或 Partition Spec 的演进主要是元数据提交，不等于数据文件已经重新组织。要让历史文件也使用新的分区布局，需要单独执行数据文件重写或维护操作。因此需要分别验证：

1. 新旧数据是否都能正确读取。
2. 新写入文件的 `spec_id` 是否发生变化。
3. 查询是否仍然能够进行分区裁剪。
4. 是否在业务允许的窗口内执行了物理重写。

## 新旧 Partition Spec 共存时的查询性能

新增分区字段只改变之后写入文件使用的 Partition Spec，不会自动改写历史文件。因此同一张表在一段时间内可能同时存在多个 `spec_id`：

```text
旧文件：days(event_time)
新文件：days(event_time) + bucket(8, customer_id)
```

Iceberg 会根据每个文件所属的 Spec 分别进行分区裁剪，而不是要求所有文件使用相同的物理布局。性能影响取决于过滤条件：

| 查询条件 | 旧文件 | 新文件 | 常见结果 |
| --- | --- | --- | --- |
| 只过滤 `event_time` | 可以使用 `days` 裁剪 | 可以使用 `days` 裁剪 | 通常影响较小 |
| 只过滤 `customer_id` | 没有 bucket 分区，只能依赖 Manifest/列统计 | 可以使用 bucket 裁剪 | 旧数据可能扫描更多文件 |
| 同时过滤时间和客户 | 使用时间分区 | 使用时间和 bucket | 新数据的扫描范围更小 |
| 不带分区条件 | 需要扫描所有候选文件 | 需要扫描所有候选文件 | 主要增加少量规划元数据 |

所以 Partition Evolution 通常带来一个渐进优化过程：新数据立即受益，旧数据的性能保持原状。只要 Partition Spec 数量和 Manifest 数量没有无节制增长，查询规划开销通常小于一次全表重写的成本。

实验 02 的 `07_query_pruning.sql` 使用同一张混合 Spec 的表展示这个过程：时间过滤可以同时作用于新旧文件，客户过滤只对新 bucket 文件提供额外裁剪。可以通过 `EXPLAIN FORMATTED`、`customers.files` 的 `spec_id` 和实际扫描指标确认差异。

如果历史数据也需要新的分区裁剪，应在观察到真实查询收益后，对选定范围执行数据文件重写或排序，而不是因为每次分区演进都立即重写整张表。重写会产生新的 Snapshot，并消耗对象存储 I/O 和计算资源。

## 兼容性注意事项

- Spark、Flink、Trino 对字段类型、删除字段和 Partition Spec DDL 的支持并不完全一致。
- 重命名和删除操作必须在所有读引擎上验证，不能只依赖 Spark 成功。
- 不兼容的变更应作为失败路径测试，并确认失败不会生成新的 Snapshot。
- 使用 Format Version 2 时，Schema 演进还会影响 Equality Delete 的字段绑定，CDC 实验需要继续验证这一点。

## `Invalid update timestamp` 排查

Iceberg 支持通过 `ALTER TABLE ... RENAME COLUMN ...` 保留原字段 ID。若提交时报：

```text
Unsupported table change: Invalid update timestamp <new>:
before the latest metadata log entry timestamp <latest>
```

应优先排查提交时间和并发状态，而不是把它判断为 DDL 不支持：

1. 比较执行 Spark 提交的 Driver 主机时间与表最近一次提交时间，确认没有 NTP 回拨或多台机器时钟偏差。
2. 确认没有另一个 Spark/Flink 客户端同时提交同一张表；必要时重启 Session，清除旧的表对象缓存后重试。
3. 如果表来自复制或迁移环境，确认 Metadata JSON 的时间线没有被带到一个时间较早的客户端。
4. 学习环境中的临时表可以删除后重建；生产表不要手工修改 Metadata JSON，应先修复时钟和提交冲突。
