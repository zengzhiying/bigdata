# 流式 CDC 与 Iceberg 当前状态

CDC 事件流与 Iceberg 表之间至少要区分两个模型：事件日志和当前状态。事件日志保留每次变更，通常以 append-only 方式写入；当前状态按业务主键只保留最新有效记录，需要去重、排序和删除语义。把两者写到同一张表会掩盖重放、迟到和删除的处理规则。

## 事件契约先于 Connector

每个事件至少应有不可变 `event_id`、业务主键、操作类型、业务版本或可排序时间，以及载荷。推荐明确以下规则：

- 相同 `event_id` 是重复投递，只保留一次。
- 同一业务主键以更高 `event_version` 获胜；版本相同时再使用事件时间和稳定的事件 ID 打破平局。
- `DELETE` 是业务状态的一部分，不应因为载荷为空而被当作坏数据丢弃。
- 无版本或无法稳定排序的事件必须规定拒绝、旁路或人工处理策略。

Iceberg 不会自动为任意 append 写入推导上述业务规则。即使表声明了标识字段或上游有主键，去重和版本选择仍需要由流处理作业、暂存层或集合式 `MERGE` 明确实现。

## Checkpoint 与 Iceberg 提交

Flink 流式 Iceberg Sink 使用 Checkpoint 协调提交。正常情况下，只有成功完成 Checkpoint 的数据才应作为一次可见 Iceberg 提交被发布。作业取消、恢复、Source 文件发现和对象存储一致性会影响实际行为，因此应在目标环境记录：Checkpoint 编号、作业尝试次数、Iceberg Snapshot ID、提交时间和原始事件计数。

不要把“一个 Checkpoint 恰好产生一个 Snapshot”写成不变式；应以实际 Snapshot 元数据和运行时日志为准。恢复测试应关注最终事件集合和当前状态是否正确，而不是只比较日志中是否出现重试。

### 业务操作、事件行与 Data File 是三件事

文件系统 JSON Source 对每一行输入只发出一条 `INSERT` 流记录。事件中的
`operation = 'UPDATE'` 或 `operation = 'DELETE'` 是保留给下游解释的业务载荷，不会把
`orders_cdc_raw` 的物理写入变成 Iceberg 行级更新或删除。因此原始事件表是可重放的
append-only 日志：业务删除事件也只是多追加一行带 `DELETE` 标记的记录。

阶段 10 的已验证运行说明了这三个层次的差异：`batch-001` 和 `batch-002` 共 6 条事件被
同一轮成功提交合并为一个 Data File；`batch-003` 的 2 条事件在后续提交中形成另一个
Data File。这里的“6 + 2”由 Source 发现时机、Checkpoint、Sink 并行度和写入分布共同决定，
不是由 2 次业务 `INSERT`、4 次业务 `UPDATE`、1 次业务 `DELETE` 的数量决定。一次成功
Checkpoint 可以写入零个、一个或多个 Data File；Snapshot、Data File 和业务操作之间都不应
假设一一对应关系。

```text
JSON event row (all are Flink INSERT records)
  -> append to orders_cdc_raw
  -> Spark deduplicates event_id and orders by version
  -> MERGE materializes orders_current
```

`orders_current` 才执行业务更新或删除。它可能生成 Delete File 或重写 Data File，实际类型
仍必须通过 Metadata Table 确认。完整的建表、两阶段物化和跨引擎验收见
[`labs/10-order-lakehouse`](../../labs/10-order-lakehouse/README.md)。

### 空闲流的空 Snapshot

流作业在没有新文件或新消息时仍会执行 Checkpoint。Flink Iceberg Sink 需要持久化已提交 Checkpoint 的进度，以便恢复后避免重复提交；因此在连续空 Checkpoint 后可以创建不含 Data File 的 `append` Snapshot。它会复用既有 Manifest，只更新类似 `flink.max-committed-checkpoint-id` 的提交信息。

阶段 6 的已验证环境中，10 秒 Checkpoint 配置下，原始事件表在 Checkpoint `1` 写入一个 Data File 和 8 条记录；之后每约 100 秒生成一个空 Snapshot，记录的 Checkpoint ID 依次为 `11`、`21`、`31`。这些 Snapshot 的 `added-data-files` 和 `added-records` 为空，`total-data-files` 与 `total-records` 保持不变。该频率由 `flink.max-continuous-empty-commits` 控制，具体有效值应随 Runtime 版本和表配置一并记录。

长时间空闲而不取消的流作业会持续增加 Snapshot 和 Metadata JSON 数量。调高空提交阈值可以减少元数据膨胀，但会降低 Checkpoint 进度写入 Iceberg 的频率；应结合恢复目标、快照保留策略和阶段 8 的维护任务一起决定。

## 文件监听与业务记录

目录轮询不是 CDC 事件。文件系统 Source 的 Enumerator 会周期性列举输入目录，并在发现未消费的完整文件时才为其创建 Split。文件中的每一行才是进入下游的 INSERT 记录。轮询、任务协调、Watermark 和 Checkpoint Barrier 可以增加网络字节指标，却不会增加 Source `numRecordsOut`、Sink `numRecordsIn` 或 Iceberg 表行数。

对于文件输入，应生成不可变的完整文件，再复制或原子移动到被监听目录；不要修改已经被 Source 发现的文件。对象存储的列举、可见性与重命名语义需要在目标环境单独验证。

## Append-only 与原生 Upsert

文件系统、对象存储文件和普通消息 Source 通常输出 INSERT-only 流。它们可以可靠地写入事件日志，却不能自行表示下游记录删除。Flink 原生 Upsert 需要上游输出带 `UPDATE_BEFORE`、`UPDATE_AFTER` 或 `DELETE` 的 Changelog，并要求 Iceberg Flink Runtime、表格式、主键/标识字段和 Sink 配置在该版本组合下都受支持。

因此建议分两步演进：先使用 append-only 输入稳定验证摄取、Checkpoint 与可重放事件日志；再使用 Flink Table API 的固定 Changelog Fixture，单独验证原生 Upsert、删除和最终状态。Fixture 必须显式产生 `UPDATE_BEFORE`、`UPDATE_AFTER` 或 `DELETE`，不能用只产生 INSERT 的 SQL `VALUES` 代替。这能避免将“文件重放后 Spark MERGE 得到正确状态”误报为“Flink 原生 Upsert 已验证”。真实 CDC Connector 的集成、offset、反序列化和恢复语义属于生产环境专有验证，不在本项目学习案例范围内。

## Delete File 与维护

当前状态表使用 Merge-on-Read 时，更新或删除可能生成 Position Delete、Equality Delete 或重写 Data File，取决于引擎、表属性、操作粒度和运行时版本。必须查询 `files`、`delete_files`、`entries` 或 `all_entries` 元数据表确认实际行为。

流式小批次提交通常也会增加小文件、Manifest 和 Snapshot 数量。阶段 8 先用独立批式 Fixture 建立可重复的文件重写、Manifest 合并、快照过期和孤儿文件 dry-run 基线；阶段 6 的流式表具备基础验证后，应将同一组度量扩展到真实小批次负载，而不应只从一次性批表推断生产效果。Flink 原生 Upsert 固定 Changelog Fixture 的验证不是维护实验的前置条件。
