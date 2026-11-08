# 实验 06 - Flink 流式 CDC、Checkpoint 与当前状态

## 实验目标

本实验开始阶段 6，使用确定性的订单 CDC 事件验证以下两层职责：

1. Flink 以流模式将 append-only CDC 事件写入 Iceberg 原始事件表，并通过 Checkpoint 提交。
2. Spark 按 `event_id` 去重、按 `order_id + event_version` 选择最新事件，再通过 `MERGE` 物化当前状态并处理删除。
3. Flink Table API 使用固定 Changelog Fixture 写入原生 Upsert 表，独立验证
   `INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER` 和 `DELETE` 到 Iceberg Sink 的语义。

该拆分是有意的：文件系统输入是 append-only，不能产生 `DELETE` / `UPDATE_AFTER` Changelog。它适合先验证流式摄取、Checkpoint、重复事件和乱序事件；已落地的固定 Changelog Fixture 单独验证 Flink 原生 Upsert。该 Fixture 使用 Flink Table API 构造带 `INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER` 和 `DELETE` 的确定性事件，不引入 Debezium、Kafka 或其他外部 Connector。不要把本实验的 Spark `MERGE` 结果当作 Flink 原生 Upsert 已验证。

## 前置条件

- Spark `3.5.9`、Flink `1.20.5`、Iceberg `1.11.0`、Java `17`；Flink 安装及 REST Catalog、对象存储配置要求沿用实验 05。
- `$FLINK_HOME/lib/` 除 Iceberg Flink Runtime 与 AWS bundle 外，还应包含与 Flink `1.20.5` 匹配的 JSON format 依赖，以支持文件系统 JSON Source。
- `workdir/.env` 设置 `CDC_INPUT_PATH`。它必须是所有 Flink TaskManager 可见的空目录；本地单机可使用 `file:///absolute/path`，集群应使用共享文件系统或对象存储路径。
- 运行前执行 Spark 重置脚本。不要让其他作业并发写入 `ice.learning_cdc` 下的两张专用表。

## 事件契约

事件字段包括 `event_id`、`operation`、`order_id`、`event_version`、`event_time` 和订单载荷。事件版本是同一订单的单调递增版本；相同 `event_id` 是重复投递；较低版本晚到是乱序投递；`DELETE` 是逻辑删除。

生成器固定产出三个批次：

- `batch-001.json`：订单 `1001`、`1002` 的初始新增。
- `batch-002.json`：`1001` 更新的重复投递、`1002` 的版本 `3` 与晚到的版本 `2`。
- `batch-003.json`：删除 `1001`，并新增 `1003`。

全部批次写入原始表后，应有 8 条原始事件、7 个不同 `event_id`；当前状态只保留 `1002`（`SHIPPED`，版本 3）和 `1003`（`CREATED`，版本 1），总金额为 `95.75`。

## 执行顺序

```text
spark-sql/00_reset_and_create.sql
datasets/generate_cdc_orders.py
flink-sql/01_stream_append.sql.template  # 由 workdir/flink-cdc-sql.sh 渲染并执行
spark-sql/02_materialize_current_state.sql
spark-sql/03_validate.sql
spark-sql/04_inspect_files.sql
trino/01_read_current_state.sql
spark-sql/05_failure_case.sql             # 可选，预期失败
spark-sql/00_reset_and_create.sql          # 重置后再执行固定 Changelog Fixture
examples/flink-java/FlinkChangelogUpsertApp
spark-sql/07_validate_flink_changelog_upsert.sql
spark-sql/08_inspect_flink_changelog_upsert.sql
trino/02_read_flink_changelog_upsert.sql   # 可选
spark-sql/06_cleanup.sql
```

先创建表并生成测试批次：

```bash
./scripts/run-spark-sql.sh labs/06-streaming-cdc/spark-sql/00_reset_and_create.sql
python3 datasets/generate_cdc_orders.py --output-dir generated-data/cdc-orders
```

在 `CDC_INPUT_PATH` 保持为空时启动流式作业：

```bash
./workdir/flink-cdc-sql.sh
```

先将 `generated-data/cdc-orders/batch-001.json`、`batch-002.json` 复制到 `CDC_INPUT_PATH`。每个批次后等待至少一个 Checkpoint 成功，再首次执行 `02_materialize_current_state.sql`；此时当前状态表应包含 `1001` 的版本 2 和 `1002` 的版本 3。

再投递 `batch-003.json` 并等待至少一个 Checkpoint 成功，然后第二次执行 `02_materialize_current_state.sql`、`03_validate.sql` 与 `04_inspect_files.sql`。此时 `1001` 的 `DELETE` 会匹配目标表中已有的版本 2 记录，因而实际执行行级删除；`1003` 会被插入。所有文件被读取并确认原始表达到 8 行后，手动取消作业；外部化 Checkpoint 会保留以供恢复测试。

恢复时使用外部环境的 Flink 作业恢复流程重新提交同一作业，并仅投递尚未成功提交的文件。文件系统 Source 与对象存储的一致性、文件重命名语义和 Checkpoint 恢复行为均需在目标环境手动确认。

流式写入结束后执行当前状态物化和验证：

```bash
./scripts/run-spark-sql.sh \
  labs/06-streaming-cdc/spark-sql/02_materialize_current_state.sql \
  labs/06-streaming-cdc/spark-sql/03_validate.sql \
  labs/06-streaming-cdc/spark-sql/04_inspect_files.sql
```

最后由已配置同一 Iceberg Catalog 的 Trino CLI 执行 `trino/01_read_current_state.sql`。完成观察后执行 `spark-sql/06_cleanup.sql`；它不会删除输入目录或共享 Warehouse。

## 固定 Flink Changelog Upsert Fixture

该路径不使用文件 Source，也不依赖 Debezium、Kafka 或其他外部 Connector。Spark 继续负责
重置基础 Namespace 和清理；`FlinkChangelogUpsertApp` 使用 Flink Table API 创建专用的
`ice.learning_cdc.orders_changelog_upsert` 表，声明 `PRIMARY KEY (order_id) NOT ENFORCED`
并启用 `write.upsert.enabled`。随后它通过 `fromChangelogStream` 写入固定事件：订单 `2001`
从 `CREATED` 更新为版本 `2` 的 `PAID`，订单 `2002` 被删除，订单 `2003` 新增为
`SHIPPED`。

先重置实验表并使用 Java 17 构建 Flink Java 模块：

```bash
./scripts/run-spark-sql.sh labs/06-streaming-cdc/spark-sql/00_reset_and_create.sql
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
mvn -f examples/flink-java/pom.xml package
```

在当前 Shell 加载 `workdir/.env` 后，将应用提交到已安装 Iceberg Flink Runtime 与 AWS bundle
的 Flink 集群：

```bash
set -a
source workdir/.env
set +a
: "${FLINK_HOME:=/opt/flink-1.20.5}"
"$FLINK_HOME/bin/flink" run \
  -c org.apache.iceberg.learning.FlinkChangelogUpsertApp \
  examples/flink-java/target/iceberg-flink-java-example-1.0.0.jar
```

作业成功结束后，使用 Spark 验证最终行集和 Snapshot，再观察实际文件类型：

```bash
./scripts/run-spark-sql.sh \
  labs/06-streaming-cdc/spark-sql/07_validate_flink_changelog_upsert.sql \
  labs/06-streaming-cdc/spark-sql/08_inspect_flink_changelog_upsert.sql
```

预期最终仅保留两行：`2001` 为 `PAID`、版本 `2`、金额 `42.50`，`2003` 为 `SHIPPED`、
版本 `1`、金额 `73.25`，总金额为 `115.75`；`2002` 不可见。`07_validate` 返回 `PASS`
表示最终状态正确。`08_inspect` 只记录实际产生的 Data File、Position Delete 或 Equality
Delete；具体文件类型受 Runtime 与表配置影响，不能预先硬编码。可选地以 Trino 执行
`trino/02_read_flink_changelog_upsert.sql` 验证跨引擎读取。

### 已验证结果

该 Fixture 已在当前外部 Flink、REST Catalog、对象存储与 Spark 环境手动验证。
`07_validate_flink_changelog_upsert.sql` 返回 `PASS`；本次写入只创建一个 Snapshot，
其中包含一个 Data File 和两个 Equality Delete File。一个 Equality Delete 对应订单
`2001` 的更新，另一个对应订单 `2002` 的删除。三个 Parquet 文件通过同一个 Snapshot
的 Manifest 与 Metadata 引用共同构成一致的表状态，查询必须同时应用 Data File 与两个
Delete File，不能孤立地解释其中任一文件。

该 Fixture 验证 Flink 原生 Changelog 到 Iceberg Upsert Sink 的核心语义，不验证外部 CDC
Connector 的连接、offset、反序列化、Schema 演进或恢复行为。

## 观察重点

- 原始表的 Snapshot 数量和 `committed_at` 应与成功 Checkpoint 提交对应；一次 Checkpoint 是否恰好对应一个 Snapshot，取决于 Source、Sink 和运行时实现，应记录实际结果而非假设。
- `02_materialize_current_state.sql` 先按 `event_id` 去重，后按版本选最新事件，因此可重复执行。生产作业还需将事件保留策略、迟到阈值和无版本事件的处理规则写入数据契约。
- 当前状态表的 `MERGE` / `DELETE` 设置为 Merge-on-Read。`04_inspect_files.sql` 观察实际的 Data File、Position Delete 或 Equality Delete；不要预先断言某一种 Delete File 必定产生。
- `05_failure_case.sql` 使用同一目标键的两条源记录，预期 `MERGE` 失败。失败后再次执行 `03_validate.sql`，确认当前状态未改变。

## 已验证行为与排查

当前外部环境使用 Flink `1.20.5`、Iceberg `1.11.0` 完成基础路径验证后，观察到以下行为。这些结果用于解释运行时现象，不应替代升级或更换 Catalog/存储后的重新验证。

### 目录扫描不是 CDC 事件

文件系统 Source 按 `source.monitor-interval` 周期性列举 `CDC_INPUT_PATH`，扫描、心跳、Checkpoint Barrier 和 Coordinator 通信都可能使 Flink Web UI 的字节指标增长。只有发现此前未消费的完整新文件、并读取其中一行 JSON 时，才会向下游发出一条 INSERT 流记录。不要向已被发现的文件追加内容；应先完整生成文件，再复制或原子移动到输入目录。

判断是否真实摄取数据时，应优先查看 Source 的 `numRecordsOut`、Sink 的 `numRecordsIn`、原始表行数和 Snapshot 摘要，而不是只看 `Bytes Received`。

### 空 Checkpoint Snapshot

本次验证中，第一个原始表 Snapshot 的摘要为 `added-data-files = 1`、`added-records = 8`，对应唯一一个 3268 字节的 Parquet Data File。后续空闲期间的 Snapshot 仍标记为 `append`，但均满足 `changed-partition-count = 0`、`manifests-created = 0`、`manifests-kept = 1`，且总数据文件数和总记录数保持为 `1`、`8`。

这些空 Snapshot 的 `flink.max-committed-checkpoint-id` 从 `1` 继续推进为 `11`、`21`、`31` 等。配合 10 秒 Checkpoint 间隔，当前 Runtime 每约 100 秒记录一次连续空 Checkpoint 的提交进度。它用于故障恢复时避免重复提交，不代表有新 CDC 记录或新 Parquet 文件。相关频率受 `flink.max-continuous-empty-commits` 配置影响；长时间空闲的生产作业应结合恢复要求调高阈值，并配合快照维护。

可使用下列查询确认每次 Snapshot 是否实际写入数据：

```sql
SELECT snapshot_id, committed_at,
       summary['added-data-files'] AS added_data_files,
       summary['added-records'] AS added_records,
       summary['flink.max-committed-checkpoint-id'] AS checkpoint_id,
       summary
FROM ice.learning_cdc.orders_cdc_raw.snapshots
ORDER BY committed_at;
```

### 当前状态表只有一个文件

若在三个批次都进入原始表后才第一次物化 `orders_current`，目标表为空，`1001` 的最新 `DELETE` 不会命中 `WHEN MATCHED ... THEN DELETE`，也不会满足允许 INSERT 的条件。因此首次提交只写入 `1002`、`1003`，小数据量和单个写任务通常会生成一个 Data File。这不是 `MERGE INTO` 自动压实，也不代表已观察到 Delete File。

按本 README 的两阶段物化顺序执行，第二次 MERGE 会让 `1001` 的删除命中既有目标记录。`02_materialize_current_state.sql` 仅对更高版本做更新，避免同版本 `1002` 在第二次执行时被无意义重写。随后以 `delete_files`、`files` 和 `all_entries` 元数据确认实际生成 Position Delete、Equality Delete 或 Data File 重写；不能从 SQL 的 `DELETE` 分支本身推断文件类型。

## 验收与状态

本案例的基础路径已在当前外部 Flink、REST Catalog、对象存储和 Trino 环境手动验证，包括流式文件监听、Checkpoint、重复/乱序事件处理、当前状态物化、元数据观察和跨引擎回读。固定 Changelog Fixture 也已完成当前外部环境验证：它独立验证主键、更新、删除、最终状态和实际 Delete File 行为，不验证 Debezium、Kafka 或其他外部 Connector 的集成、offset、反序列化和恢复语义。实际 Snapshot 与文件结果已同步到兼容性矩阵。
