# 实验 10 - 跨引擎订单湖仓综合项目

本实验将前九个阶段的独立能力收敛为一个隔离、可重复的订单湖仓闭环：Flink 用
Checkpoint 将 append-only CDC 事件写入原始 Iceberg 表；Spark 去重、按版本物化当前态，
执行质量门禁、Schema Evolution、指标构建和 Data File 重写；Trino 从同一 Catalog 回读
当前态与指标。所有表位于专用 namespace `ice.learning_capstone`，不会复用或删除其他实验表。

## 目标和边界

1. 验证 Flink、Spark、Trino 对同一 REST Catalog 和 Warehouse 的读写互操作。
2. 验证重复投递、乱序事件和逻辑删除经过 Spark `MERGE` 后得到确定的当前态。
3. 验证新增 `fulfillment_region` 列、质量门禁、日指标表和 Data File 重写不会改变逻辑数据。
4. 验证通过快照、Metadata Table 和跨引擎查询验收，而非仅依赖作业日志。

文件系统输入仍是 append-only，不能模拟 Flink `UPDATE_AFTER` 或 `DELETE` Changelog；
Flink 原生 Upsert 的独立验证保留在实验 06。这里的 CDC 删除语义由 Spark 对原始事件执行
`MERGE` 实现。不要在此实验中运行快照过期、孤儿文件删除、对象存储删除或备份恢复。

## 前置条件

- Java 17、Spark `3.5.9`、Flink `1.20.5`、Iceberg `1.11.0`、Trino `476`，版本来源见
  [`config/versions.properties`](../../config/versions.properties)。
- 配置 `workdir/.env`，并让 Spark、Flink 与 Trino 指向同一个 REST Catalog 和 Warehouse。
- `CDC_INPUT_PATH` 必须是空目录且所有 Flink TaskManager 可读。Flink 的 JSON format 依赖、
  Iceberg Flink Runtime 和 AWS bundle 必须按实验 06 的要求放在 `$FLINK_HOME/lib/`。
- 运行期间不要让其他作业写入 `learning_capstone` 的三张表，也不要复用同一个输入目录。

先在项目根目录选择 Java 17 并检查环境：

```bash
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
./scripts/check-environment.sh --strict
```

## 数据契约

本实验复用确定性的 [`datasets/generate_cdc_orders.py`](../../datasets/generate_cdc_orders.py)。
三个 JSONL 批次包含 8 条原始事件和 7 个不同 `event_id`：`1001` 先更新后删除，`1002`
包含较低版本的晚到事件，`1003` 是最终新增订单。最终当前态必须只包含：

| order_id | order_status | event_version | fulfillment_region | order_total |
| --- | --- | --- | --- | --- |
| 1002 | SHIPPED | 3 | east | 80.25 |
| 1003 | CREATED | 1 | west | 15.50 |

因此当前态和指标表总金额均为 `95.75`。`event_id` 是去重键，`order_id + event_version`
决定当前态；两者都是数据契约，不是 Iceberg 自动强制的主键约束。

## 执行顺序

```text
spark-sql/00_reset_and_create.sql
datasets/generate_cdc_orders.py
flink-sql/01_stream_append.sql.template
spark-sql/01_materialize_current_state.sql  # 批次 001、002 后首次执行
spark-sql/01_materialize_current_state.sql  # 批次 003 后再次执行
spark-sql/02_validate_current_state.sql
spark-sql/03_quality_and_evolve.sql
trino/01_validate_cross_engine.sql
spark-sql/04_maintenance.sql
spark-sql/05_final_validate.sql
spark-sql/06_cleanup.sql
```

### 1. 建表并生成输入

```bash
./scripts/run-spark-sql.sh labs/10-order-lakehouse/spark-sql/00_reset_and_create.sql
python3 datasets/generate_cdc_orders.py --output-dir generated-data/capstone-cdc-orders
```

确认 `CDC_INPUT_PATH` 为空后，用 streaming mode 启动 Flink。`--template` 只替换输入目录，
不会将路径或凭据写入 Git；不带参数的 `workdir/flink-cdc-sql.sh` 仍执行实验 06 模板。

```bash
FLINK_RUNTIME_MODE=streaming ./workdir/flink-cdc-sql.sh \
  --template labs/10-order-lakehouse/flink-sql/01_stream_append.sql.template
```

将 `batch-001.json` 和 `batch-002.json` 完整复制或原子移动到 `CDC_INPUT_PATH`，每批等待至少
一次成功 Checkpoint 后执行首次物化：

```bash
./scripts/run-spark-sql.sh labs/10-order-lakehouse/spark-sql/01_materialize_current_state.sql
```

此时应有 `1001` 的版本 2 与 `1002` 的版本 3。不要追加已被 Source 发现的文件。

### 2. 完成 CDC 并验收当前态

投递 `batch-003.json`，等待成功 Checkpoint 后再次执行物化和验证：

```bash
./scripts/run-spark-sql.sh \
  labs/10-order-lakehouse/spark-sql/01_materialize_current_state.sql \
  labs/10-order-lakehouse/spark-sql/02_validate_current_state.sql
```

`raw_event_check` 和 `current_state_check` 必须为 `PASS`。确认原始表达到 8 条记录后手动
取消 Flink 作业；保留的外部化 Checkpoint 可由目标环境的正常恢复流程使用。作业取消后不再
向输入目录投递文件。

### 3. 质量、演进和跨引擎读取

质量脚本先以 `assert_true` 阻止不符合当前态契约的结果，再新增并填充
`fulfillment_region`，最后重建每日指标表：

```bash
./scripts/run-spark-sql.sh labs/10-order-lakehouse/spark-sql/03_quality_and_evolve.sql
```

随后使用已配置同一 Iceberg Catalog 的 Trino CLI 或 SQL 客户端执行
`trino/01_validate_cross_engine.sql`。当前态和指标两个 `PASS` 都是 Spark/Flink/Trino
互操作的验收结果；若 Trino 不认识新列，应停止并检查 Catalog、Metadata 刷新和连接的
Warehouse，而不是在另一引擎重建表。

### 4. 维护与最终验收

```bash
./scripts/run-spark-sql.sh \
  labs/10-order-lakehouse/spark-sql/04_maintenance.sql \
  labs/10-order-lakehouse/spark-sql/05_final_validate.sql
```

`04_maintenance.sql` 以 `rewrite-all` 对当前态运行一次受控 bin-pack 重写，并在同一 Spark
会话内用双向 `EXCEPT ALL` 对比重写前后逻辑数据。成功时输出
`maintenance_logical_data_check=PASS`，`snapshots` 中应出现 `replace` 操作。重写的实际
Data File 或 Delete File 数量取决于 Runtime，不应写死为固定值。该步骤不执行快照过期或
孤儿文件删除。

`05_final_validate.sql` 的三个 `assert_true` 返回 `NULL` 表示成功，最后一行
`capstone_final_validation=PASS` 表示当前态、指标和快照保留检查均满足契约。

将 [`validation-record.md.template`](validation-record.md.template) 复制到仓库外或被忽略的
本地目录，记录实际版本、Flink Checkpoint、所有 PASS/断言、Trino 结果和维护 Snapshot。只有
三种引擎都完成实际运行后，才可将阶段 10 标记为“已验证”。

## 清理和故障处理

```bash
./scripts/run-spark-sql.sh labs/10-order-lakehouse/spark-sql/06_cleanup.sql
```

清理只删除这三张专用表；不会删除 `learning_capstone` namespace、共享 Warehouse、输入文件
或外部化 Checkpoint。生成数据位于被忽略的 `generated-data/`，按运行环境的文件生命周期
策略自行处理。

如果 Flink 没有读到事件，确认输入目录在作业启动前为空、文件是完整新文件且对所有
TaskManager 可见。若当前态缺少 `1001` 的删除效果，通常是首次物化执行得太晚；应重置后按
两个阶段物化。若维护脚本失败，不要手工删除对象；保留 Snapshot 和错误输出，并按
[`labs/08-performance-and-maintenance`](../08-performance-and-maintenance/README.md) 的
维护安全边界排查。
