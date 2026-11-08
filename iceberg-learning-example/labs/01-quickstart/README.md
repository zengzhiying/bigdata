# 实验 01 - 快速开始

## 实验目标

通过外部 Spark 环境创建一张 Iceberg 表，查看快照和元数据，再使用 Trino 读取同一张表。

## 前置条件

- 外部管理的 Iceberg Catalog 和 Warehouse。
- 配置了 `iceberg-spark-runtime-3.5_2.12:1.11.0` 的 Spark `3.5.9`。
- Spark Session 已启用 `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`，以支持 UPDATE、DELETE 和 MERGE。
- 使用同一 Catalog 配置 Iceberg Connector 的 Trino `476`。
- 如果运行 Java 示例，需要 Java `17`。

## 配置

将 `workdir/.env.example` 复制为被忽略的 `workdir/.env`，填写 Spark 包装脚本所需的 Catalog、Warehouse、S3 Endpoint 和凭据。其他引擎的连接参数仍由外部部署配置提供；`config/application.example.yml` 只是声明式参考模板。

本实验中的 SQL 假设外部 Spark Session 将 Iceberg Catalog 暴露为 `ice`，且 Trino 配置了同名 Catalog。如果环境使用其他名称，请在执行前替换 SQL 中的 Catalog 限定符：

- `spark-sql/00_reset.sql`：可选地删除本实验表，便于从中断状态重跑。
- `spark-sql/01_create_and_inspect.sql`：创建并查看表结构。
- `spark-sql/02_insert_and_query.sql`：插入订单并查询数据。
- `spark-sql/03_update_delete_merge.sql`：执行 UPDATE、DELETE 和 MERGE。
- `spark-sql/04_snapshot_and_metadata.sql`：查询快照、历史和数据文件元数据。
- `spark-sql/05_time_travel.sql`：按快照 ID 和时间读取历史数据。
- `spark-sql/06_cleanup.sql`：删除本实验表。
- `spark-sql/07_validate.sql`：检查最终行数和删除/MERGE 结果。
- `trino/01_read_and_inspect.sql`：使用 Trino 读取同一张表及其元数据。

## 执行顺序

按以下顺序执行 Spark SQL 文件，然后执行 Trino 文件：

```text
00_reset.sql             # 仅在需要重跑时执行
01_create_and_inspect.sql
02_insert_and_query.sql
03_update_delete_merge.sql
04_snapshot_and_metadata.sql
05_time_travel.sql
trino/01_read_and_inspect.sql
07_validate.sql
06_cleanup.sql
```

`00_reset.sql` 会删除本实验专用的 `ice.learning.orders`，只应在确认该表属于本实验时执行。这样可以在上一次实验中断后安全重跑，而不会删除共享 Warehouse 中的其他表。

`05_time_travel.sql` 和 Trino SQL 中的 `<SNAPSHOT_ID>` 必须替换为元数据查询返回的真实历史快照 ID；Spark SQL 中的 `<COMMIT_TIME>` 也必须替换为 `committed_at` 的实际值。占位符不能直接提交给引擎执行。

## 实验步骤

1. 创建 Namespace `learning` 和表 `learning.orders`。
2. 插入至少五条带事件时间和客户 ID 的订单。
3. 查询表并记录第一个 Snapshot ID。
4. 查询表的 Snapshots 和 Files 元数据表。
5. 使用 Trino 读取表并比较行数。
6. 清理时只删除实验表，不要删除共享 Warehouse。

## 预期结果

- Spark 和 Trino 都可以读取该表。
- 可以看到至少一个已提交的 Snapshot。
- 元数据查询能够识别实验写入的数据文件。
- 不同引擎查询出的行数一致。
- 初始插入后有 5 条订单；完成删除和 MERGE 后仍有 5 条订单，其中订单 `1004` 被删除，订单 `1006` 被新增。
- `07_validate.sql` 输出两个 `PASS`，分别表示最终行数和删除/MERGE 结果符合预期。

## 可重复运行

推荐通过项目脚本执行单个 SQL 文件；脚本会统一注入 Spark、REST Catalog 和 S3 配置：

```bash
./workdir/spark-sql.sh -f labs/01-quickstart/spark-sql/00_reset.sql
./workdir/spark-sql.sh -f labs/01-quickstart/spark-sql/01_create_and_inspect.sql
```

后续文件也使用同一命令逐个执行。不要把真实凭据写入 SQL；配置来自 `workdir/.env`，模板见 `workdir/.env.example`。
