# 实验 05 - Flink、Trino 跨引擎互操作

## 实验目标

本实验由三组相互独立的批处理路径组成，验证 Spark、Flink 和 Trino 是否连接到同一个 REST Catalog 和 Warehouse：

1. Flink SQL 读取实验 01 由 Spark 创建的表，创建并写入新表，再由 Spark 和 Trino 读取。
2. Flink Java Table API 创建并写入新表，再由 Spark 和 Trino 读取。
3. Trino 创建 Format V2 表，执行 Schema Evolution、MERGE、UPDATE 和 DELETE，再由 Spark 读取。

本实验只覆盖批式读取、批式写入和 Trino 的行级操作，不覆盖 Flink 流式 Checkpoint、流式 Upsert 或流式 Delete File。它们属于后续 CDC 实验。Trino 行级操作产生的数据文件或 Delete File 类型必须通过元数据表观察，不能从 SQL 语句本身推断。

`02_write_flink_table.sql` 使用 `PARTITIONED BY (event_time)` 身份分区。当前 Flink SQL 的 `CREATE TABLE` 解析器在此处只接受列名，不能直接使用 Spark SQL 中的 `days(event_time)` 隐藏分区转换；后者属于阶段 3 的 Spark 演进实验范围。

这个 Flink DDL 仅用于观察该引擎的建表语法和 Spark 回读行为，不是生产表设计建议。实际跨引擎表优先由 Spark 负责 DDL，以保留 `days(event_time)` 等隐藏分区、Partition Spec 演进和排序能力；Flink 应读取 Spark 创建的表，或向该表追加/流式写入。这样可以避免为兼容 Flink DDL 而将生产表退化为按完整时间戳的身份分区。

## 前置条件

- 已完成实验 01 的创建和插入步骤，保留 `ice.learning.orders`，不要提前执行其清理脚本。
- 在 `workdir/.env` 中设置 `FLINK_HOME`；将 `iceberg-flink-runtime-1.20-1.11.0.jar` 和 `iceberg-aws-bundle-1.11.0.jar` 安装到 `$FLINK_HOME/lib/`，以便 SQL Gateway 在初始化 REST Catalog 时可见。
- 已启动本机 Flink Standalone/Session Cluster，且 JobManager REST Endpoint 可通过 `http://localhost:8081` 访问。进入 SQL Client 和创建 Catalog 不需要 Cluster；执行 `SELECT`、`INSERT` 或 DDL 作业时需要它。
- Flink、Spark 指向同一个 REST Catalog、Warehouse 和对象存储；Catalog 在不同引擎中的注册名可以不同。
- 执行 Flink Java 示例时，使用 Java `17` 构建，且由 `$FLINK_HOME/lib/` 提供 Flink、Iceberg Flink Runtime 和 AWS bundle。
- Trino `476` 配置了同一个 REST Catalog，并允许创建 `learning_trino` Schema 和专用实验表。

## 执行顺序

```text
# Flink SQL 写入路径
labs/01-quickstart/spark-sql/01_create_and_inspect.sql
labs/01-quickstart/spark-sql/02_insert_and_query.sql
flink-sql/01_read_spark_table.sql
flink-sql/02_write_flink_table.sql
flink-sql/03_validate.sql
spark-sql/04_read_flink_table.sql
trino/01_read_flink_sql_table.sql

# Flink Java 写入路径
examples/flink-java/FlinkOrdersApp
spark-sql/05_read_flink_java_table.sql
trino/02_read_flink_java_table.sql

# Trino 写入、演进和行级操作路径
trino/03_write_and_evolve.sql
trino/04_validate_trino_table.sql
spark-sql/06_read_trino_table.sql

# 专用表清理
trino/05_cleanup_trino_table.sql
examples/flink-java/FlinkOrdersApp --cleanup
flink-sql/05_cleanup.sql
```

推荐使用项目包装脚本。它读取 `workdir/.env`，使用 Flink 全局类路径中的 Iceberg Runtime 和 AWS bundle，并为每个 SQL 文件创建同一个 REST Catalog；不必将凭据写入或替换到 SQL 文件。

实验中的 SQL 是有界批处理，`workdir/flink-sql.sh` 默认设置 `FLINK_RUNTIME_MODE=batch`，因此 `ORDER BY order_id` 等全局排序可以执行。后续流式 CDC 实验可在本地 `.env` 中设置 `FLINK_RUNTIME_MODE=streaming`；流模式不支持按任意业务字段进行无界全局排序。

推荐解压完整 Hadoop `3.3.4` 客户端发行版，并在 `workdir/.env` 设置 `HADOOP_HOME` 和 `HADOOP_CONF_DIR`。包装脚本会调用 `hadoop classpath --glob`，让 SQL Gateway 读取 Hadoop `Configuration` 与 `HdfsConfiguration`，而不修改 `$FLINK_HOME/lib/`。不要把 Hadoop 的全部传递依赖复制到 Flink `lib`，它们可能覆盖 Flink 自带的库。

在启动 Standalone/Session Cluster 的 Shell 中也要提供完整 Hadoop 类路径：

```bash
set -a
source workdir/.env
set +a
export HADOOP_CLASSPATH="$("$HADOOP_HOME/bin/hadoop" classpath --glob)"
"$FLINK_HOME/bin/start-cluster.sh"
```

使用 `ss -ltn '( sport = :8081 )'` 或 Flink Web UI 确认 JobManager 已启动后，再运行 `workdir/flink-sql.sh`。

不传参数时，脚本会直接进入已初始化 Catalog 的 Flink SQL 交互式命令行：

```bash
./workdir/flink-sql.sh
```

```bash
./workdir/flink-sql.sh \
  labs/05-flink-interoperability/flink-sql/01_read_spark_table.sql \
  labs/05-flink-interoperability/flink-sql/02_write_flink_table.sql \
  labs/05-flink-interoperability/flink-sql/03_validate.sql
```

完成 Spark 回读后，再单独执行清理：

```bash
./workdir/flink-sql.sh labs/05-flink-interoperability/flink-sql/05_cleanup.sql
```

Flink Java 示例的构建和提交命令见 [`examples/flink-java/README.md`](../../examples/flink-java/README.md)。它使用独立的 `learning_flink_java.orders_from_flink_java` 表，不影响 Flink SQL 路径。

Trino 文件由已配置 Iceberg Catalog 的 Trino CLI 或其他 SQL 客户端执行。`trino/03_write_and_evolve.sql` 会重置仅供本实验使用的 `ice.learning_trino.orders_from_trino`；执行后依次运行 `trino/04_validate_trino_table.sql` 和 `spark-sql/06_read_trino_table.sql`，最后才执行 Trino 清理文件。

`flink-sql/00_catalog.sql` 保留为不使用包装脚本时的手工 SQL Client 模板。不能把 Spark 的 Runtime JAR 用作 Flink Runtime。

包装脚本不会把凭据提交到仓库，但 SQL Client 在失败时可能将初始化 SQL 写入 Flink 日志。应限制日志访问，并在出现这类失败后按凭据管理策略轮换访问密钥。

## 预期结果

- `01_read_spark_table.sql` 能读取实验 01 的五条初始订单，并输出其 Snapshot 元数据。
- `02_write_flink_table.sql` 创建 `learning_flink.orders_from_flink` 并写入三条订单。
- `03_validate.sql` 返回 `PASS`，且 `orders_from_flink$snapshots` 至少包含一次提交。
- `04_read_flink_table.sql` 和 `trino/01_read_flink_sql_table.sql` 返回相同的三条记录和总金额 `145.75`。
- Flink Java 示例写入 `learning_flink_java.orders_from_flink_java` 后，`spark-sql/05_read_flink_java_table.sql` 和 `trino/02_read_flink_java_table.sql` 返回三条记录和总金额 `140.75`。
- Trino 写入路径完成字段新增、字段重命名、MERGE、UPDATE、DELETE 后，`trino/04_validate_trino_table.sql` 与 `spark-sql/06_read_trino_table.sql` 返回三条记录和总金额 `160.25`，且 Schema 中使用 `fulfillment_status` 与 `source`。
- `orders_from_trino$files` 的 `content` 聚合结果记录实际 Data File / Delete File 行为；不要预先假设必定出现 Position Delete 或 Equality Delete。

## 清理

完成对应读取后，分别执行 `trino/05_cleanup_trino_table.sql`、Flink Java 示例的 `--cleanup` 和 `flink-sql/05_cleanup.sql`。它们只删除各自的专用表，不会影响实验 01 的表或共享 Warehouse。

## 记录结果

Flink SQL/Java 读取与写入、Spark/Trino 读取 Flink 表、Trino 写入、Schema/分区演进和行级操作路径均已在项目当前外部环境手动验证。Flink Java 的 `INSERT OVERWRITE` 提交 Snapshot ID 为 `3163989102682253507`；Flink 显示 `2026-08-19T07:19:54.118Z`，Spark 显示 `2026-08-19 15:19:54.118`，两者是 UTC 与 UTC+8 的同一提交时刻。跨引擎验证应优先比较 Snapshot ID 和操作类型；如需比较文本时间，可将 Spark 会话时区设为 UTC。后续环境或版本变更后，应重新执行本实验，并在 `config/compatibility-matrix.md` 中记录实际引擎、Catalog、存储、文件类型和结果。
