# 外部环境准备指南

本项目不部署基础设施，但为了方便学习，提供一套尽量精简的准备步骤。可以先使用单机模式验证 Spark 和 Iceberg，再接入共享对象存储、Catalog、Trino 和 Flink。

## 1. 版本基线

| 组件 | 推荐版本 |
| --- | --- |
| Java | 17 |
| Python | 3.11 或 3.12 |
| Iceberg | 1.11.0 |
| Spark | 3.5.9，Scala 2.12 |
| Flink | 1.20.5 |
| Trino | 476 |

完整版本配置见 [`config/versions.properties`](../config/versions.properties)。

## 2. 准备本地工具

### Java 17

本机已确认 JDK 17 位于 `/usr/local/jdk-17.0.14+7`。仅在当前 Shell 中启用：

```bash
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
java -version
```

也可以使用项目根目录的 `.mise.toml` 管理项目级 Java 版本，具体见 [`development-environment.md`](development-environment.md)。

### Python

Python 3.11 和 3.12 均可使用。建议为项目创建独立虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
python --version
python -m pip install --upgrade pip
```

Python 依赖准备好后，再根据示例需要安装 PyIceberg、PySpark 或数据生成器依赖。不要使用系统级 `pip` 安装项目依赖。

项目提供的 Spark 包装脚本使用 `workdir/.env` 注入 Catalog、Warehouse、S3 Endpoint 和凭据。复制 `workdir/.env.example` 后再执行检查：

```bash
cp workdir/.env.example workdir/.env
./scripts/check-environment.sh
```

在准备实际实验前可以使用 `./scripts/check-environment.sh --strict`；该模式会把 Java、配置文件、Spark 可执行文件和 Runtime JAR 的缺失视为失败。

## 3. 选择存储和 Catalog

没有 Hadoop 或 Hive 环境时，优先选择 REST Catalog；如果要学习 Branch、Tag 和 WAP，再增加 Nessie。详细对比见 [`catalog-selection.md`](catalog-selection.md)。

### 最小单机模式

适合学习表操作、快照和元数据。使用共享文件系统路径作为 Warehouse，并使用 Hadoop Catalog：

```text
/data/iceberg-learning/warehouse
```

注意：单机模式不能模拟多节点共享存储，也不适合生产并发写入。Spark、Trino 和 Flink 进程必须能够访问同一个绝对路径。

### 共享外部环境

更接近生产的最小组合是：

```text
S3 或 S3-compatible Warehouse
        +
REST Catalog（推荐）或 Nessie Catalog
        +
Spark / Trino / Flink
```

所有引擎必须指向同一个 Catalog 和 Warehouse。项目中的示例默认使用 Catalog 名称 `ice`，实际环境可以替换为自己的名称。

## 4. 准备 Spark 3.5.9

安装或使用已有 Spark 3.5.9，并确认 Scala 二进制版本为 2.12：

```bash
spark-submit --version
spark-sql --version
```

外部环境需要提供 Iceberg Runtime：

```text
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
```

如果是在本地 Spark 进程中临时运行，可以使用依赖参数获取 Runtime；如果是共享集群，应由集群管理员统一安装，避免同一进程加载多个 Iceberg 版本。

单机 Hadoop Catalog 的 Spark 配置示例：

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.type=hadoop
spark.sql.catalog.ice.warehouse=/data/iceberg-learning/warehouse
```

将这些配置传给 Spark SQL 后，即可执行：

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.type=hadoop \
  --conf spark.sql.catalog.ice.warehouse=/data/iceberg-learning/warehouse \
  -f labs/01-quickstart/spark-sql/01_create_and_inspect.sql
```

共享 S3、REST 或 JDBC Catalog 的配置由外部平台提供，项目只保存不含凭据的模板。

## 5. 准备 Trino 476

确认 Trino 已启用 Iceberg Connector：

```bash
trino --version
```

Trino 不需要加载 Spark 的 Iceberg Runtime JAR，而是在 Trino Catalog 配置中启用 Iceberg Connector。单机 Hadoop Catalog 的配置思路如下，具体属性名以部署版本文档为准：

```properties
connector.name=iceberg
iceberg.catalog.type=hadoop
iceberg.catalog.warehouse=/data/iceberg-learning/warehouse
```

Trino 使用的 Warehouse 和 Catalog 必须与 Spark 相同。启动后验证：

```sql
SHOW SCHEMAS FROM ice;
SHOW TABLES FROM ice.learning;
SELECT COUNT(*) FROM ice.learning.orders;
```

## 6. 准备 Flink 1.20.5（阶段 5、6 和 10）

Flink 用于执行 `labs/05-flink-interoperability` 的跨引擎批处理实验，以及
`labs/06-streaming-cdc`、`labs/10-order-lakehouse` 的流式 CDC 原始事件摄取；它应与
Spark 和 Trino 指向同一个 Catalog 和 Warehouse。

在跨引擎实践中，推荐由 Spark 执行 Iceberg DDL，并让 Flink 读取或写入已存在的表。Spark 对隐藏分区转换、Partition Spec 演进和排序的 DDL 支持更完整；实验 05 中的 Flink DDL 仅用于演示引擎语法限制，不应作为生产分区策略模板。

Flink 环境需要提供：

```text
org.apache.iceberg:iceberg-flink-runtime-1.20:1.11.0
```

项目提供 `workdir/flink-sql.sh`，它从 `workdir/.env` 读取与 Spark 相同的 Catalog、S3 和凭据变量，并在运行时生成 Flink SQL Client 的 Catalog 初始化文件。将 `iceberg-flink-runtime-1.20-1.11.0.jar` 和 `iceberg-aws-bundle-1.11.0.jar` 安装到 `$FLINK_HOME/lib/`；SQL Gateway 初始化 REST Catalog 时需要从全局类路径加载它们，不能再通过 `-j` 重复加载。精简 Flink 发行版不包含 Iceberg Catalog 所需的 Hadoop `Configuration` 与 `HdfsConfiguration` API。推荐解压完整 Hadoop 客户端发行版，并在 `.env` 设置 `HADOOP_HOME` 与 `HADOOP_CONF_DIR`；包装脚本会自动调用 `hadoop classpath --glob` 并导出 `HADOOP_CLASSPATH`。这样不需要将 Hadoop 的传递依赖复制到 `$FLINK_HOME/lib/`，避免覆盖 Flink 自带库。将 `FLINK_HOME` 和 `HADOOP_HOME` 追加到本地 `.env` 后，可执行：

```bash
./workdir/flink-sql.sh labs/05-flink-interoperability/flink-sql/01_read_spark_table.sql
```

省略 SQL 文件参数会进入已初始化 Catalog 的交互式 Flink SQL Client：

```bash
./workdir/flink-sql.sh
```

执行 SQL 前还需要可用的 Flink Standalone/Session Cluster。若使用本机默认配置，在启动 Cluster 的 Shell 中加载 Hadoop 类路径后执行：

```bash
set -a
source workdir/.env
set +a
export HADOOP_CLASSPATH="$("$HADOOP_HOME/bin/hadoop" classpath --glob)"
"$FLINK_HOME/bin/start-cluster.sh"
```

默认 JobManager REST Endpoint 为 `http://localhost:8081`。进入 SQL Client、`SHOW CATALOGS` 和创建 Catalog 可能在未启动 Cluster 时成功，但 `SELECT`、`INSERT`、`CREATE TABLE AS SELECT` 等会提交作业的语句需要该 Endpoint 可达。

生成的初始化 SQL 不会写入仓库，但 Flink 可能在失败日志中显示 Catalog 属性。生产环境应使用外部凭据提供机制或限制日志访问；发生失败后按凭据轮换策略处理。

不使用包装脚本时，REST Catalog 的 Flink SQL 配置形态如下，完整的无凭据模板见 [`../labs/05-flink-interoperability/flink-sql/00_catalog.sql`](../labs/05-flink-interoperability/flink-sql/00_catalog.sql)：

```sql
CREATE CATALOG ice WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '<CATALOG_URI>',
    'warehouse' = '<CATALOG_WAREHOUSE>',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);

USE CATALOG ice;
```

为 S3-compatible Warehouse 还需要在 Catalog 中提供 Endpoint、Region、Path-style 开关和凭据；使用环境变量或外部部署配置注入，不要将凭据保存到 SQL。Flink Runtime JAR 必须与版本基线匹配，不能复用 Spark Runtime JAR。

### 流式 CDC 额外要求

`workdir/.env.example` 默认 `FLINK_RUNTIME_MODE=batch`，以支持实验 05 的有界 SQL。执行
实验 06 或实验 10 时，只在启动 CDC 作业的命令上覆盖为 `streaming`；不要为了流式作业
永久修改其他批式实验的默认行为：

```bash
FLINK_RUNTIME_MODE=streaming ./workdir/flink-cdc-sql.sh \
  --template labs/10-order-lakehouse/flink-sql/01_stream_append.sql.template
```

阶段 10 的 `--template` 参数只选择仓库中不含凭据的 SQL 模板；`CDC_INPUT_PATH` 仍从被忽略的
`workdir/.env` 注入。该路径必须在作业启动前为空、对所有 TaskManager 可见，并且只接收完整
的新文件。不要追加已被 Source 发现的文件，也不要让实验 06 与实验 10 共用输入目录。

文件系统 JSON Source 还需要与 Flink `1.20.5` 匹配的 JSON format 依赖。它与 Iceberg
Runtime、AWS bundle 和 Hadoop 客户端类路径共同构成流式作业前置条件；缺少其中任一项时，
应先按 [实验 06 的前置条件](../labs/06-streaming-cdc/README.md) 排查，而不是在
`$FLINK_HOME/lib/` 任意复制不匹配的依赖。每批文件投递后等待至少一次成功 Checkpoint，
再执行 Spark 当前态物化；实际文件数和 Snapshot 数应从 Metadata Table 读取，不能由业务
`operation` 字段推断。

## 7. 最小验证流程

按以下顺序确认环境：

1. `java -version` 输出 Java 17。
2. `spark-sql --version` 输出 Spark 3.5.9。
3. 使用 Spark 执行 `labs/01-quickstart/spark-sql/01_create_and_inspect.sql`。
4. 使用 Spark 执行插入和元数据实验。
5. 使用 Trino 查询 `ice.learning.orders`。
6. 使用一个真实 Snapshot ID 验证 Time Travel。
7. 使用 `labs/05-flink-interoperability` 验证 Flink 读取 Spark 表、Flink 写表和 Spark 回读。
8. 使用专用空输入目录执行 `labs/10-order-lakehouse`，验证 Flink Checkpoint 原始事件写入、
   Spark 当前态物化和 Trino 回读。

## 8. 常见问题

Spark 离线依赖、多个本地 JAR、Iceberg `S3FileIO` 的 AWS SDK、REST namespace 404 和 S3 Region 配置见 [`faq-spark-rest-minio.md`](faq-spark-rest-minio.md)。

- **找不到 Iceberg 类**：确认 Spark/Flink Runtime JAR 已加载，且没有多个 Iceberg 版本。
- **表在 Trino 中不存在**：确认 Trino 与 Spark 使用同一个 Catalog 和 Warehouse，路径权限也一致。
- **本地表无法被远程集群读取**：单机 Hadoop Catalog 使用本地路径，只适合单机；远程环境应改用共享对象存储。
- **提交冲突**：确认是否有多个客户端同时写入，并根据最新 Snapshot 重试。
- **凭据错误**：检查环境变量或外部密钥配置，不要把密钥写入项目文件。
