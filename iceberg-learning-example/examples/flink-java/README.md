# Flink Java Iceberg API 示例

`FlinkOrdersApp` 使用 Flink Table API 在 REST Catalog 中创建
`ice.learning_flink_java.orders_from_flink_java`，写入三条订单，再读取行数据和
Snapshot 元数据。它与 `labs/05-flink-interoperability` 使用同一组外部连接参数，
用于补齐 Flink Java 的最小读写路径。

本示例是有界批处理程序。Iceberg Flink Runtime 和 AWS bundle 由外部 Flink
发行版从 `$FLINK_HOME/lib/` 提供；不要把它们打进应用 JAR，也不要使用 Spark
Runtime JAR。

同一模块还包含 `FlinkChangelogUpsertApp`，供实验 06 使用。它以有界的固定
Changelog 流验证 Iceberg Flink Sink 的原生 Upsert 语义，目标表和验收步骤见
[`labs/06-streaming-cdc/README.md`](../../labs/06-streaming-cdc/README.md)。

## 构建与运行

在当前 Shell 中加载本地配置，并使用 Java 17 构建：

```bash
set -a
source workdir/.env
set +a
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
mvn -f examples/flink-java/pom.xml package
```

确认 `$FLINK_HOME/lib/` 已安装
`iceberg-flink-runtime-1.20-1.11.0.jar` 和 `iceberg-aws-bundle-1.11.0.jar`，
并已按实验 05 的说明启动 Flink 集群后提交：

```bash
"$FLINK_HOME/bin/flink" run \
  -c org.apache.iceberg.learning.FlinkOrdersApp \
  examples/flink-java/target/iceberg-flink-java-example-1.0.0.jar
```

之后可以分别运行实验 05 中的 Spark 和 Trino 读取脚本，验证其他引擎读取
`learning_flink_java.orders_from_flink_java`。重复运行会以固定的三条订单覆盖该
示例表数据；验证完成后使用清理参数删除该示例表：

```bash
"$FLINK_HOME/bin/flink" run \
  -c org.apache.iceberg.learning.FlinkOrdersApp \
  examples/flink-java/target/iceberg-flink-java-example-1.0.0.jar --cleanup
```

本示例已在项目当前外部环境手动验证。Flink `INSERT OVERWRITE` 提交的 Snapshot ID
为 `3163989102682253507`，Spark 回读到相同 Snapshot ID 和 `overwrite` 操作。Flink
显示的 `2026-08-19T07:19:54.118Z` 与 Spark 显示的 `2026-08-19 15:19:54.118` 分别是
UTC 和 UTC+8 的同一时刻。引擎、Catalog、存储或版本变更后，应重新执行，并在
`config/compatibility-matrix.md` 中记录实际环境和结果。
