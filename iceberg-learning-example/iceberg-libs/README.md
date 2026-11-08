# 下载 Iceberg 的 Spark 和 Flink Runtime 依赖

`pom.xml` 会下载以下 Iceberg `1.11.0` 依赖：

- `iceberg-spark-runtime-3.5_2.12`，供 Spark `3.5.9` 使用。
- `iceberg-flink-runtime-1.20`，供 Flink `1.20.5` 使用。
- `iceberg-aws-bundle`，供使用 `org.apache.iceberg.aws.s3.S3FileIO` 的 Spark 或 Flink 使用。
- `slf4j-api`，供独立运行的 Iceberg Java 元数据检查器加载日志 API。

Spark 和 Flink Runtime 不可混用。`workdir/spark-sql.sh` 加载 `iceberg-libs/` 中的 Spark Runtime 和 AWS bundle；将 Flink Runtime 与 AWS bundle 安装到 `$FLINK_HOME/lib/`，`workdir/flink-sql.sh` 会从全局类路径加载它们，不能再通过 `-j` 重复加载。Flink 使用完整 Hadoop 客户端发行版：设置 `HADOOP_HOME` 后，包装脚本会自动导出 `HADOOP_CONF_DIR` 与 `HADOOP_CLASSPATH`。不要把 Hadoop 的完整传递依赖集合复制到 `$FLINK_HOME/lib/`，否则可能覆盖 Flink 自带的库。离线下载、多个本地 JAR、AWS SDK 版本和 Region 配置说明见 [`../docs/faq-spark-rest-minio.md`](../docs/faq-spark-rest-minio.md)。

在当前目录下执行：

```bash
mvn -f pom.xml dependency:copy-dependencies \
    -DoutputDirectory=./ \
    -DincludeScope=runtime
```
