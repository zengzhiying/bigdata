# Spark Java Iceberg API 示例

`IcebergOrdersApp` 使用与项目 SQL 包装脚本相同的 REST Catalog 和 S3FileIO 配置，创建 `ice.learning_api.orders_java`，执行一次可重复的 `MERGE`，再显示行数据和 Snapshot 元数据。

## 构建与运行

先在当前 Shell 加载 `workdir/.env`，并确保 `SPARK_HOME` 和 `ICEBERG_LIB_DIR` 指向外部 Spark 与本地 Runtime JAR。构建应用：

```bash
mvn -f examples/spark-java/pom.xml package
```

提交时需要显式加载 Spark Runtime 和 AWS bundle；应用 JAR 本身不打包 Spark：

```bash
"$SPARK_HOME/bin/spark-submit" \
  --jars "$ICEBERG_LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,$ICEBERG_LIB_DIR/iceberg-aws-bundle-1.11.0.jar" \
  --class org.apache.iceberg.learning.IcebergOrdersApp \
  examples/spark-java/target/iceberg-spark-java-example-1.0.0.jar
```

第二次运行会更新同一订单而不是重复插入。只清理该示例表：

```bash
"$SPARK_HOME/bin/spark-submit" \
  --jars "$ICEBERG_LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,$ICEBERG_LIB_DIR/iceberg-aws-bundle-1.11.0.jar" \
  --class org.apache.iceberg.learning.IcebergOrdersApp \
  examples/spark-java/target/iceberg-spark-java-example-1.0.0.jar --cleanup
```

本示例已在项目当前外部环境手动验证。引擎、Catalog、存储或版本变更后，应重新执行。
