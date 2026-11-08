# PySpark Iceberg API 示例

`iceberg_orders.py` 通过 PySpark 创建 `ice.learning_api.orders_pyspark`，执行可重复的 `MERGE`，并输出数据和 Snapshot 元数据。它不依赖 Spark Java 示例，可以单独运行。

在加载 `workdir/.env` 后，通过与项目一致的 Runtime JAR 启动：

```bash
"$SPARK_HOME/bin/spark-submit" \
  --jars "$ICEBERG_LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,$ICEBERG_LIB_DIR/iceberg-aws-bundle-1.11.0.jar" \
  examples/spark-python/iceberg_orders.py
```

如果通过独立 Python 环境运行而不是 Spark 发行版自带的 PySpark，请安装与 Spark 基线完全一致的 `pyspark==3.5.9`，依赖清单见 `requirements.txt`。

清理只删除该示例表：

```bash
"$SPARK_HOME/bin/spark-submit" \
  --jars "$ICEBERG_LIB_DIR/iceberg-spark-runtime-3.5_2.12-1.11.0.jar,$ICEBERG_LIB_DIR/iceberg-aws-bundle-1.11.0.jar" \
  examples/spark-python/iceberg_orders.py --cleanup
```

本示例已在项目当前外部环境手动验证。引擎、Catalog、存储或版本变更后，应重新执行。
