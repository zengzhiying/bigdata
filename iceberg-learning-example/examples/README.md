# API 示例

本目录中的示例展示如何通过 API 连接项目默认的 REST Catalog。它们不替代 `labs/` 中的引导式 SQL 实验；每个示例都使用 `learning_api` 下的专用表，以便独立运行和清理。

- `spark-java/`：使用 Spark Java API 创建、MERGE、读取和清理 Iceberg 表。
- `spark-python/`：使用 PySpark 完成同样的 Spark API 路径。
- `flink-java/`：使用 Flink Table API 创建、覆盖写入、读取和清理 Iceberg 表。
- `pyiceberg/`：使用 PyIceberg 加载一个已存在的表并读取 Schema、当前位置和 Snapshot 元数据。
- `iceberg-java-metadata/`：直接通过 Iceberg REST Catalog API 检查 Metadata JSON、Manifest List、Manifest，并演示乐观并发提交冲突。

所有示例从进程环境读取下列连接参数，名称与 `workdir/.env.example` 保持一致：`CATALOG_NAME`、`CATALOG_URI`、`CATALOG_WAREHOUSE`、`S3_ENDPOINT`、`AWS_REGION`、`MINIO_ACCESS_KEY_ID` 和 `MINIO_SECRET_ACCESS_KEY`。执行前可在当前 Shell 中加载本地文件：

```bash
set -a
source workdir/.env
set +a
```

不要提交实际凭据。示例创建的表可通过各目录 README 中的 `--cleanup` 命令删除。
