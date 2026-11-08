# 实验 07 - Metadata JSON、Manifest 与提交协议

## 实验目标

本实验进入阶段 7，以独立的 Spark 表建立一个小型、可重复的提交历史，并观察：

1. Metadata JSON、Snapshot、Manifest List、Manifest 和 Data File 的引用链。
2. 数据提交与仅修改表属性的 Metadata 提交之间的区别。
3. Catalog 如何以当前 Metadata 位置作为乐观并发提交的比较基准。

实验不依赖实验 06 的 CDC 表或外部输入状态。SQL 使用 Spark；直接读取 Metadata JSON
和模拟旧 Metadata 提交冲突的部分使用项目内的 Java 工具。

## 前置条件

- 外部 Spark `3.5.9`、Iceberg `1.11.0`、Java `17`，并按项目的 REST Catalog 和
  对象存储配置启动。Catalog 名默认为 `ice`；如不同，请替换 SQL 中的 `ice`。
- 本地构建 Java 检查器时使用 `/usr/local/jdk-17.0.14+7`，并确保 Maven 可用。
- 表 `ice.learning_metadata.orders` 为本实验专用；不要让其他写入者同时修改它。
- Java 工具从进程环境读取 `CATALOG_URI`、`CATALOG_WAREHOUSE`、`S3_ENDPOINT`、
  `MINIO_ACCESS_KEY_ID`、`MINIO_SECRET_ACCESS_KEY` 等参数，可从未提交的
  `workdir/.env` 加载。

## 文件顺序

```text
spark-sql/00_reset.sql
spark-sql/01_create_and_commit.sql
spark-sql/02_inspect_metadata.sql
examples/iceberg-java-metadata/MetadataInspectorApp --metadata-json
examples/iceberg-java-metadata/MetadataInspectorApp --demonstrate-conflict  # 预期捕获冲突
spark-sql/04_validate.sql
spark-sql/05_cleanup.sql
```

先创建包含三次数据提交和一次 Metadata-only 提交的表：

```bash
./scripts/run-spark-sql.sh \
  labs/07-metadata-and-commit-protocol/spark-sql/00_reset.sql \
  labs/07-metadata-and-commit-protocol/spark-sql/01_create_and_commit.sql \
  labs/07-metadata-and-commit-protocol/spark-sql/02_inspect_metadata.sql
```

随后按 [`examples/iceberg-java-metadata/README.md`](../../examples/iceberg-java-metadata/README.md)
构建并运行检查器。运行 `--metadata-json` 时，将输出中的 `metadata_file`、
`manifest_list` 和 `manifest` 路径与 Spark 元数据表中的 `file`、`manifest_list` 和
`path` 对照，建立完整引用链。

## 观察重点

- `metadata_log_entries` 的每一行都是较早的 Metadata JSON 文件。`ALTER TABLE SET
  TBLPROPERTIES` 没有写数据文件或新 Snapshot，但会让当前 Metadata JSON 前进。
- `snapshots.manifest_list` 固定某次 Snapshot 使用的 Manifest 集合；`manifests.path`
  则是 Manifest 文件的位置。`files` 只显示当前有效文件，`all_entries` 还保留已删除
  或替换文件的历史条目。
- Java 检查器调用 `TableMetadataParser.toJson` 输出 Catalog 当前加载的真实 Metadata
  JSON，而不是根据 Warehouse 路径猜测文件名。
- `--demonstrate-conflict` 让两个独立客户端使用同一份旧 Metadata 添加不同 Schema
  字段。第一次成功后，第二次必须失败，说明 Catalog 会拒绝依赖过期 Schema 的提交。
  不同的 Metadata 更新并不必然冲突，例如两个独立的表属性更新可以被 REST Catalog
  合并；应用层应重新加载元数据、判断业务语义后再重试，而不是盲目重复提交。

## 预期结果

- 数据表保留 `7101`、`7102`、`7103` 三行，其中 `7102` 为 `PAID`、金额 `26.00`。
- `snapshots` 至少有三次数据提交；`metadata_log_entries` 至少有一条历史 Metadata
  文件记录，具体数量取决于 Catalog 和 Runtime。
- `manifests` 至少返回一个 Manifest，Java 工具输出对应的 Manifest List 与 Manifest
  路径。
- 并发演示输出 `concurrent_commit_check=PASS`，且不会让基于旧 Schema 的第二个字段提交覆盖第一个字段。
- `04_validate.sql` 的三项检查均返回 `PASS`。

## 失败路径和安全边界

`MetadataInspectorApp --demonstrate-conflict` 中第二次提交预期抛出并捕获
`CommitFailedException`。若该提交意外成功，程序会以非零状态失败；不要把该情况视为
正常结果，也不要通过直接编辑 Metadata JSON 修复。

该演示直接使用 `TableOperations`，只适合隔离的学习表。生产写入应使用 Spark、Flink
或 Iceberg 的高层事务 API，并根据提交失败类型实施有界重试和业务冲突处理。

## 清理

执行 `spark-sql/05_cleanup.sql`，它只删除本实验表：

```bash
./scripts/run-spark-sql.sh labs/07-metadata-and-commit-protocol/spark-sql/05_cleanup.sql
```
