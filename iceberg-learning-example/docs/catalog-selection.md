# Catalog 选型指南

没有 Hadoop 或 Hive 环境时，Iceberg 仍然可以正常使用。Catalog 不等于 Hadoop 或 Hive，关键是选择一个能被 Spark、Flink 和 Trino 共同访问的 Catalog 实现。

## 推荐结论

本项目建议采用以下顺序：

1. **REST Catalog：主学习路径和跨引擎默认选择。**
2. **Nessie Catalog：学习 Branch、Tag、WAP 和多环境发布时使用。**
3. **JDBC Catalog：已有 PostgreSQL/MySQL 时作为简单替代方案。**
4. **Hadoop Catalog：仅用于本地单机实验和自动化测试。**

## 对比

| Catalog | 是否需要 Hadoop/Hive | 是否需要数据库 | 适合场景 | 建议 |
| --- | --- | --- | --- | --- |
| REST Catalog | 否 | 取决于服务端实现 | 多引擎共享、标准化接入 | 首选 |
| Nessie | 否 | 取决于部署方式 | 分支、标签、WAP、多环境 | 高级实验 |
| JDBC Catalog | 否 | 是 | 已有关系数据库、部署简单 | 可选 |
| Hadoop Catalog | 否 | 否 | 单机本地文件系统 | 仅入门 |
| In-memory Catalog | 否 | 否 | 单元测试 | 仅测试 |
| Hive Catalog | 通常需要 Hive Metastore | 否 | 已有 Hive 平台 | 当前不推荐 |

## 1. REST Catalog：默认选择

REST Catalog 是 Iceberg 的标准化服务接口。Spark、Flink 和 Trino 通过 HTTP 访问 Catalog，数据文件仍然保存在 S3、S3-compatible 存储或其他 Warehouse 中。

它的优点是：

- 不依赖 Hadoop 和 Hive Metastore。
- 适合 Spark、Flink、Trino 跨引擎访问。
- Catalog 地址和存储地址解耦。
- 可以由不同的 REST Catalog 服务实现提供后端存储。
- 更接近现代湖仓生产架构。

Spark 配置示例：

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.ice.uri=https://catalog.example.com
spark.sql.catalog.ice.warehouse=s3://warehouse
```

Flink SQL 配置示例：

```sql
CREATE CATALOG ice WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'https://catalog.example.com',
    'warehouse' = 's3://warehouse'
);
```

Trino Catalog 配置示例：

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://catalog.example.com
iceberg.rest-catalog.warehouse=s3://warehouse
```

具体认证方式、S3 凭据、TLS 和 Warehouse 参数由外部 Catalog 服务提供，本项目不保存密钥。

## 2. Nessie：高级特性选择

Nessie 可以作为 Iceberg Catalog，并提供类似 Git 的 Branch 和 Tag 能力。它适合学习：

- 开发分支和生产分支。
- Tag 固定发布版本。
- WAP 隔离写入。
- 多环境数据发布。
- 并行实验和回滚。

Nessie 不要求 Hadoop 或 Hive，但仍需要一个可供表数据使用的 Warehouse。对于只学习 CRUD、快照和元数据的阶段，不必一开始引入 Nessie；在 Branch、Tag 阶段再接入更合适。

## 3. JDBC Catalog：已有数据库时使用

JDBC Catalog 使用 PostgreSQL 或 MySQL 保存 Catalog 元数据，不依赖 Hive Metastore。它适合已有关系数据库、希望使用较简单服务组合的环境。

需要注意：

- 数据库只保存 Catalog 元数据，数据文件仍然保存到 Warehouse。
- 需要管理数据库连接、权限、备份和高可用。
- 跨引擎使用前必须确认 Spark、Flink、Trino 的 JDBC Catalog 配置一致。

如果没有现成数据库，不建议为了入门实验专门引入 JDBC Catalog。

## 4. Hadoop Catalog：单机模式

Hadoop Catalog 不需要 Hadoop 集群，它也可以使用本地文件系统或共享文件系统路径。因此它适合快速开始：

```text
spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.hadoop.HadoopCatalog
spark.sql.catalog.ice.warehouse=/data/iceberg-learning/warehouse
```

但它不适合作为远程多节点环境的长期方案：所有进程必须看到同一个路径，权限和并发语义也依赖底层文件系统。

## 本项目的选择

- `labs/01-quickstart` 可以使用 Hadoop Catalog 完成单机学习。
- 多引擎实验优先使用 REST Catalog。
- Branch、Tag 和 WAP 实验增加 Nessie Catalog 配置。
- JDBC Catalog 作为独立对比实验，不作为主路径依赖。

REST Catalog 与 MinIO 的最小学习环境准备步骤见 [REST Catalog + MinIO 最小搭建指南](rest-catalog-minio-setup.md)。
