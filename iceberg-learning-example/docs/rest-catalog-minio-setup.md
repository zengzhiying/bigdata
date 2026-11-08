# REST Catalog + MinIO 最小搭建指南

## 重要说明

REST Catalog 是 Iceberg 定义的一套 HTTP 协议，不是唯一的服务端程序。本文为了学习使用一个兼容 Iceberg REST 协议的参考服务作为示例；生产环境应选择有持久化、认证和高可用能力的 REST Catalog 实现，例如 Nessie 或 Apache Polaris。

本文中的容器编排只作为外部环境准备参考，不放入本项目的 `infra/` 目录，也不由项目脚本自动启动。

## 1. 架构

```text
Spark / Flink / Trino
        │
        │ Iceberg REST API
        ▼
REST Catalog 服务 :8181
        │
        │ S3 API
        ▼
MinIO :9000
  └── warehouse bucket
```

Catalog 服务负责表名、Schema、Snapshot 和提交；MinIO 保存 Data File、Delete File、Manifest 和 Metadata JSON。Catalog 服务自己的控制面状态是否也保存到 MinIO，取决于具体服务实现，不能由 REST 协议本身推断。

## 2. 准备 MinIO

可以使用已有 MinIO，也可以在项目目录之外创建一个临时 Compose 目录。下面的命令只用于学习环境：

```bash
mkdir -p "$HOME/iceberg-rest-env"
cd "$HOME/iceberg-rest-env"
```

创建 `compose.yml`：

```yaml
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"

  rest:
    # 替换为已经确认与 Iceberg 1.11.0 兼容的 REST Catalog 镜像和版本。
    image: ${ICEBERG_REST_IMAGE:?set ICEBERG_REST_IMAGE first}
    depends_on:
      - minio
    ports:
      - "8181:8181"
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_REGION: us-east-1
      # Iceberg 表的 Warehouse 根路径，不是 REST Catalog 控制面数据库的位置。
      CATALOG_WAREHOUSE: s3://warehouse
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
```

`CATALOG_WAREHOUSE` 通常用于确定 Iceberg 表的默认位置，例如 `s3://warehouse/learning/orders`，其下会保存 Data File、Manifest 和 Metadata JSON。它不自动决定 REST Catalog 服务自己的表注册信息、当前元数据引用或版本状态保存在哪里。不同 REST Catalog 实现的镜像名称和环境变量也不同，启动前必须根据所选服务端的官方文档确认 Warehouse、S3 Endpoint、认证和控制面持久化配置；不要直接把 `latest` 用于生产环境。

启动 MinIO 和 REST 服务：

```bash
export ICEBERG_REST_IMAGE=your-rest-catalog-image:verified-tag
docker compose up -d
```

如果 REST 服务需要显式的 MinIO Endpoint，服务容器内应使用：

```text
http://minio:9000
```

宿主机上的 Spark、Flink 和 Trino 则使用：

```text
http://localhost:9000
```

## 3. 创建 Warehouse Bucket

使用 MinIO Client 创建 `warehouse` Bucket。也可以通过 MinIO Console `http://localhost:9001` 创建：

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb --ignore-existing local/warehouse
mc ls local
```

如果没有安装 `mc`，可以直接使用 Console，或者使用已有的 S3 管理工具创建 Bucket。

## 4. Spark 连接 REST Catalog

Spark 3.5.9 必须加载：

```text
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0
org.apache.iceberg:iceberg-aws-bundle:1.11.0  # 使用 S3FileIO 时需要
```

宿主机运行 Spark 时，配置中的 MinIO Endpoint 使用 `localhost`：

```bash
# 使用本地 jar 包可以将 --packages 换成 --jars
spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.ice.uri=http://localhost:8181 \
  --conf spark.sql.catalog.ice.warehouse=s3://warehouse \
  --conf spark.sql.catalog.ice.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.ice.client.region=us-east-1 \
  --conf spark.sql.catalog.ice.s3.endpoint=http://localhost:9000 \
  --conf spark.sql.catalog.ice.s3.path-style-access=true \
  --conf spark.sql.catalog.ice.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.ice.s3.secret-access-key=minioadmin
```

然后执行本项目的第一个实验：

```bash
spark-sql -f /opt/iceberg-learning-example/labs/01-quickstart/spark-sql/01_create_and_inspect.sql
```

如果 Spark 已经由集群统一提供 Runtime JAR，不要再次使用 `--packages`，避免加载多个 Iceberg 版本。

## 5. Trino 连接 REST Catalog

在 Trino 的 Catalog 配置目录创建例如 `ice.properties`。Trino 容器访问 MinIO 时，Endpoint 应使用服务名 `minio`；如果 Trino 在宿主机运行，则使用 `localhost`：

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:8181
iceberg.rest-catalog.warehouse=s3://warehouse

fs.native-s3.enabled=true
s3.endpoint=http://localhost:9000
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

重启 Trino 后验证：

```sql
SHOW SCHEMAS FROM ice;
SHOW TABLES FROM ice.learning;
SELECT COUNT(*) FROM ice.learning.orders;
```

Trino 的 S3 属性可能随部署方式或版本变化，若启动时报未知配置项，应以 Trino 476 的 Iceberg Connector 文档为准。

## 6. Flink 连接 REST Catalog

Flink 1.20.5 需要加载：

```text
org.apache.iceberg:iceberg-flink-runtime-1.20:1.11.0
```

Flink SQL 示例：

```sql
CREATE CATALOG ice WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://localhost:8181',
    'warehouse' = 's3://warehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'client.region' = 'us-east-1',
    's3.endpoint' = 'http://localhost:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'minioadmin',
    's3.secret-access-key' = 'minioadmin'
);
```

## 7. 最小验证

按顺序执行：

1. MinIO Console 可以访问，`warehouse` Bucket 存在。
2. REST Catalog 的健康检查或根 API 可以访问。
3. Spark 可以执行 `CREATE NAMESPACE` 和 `CREATE TABLE`。
4. MinIO 的 `warehouse` Bucket 出现 Metadata JSON 和数据文件。
5. Trino 可以查询同一张表。
6. Spark 查询 `table.snapshots`，并使用 Snapshot ID 执行 Time Travel。
7. Flink 读取同一张表，确认跨引擎可见。

## 8. Warehouse 一致性

同一个共享 Catalog 下，Spark、Flink 和 Trino 必须最终解析到同一个表位置。最简单且最安全的做法是统一配置：

```text
s3://warehouse
```

需要注意：

- REST Catalog 服务通常会记录每张表的实际 Location，服务端记录是已存在表的权威来源。
- 客户端的 `warehouse` 配置在不同实现中可能是默认值，也可能被服务端返回的 Location 覆盖。
- 即使客户端配置可以省略，也不要让 Spark、Flink 和 Trino 使用不同的默认 Warehouse。
- Docker 容器内和宿主机上的 MinIO Endpoint 可以不同，例如 `http://minio:9000` 与 `http://localhost:9000`，但必须指向同一个 MinIO 服务和同一个 Bucket。
- 如果新建表时使用了不同 Warehouse，可能造成新表位置错误、权限异常或跨引擎无法读取。

因此当前项目建议所有引擎统一使用：

```text
Catalog URI: https://catalog.example.com
Warehouse:   s3://warehouse
S3 Endpoint: 同一个 MinIO 或 S3 服务
```

## 9. Catalog 状态和 Warehouse 的存储关系

MinIO 可以同时作为 Warehouse 和某些 REST Catalog 实现的持久化后端，但两者仍是不同的逻辑数据：

```text
MinIO
├── iceberg-warehouse/   # Iceberg 表数据和表元数据文件
└── catalog-state/       # 可选：REST Catalog 控制面状态
```

如果 REST Catalog 使用内存或数据库保存控制面状态，MinIO 中仍然会有 Iceberg Metadata JSON，但 REST 服务重启后是否还能通过表名找到这些文件，取决于控制面状态是否持久化。

## 10. 学习环境与生产环境的差异

本文使用的最小配置只适合学习：

- `minioadmin/minioadmin` 只能用于本地测试。
- REST 服务必须使用持久化后端，不能依赖内存状态。
- 生产环境需要 TLS、认证、授权、密钥管理和审计。
- REST 服务、MinIO 和 Trino 应使用固定版本或镜像 digest。
- Spark、Flink、Trino 中只能加载与项目基线一致的 Iceberg Runtime 或 Connector。
