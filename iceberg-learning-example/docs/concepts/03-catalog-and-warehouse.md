# 03 - Catalog 与 Warehouse

## Catalog

Catalog 是表名到表元数据位置之间的管理层。计算引擎通过 Catalog 完成以下操作：

- 创建和删除 Namespace。
- 创建、加载、重命名和删除表。
- 读取当前表元数据位置。
- 在并发提交时完成原子更新或报告冲突。

常见 Catalog 类型包括：

| 类型 | 适用场景 |
| --- | --- |
| REST Catalog | 服务化部署、跨引擎访问和统一管理。 |
| JDBC Catalog | 使用关系数据库保存 Catalog 元数据。 |
| Hive Catalog | 与已有 Hive Metastore 集成。 |
| Nessie Catalog | 需要分支、标签和多环境发布。 |
| Hadoop Catalog | 适合简单文件系统场景，不依赖独立服务。 |

## Warehouse

Warehouse 是表数据和表元数据文件的存储根路径，例如：

```text
s3://company-lake/warehouse
```

Catalog 管理“表叫什么、当前元数据在哪里”；Warehouse 保存“元数据文件、Manifest 和数据文件”。两者职责不同，但需要在外部环境中正确配合。

## Catalog 不是只读服务

Catalog 是一个可读写的控制面，不只是生成目录或查询元数据。以 REST Catalog 为例，它通常提供：

| 操作 | 作用 |
| --- | --- |
| 查询表 | 根据 Namespace 和表名返回当前表元数据位置。 |
| 创建表 | 注册表名、Schema、分区规范和初始元数据。 |
| 更新表 | 提交新的 Schema、Partition Spec、Snapshot 或表属性。 |
| 删除和重命名表 | 修改 Catalog 中的表注册信息。 |
| 并发提交 | 确认当前元数据位置没有被其他客户端改变，再原子更新引用。 |
| 分支和标签 | 在支持引用管理的 Catalog 中更新 Branch、Tag 等引用。 |

Catalog 服务通常不会接收每一行数据，也不负责代替 Spark 或 Flink 写入数据文件。它主要负责表级状态和提交协调。

## Catalog 和 Warehouse 可以共用后端吗

可以共用同一个 MinIO 集群，但这不是 REST Catalog 协议的强制要求。需要区分两个层次：

- **Warehouse 存储**：保存 Iceberg 的 Data File、Delete File、Manifest 和 Metadata JSON。
- **Catalog 控制面存储**：保存表注册信息、当前元数据引用、Namespace、并发提交状态，以及 Branch/Tag 等引用。

REST Catalog 规范只定义访问协议，不规定服务端必须使用哪一种持久化后端。因此不同实现可能采用：

- 内存存储：适合临时 Demo，服务重启后 Catalog 注册信息可能丢失。
- JDBC 数据库：适合需要持久化和事务控制的环境。
- Nessie 或 Polaris 自己的版本存储。
- 文件系统或对象存储：部分实现可以把控制面状态保存到 S3-compatible 存储。

所以“元信息是否存放在 MinIO 上”不能只由 REST Catalog 这个名称判断，必须查看具体服务端实现的配置。即使 Catalog 控制面使用数据库，Iceberg 的 Metadata JSON 仍然通常在 Warehouse 中。

例如，配置：

```text
CATALOG_WAREHOUSE=s3://warehouse
```

通常表示新表的默认位置从 `s3://warehouse` 开始，表的 Data File、Manifest 和 Metadata JSON 会写入对应的表路径。这不是“只用于拼接路径”，但它也不代表 REST Catalog 的控制面状态一定保存在该 S3 路径中。

在学习环境中，可以让 REST Catalog 和 Warehouse 共用同一个 MinIO 集群，例如：

```text
MinIO
├── iceberg-warehouse/   # Data File、Manifest、Metadata JSON
└── catalog-state/       # 仅当 REST Catalog 实现支持用 S3 保存控制面状态时使用
```

生产环境建议至少做到逻辑隔离：使用不同 Bucket 或 Prefix、不同访问策略和最小权限。即使底层是同一个 MinIO 集群，也不要让 Catalog 服务拥有不必要的数据读写权限。

## 一次写入的典型流程

```text
1. Spark/Flink 向 Catalog 查询当前表状态
        ↓
2. 计算引擎通过 FileIO 写 Data File 或 Delete File 到 Warehouse
        ↓
3. 计算引擎生成 Manifest 和新的 Metadata JSON 到 Warehouse
        ↓
4. 计算引擎向 Catalog 提交新的 Metadata JSON 位置
        ↓
5. Catalog 校验旧状态并原子更新当前表引用
```

如果第 5 步提交失败，新文件可能暂时留在 Warehouse 中，但因为没有被当前 Snapshot 引用，所以不会出现在表的正常查询结果中；后续维护任务可以清理这类未引用文件。

## 一次读取的典型流程

```text
1. Spark/Flink/Trino 向 Catalog 查询表
        ↓
2. Catalog 返回当前 Metadata JSON 位置
        ↓
3. 计算引擎从 Warehouse 读取 Metadata JSON、Manifest 和文件统计
        ↓
4. 计算引擎规划需要扫描的 Data File
        ↓
5. 计算引擎从 Warehouse 读取数据文件
```

因此，Catalog 主要参与表级元数据的读取和提交；查询规划和数据扫描仍由 Spark、Flink 或 Trino 完成。

## 两种“元数据”的区别

这里容易混淆两类元数据：

- **Catalog 注册信息**：表名、Namespace、当前 Metadata JSON 位置、提交版本或引用，通常保存在 Catalog 服务自己的后端。
- **Iceberg 表元数据**：Metadata JSON、Snapshot、Manifest List、Manifest，通常保存在 Warehouse。

Catalog 服务返回 Warehouse 中的元数据位置，并负责更新这个位置；它不等于 Warehouse 中的 Metadata JSON 文件。

## 配置边界

本项目不创建或部署 Catalog、对象存储和数据库，只读取外部环境配置。示例配置统一通过环境变量或本地忽略配置文件注入：

```text
Catalog URI
Warehouse location
Credential provider
Region or endpoint
```

凭据不应写入 SQL、Java 源码或提交到 Git 的配置文件。

## 选择 Catalog 时的考虑

- 个人学习可以使用外部 REST、JDBC 或 Hadoop Catalog。
- 多引擎共享表时应使用所有引擎都支持的 Catalog。
- 需要分支和标签时再引入 Nessie 或支持引用管理的 REST Catalog。
- 生产环境需要关注高可用、认证、授权和提交冲突处理。

Spark 如何根据 Catalog 注册名和三段式表名选择目标，请参见 [06 - Spark 如何选择 Iceberg Catalog](06-spark-catalog-routing.md)。
