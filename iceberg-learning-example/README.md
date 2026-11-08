# Apache Iceberg 学习示例

这是一个以学习、实验和可复用示例为核心的 Apache Iceberg 项目。项目不部署 MinIO、PostgreSQL、Spark、Flink、Trino 或其他基础设施；这些服务由外部环境提供，本项目通过配置连接并验证行为。

## 版本基线

主学习路径采用以下版本线；集成验证状态记录在兼容性矩阵中：

| 组件 | 版本 |
| --- | --- |
| Apache Iceberg | `1.11.0` |
| Apache Spark | `3.5.9` |
| Spark Scala binary version | `2.12` |
| Apache Flink | `1.20.5` |
| Trino | `476` |
| Java | `17` |
| Python | `3.11` |

版本唯一来源和验证状态请参见 [`config/versions.properties`](config/versions.properties) 与 [`config/compatibility-matrix.md`](config/compatibility-matrix.md)。

## 项目目录

- `LEARNING_PLAN.md` - 分阶段学习路径和里程碑。
- [`docs/`](docs/README.md) - 概念、引擎行为、内部原理、性能、生产实践和常见问题笔记。
- `examples/` - Spark Java、PySpark、Flink Java、PyIceberg 和 Iceberg Java 元数据 API 示例。
- `labs/` - 包含前置条件、命令、预期结果和清理步骤的引导式实验。
- `datasets/` - 可重复的订单 CDC 事件生成器；生成数据不会提交到仓库。
- `config/` - 版本锁定和外部环境配置模板。密钥不得提交到 Git。
- `scripts/` - 环境检查和示例运行脚本，不负责部署基础设施。

## 快速开始

1. 按 [`docs/environment-setup.md`](docs/environment-setup.md) 准备或确认外部基础环境。
2. 按 [`docs/development-environment.md`](docs/development-environment.md) 的说明启用项目级 Java 17。
3. 将 `workdir/.env.example` 复制为被忽略的 `workdir/.env`，填写 Spark 与 Flink 运行所需的外部服务地址、凭据和安装路径；`config/application.example.yml` 作为其他引擎的声明式配置参考。
4. 运行 `scripts/check-environment.sh` 检查本地工具和配置。
5. 从 [`labs/01-quickstart/README.md`](labs/01-quickstart/README.md) 开始。

当前已落地的实验包括：

- `labs/01-quickstart`：Spark CRUD、MERGE、元数据、Time Travel 和 Trino 读取。
- `labs/02-schema-partition-evolution`：字段演进、字段 ID、隐藏分区和 Partition Spec 演进（详见 `labs/02-schema-partition-evolution/README.md`）。
- `labs/03-snapshot-governance`：Snapshot 历史、回滚、过期、Branch、Tag 和 WAP（详见 `labs/03-snapshot-governance/README.md`）。
- `labs/04-delete-files`：Format V2、Merge-on-Read、Position Delete/Equality Delete 元数据观察（详见 `labs/04-delete-files/README.md`）。
- `labs/05-flink-interoperability`：Flink SQL/Java 读取与写入、Trino 读取 Flink 表、Trino 写入与 Spark 回读的跨引擎案例（详见 `labs/05-flink-interoperability/README.md`）。
- `labs/06-streaming-cdc`：Flink 流式 append、Checkpoint、重复/乱序 CDC 事件、Spark 当前状态物化、固定 Changelog Upsert Fixture 和跨引擎回读（详见 `labs/06-streaming-cdc/README.md`；当前路径已手动验证）。
- `labs/07-metadata-and-commit-protocol`：Metadata JSON、Manifest List、Manifest、Metadata-only 提交和乐观并发冲突（详见 `labs/07-metadata-and-commit-protocol/README.md`；已在当前外部 REST Catalog 与对象存储环境手动验证）。
- `labs/08-performance-and-maintenance`：小文件诊断、Data File bin-pack/排序重写、Manifest 重写、Snapshot 过期和孤儿文件 dry-run（详见 `labs/08-performance-and-maintenance/README.md`；已在当前外部 REST Catalog、MinIO 与 Spark 环境手动验证）。
- `labs/09-production-practices`：数据质量断言、表健康报告、错误写入回滚、订单表设计、最小权限契约与隔离备份恢复演练（详见 `labs/09-production-practices/README.md`；当前已落地路径均已于 2026-09-07 完成外部环境手动验证）。
- `labs/10-order-lakehouse`：Flink CDC 原始事件、Spark 当前态物化与 Schema Evolution、质量门禁、指标构建、受控维护和 Trino 跨引擎回读（详见 `labs/10-order-lakehouse/README.md`；已于 2026-09-15 完成当前外部环境端到端手动验证）。

当前已落地的 API 示例包括：

- `examples/spark-java`：创建、MERGE、读取和清理 REST Catalog 中的 Iceberg 表。
- `examples/spark-python`：使用 PySpark 执行同等的 Spark API 路径。
- `examples/flink-java`：使用 Flink Table API 创建、覆盖写入、读取和清理 REST Catalog 中的 Iceberg 表。
- `examples/pyiceberg`：直接通过 PyIceberg 读取已有表的 Schema 和 Snapshot 元数据。
- `examples/iceberg-java-metadata`：直接通过 Iceberg REST Catalog API 检查 Metadata JSON 和 Manifest 引用，并演示旧 Metadata 的提交冲突。

`labs/03-snapshot-governance` 和 `labs/04-delete-files` 已在项目当前外部 Spark 环境手动验证。实验 05 的 Flink SQL/Java、Trino 读取 Flink、Trino 写入和高级特性案例，以及 Spark Java、PySpark、PyIceberg API 示例均已在当前外部环境完成手动验证。Flink Java 写入后，Flink 和 Spark 返回相同的 Snapshot ID；Flink 使用 UTC 文本显示提交时刻，Spark 按会话时区显示同一时刻。Branch、Tag 仍需要支持引用管理的 Catalog，WAP 仍需要对应 Runtime 的 `publish_changes` 支持；Delete File 的实际类型应通过元数据表确认。Trino 和 Flink 对这些高级能力的覆盖范围见兼容性矩阵。

实验 06 的基础 CDC 路径与 Flink 原生 Upsert 固定 Changelog Fixture 均已在当前外部环境手动验证：文件系统 append-only Source 的流式摄取和 Checkpoint、重复/乱序事件处理、Spark 当前状态物化、元数据观察及 Trino 回读均已完成。固定 Fixture 写入一个原子 Snapshot，其中包含一个 Data File 和两个 Equality Delete File；该 Fixture 不引入 Debezium、Kafka 等外部 Connector。

遇到 Spark 离线依赖、`--jars`、S3FileIO、REST namespace 或 MinIO Region 问题时，参见 [`docs/faq-spark-rest-minio.md`](docs/faq-spark-rest-minio.md)。

Spark DataFrame 追加、动态分区覆盖、`MERGE INTO` 的选择，以及它们与 Hudi 写入语义的差异，参见 [`docs/concepts/09-spark-write-patterns-and-hudi.md`](docs/concepts/09-spark-write-patterns-and-hudi.md)。

示例会明确说明由哪个引擎执行。提交任务到集群的命令属于用户的外部环境，本项目只记录用法，不负责部署集群。

阶段 10 将前述能力组合为独立 namespace 的端到端路径，并已于 2026-09-15 在当前外部
Spark、Flink 和 Trino 环境完成手动验证。它的 Flink 文件 Source 仍是 append-only；重复、
乱序和逻辑删除由 Spark 的确定性 `MERGE` 物化为当前态，Flink 原生 Changelog Upsert 的
独立语义验证仍见实验 06。该验证状态只适用于当前版本基线和外部环境。

当外部 Spark Worker 使用兼容的 Python 运行时，Python `3.12` 也可用于数据生成器、PyIceberg 和 PySpark 示例。Python `3.11` 仍作为可复现基线。

## 范围约定

每个示例都应在没有隐藏本地状态的情况下重复运行，并说明所需 Catalog、Warehouse、引擎版本、输入数据、预期输出和清理步骤。不要提交凭据、生成数据、集群日志或下载的 Runtime JAR。
