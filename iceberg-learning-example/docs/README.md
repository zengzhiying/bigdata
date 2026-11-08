# 文档导航

本目录记录 Apache Iceberg 的概念、外部环境配置和问题排查。学习实验的可执行
步骤、预期结果和清理方式位于 [`labs/`](../labs/)；本文档只提供背景、决策依据和
导航，不重复实验内容。

## 推荐学习路径

1. [环境准备](environment-setup.md) 与 [项目级 Java 17 配置](development-environment.md)：
   先连接外部 Spark、Flink、Trino、REST Catalog 和对象存储环境。
2. [Iceberg 概览](concepts/01-iceberg-overview.md)、[表格式](concepts/02-table-format.md) 与
   [Catalog 和 Warehouse](concepts/03-catalog-and-warehouse.md)：理解表、文件和元数据的基本边界。
3. 从 [`labs/01-quickstart/`](../labs/01-quickstart/) 开始运行 CRUD、元数据查询和 Time Travel，
   再结合 [Metadata 结构](concepts/04-metadata-structure.md) 与
   [Snapshot 和 ACID](concepts/05-snapshot-and-acid.md) 阅读。
4. 依次完成 Schema/分区演进、Snapshot 治理和多引擎互操作：
   [Schema 和 Partition Evolution](concepts/07-schema-and-partition-evolution.md)、
   [Snapshot 治理和 Delete File](concepts/08-snapshot-governance-and-delete-files.md) 与
   [`labs/02-schema-partition-evolution/`](../labs/02-schema-partition-evolution/)、
   [`labs/03-snapshot-governance/`](../labs/03-snapshot-governance/)、
   [`labs/04-delete-files/`](../labs/04-delete-files/) 和
   [`labs/05-flink-interoperability/`](../labs/05-flink-interoperability/)。
5. 完成流式、元数据、维护和生产实践：
   [流式 CDC](concepts/10-streaming-cdc.md)、[性能与维护](concepts/11-performance-and-maintenance.md)、
   [生产质量与恢复](concepts/12-production-quality-and-recovery.md)、
   [表设计、权限与备份](concepts/13-table-design-access-and-backup.md)，以及实验 06--10。

完整的阶段目标、验收标准与验证状态见 [`LEARNING_PLAN.md`](../LEARNING_PLAN.md)。

## 按主题查找

| 主题 | 概念与决策 | 可执行验证 |
| --- | --- | --- |
| Catalog、Warehouse 与 Spark 路由 | [Catalog 和 Warehouse](concepts/03-catalog-and-warehouse.md)、[Spark Catalog 路由](concepts/06-spark-catalog-routing.md)、[Catalog 选型](catalog-selection.md) | [`labs/01-quickstart/`](../labs/01-quickstart/)、[`labs/05-flink-interoperability/`](../labs/05-flink-interoperability/) |
| Schema、分区和写入语义 | [Schema 和 Partition Evolution](concepts/07-schema-and-partition-evolution.md)、[Spark 写入模式与 Hudi](concepts/09-spark-write-patterns-and-hudi.md) | [`labs/02-schema-partition-evolution/`](../labs/02-schema-partition-evolution/) |
| Snapshot、引用与 Delete File | [Snapshot 和 ACID](concepts/05-snapshot-and-acid.md)、[Snapshot 治理和 Delete File](concepts/08-snapshot-governance-and-delete-files.md) | [`labs/03-snapshot-governance/`](../labs/03-snapshot-governance/)、[`labs/04-delete-files/`](../labs/04-delete-files/) |
| 流式和 CDC | [流式 CDC 与 Iceberg 当前状态](concepts/10-streaming-cdc.md) | [`labs/06-streaming-cdc/`](../labs/06-streaming-cdc/)、[`labs/10-order-lakehouse/`](../labs/10-order-lakehouse/) |
| 元数据与提交协议 | [Metadata 结构](concepts/04-metadata-structure.md) | [`labs/07-metadata-and-commit-protocol/`](../labs/07-metadata-and-commit-protocol/)、[`examples/iceberg-java-metadata/`](../examples/iceberg-java-metadata/) |
| 性能与维护 | [性能与维护](concepts/11-performance-and-maintenance.md) | [`labs/08-performance-and-maintenance/`](../labs/08-performance-and-maintenance/) |
| 数据质量、表设计、权限和恢复 | [生产质量与恢复](concepts/12-production-quality-and-recovery.md)、[表设计、权限与备份](concepts/13-table-design-access-and-backup.md) | [`labs/09-production-practices/`](../labs/09-production-practices/)、[`labs/10-order-lakehouse/`](../labs/10-order-lakehouse/) |

## 环境与排障

- [REST Catalog 与 MinIO 配置](rest-catalog-minio-setup.md)：外部基础设施的连接配置和验证。
- [Spark、REST Catalog 和 MinIO 常见问题](faq-spark-rest-minio.md)：依赖、S3FileIO、namespace 和 Region 问题的排查入口。
- [`config/compatibility-matrix.md`](../config/compatibility-matrix.md)：当前版本基线下 Spark、Flink 和 Trino 的能力范围与手动验证状态。

## 维护约定

新主题优先补充到最贴近学习阶段的 `concepts/` 文档，并从对应 lab 建立双向链接。
只有某个主题需要独立、长期维护的多篇文章时，才新增新的文档分类目录；避免将相同内容
同时放入按阶段和按主题的两套目录结构。
