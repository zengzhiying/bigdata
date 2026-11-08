# Iceberg 学习计划

本计划从表格式基本概念开始，逐步覆盖日常开发、高级特性、跨引擎应用和生产最佳实践。每个实验都应同时提供概念说明、可运行代码、预期结果和清理方式。

## 学习阶段

1. **快速开始与术语**
   - Catalog、Namespace、Table、Data File、Delete File、Manifest、Snapshot。
   - 创建第一张表，写入、读取并查看元数据。
2. **表操作**
   - INSERT、UPDATE、DELETE、MERGE、表属性和元数据表。
3. **Schema 和分区演进**
   - 字段 ID、字段重命名、字段新增、隐藏分区、分区变更和排序。
4. **快照与治理**
   - Time Travel、回滚、快照保留、Branch、Tag 和 WAP。
5. **多引擎互操作**
   - Spark SQL/Java/Python、Flink SQL/Java、Trino SQL 的读写差异。
6. **流处理与 CDC**
   - 流式写入、Checkpoint、Upsert、Equality Delete、Position Delete 和乱序数据。
7. **元数据与内部原理**
   - Metadata JSON、Manifest List、Manifest、提交协议、并发冲突和 Catalog。
8. **性能与维护**
   - 小文件、Manifest 合并、数据文件重写、排序、快照过期和孤儿文件清理。
9. **生产实践**
   - 表设计、安全、权限、监控、数据质量和备份恢复。
10. **综合项目**
    - 构建一个跨引擎的订单湖仓案例，完成批流写入、CDC、演进、治理和性能验证。

## 验收标准

- 每个实验都可以使用外部环境独立运行，且不依赖隐藏状态。
- 关键表状态通过快照、元数据表或查询结果验证，而不是只打印日志。
- 至少提供正常路径、失败路径和清理路径。
- Spark、Flink 和 Trino 的行为差异记录在对应实验中。
- 所有版本、Catalog 地址和存储凭证通过配置或环境变量注入。

## 当前进度

- [x] 阶段 1：概念文档和 Spark/Trino 快速开始。
- [x] 阶段 2：INSERT、UPDATE、DELETE、MERGE、元数据表和 Time Travel。
- [x] 阶段 3 的基础部分：Schema Evolution 和 Partition Evolution。
- [x] 阶段 4：Snapshot 治理、Branch、Tag、WAP 和 Delete File。
- [x] 阶段 5：Spark Java/PySpark/PyIceberg、Flink SQL/Java 与 Trino 的当前读写和高级特性案例均已在当前外部环境完成手动验证；跨引擎 Snapshot ID 一致，时间显示按各引擎会话时区解释。
- [x] 阶段 6：Flink 流式 append、Checkpoint、可重复 CDC 事件、重复/乱序去重、Spark 当前状态物化、Delete File 观察、Trino 回读和 Flink 原生 Upsert 固定 Changelog Fixture 均已在当前外部环境手动验证；Fixture 的一个原子 Snapshot 包含一个 Data File 和两个 Equality Delete File。
- [x] 阶段 7：Metadata JSON、Manifest List、Manifest、Metadata-only 提交和乐观并发冲突实验已在当前外部 REST Catalog 与对象存储环境手动验证。
- [x] 阶段 8：小文件诊断、Data File bin-pack/排序重写、Manifest 重写、Snapshot 过期和 `S3FileIO` Prefix Listing 孤儿文件 dry-run 已在当前外部 REST Catalog、MinIO 与 Spark 环境手动验证；真实孤儿文件删除不属于仓库自动化路径。
- [x] 阶段 9：数据质量、表健康报告、坏数据与误操作回滚、订单表设计、最小权限契约和隔离备份恢复演练均已于 2026-09-07 在当前外部环境完成手动验证。
- [x] 阶段 10：跨引擎订单湖仓综合实验已于 2026-09-15 在当前外部 Spark、Flink 和 Trino 环境完成端到端手动验收。

阶段 4 的实验已落地并完成外部环境手动验证：

- `labs/03-snapshot-governance`：Snapshot 历史、Time Travel、Rollback、Snapshot 过期、Branch、Tag 和 WAP。
- `labs/04-delete-files`：Format Version 2、Merge-on-Read、行级变更和 Delete File 元数据。

上述实验已按项目当前外部环境手动验证通过。该状态只适用于当前版本基线；生产环境的升级回归和基础设施变更验证应遵循所属团队的运维流程，不作为本项目学习案例的一部分。

## 下一步计划

1. 主学习计划阶段 1–10 均已完成；后续版本、Catalog 或基础设施变更后，按对应实验执行回归验证并更新兼容性矩阵。
