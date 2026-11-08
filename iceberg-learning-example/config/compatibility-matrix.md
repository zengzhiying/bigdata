# 兼容性矩阵

本矩阵描述项目版本基线。`已验证` 表示对应实验已经在外部环境运行过；`待验证` 表示版本已经选定，但仍需要执行集成验证。

| Iceberg | 引擎 | Runtime 坐标 / Connector | 状态 | 说明 |
| --- | --- | --- | --- | --- |
| 1.11.0 | Spark 3.5.9、Scala 2.12 | `iceberg-spark-runtime-3.5_2.12:1.11.0` | 已验证（实验 01–10） | Spark 补丁版本使用 3.5 Runtime 版本线；实验 10 已于 2026-09-15 由项目维护者确认完成当前外部环境端到端手动验证。 |
| 1.11.0 | Flink 1.20.5 | `iceberg-flink-runtime-1.20:1.11.0` | 已验证（Flink SQL） | Iceberg Runtime 按 Flink 1.20 版本线发布；REST Catalog 的 Flink SQL 读 Spark / 写 Iceberg / Spark 回读实验已在当前外部环境手动验证。 |
| 1.11.0 | Trino 476 | Iceberg Connector | 已验证（已有实验） | 已有实验的读取和元数据路径已手动验证；需要单独验证各项高级特性的 Connector 支持情况。 |
| 1.11.0 | Python 3.11/3.12 | PyIceberg `0.9.1` | 已验证（当前示例） | PyIceberg 使用独立的版本线，REST Catalog 元数据读取示例已在当前外部环境手动验证。 |

## 验证清单

- 确认 Spark 和 Flink 只加载一个 Iceberg Runtime JAR 版本。
- 确认 Spark 使用 Scala 2.12 和 Java 17。
- 分别使用 Spark、Flink 和 Trino 创建并读取表。
- 验证 Schema Evolution、Partition Evolution、Time Travel、MERGE 和 DELETE。
- 验证不同引擎对 Equality Delete 和 Position Delete 的行为。
- 将不支持的特性或引擎差异记录在对应实验中。

## 实验验证状态

| 实验 | Spark SQL | Trino | Flink | 状态 |
| --- | --- | --- | --- | --- |
| `labs/01-quickstart` | CRUD、MERGE、Snapshot、Time Travel | 读取、元数据、Time Travel | 未覆盖 | 已由项目维护者手动验证 Spark/Trino；Flink 未覆盖 |
| `labs/02-schema-partition-evolution` | Schema 和 Partition Spec 演进 | 读取和元数据 | 未覆盖 | 已由项目维护者手动验证 Spark/Trino；Flink 未覆盖 |
| `labs/03-snapshot-governance` | Snapshot、Rollback、过期、Branch/Tag/WAP（后者依赖 Catalog） | 未覆盖 | 未覆盖 | 已由项目维护者在外部 Spark 环境手动验证 |
| `labs/04-delete-files` | Format V2、Merge-on-Read、Delete File 元数据 | 未覆盖 | 未覆盖 | 已由项目维护者在外部 Spark 环境手动验证 |
| `labs/05-flink-interoperability` | 读取 Flink SQL/Java 和 Trino 写入表；Snapshot、Files 元数据 | 读取 Flink SQL/Java 表；创建、Schema Evolution、分区、MERGE、UPDATE、DELETE 和元数据观察 | SQL 读取 Spark 表、写表、Snapshot 元数据；Java Table API 读写 | 当前全部路径已在外部环境手动验证；Flink Java `INSERT OVERWRITE` 写入的 Snapshot ID 与 Spark 一致，Flink 的 UTC 时间和 Spark 的 UTC+8 显示相差 8 小时但表示同一时刻 |
| `labs/06-streaming-cdc` | 创建原始/当前表、按事件 ID 与版本物化当前状态、Snapshot 与 Delete File 观察；固定 Fixture 验证 Flink 原生 Upsert | 当前状态和 Snapshot 回读 | 文件系统 append-only Source、流式写入和 Checkpoint；Table API 固定 Changelog Upsert | 当前全部路径已在外部环境手动验证：基础路径的首个 Snapshot 写入 1 个 Data File/8 条记录，后续每 10 个空 Checkpoint 记录一个空 Snapshot；固定 Upsert Fixture 的 `07_validate` 返回 `PASS`，一个原子 Snapshot 包含 1 个 Data File 与 2 个 Equality Delete File，分别对应更新与删除。 |
| `labs/07-metadata-and-commit-protocol` | Metadata JSON、Metadata Log、Snapshot、Manifest 和文件条目观察 | 未覆盖 | 未覆盖 | 已在当前外部 REST Catalog 与对象存储环境手动验证；Java 检查器读取实际 Metadata JSON、Manifest List 和 Manifest，并以两个客户端确认旧 Schema 提交返回 `CommitFailedException` |
| `labs/08-performance-and-maintenance` | 小文件基线、Data File bin-pack/排序重写、Manifest 重写、特定 Snapshot 过期、孤儿文件 dry-run | 未覆盖 | 未覆盖 | 已在当前外部 REST Catalog、MinIO 与 Spark 环境手动验证：排序重写后的文件按 `customer_id` Min/Max 形成可裁剪区间；Data File 重写与 Manifest 重写各产生一个 `replace` Snapshot；过期全部旧引用后旧 Data File 被清理；`S3FileIO` 需设置 `prefix_listing => true` 以完成 `s3://` Location 的孤儿文件 dry-run，当前无候选对象。真实孤儿文件删除不属于仓库自动化路径。 |
| `labs/09-production-practices` | 质量断言、表健康报告、坏数据及错误 UPDATE 回滚、订单表设计、最小权限契约和隔离备份恢复 | 未覆盖 | 未覆盖 | 已于 2026-09-07 由项目维护者确认完成当前外部环境手动验证。恢复后双向差集均为空，完整行断言及 main 基线引用断言返回 NULL；表设计契约、reader/writer/admin 最小权限边界，以及专属 Location 的对象副本、原路径恢复和 `register_table` 验收均已完成验证。 |
| `labs/10-order-lakehouse` | 原始事件建表、Spark 当前态 MERGE、质量门禁、Schema Evolution、指标构建、Data File 重写和最终断言 | 当前态与指标回读、Snapshot 汇总 | 文件系统 append-only Source、Checkpoint 写入原始事件 | 已于 2026-09-15 由项目维护者确认完成当前外部环境端到端手动验证：Flink Checkpoint 写入原始事件，Spark 的 8/7 原始事件去重、2 行/95.75 当前态、质量/演进/维护逻辑一致性和最终断言均通过，Trino 当前态与指标两项检查均为 PASS。维护产生 `replace` Snapshot；具体 ID 和作业输出记录在本地验证记录中。 |

## 已验证的 API 示例

| 示例 | API 路径 | 表操作 | 状态 |
| --- | --- | --- | --- |
| `examples/spark-java` | Spark Java | 创建、MERGE、读取 Snapshot、清理 | 已在当前外部环境手动验证 |
| `examples/spark-python` | PySpark | 创建、MERGE、读取 Snapshot、清理 | 已在当前外部环境手动验证 |
| `examples/flink-java` | Flink Table API | REST Catalog 创建、覆盖写入、读取 Snapshot、清理 | 已在当前外部环境手动验证；Spark 读取到相同 Snapshot ID 和 `overwrite` 操作 |
| `examples/pyiceberg` | PyIceberg REST Catalog | 读取 Schema、Metadata Location、当前 Snapshot | 已在当前外部环境手动验证 |

“已落地”只表示 SQL、代码和说明已提交；“已验证”表示对应路径已在项目当前外部环境手动运行。实验 03、04 当前只完成 Spark 手动验证；实验 05 的 Flink SQL/Java、Trino 读取和写入、Schema/分区/行级操作，以及全部当前 API 示例均已完成当前环境验证。Flink 的 `committed_at` 以 UTC 文本显示，Spark 会按会话时区显示同一时刻；比较时应优先使用 Snapshot ID，或显式统一会话时区。后续应持续补充实际引擎补丁版本、Catalog 实现、存储后端和结果。

此前已用项目内的 Spark 3.5.9、Iceberg 1.11.0、Java 17 和临时 Hadoop Catalog 做过 SQL smoke test；该结果只验证 SQL/DDL 和本地运行时，不替代 REST Catalog、对象存储或 Trino 的集成验证。实验 01–10 的 Spark SQL、实验 05 的新增 Trino/Flink 路径以及实验 10 的 Flink Checkpoint/Spark/Trino 综合路径，均已按项目维护者记录在外部环境手动验证。实验 09 和 10 分别由项目维护者于 2026-09-07、2026-09-15 确认完成全部当前已落地路径的外部手动验证；具体 Snapshot ID、健康指标数值及 CLI 退出码未在反馈中提供，不填入推测值。
