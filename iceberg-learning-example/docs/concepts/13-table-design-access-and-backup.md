# 生产实践：表设计、最小权限与备份恢复

## 从访问模式设计表

Iceberg Schema 应以业务语义和演进稳定性为先，而不是以当前某个文件布局为先。订单示例
中 `order_id` 是业务键，`source_event_id` 与版本用于幂等和事件顺序判定，金额使用定点
`DECIMAL` 而不是浮点数，创建时间和源更新时间分开表示业务发生与上游变更。Iceberg 的
字段 ID 支持安全重命名和新增字段，但不替代唯一约束：键冲突、空值和跨批次重复仍需要
在摄取或质量门禁中验证。

分区选择应对应常见扫描范围。实验的 `days(order_created_at)` 适合按日期回溯、日报和
维护；若大多数查询只按客户点查，细分到客户 ID 会产生高基数小分区，往往比不分区加上
合适的排序/重写更差。分区变换对读取者隐藏，查询仍使用原始时间谓词。目标文件大小和
写入分发模式是运行时策略，不是永远正确的常量；应结合实验 08 的文件、Manifest 和扫描
指标调整。

表属性中的 `business-owner`、`data-classification`、`retention-policy` 有助于发现和治理，
但只是元数据标签。Spark 将 `owner` 作为当前执行用户的保留属性，不能用它记录业务
责任人；这些标签也不能自行控制谁能读取表或删除对象。

## 权限跨越三个层面

一次 Iceberg 查询至少涉及引擎身份、Catalog 和对象存储。Catalog 决定谁能发现和加载表，
对象存储权限决定能否读取 Metadata JSON、Manifest、Data File 和 Delete File，引擎身份
决定 Spark/Flink/Trino 进程实际携带哪个凭据。任意一层过宽都可能绕过其他层的意图。

最小权限建议把 reader、writer、maintainer、admin 分开。reader 只需受限表前缀的列举和
读取；writer 只获取提交所需的最小写入范围；maintainer 的删除权限仅在维护窗口使用；
admin 的 Namespace、授权和恢复权限不能作为长期作业身份。每个角色使用独立、可撤销的
身份和审计记录，避免将高权限静态密钥复制到所有客户端环境。

## 备份不同于 Time Travel

Snapshot 回滚和 Time Travel 依赖原始 Catalog Metadata 与对象仍然存在。快照过期、对象
丢失、Catalog 丢失或错误删除都可能使这些功能失效。完整备份需要在一个可解释的恢复点
同时覆盖 Catalog 状态、全部 Iceberg Metadata、Manifest、Data File、Delete File 和相关
对象存储控制面策略。

Metadata 中的文件 Location 通常是绝对 URI。因此将对象复制到新前缀后直接注册副本中的
Metadata JSON，可能仍然读取旧前缀；只要旧对象仍在，这种“恢复成功”没有价值。简单的
恢复策略是在隔离表根目录内复制、并在故障后还原到原路径，再注册已验证的 Metadata JSON。
跨前缀或跨账户恢复需要受控路径重写，并独立核对重写后所有引用。

恢复验收至少包含：冻结写入和维护、保存恢复点与对象清单、验证副本完整、注册 Catalog、
读取逻辑数据、检查 Data/Delete File，再在隔离环境模拟真实不可用条件。不要把无数据
丢失条件下的 `register_table` 演示描述为完整灾难恢复证明。
