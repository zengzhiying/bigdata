# 实验 09 - 数据质量、表健康与误操作恢复

阶段 9 第一批实验使用独立订单表，演示质量检查失败如何中止流程、如何读取表健康指标，
以及如何从坏数据写入和错误 UPDATE 恢复。当前状态为**已在当前外部环境完成手动验证**，
由项目维护者于 2026-09-07 确认。
本实验当前已落地的第一、二批路径均已完成当前外部环境验证，阶段 9 已完成。故障排查
见 [Spark、REST Catalog 与 MinIO 帮助手册](../../docs/faq-spark-rest-minio.md)；生产环境
升级回归属于专有运维流程，不作为本实验内容。Flink 原生 Upsert 仍属于阶段 6。

## 已验证结果

- 第一批实验的手动验证已完成，包括质量检查、健康报告、坏数据及错误 UPDATE 的回滚恢复。
- 恢复后 `unexpected_orders` 和 `missing_orders` 均返回 0 行，两个完整行断言返回 NULL。
- 当前 `main` 引用与记录的基线 Snapshot ID 相同，对应引用断言返回 NULL。`assert_true`
  返回 NULL 表示条件成立，抛出异常才表示失败。
- 健康报告的交互执行需先运行三条 `SET lab09.*`；恢复引用检查仍需通过 `--hivevar`
  提供 `baseline_snapshot_id`，或在交互执行时将占位表达式替换为已记录的实际基线 ID。
- 订单表设计 Fixture、reader/writer/admin 最小权限验证，以及专属 Location 的对象副本、
  原路径恢复、完整 Metadata JSON 注册和恢复验收均已完成。

本记录依据项目维护者反馈，不补写未提供的 Snapshot ID、健康指标数值、权限拒绝详情或
CLI 退出码。
后续版本或环境变更后，应按本实验重新验证并保存详细记录。

## 前置条件

- 外部 Spark `3.5.9`、Iceberg `1.11.0`、REST Catalog 与 MinIO，Java 使用
  `/usr/local/jdk-17.0.14+7`。按项目环境文档配置 `workdir/.env`。
- 默认 Catalog 为 `ice`；如使用其他名称，需要同步替换本实验 SQL 中的 Catalog。
- `ice.learning_production.orders` 是本实验专用表，输入由 SQL 固定生成，不依赖其他实验。
- 演练期间暂停此表的其他写入及快照过期任务，保留基线 Snapshot 和引用文件。
  回滚改变整张表的当前引用，可能隐藏基线之后其他写入者的合法提交。
- 从仓库根目录执行命令，不要启用 Spark SQL 的忽略错误选项。

## 1. 建立基线并检查正常数据

```bash
export JAVA_HOME=/usr/local/jdk-17.0.14+7
export PATH="$JAVA_HOME/bin:$PATH"
./scripts/run-spark-sql.sh \
  labs/09-production-practices/spark-sql/00_reset.sql \
  labs/09-production-practices/spark-sql/01_create_and_seed.sql \
  labs/09-production-practices/spark-sql/02_quality_gate.sql
```

重置会删除本实验已有表。首次创建后应有三条订单，金额总和 `115.75`，质量检查输出
`PASS`。`assert_true` 成功时本身返回 NULL，这不表示检查失败。

保存建表脚本输出的 `baseline_snapshot_id`，后续两次恢复使用同一个 ID。
Snapshot ID 不是递增版本号；应从 `refs` 的 `main` 引用获取当前值。

## 2. 读取表健康报告

```bash
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/03_health_report.sql
```

脚本开头用三条 `SET lab09.*` 设置默认阈值：小文件为 `1048576` 字节、最大小文件
占比为 `0.5`、最大快照年龄为 `3600` 秒。修改开头的数值即可调整报告；完整重跑脚本
会重新设置这些值，不再使用 `--hivevar` 传参。

在交互式 Spark SQL 中逐条执行时，必须先执行这三条 SET，再执行 SELECT，并保持在
同一会话。仅复制 SELECT 可能使变量被替换为空，导致 `>` 后缺少数值的解析错误，
报错位置可能指向前面的 `unix_timestamp`。小文件字节阈值需大于 0，占比阈值在
`[0, 1]` 内，快照年龄阈值为非负秒数；参数作为 SQL 字面量使用，仅填写可信数值。

报告包含当前 Snapshot 及其年龄、提交摘要、Data File 数量和大小、小文件占比、
Position/Equality Delete File 数量、Manifest 数量、保留快照数量和引用。
三行样例通常全部是小文件，出现 `WARN` 符合预期；健康报告不会因此失败或触发维护。
本表采用 Copy-on-Write，Delete File 为零也是正常结果。

快照年龄表示当前引用对应 Snapshot 的提交年龄，不等于业务数据新鲜度。
Flink 空提交能产生新 Snapshot，Metadata-only 更新可能不产生 Snapshot，回滚则会让
当前 Snapshot 指向较早提交。生产监控还需结合业务事件时间、Source 延迟及 Checkpoint。
这些查询在多个语句间不构成一致性事务，因此本实验要求无并发写入。

## 3. 坏数据写入与质量失败

```bash
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/04_inject_bad_data.sql
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/02_quality_gate.sql
```

第二条命令预期抛出 `Order quality gate failed` 并返回非零退出码；请单独执行，
不要把后面的恢复命令接到成功条件 `&&` 后。记录实际退出码和异常。
若命令意外成功，应停止演练并检查是否启用了忽略 SQL 错误的配置。

此时应有 5 行、金额 `120.75`，重复或空主键指标为 1，必填字段缺失为 1，
非法金额为 1，非法状态为 1。Iceberg 不自动提供业务主键唯一约束；本实验允许 NULL，
以便在数据已提交后演示检测和恢复。实际发布流程应在 staging/分支上验收后再发布。

## 4. 回滚并严格验收

将 `spark-sql/06_rollback.sql.template` 复制到本地临时目录，替换两处
`<BASELINE_SNAPSHOT_ID>`，检查目标表和 ID 后执行。模板不能直接作为 SQL 运行。

```bash
lab09_tmp_dir=$(mktemp -d /tmp/iceberg-lab09.XXXXXX)
cp labs/09-production-practices/spark-sql/06_rollback.sql.template "$lab09_tmp_dir/rollback.sql"
# 编辑 "$lab09_tmp_dir/rollback.sql"，填写已记录的基线 ID。
./scripts/run-spark-sql.sh "$lab09_tmp_dir/rollback.sql"
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/02_quality_gate.sql
# 将下列占位值替换为同一个实际 ID 后执行。
./workdir/spark-sql.sh --hivevar baseline_snapshot_id=REPLACE_WITH_BASELINE_ID \
  -f labs/09-production-practices/spark-sql/07_validate_recovery.sql
```

预期质量检查与恢复检查均为 `PASS`；完整行数据通过双向 `EXCEPT ALL` 比较，包含重复行
差异检查；`main` 必须指向基线 ID。异常快照可能仍在 `snapshots` 中，这是正常的：回滚
没有物理删除它们。不要按最新 `committed_at` 判断当前快照，应查看 `refs` 和 `history`。

基线必须仍存在且为当前 Snapshot 的祖先。若基线已过期，停止此路径，不要编辑
Metadata JSON 或猜测对象路径。这里演示逻辑回滚，不构成丢失文件后的备份恢复。

## 5. 错误 UPDATE 与再次恢复

完成上一次恢复验收后，再执行：

```bash
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/05_wrong_update.sql
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/02_quality_gate.sql
```

行数仍为 3，主键、必填、金额范围和状态检查都合法，但金额变为 `145.75`，对账断言
必须失败。重新执行第 4 步，仍回滚到最初基线，确认总额恢复为 `115.75` 且完整行一致。
质量规则中的行数和总额是本实验数据契约，不能直接作为生产通用阈值。

## 6. 记录与清理

复制 [验证记录模板](validation-record.md.template) 到仓库外或被忽略的本地目录，记录
环境、阈值、两次失败退出码、回滚前后引用和断言结果。连接地址应脱敏，不记录凭据。
本次手动验证状态已同步到 `config/compatibility-matrix.md`；后续重新验证后再更新记录，
不能仅凭本地基础检查标记已验证。

```bash
./scripts/run-spark-sql.sh labs/09-production-practices/spark-sql/08_cleanup.sql
```

清理仅删除本实验表，不删除 Namespace、共享 Warehouse 或手动保存的记录。
物理文件清理由 Catalog 和 FileIO 的 DROP 行为决定；本实验不运行孤儿文件删除。
更详细的说明见 [生产质量与恢复笔记](../../docs/concepts/12-production-quality-and-recovery.md)
和 [表设计、最小权限与备份恢复笔记](../../docs/concepts/13-table-design-access-and-backup.md)。

## 7. 第二批：表设计、最小权限与隔离备份恢复

表设计 Fixture 位于 `table-design/`，使用独立的 `ice.learning_design.orders_contract` 表：

```bash
./scripts/run-spark-sql.sh \
  labs/09-production-practices/table-design/01_create_orders_contract.sql \
  labs/09-production-practices/table-design/02_inspect_contract.sql
```

它演示业务键与 Source 事件键、定点金额、日期隐藏分区、写入分发和治理标签。该表的键是
数据契约而非 Iceberg 自动执行的主键约束；断言成功返回 NULL 与 `PASS`。完成观察后执行
`table-design/03_cleanup.sql`。

最小权限 Fixture 位于 `security/`。先由管理员创建表，再依次以 reader 和 writer 的独立
身份执行脚本，具体角色与对象前缀权限见 [访问契约](security/access-contract.md)。不要在
当前共享 `workdir/.env` 中互相覆盖身份凭据；为每个身份使用被忽略的环境文件，并以
`ICEBERG_ENV_FILE` 选择。reader 写入模板的非零授权失败是预期结果，实际结果填入
[权限记录模板](security/access-record.md.template)。

隔离备份恢复 Fixture 位于 `backup-recovery/`，使用专用 Location、外部受控对象复制和
Catalog `register_table` 控制面恢复。详细步骤、安全边界和验收见
[备份恢复演练](backup-recovery/README.md)。它不运行对象复制、删除或灾难模拟自动化。
