# 隔离备份与 Catalog 恢复演练

该演练验证两个独立层面：对象存储中完整表树的备份，以及 Catalog 重新注册既有
Metadata JSON 的控制面恢复。它使用专用 Location，当前状态为**已于 2026-09-07 在当前
外部环境完成手动验证**。不对共享 Warehouse、现有学习表或生产表执行复制、删除、覆盖
或注册操作。

## 选择隔离位置

在开始前创建两个空的、不同的前缀，例如：

```text
s3://warehouse/labs/09/recovery-source/
s3://warehouse-backup/labs/09/2026-09-07/recovery-source/
```

第一个是源表根目录，第二个是不可变备份副本。两者不能是 Warehouse 根目录或某个
Namespace 目录。为本次演练暂停源表写入、重试、快照过期、重写和孤儿文件清理；记录
执行者、源/目标前缀、对象数和总字节数。对象存储的版本控制、锁定、跨区域复制及密钥
管理由外部平台管理，本仓库不会配置或保存它们。

## 创建并冻结恢复点

将 [01_create_fixture.sql.template](01_create_fixture.sql.template) 复制到未跟踪的临时
文件，替换 `<ISOLATED_TABLE_LOCATION>` 为源表根目录后执行。模板中的 `DROP TABLE`
只针对实验标识，仍应先确认该表归属本演练。

```bash
backup_lab_tmp=$(mktemp -d /tmp/iceberg-backup-lab.XXXXXX)
cp labs/09-production-practices/backup-recovery/01_create_fixture.sql.template \
  "$backup_lab_tmp/create_fixture.sql"
# 编辑 "$backup_lab_tmp/create_fixture.sql"，填写专属源表 Location。
./scripts/run-spark-sql.sh "$backup_lab_tmp/create_fixture.sql"
./scripts/run-spark-sql.sh \
  labs/09-production-practices/backup-recovery/02_capture_recovery_point.sql
```

保存 `DESCRIBE TABLE EXTENDED` 输出的 Location，`refs.main` 的 Snapshot ID，当前 Metadata
JSON 路径、逻辑行集、文件统计和对象副本完成标识。Snapshot 和 Metadata JSON 都需要
记录，但任意一个文件都不是表的完整备份。

## 复制完整表树

使用平台受控的复制工具或备份作业，将**源表根目录下的全部对象**复制到备份前缀，保留
相对路径。复制范围必须包括 `metadata/`、Data File、Delete File（如有）以及 Manifest/
Manifest List。不要只复制当前 Metadata JSON、只复制 `data/`，或依靠对象名称推测引用。

这里使用 MinIO 工具复制例如：

```bash
mc cp --recursive minio/warehouse/labs/09/recovery-source/data minio/warehouse-backup/labs/09/2026-09-07/recovery-source/
mc cp --recursive minio/warehouse/labs/09/recovery-source/metadata minio/warehouse-backup/labs/09/2026-09-07/recovery-source/
```

复制完成后，用平台工具比较源和备份的对象数量、对象大小和校验和（若服务提供），并将
结果写入 [恢复记录模板](recovery-record.md.template)。由于 Iceberg 文件路径通常是绝对
URI，备份前缀中的 Metadata JSON 在源对象仍可访问时也可能读取源前缀；这不能证明备份
副本可独立恢复。

## 控制面恢复演练

可以先删除原来的表，模拟丢失的情况：

```sql
drop table ice.learning_recovery.orders_backup_fixture;
```

然后删除底层文件：

```bash
mc rm --recursive --force minio/warehouse/labs/09/recovery-source/
# 恢复备份
mc mb minio/warehouse/labs/09/recovery-source/
mc cp --recursive minio/warehouse-backup/labs/09/2026-09-07/recovery-source/ minio/warehouse/labs/09/recovery-source/
# 或者采用镜像的方式恢复更简单
mc rm --recursive --force \
  minio/warehouse/labs/09/recovery-source
# 不需要创建目录，复制后即保留层级
mc mirror --overwrite \
  minio/warehouse-backup/labs/09/2026-09-07/recovery-source \
  minio/warehouse/labs/09/recovery-source
```

在恢复验证窗口内，先通过外部平台将备份对象**恢复到原始专属源 Location**，再使用
备份中对应的 Metadata JSON 相对路径得到原始 Location 下的完整 URI。随后将
[03_register_restored_table.sql.template](03_register_restored_table.sql.template) 复制到临时
文件，填写该 URI，执行注册和验证：

```bash
cp labs/09-production-practices/backup-recovery/03_register_restored_table.sql.template \
  "$backup_lab_tmp/register_restored.sql"
# 编辑 "$backup_lab_tmp/register_restored.sql"，填写已恢复到原始 Location 的 Metadata JSON。
./scripts/run-spark-sql.sh "$backup_lab_tmp/register_restored.sql"
./scripts/run-spark-sql.sh \
  labs/09-production-practices/backup-recovery/04_validate_restored_table.sql
```

`register_table` 只将指定 Metadata JSON 登记到 Catalog；它不复制对象，不检查每个 Data
File 是否存在，也不修复 Metadata 中的路径。若恢复到新前缀，必须使用与当前 Iceberg
版本和 Catalog 兼容的路径重写流程，并重新验证所有 Metadata、Manifest、Data File 和
Delete File 引用；这属于独立迁移演练，不能把简单 `register_table` 当作跨前缀恢复。

对现有表进行真正的灾难模拟会影响 Catalog 引用或对象可用性，未包含在仓库自动命令中。
只有在隔离环境中、已确认复制完整且有外部变更批准时，才移除 Catalog 注册或模拟源对象
不可用。常规验证应优先在保留源表的条件下演练重新注册和逻辑验收。

## 验收与清理

恢复表应返回三条固定记录、金额 `99.75`、至少一个当前 Data File，且断言返回 NULL 和
`PASS`。这证明注册后的表读取了预期 Fixture，但完整对象恢复仍须以外部对象清单/校验和
和隔离故障演练为依据。

确认记录后，以管理员身份执行：

```bash
./scripts/run-spark-sql.sh labs/09-production-practices/backup-recovery/05_cleanup.sql
```

该 SQL 只移除两个实验 Catalog 表引用，不会删除备份前缀中的对象。不要将备份数据的
删除纳入此实验的自动化清理；保留与清理由外部备份策略决定。
