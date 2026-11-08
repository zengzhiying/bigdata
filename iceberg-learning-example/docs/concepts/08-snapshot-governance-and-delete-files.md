# 08 - Snapshot 治理、引用和 Delete File

## Snapshot 是表状态的版本

Iceberg 的一次成功提交会生成新的 Metadata JSON、Snapshot 和 Manifest 引用，并通过 Catalog 将当前表指针原子地移动到新状态。Snapshot 是逻辑版本，不等于一个单独的数据文件。

常用的观察入口包括：

- `snapshots`：查看 Snapshot ID、父 Snapshot、提交时间、操作类型和摘要。
- `history`：查看哪个 Snapshot 在什么时间成为当前状态。
- `entries`：查看某个 Snapshot 新增、删除或保留的 Manifest Entry。
- `files`、`delete_files`：查看当前 Snapshot 可见的 Data File 和 Delete File。

Snapshot ID 可以用于 `VERSION AS OF`、回滚和审计对比。只要对应 Snapshot 尚未过期，历史数据就仍然可以读取。

- snapshots：列出表当前元数据中仍存在的所有 Snapshot，包括当前快照、当前引用链外的旧快照，以及可能被 Branch/Tag 引用的快照。它的 operation、summary 用于查看每个 Snapshot 对应的提交类型与摘要。
- history：记录某个 Snapshot 曾在什么时间被设为当前 Snapshot，即主引用的切换历史。回滚会新增一次“将旧 Snapshot 重新设为当前”的历史记录。

因此，回滚后会出现这种情况：

```
snapshots:
  S1  append
  S2  append
  S3  overwrite / delete
  # S1、S2、S3 都还存在

history:
  t1  S1  true
  t2  S2  false
  t3  S3  false
  t4  S1  true   # rollback 后 S1 再次成为当前状态
```

其中 `history.is_current_ancestor = true` 表示该 Snapshot 位于当前状态的祖先链上；false 表示它虽然仍存在，但不属于当前引用链。

## Rollback 和 Time Travel 的区别

Time Travel 只针对一次查询选择历史 Snapshot，不改变表的当前状态。Rollback 则把表的当前引用移动到一个已有 Snapshot：

```sql
CALL catalog.system.rollback_to_snapshot(
  'namespace.table',
  <SNAPSHOT_ID>
);
```

回滚不会把历史提交重新写成新文件，也不会自动删除回滚之后的 Snapshot。回滚后继续写入会形成新的提交分支，因此应在 `history` 和 `snapshots` 中记录操作前后的关系。

## Snapshot 过期

Snapshot 会持续引用 Metadata JSON、Manifest、Data File 和 Delete File。长期不清理会导致元数据和存储成本增长，过早清理则可能破坏审计和恢复能力。

Iceberg 的 `expire_snapshots` 通常按时间和最少保留数量执行：

```sql
CALL catalog.system.expire_snapshots(
  table => 'namespace.table',
  older_than => TIMESTAMP '2026-01-01 00:00:00',
  retain_last => 2
);
```

执行前应确认：

1. 保留窗口覆盖业务审计、回滚和备份需求。
2. 没有长时间运行的查询仍依赖待过期 Snapshot。
3. Catalog 和 Warehouse 的时钟一致。
4. 清理结果通过 `snapshots`、`files` 和存储指标复核。

Snapshot 过期是元数据治理操作，不应替代数据文件重写或孤儿文件清理。不同实现对未引用文件的删除时机也可能不同。

## Branch 和 Tag

Branch 和 Tag 是表引用（reference）：

- Branch 指向一个 Snapshot，并允许继续产生新提交。
- Tag 指向一个固定发布点，通常不接受后续写入。
- 两者都依赖 Catalog 的引用管理能力，不是 Warehouse 中的目录名。

典型 Spark SQL 形式如下，具体语法和支持范围需要根据 Catalog 与 Iceberg Runtime 验证：

```sql
ALTER TABLE catalog.namespace.table
CREATE BRANCH audit AS OF VERSION <SNAPSHOT_ID>;

ALTER TABLE catalog.namespace.table
CREATE TAG release_2026_03 AS OF VERSION <SNAPSHOT_ID>;
```

REST 协议本身不代表服务一定实现 Branch/Tag。项目实验将这部分单独标记为可选，并建议使用 Nessie 或其他明确支持引用管理的 Catalog。

## WAP（Write-Audit-Publish）

WAP 把写入、审核和发布拆成不同步骤：

1. 写入者把新 Snapshot 提交到审计 Branch。
2. 校验数据质量、Schema、行数和业务规则。
3. 通过 Catalog 发布或合并审计 Branch 的变更。
4. 默认引用才看到审核后的状态。

Spark 中常见的配置方式是启用表级 WAP，并为一次暂存提交设置 WAP ID：

```sql
ALTER TABLE catalog.namespace.table SET TBLPROPERTIES (
  'write.wap.enabled' = 'true'
);

-- Spark SQL 的 SET 会将单引号保留在配置值中，因此 WAP ID 不加引号。
SET spark.wap.id = audit-202603;
INSERT INTO catalog.namespace.table VALUES (...);
RESET spark.wap.id;
```

发布通常使用 `publish_changes` 等 Catalog procedure，并传入同一个 WAP ID：

```sql
CALL catalog.system.publish_changes(
  table => 'namespace.table',
  wap_id => 'audit-202603'
);
```

也有引擎支持把写入目标设置为 WAP Branch；`spark.wap.id` 与 `spark.wap.branch` 不能同时设置。参数形式会随着 Iceberg Runtime 和 Catalog 实现而变化。实验样例保留了占位和说明，不把某个 Catalog 的实现细节伪装成通用保证。

WAP 的关键验证不是“写入命令成功”，而是确认：

- 审计引用可见新记录。
- 默认引用在发布前不可见。
- 发布后默认引用可见。
- 发布失败不会改变默认 Snapshot。

## Delete File

Format Version 2 支持把行级变更表达为 Delete File，而不必立即重写所有受影响的 Data File。常见内容类型为：

| `content` | 类型 | 说明 |
| --- | --- | --- |
| `0` | Data File | 记录实际数据。 |
| `1` | Position Delete | 按 Data File 路径和行位置删除。 |
| `2` | Equality Delete | 按一个或多个字段值删除。 |

Delete File 由引擎在扫描时应用。它的存在不会让被删除的行重新出现在查询结果中；只有在对应 Snapshot 仍然可见且引擎正确处理删除文件时，查询结果才是最终逻辑状态。

Spark 表可以声明 Merge-on-Read：

```sql
ALTER TABLE catalog.namespace.table SET TBLPROPERTIES (
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);
```

这只是写入策略，不能保证每个引擎或每种操作一定生成 Position Delete 或 Equality Delete。`DELETE`、`UPDATE`、`MERGE` 的实际文件类型必须通过 `delete_files`、`all_delete_files` 和 `entries` 元数据表检查。

### Position Delete 不一定无限增量累加

MOR 的含义是保留受影响的 Data File 并在读取时应用删除，不等于当前 Snapshot 必然为每次删除永久保留一个独立的 Position Delete File。

在 Spark `3.5.9` 与 Iceberg `1.11.0` 中，后一次针对同一 Data File 的 Position Delete 可以加载已有删除位置，将旧、新位置合并后写入新的替代 Delete File。新的当前 Snapshot 引用替代文件，先前的 Delete File 从当前引用中移除，但仍可能被历史 Snapshot 引用：

```text
Snapshot S1: data-a.parquet
Snapshot S2: data-a.parquet + delete-v1.parquet (position 10)
Snapshot S3: data-a.parquet + delete-v2.parquet (position 10, 20)
```

在上述例子中，`delete-v1.parquet` 不参与 S3 的当前查询，但在 S2 尚未过期时仍用于 Time Travel。这种合并减少了当前读取需要叠加的 Position Delete File 数量，仍属于 MOR，因为 `data-a.parquet` 继续被当前 Snapshot 引用。

判断时应看 Metadata Table，而不是仅看对象存储中的文件数量：

- 新文件为 `content = 1`，且 `referenced_data_file` 指向仍被当前 Snapshot 引用的 Data File，表示 Position Delete 合并或替换。
- 新文件为 `content = 0`，且原 Data File 不再被当前 Snapshot 引用，表示 COW Data File 重写，或维护操作已将删除物化。

删除涉及不同 Data File、不同分区，或受任务并行度和 Delete File 粒度影响时，当前 Snapshot 仍可能引用多个 Delete File。

## Delete File 与维护

Delete File 会增加读取时的合并成本，长期积累可能导致扫描变慢。生产维护通常需要结合：

- 数据文件重写，将删除应用到新的 Data File。
- Manifest 重写，降低元数据规划成本。
- Snapshot 过期，移除不再需要的历史引用。
- 孤儿文件清理，处理已不被任何有效 Snapshot 引用的对象。

不能直接从 Warehouse 删除 Delete File。文件是否仍被有效 Snapshot 引用，必须通过 Iceberg 元数据关系判断。

## 跨引擎验证边界

Spark、Flink 和 Trino 对以下能力的支持并不完全一致：

- Branch、Tag、WAP 的创建和发布。
- Equality Delete 和 Position Delete 的读取。
- `delete_files` 等元数据表字段与语法。
- Merge-on-Read 与 Copy-on-Write 的默认选择。

因此 `labs/03-snapshot-governance` 和 `labs/04-delete-files` 都要求记录实际引擎版本、Catalog 实现、Storage 后端和元数据结果；“SQL 文件已提交”不等于“兼容性已经保证”。
