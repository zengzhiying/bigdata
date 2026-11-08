# 实验 08 - 性能与维护

## 实验目标

本实验进入阶段 8，使用两张互不影响的 Spark Iceberg 表建立可重复的小文件负载，并观察：

1. `files`、`manifests`、`snapshots` 和 `metadata_log_entries` 如何描述文件与元数据开销。
2. `rewrite_data_files` 的 `binpack` 与 `sort` 策略如何在不改变逻辑数据的前提下重写 Data File。
3. `rewrite_manifests` 如何减少当前 Snapshot 需要读取的 Manifest 数量。
4. Snapshot 过期和孤儿文件识别的安全边界。

这不是吞吐或延迟基准测试。文件数量、文件大小和 Procedure 返回值受 Spark 并发度、压缩、对象存储及 Runtime 实现影响；实验验证的是元数据关系和逻辑结果，而不是固定的耗时或固定数量。

## 当前验证状态

本实验已在当前外部 Spark `3.5.9`、Iceberg `1.11.0`、REST Catalog 与 MinIO 环境手动验证。排序重写后的 Data File 按 `customer_id` Min/Max 形成可裁剪区间；Data File 重写和 Manifest 重写分别生成一个 `replace` Snapshot；过期所有旧引用后，重写前的 Data File 已从对象存储清理。`remove_orphan_files` 在 `s3://` Warehouse 下需启用 `prefix_listing => true`，当前 dry-run 未返回候选对象。真实孤儿文件删除仍只允许在隔离 Location 中人工确认后执行。

## 前置条件

- 外部 Spark `3.5.9`、Iceberg `1.11.0`、Java `17`，并按项目 REST Catalog 与对象存储配置启动。
- Spark 启用 `IcebergSparkSessionExtensions`。项目的 `workdir/spark-sql.sh` 已提供该配置。
- Catalog 名默认为 `ice`；如不同，请替换 SQL 中的 `ice`。
- 表 `ice.learning_maintenance.events_binpack` 和 `ice.learning_maintenance.events_sort` 必须是本实验独占表；运行期间不能有其他读写者或引用依赖其历史 Snapshot。
- 运行维护 Procedure 的身份需要有实验表 Location 下的读取权限；`expire_snapshots` 和非 dry-run 的孤儿文件清理还需要删除权限。
- 本项目的 Warehouse 使用 `s3://` 与 Iceberg `S3FileIO`。孤儿文件 dry-run 必须使用脚本中的 `prefix_listing => true`，以通过 `S3FileIO` 列举对象；默认 Hadoop 列举会因没有 `s3://` Hadoop FileSystem 而失败。

## 文件顺序

```text
spark-sql/00_reset.sql
spark-sql/01_create_fragmented_tables.sql
spark-sql/02_capture_baseline.sql
spark-sql/03_rewrite_data_files.sql
spark-sql/04_rewrite_manifests.sql
spark-sql/05_inspect_snapshots.sql
spark-sql/06_expire_snapshots.sql.template  # 手动填写 Snapshot ID 后才运行
spark-sql/07_orphan_file_dry_run.sql
spark-sql/08_validate.sql
spark-sql/09_cleanup.sql
```

先执行不涉及物理删除的主路径：

```bash
./scripts/run-spark-sql.sh \
  labs/08-performance-and-maintenance/spark-sql/00_reset.sql \
  labs/08-performance-and-maintenance/spark-sql/01_create_fragmented_tables.sql \
  labs/08-performance-and-maintenance/spark-sql/02_capture_baseline.sql \
  labs/08-performance-and-maintenance/spark-sql/03_rewrite_data_files.sql \
  labs/08-performance-and-maintenance/spark-sql/04_rewrite_manifests.sql \
  labs/08-performance-and-maintenance/spark-sql/05_inspect_snapshots.sql \
  labs/08-performance-and-maintenance/spark-sql/07_orphan_file_dry_run.sql \
  labs/08-performance-and-maintenance/spark-sql/08_validate.sql
```

`06_expire_snapshots.sql.template` 不是可直接执行的 SQL。将它复制为未跟踪的本地 `.sql` 文件，填入 `05_inspect_snapshots.sql` 返回的一个非当前 Snapshot ID，再单独执行。这个设计避免用环境相关的绝对时间或脚本猜测 Snapshot ID。

## 实验设计

`01_create_fragmented_tables.sql` 对每张表执行 12 次独立 `INSERT`，每次写入 10 条带填充 payload 的记录。这样能稳定产生多个很小的提交和 Data File，不依赖某个 Spark 集群的默认 shuffle 分区数。

两张表包含完全相同的 120 条记录，但维护策略不同：

| 表 | 重写策略 | 对比目标 |
| --- | --- | --- |
| `events_binpack` | `binpack` | 合并小文件，不承诺记录排序。 |
| `events_sort` | `sort`，按 `customer_id, event_id` | 合并小文件并显式重排记录。 |

两种策略不能在同一张表上顺序执行后再比较，否则第二次重写会覆盖第一次的物理布局。`03_rewrite_data_files.sql` 输出每个 Procedure 的重写文件数、写入文件数、重写字节数和失败数；应将这些结果与 `02_capture_baseline.sql` 的文件指标一起记录。

## 观察与验收

- 重写前，每张表有 12 次 append 提交；当前 Data File 数通常接近 12，但不能将该数量视为跨 Runtime 保证。
- 重写后，`08_validate.sql` 必须确认两张表仍各有 120 条记录，且 `event_id` 总和为 `127260`。这证明重写不改变逻辑行集。
- 重写后当前 Data File 数应小于 12；确切数量由目标文件大小、压缩和任务布局决定。
- `03_rewrite_data_files.sql` 会输出 `events_sort` 每个文件的 `customer_id` 和 `event_id` Min/Max 区间。文件区间明显变窄且分散时，排序才为文件裁剪提供了实际条件。
- `events_sort` 的 `EXPLAIN FORMATTED` 只用于确认 `customer_id = 3` 已下推给 Iceberg Scan。它不显示最终候选文件数，需结合文件区间和 Spark SQL 运行指标判断是否实际少读了文件；它不能替代生产压测。
- `rewrite_manifests` 的前后查询用于比较当前 Manifest 数。它会创建新的 Snapshot（通常显示为 `replace`），但复用同一逻辑 Data File 集；小表可能只合并出一个 Manifest，也可能因 Spec、删除文件或 Runtime 策略保留多个 Manifest，以 Procedure 返回值和 Metadata Table 为准。
- 执行 Snapshot 过期后，确认目标 Snapshot 不再出现在 `snapshots` 中；同时确认未过期 Snapshot、当前数据和任何需要保留的引用仍可读取。

## 重写提交与 Snapshot

一个 Snapshot 是某时刻完整、一致的文件引用集合，不等于单个 Parquet 文件或单个 Spark Task 的输出。一次成功的 Data File 重写可以替换多个旧 Parquet、写入多个新 Parquet，并同时生成或重用多个 Manifest；这些变化通过一次 Catalog 提交原子地成为新的 `replace` Snapshot：

```text
旧 Snapshot
  -> data-a.parquet, data-b.parquet, data-c.parquet

rewrite_data_files 成功提交

新 Snapshot
  -> data-1.parquet, data-2.parquet
```

读者只会看到旧 Snapshot 或新 Snapshot，不会看见一半旧文件已移除、一半新文件尚未加入的中间状态。物理文件会在最终 Catalog 提交前写入对象存储；若提交失败，它们不会被表 Metadata 引用，可能作为孤儿文件留待后续受控清理。

`rewrite_manifests` 的情况不同：它不替换当前 Data File 集合，而是改写该集合的 Manifest 和 Manifest List 引用，因此仍需创建新的 Snapshot 固定新引用链：

```text
旧 Snapshot -> 旧 Manifest List / 多个旧 Manifest -> 同一组 Data File
新 Snapshot -> 新 Manifest List / 重写后的 Manifest -> 同一组 Data File
```

因此本实验中，同一张表在 Data File 重写后再执行 Manifest 重写，通常会多出两个 `replace` Snapshot。第一个的 Summary 应显示 Data File 新增和删除；第二个的逻辑数据与当前 Data File 路径应保持不变，但 Snapshot ID、Manifest List 和 Manifest 路径会前进。可用以下 SQL 对照：

```sql
SELECT snapshot_id,
       committed_at,
       operation,
       summary['changed-partition-count'] AS changed_partition_count,
       summary['added-data-files'] AS added_data_files,
       summary['deleted-data-files'] AS deleted_data_files,
       summary['added-records'] AS added_records,
       summary['deleted-records'] AS deleted_records,
       manifest_list
FROM ice.learning_maintenance.events_sort.snapshots
ORDER BY committed_at;
```

默认情况下，一次 `rewrite_data_files` 调用即使覆盖多个分区或 file group，也会以一个原子 Snapshot 提交。只有显式指定 `partial-progress.enabled = true` 时，大型重写才可以分成多次独立的 `replace` Snapshot；这不属于本实验的默认路径。表属性修改等 Metadata-only 提交则可能只推进 Metadata JSON，不创建新的数据 Snapshot，不能与 Manifest 重写混为一谈。

## 排序、文件区间与查询裁剪

对于 `events_sort` 的排序键 `(customer_id, event_id)`，先使用 `files` Metadata Table 查看重写后的 Data File 统计：

```sql
SELECT file_path,
       record_count,
       file_size_in_bytes,
       readable_metrics.customer_id.lower_bound AS customer_id_min,
       readable_metrics.customer_id.upper_bound AS customer_id_max,
       readable_metrics.event_id.lower_bound AS event_id_min,
       readable_metrics.event_id.upper_bound AS event_id_max
FROM ice.learning_maintenance.events_sort.files
WHERE content = 0
ORDER BY customer_id_min, customer_id_max, event_id_min, event_id_max, file_path;
```

`readable_metrics` 是 Spark 对 Iceberg Data File 列统计的可读视图。Iceberg 会将这些 Data File 统计写入 Manifest，并在扫描规划时使用谓词排除不可能命中的文件。例如，若同一查询涉及的文件范围为 `0~1`、`2~3`、`4~6`、`6~8`、`8~10`，`customer_id = 3` 只需要保留 `2~3` 的文件；其他文件在打开 Parquet 前就可以跳过。

```text
customer_id = 3
  -> Manifest/Data File Min/Max：排除不包含 3 的 Parquet 文件
  -> Parquet Row Group/Page 统计：继续跳过不包含 3 的块（实现和配置相关）
  -> Spark 对保留数据执行最终 Filter
```

下列计划说明谓词已经传入 Iceberg Scan：

```sql
EXPLAIN FORMATTED
SELECT event_id, customer_id
FROM ice.learning_maintenance.events_sort
WHERE customer_id = 3;
```

若 `BatchScan` 显示 `filters=customer_id IS NOT NULL, customer_id = 3`，表示发生了谓词下推；这本身不等于已证明文件裁剪。文件区间决定理论候选集，Spark UI 的 SQL 详情中的 `BatchScan` 输入文件数、输入字节数或扫描分区数（指标名称随 Runtime 而异）用于确认实际扫描量。

首排序键最重要：`customer_id = 3`，以及同时带 `customer_id` 和 `event_id` 范围的查询，最可能受益。只按第二排序键 `event_id` 过滤时，多个文件的 `event_id` Min/Max 可能重叠，不能期待同样的文件裁剪。再加入时间范围能先利用 `days(event_time)` 分区裁剪：

```sql
SELECT event_id, customer_id
FROM ice.learning_maintenance.events_sort
WHERE event_time >= TIMESTAMP '2026-08-01 00:00:00'
  AND event_time < TIMESTAMP '2026-08-02 00:00:00'
  AND customer_id = 3;
```

Parquet 不是 B-Tree 或点查索引。排序使相邻键更可能集中于少数文件、Row Group 和 Page，从而让 Min/Max、Page Index 或 Bloom Filter（是否生成和使用取决于 Writer、Reader、文件与配置）跳过更多数据；最终仍需读取候选列块并执行最终过滤。

## Snapshot 过期与孤儿文件的安全边界

`expire_snapshots` 会使不再受有效 Snapshot 引用的历史文件具备后续删除条件，但它不等于立即、完整地清理所有对象。开始前需要确认：

- 待过期 Snapshot 不被 Branch、Tag、审计、备份或长时间查询使用。
- 实验表没有其他写入者，且 Catalog、Spark 与对象存储的时钟没有明显偏差。
- 只传入从本实验表查询出的非当前 Snapshot ID；不要用猜测的 ID 或生产表名称。

### Metadata JSON 保留与 Snapshot 过期不同

Snapshot 过期和 Metadata JSON 保留是两套独立生命周期。即使 `snapshots` 中只剩一个当前 Snapshot，`metadata/` 目录仍可能保留多个历史 Metadata JSON，用于表版本历史、提交恢复和 `metadata-log`。先查看当前表仍明确引用的 Metadata JSON：

```sql
SELECT timestamp, file
FROM ice.learning_maintenance.events_binpack.metadata_log_entries
ORDER BY timestamp;
```

仍出现在这个结果中的路径不能手工删除。`clean_expired_metadata => true` 会随 Snapshot 过期清理不再使用的 Schema、Partition Spec、Sort Order 等元数据状态，但不表示立即删除全部历史 Metadata JSON。Metadata JSON 的版本保留由表属性控制：

```sql
SHOW TBLPROPERTIES ice.learning_maintenance.events_binpack;
```

对隔离学习表，如确实只需保留少量 Metadata 历史，可以配置后续提交的清理策略：

```sql
ALTER TABLE ice.learning_maintenance.events_binpack
SET TBLPROPERTIES (
    'metadata.previous-versions-max' = '2',
    'metadata.delete-after-commit.enabled' = 'true'
);
```

该配置主要影响之后的提交，不应假设会立即追溯删除所有既有 JSON。只有路径不再存在于有效 Metadata 引用链、已超过安全年龄窗口后，才可能被 `remove_orphan_files` 识别。先执行 dry-run 并人工核对候选路径；不要通过对象存储控制台直接删除 `metadata/` 目录中的 JSON。

`07_orphan_file_dry_run.sql` 永远以 `dry_run => true` 调用 `remove_orphan_files`，不会删除对象。它还显式设置 `prefix_listing => true`：默认路径使用 Hadoop `FileSystem` 递归列举，而项目的 `s3://` Warehouse 由 Iceberg `S3FileIO`（AWS SDK v2）访问，未配置 Hadoop `s3` FileSystem 时会报 `UnsupportedFileSystemException: No FileSystem for scheme "s3"`。Prefix Listing 改由 `S3FileIO` 列举对象，适合本实验的隔离小表；大规模生产扫描需要评估列举成本，或准备匹配 Hadoop 版本的 `hadoop-aws`、S3A 配置和 `s3a://` 路径。

该 Procedure 仍以实验表参数限定扫描范围。若要在外部环境继续练习真实删除，应另建隔离 Warehouse 或实验专属 Prefix，先使用对象存储工具放入一个可识别且已超过保留窗口的测试对象，确认 dry-run 只返回该对象后，才可在人工确认的本地脚本中移除 `dry_run`。不要把真实删除命令提交到本仓库。

## 失败路径

- 对不存在的表执行任一 Procedure，预期失败且不会创建或删除文件。
- 将 `rewrite_data_files` 的 `strategy` 改为不支持的值，预期失败；随后重新执行 `08_validate.sql`，逻辑行集必须不变。
- 在 `06` 中传入当前 Snapshot ID、被引用 Snapshot ID 或错误表名时，预期 Procedure 拒绝或保留所需状态；不要通过手工修改 Metadata JSON 规避。
- `remove_orphan_files` 的 normal path 只允许 dry run。任何真实删除都必须在仓库外、隔离 Location、完成候选文件人工核对后执行。

## 清理

完成观察后执行：

```bash
./scripts/run-spark-sql.sh labs/08-performance-and-maintenance/spark-sql/09_cleanup.sql
```

它只删除本实验的两张表，不会删除 Namespace、共享 Warehouse 或其他实验的对象。表删除后的物理文件清理行为由 Catalog、FileIO 和对象存储实现决定；不要因为学习表已删除就对共享 Location 执行广泛的孤儿文件清理。
