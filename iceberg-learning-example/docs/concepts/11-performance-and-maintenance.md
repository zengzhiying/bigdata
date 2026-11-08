# 11 - 性能与维护

## 先从元数据确认问题

Iceberg 查询不会通过列举 Warehouse 目录发现数据文件。一次扫描通常先读取当前 Metadata JSON、Manifest List 和 Manifest，再依据分区与列统计选择候选文件。因此性能诊断应先区分问题所在层级：

| 现象 | 优先观察 | 常见维护动作 |
| --- | --- | --- |
| 同一查询需要规划很多小 Data File | `files` 的文件数、大小和分区分布 | `rewrite_data_files`。 |
| 规划阶段需要加载很多 Manifest | `manifests`、`snapshots.manifest_list` | `rewrite_manifests`。 |
| 行级变更让读时需要应用 Delete File | `delete_files`、`entries`、`all_entries` | 数据文件或 Position Delete File 重写。 |
| 历史版本和 Metadata JSON 持续增长 | `snapshots`、`history`、`metadata_log_entries` | 在保留策略下过期 Snapshot。 |
| Location 中出现未被 Metadata 引用的旧对象 | Metadata 引用链与对象列表的差集 | 受控地识别、再删除孤儿文件。 |

对象存储中“文件很多”不是充分结论。先用 Metadata Table 找到当前 Snapshot 实际引用的文件，再判断它们是否构成瓶颈；历史 Snapshot、Branch、Tag 和长时间查询也可能继续合法地引用旧文件。

## 小文件与 Data File 重写

小文件通常来自高频小批次写入、过高并行度、分区过细或频繁单分区提交。它会增加：

- Driver 规划与 Manifest 读取开销。
- 对象存储请求数和任务调度开销。
- Delete File 与 Data File 匹配时需要处理的文件数量。

Spark 的 `rewrite_data_files` 会基于当前有效文件创建新的 Data File，并以一个新的 Snapshot 原子替换旧引用。读取者只能看到重写前或重写后的完整文件集合，逻辑数据不应改变。

```sql
CALL catalog.system.rewrite_data_files(
    table => 'namespace.table',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '268435456')
);
```

`target-file-size-bytes` 是重写目标，不是严格的最终文件大小保证。压缩比、Spark 任务布局、分区边界、文件格式和输入文件分组都可能使实际值不同。评估应同时记录重写前后 Data File 数、总字节数、平均文件大小、重写 Procedure 返回值和查询扫描指标。

## Bin-pack 与排序重写

`binpack` 主要合并小文件；它不会因为用户给出过滤条件而自动建立有利于该条件的记录顺序。若某类查询长期按特定列范围过滤，可以使用 `sort` 策略和明确的排序规则：

```sql
CALL catalog.system.rewrite_data_files(
    table => 'namespace.table',
    strategy => 'sort',
    sort_order => 'customer_id ASC NULLS LAST, event_id ASC NULLS LAST',
    options => map('target-file-size-bytes', '268435456')
);
```

排序可能改善文件级列统计的局部性，因而帮助范围过滤跳过更多文件；它同时需要额外 shuffle、排序和写入成本。不要仅因为表定义了 Sort Order 就假设任意查询都会加速。应从真实查询模式、`EXPLAIN`、扫描文件数和端到端指标判断收益。

分区与排序解决的问题不同：分区主要减少候选分区，排序主要改善分区内或未分区列的文件统计边界。Schema 或 Partition Spec 演进也不会自动重写历史文件；只有实际观察到查询收益足以覆盖维护成本时，才应对选定范围执行物理重写。

### 从排序到文件裁剪

排序本身不提供 B-Tree 或主键点查索引。它的主要作用是让相邻键更可能聚集在同一 Data File，使每个文件的列 Min/Max 范围更窄。Iceberg 将 Data File 的列统计写入 Manifest，扫描规划先据此裁剪文件；保留文件进入 Parquet Reader 后，Row Group 统计、Page Index 或 Bloom Filter 还可能继续减少读取量，具体取决于 Writer、Reader、文件内容和配置。

```text
customer_id = 3
  -> Manifest / Data File Min/Max：排除整个 Parquet 文件
  -> Parquet Row Group / Page 统计：排除文件内不可能命中的块
  -> 向量化读取候选列，并执行最终行级 Filter
```

用 Spark 的 `files` Metadata Table 可直接检查 Iceberg 用于文件规划的可读统计：

```sql
SELECT file_path,
       record_count,
       file_size_in_bytes,
       readable_metrics.customer_id.lower_bound AS customer_id_min,
       readable_metrics.customer_id.upper_bound AS customer_id_max,
       readable_metrics.event_id.lower_bound AS event_id_min,
       readable_metrics.event_id.upper_bound AS event_id_max
FROM catalog.namespace.table.files
WHERE content = 0
ORDER BY customer_id_min, customer_id_max, event_id_min, event_id_max, file_path;
```

假设排序后，一个分区的文件范围依次为 `0~1`、`2~3`、`4~6`、`6~8`、`8~10`，则 `customer_id = 3` 只需要保留 `2~3` 的文件。若每个文件仍都是 `0~10`，或每个分区只有一个文件，排序可能已执行但无法在文件级别跳过更多文件。

`EXPLAIN FORMATTED` 中的如下内容只证明谓词下推到 Iceberg Scan：

```text
BatchScan ... [filters=customer_id IS NOT NULL, customer_id = 3]
```

它不打印 Iceberg 最终选择的 Data File 清单。应先从文件区间推导候选集合，再在 Spark UI 或事件日志中比较带过滤与不带过滤查询的 `BatchScan` 输入文件数、输入字节数或扫描分区数；具体指标名称因 Spark 和 Iceberg Runtime 而异。

对于复合排序键 `(customer_id, event_id)`，首排序键具有最强的聚集效果：`customer_id = 3`，以及同时过滤 `customer_id` 和 `event_id` 的查询，通常最容易受益。只过滤第二列 `event_id` 时，多个文件的事件 ID 区间可能明显重叠，不能预期同样的裁剪效果。若表还按日期分区，查询应同时带原始时间列范围，使分区裁剪先排除无关日期，再由排序列缩小分区内的文件集合。

## Manifest 重写

每次产生 Snapshot 的数据提交都会产生或复用 Manifest。大量小提交可能使一个 Snapshot 的 Manifest List 指向大量小 Manifest，即使 Data File 本身并不大。`rewrite_manifests` 只整理 Manifest 元数据，不重写 Data File：

```sql
CALL catalog.system.rewrite_manifests(
    table => 'namespace.table',
    use_caching => true
);
```

因此它适合处理元数据规划开销，不能替代小文件合并。反过来，Data File 重写会形成新 Snapshot 并可能增加 Metadata 工作，但也不保证把 Manifest 数降到最小。两类操作应分别测量、按各自阈值调度。

### 重写提交的原子性

Snapshot 是某个时刻完整、一致的表文件引用集合，不对应单个 Parquet 文件，也不对应单个 Spark Task。一次成功的 `rewrite_data_files` 可以替换多个旧 Data File、写出多个新 Data File、创建多个 Manifest，并在最终提交时将它们一起纳入一个新的 `replace` Snapshot：

```text
旧 Snapshot
  -> data-a.parquet, data-b.parquet, data-c.parquet

rewrite_data_files 的单次原子提交

新 Snapshot
  -> data-1.parquet, data-2.parquet
```

Catalog 原子地将当前 Metadata 指针从旧表状态移动到新表状态。因此读取者只会看到重写前或重写后的完整文件集合，不会看到文件替换到一半的中间状态。Data File 和 Manifest 通常先在对象存储中写好；若最终提交失败，这些对象不会进入表的有效引用链，可能成为后续需要谨慎处理的孤儿文件。

`rewrite_manifests` 不改变当前 Data File 集合，却会改写 Manifest 和 Manifest List。由于 Snapshot 需要固定它所使用的 Manifest List，Manifest 重写也会创建一个新的 Snapshot，通常显示为 `operation = replace`：

```text
旧 Snapshot -> 旧 Manifest List / 多个旧 Manifest -> 同一组 Data File
新 Snapshot -> 新 Manifest List / 重写后的 Manifest -> 同一组 Data File
```

因此连续执行 Data File 重写和 Manifest 重写，通常会得到两个 `replace` Snapshot：前者的 Summary 显示 Data File 新增和删除，后者复用同一逻辑 Data File 路径但推进 Snapshot ID、Manifest List 和 Manifest 路径。可通过 `snapshots.summary`、`snapshots.manifest_list`、`manifests` 和 `files` Metadata Table 对照验证。

默认情况下，一次 `rewrite_data_files` 即使覆盖多个分区或 file group，也会合并为一个原子 Snapshot。显式启用 `partial-progress.enabled = true` 后，大型重写才可以拆成多次独立 `replace` Snapshot，以降低单次重写的失败重做范围；代价是中间重写结果会作为独立表版本依次可见。`ALTER TABLE SET TBLPROPERTIES` 等 Metadata-only 提交则可能只推进 Metadata JSON，不创建新的数据 Snapshot，不能与 Manifest 重写混淆。

## 快照过期与孤儿文件

Snapshot 过期从表元数据中移除不再需要的历史状态。它的保留窗口必须覆盖审计、回滚、备份、引用和长时间运行查询的要求：

```sql
CALL catalog.system.expire_snapshots(
    table => 'namespace.table',
    older_than => TIMESTAMP '2026-01-01 00:00:00',
    retain_last => 2
);
```

也可以对经过人工确认的特定 Snapshot ID 使用 `snapshot_ids`，特别适合学习或故障演练环境。不要把固定日期或自动推断的 Snapshot ID 当作生产保留策略。

### Metadata JSON 有独立保留策略

Snapshot 数量与 Warehouse 中 Metadata JSON 数量不是一一对应关系。即使所有历史 Snapshot 都已过期，只保留一个当前 Snapshot，表仍可能保留多个 Metadata JSON，用于表版本历史、提交恢复和 `metadata-log`。`metadata_log_entries` 能列出当前表 Metadata 仍记录的历史 Metadata JSON 路径：

```sql
SELECT timestamp, file
FROM catalog.namespace.table.metadata_log_entries
ORDER BY timestamp;
```

返回结果中的路径仍属于有效 Metadata 引用链，不能从对象存储中手工删除。`clean_expired_metadata => true` 随 Snapshot 过期清理不再使用的 Schema、Partition Spec、Sort Order 等状态，但不表示无条件删除全部历史 Metadata JSON。

Metadata JSON 保留由表属性控制：

```sql
ALTER TABLE catalog.namespace.table
SET TBLPROPERTIES (
    'metadata.previous-versions-max' = '2',
    'metadata.delete-after-commit.enabled' = 'true'
);
```

`metadata.previous-versions-max` 限制保留的历史 Metadata 版本数量；启用 `metadata.delete-after-commit.enabled` 后，后续提交可以删除超出窗口且不再需要的版本。该策略主要对之后的提交生效，不应假设它会立即追溯清理已经积累的所有 JSON。并且每次表提交本身都会产生新的 Metadata JSON，因此短期对象数量可能先增加。

当某个 Metadata JSON 已不在 `metadata_log_entries` 或其他有效 Metadata 引用链中，且超过孤儿文件安全窗口后，才应通过 `remove_orphan_files` 的 dry-run 识别并人工核对。不要以目录文件数判断 Snapshot 维护是否成功，也不要绕过 Iceberg Metadata 直接删除 `metadata/` 目录。

孤儿文件是不存在于任何有效 Metadata 引用链中的对象，常见于失败提交、过期 Snapshot 后的残留或中断维护任务。它不能只通过对象存储目录判断：一个看似旧的 Data File 可能仍被 Tag、Branch 或可 Time Travel 的 Snapshot 引用。

```sql
CALL catalog.system.remove_orphan_files(
    table => 'namespace.table',
    dry_run => true,
    prefix_listing => true
);
```

`prefix_listing => true` 让支持 Prefix Operation 的 FileIO（例如 Iceberg `S3FileIO`）列举对象，而不是使用 Hadoop `FileSystem`。当表 Location 是 `s3://`、运行时只配置 `S3FileIO` 而没有 Hadoop `s3` FileSystem 时，默认列举会报 `UnsupportedFileSystemException: No FileSystem for scheme "s3"`；此时 Prefix Listing 可避免引入 Hadoop S3A 依赖。对于大范围生产扫描，应评估单次 Prefix Listing 的对象数量和 Driver 开销；另一条路径是使用与 Spark Hadoop 版本匹配的 `hadoop-aws`、完整 S3A 配置和 `s3a://` Location。

先 dry run 并核对每个候选路径，再考虑删除。真实删除必须限制在表或实验专属 Location，保留足够长的对象年龄窗口，并避免与写入、提交重试或异步复制并发。不要对共享 Warehouse 根目录执行广泛的文件清理，也不要把未经核对的非 dry-run 命令作为定时任务。

## 推荐维护闭环

```text
监控元数据和查询指标
  -> 确认 Data File / Manifest / Delete File / Snapshot 中的实际瓶颈
  -> 在隔离窗口执行定向 rewrite
  -> 校验逻辑行集、Snapshot 和文件引用
  -> 按保留策略过期历史 Snapshot
  -> dry run 核对孤儿对象，再进行受控删除
```

阶段 8 的实验用独立的批式小文件 Fixture 保证可重复性。阶段 6 的流式表已具备基础验证时，
可将相同指标采集方法扩展到真实小批次负载；Flink 原生 Upsert 固定 Changelog Fixture 的
验证不是运行维护实验的前置条件。阶段 10 展示了最小的安全衔接：在 Spark 会话中先保存
`orders_current` 的逻辑行集，执行受控 `rewrite_data_files`，再用双向 `EXCEPT ALL` 验证
重写前后逻辑数据相同，并通过 `snapshots` 确认 `replace` 提交。完整命令与边界见
[`labs/10-order-lakehouse`](../../labs/10-order-lakehouse/README.md)。
