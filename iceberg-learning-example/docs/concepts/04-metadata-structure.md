# 04 - 元数据结构

## 元数据树

Iceberg 使用一组相互引用的文件描述表状态：

```text
当前 Metadata JSON
    ├── Schema
    ├── Partition Specs
    ├── Sort Orders
    └── Snapshots
          └── Manifest List
                └── Manifest
                      └── Data File / Delete File
```

查询从当前 Metadata JSON 找到当前 Snapshot，再通过 Manifest List 和 Manifest 规划需要扫描的文件。

## Snapshot

Snapshot 表示一次成功提交后的表状态，通常包含：

- Snapshot ID。
- 父 Snapshot ID。
- 提交时间。
- 操作类型，例如 append、overwrite 或 delete。
- Manifest List 位置。
- 操作摘要。

旧 Snapshot 仍然可以被保留，因此查询可以读取历史版本。

## Manifest

Manifest 是数据文件索引。它通常保存文件路径、文件大小、记录数、分区统计和列统计等信息。查询引擎可以先用这些信息过滤文件，再读取真正的数据。

Manifest List 记录某个 Snapshot 使用的 Manifest 集合。它把“某次提交使用哪些 Manifest”固定下来，使查询规划不必重新扫描所有文件。

## 元数据表

Spark 和 Trino 通常可以通过元数据表查看 Iceberg 内部状态。常用对象包括：

```text
table.snapshots
table.history
table.files
table.manifests
table.partitions
table.refs
```

不同引擎的语法略有差异，实验中要分别记录查询方式和字段差异。

## 从元数据表到物理文件

元数据表适合关联和筛选，但它们不应替代对真实文件引用的理解。一次观察可按以下顺序进行：

1. 从 `metadata_log_entries` 找到当前 Metadata JSON 之前的版本。
2. 从 `snapshots.manifest_list` 找到某个 Snapshot 的 Manifest List。
3. 从 `manifests.path` 找到该 Snapshot 引用的 Manifest。
4. 用 `files` 观察当前有效文件，用 `all_entries` 观察曾被新增、删除或替换的文件。

当前 Metadata JSON 可以通过 Iceberg `TableMetadataParser` 读取，不要根据 Warehouse
目录名称猜测它的位置，更不要手工编辑 JSON。实验 07 将这条链路和一个旧 Metadata 的
并发提交冲突放入可重复实验中。

## 为什么元数据很重要

元数据不仅用于解释 Iceberg 的内部实现，也是生产排障的重要依据：

- 查询扫描文件过多时，检查分区和文件统计。
- 小文件问题可以从 Files 元数据观察。
- 提交冲突可以从 History 和 Snapshots 还原。
- 快照和孤儿文件维护需要依据元数据关系判断。
