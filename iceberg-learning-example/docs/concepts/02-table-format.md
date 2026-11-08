# 02 - Iceberg 表格式

## 表格式和文件格式

Parquet、ORC 和 Avro 是文件格式，描述单个文件内部如何编码列式或行式数据。Iceberg 是表格式，描述多个数据文件如何组成一张表，以及表如何进行事务提交、演进和查询。

一张 Iceberg 表通常由以下部分组成：

```text
Table metadata JSON
    └── Snapshot
          └── Manifest List
                └── Manifest
                      ├── Data File
                      └── Delete File
```

## Schema 和字段 ID

Iceberg Schema 为字段分配稳定的字段 ID。字段名称可以修改，但字段 ID 保持不变，因此重命名不会把旧字段误认为新字段。

示例：

```text
旧 Schema: customer_name (field id = 5)
新 Schema: buyer_name    (field id = 5)
```

这里发生的是字段名称变化，而不是删除旧字段再创建新字段。

## 分区规范

Iceberg 通过 Partition Spec 描述分区转换，例如：

- `days(event_time)`：按事件日期分区。
- `months(event_time)`：按事件月份分区。
- `bucket(16, customer_id)`：按哈希桶分区。
- `truncate(8, customer_id)`：按截断值分区。

分区字段可以来自 Schema 中的字段，也可以是由 Transform 计算出的隐藏字段。查询引擎根据过滤条件推导分区范围，而不要求用户直接查询物理分区列。

## Format Version

Iceberg Format Version 1 提供基础快照和表演进能力。Format Version 2 增加了面向行级更新和删除的 Delete File 语义。

项目中的行级操作实验应明确记录表的 Format Version，以及各计算引擎对 Equality Delete 和 Position Delete 的支持情况。

## 学习要点

- 文件格式解决“文件内部如何存储”。
- 表格式解决“文件如何组成表并保持一致”。
- Schema 使用字段 ID 保障演进安全。
- Partition Spec 可以演进，历史数据不必全部重写。
- Format Version 2 为更新和删除提供了更完整的基础。
