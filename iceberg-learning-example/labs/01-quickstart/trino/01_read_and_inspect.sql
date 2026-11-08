-- 在将 Iceberg Catalog 配置为 `ice` 的 Trino 476 中执行此文件。

SELECT *
FROM ice.learning.orders
ORDER BY order_id;

SELECT snapshot_id, committed_at, operation
FROM ice.learning."orders$snapshots"
ORDER BY committed_at;

SELECT file_path, file_size_in_bytes, record_count
FROM ice.learning."orders$files";

-- 将 <SNAPSHOT_ID> 替换为 Spark 元数据查询得到的历史快照 ID。
SELECT order_id, order_status, order_total
FROM ice.learning.orders
FOR VERSION AS OF <SNAPSHOT_ID>
ORDER BY order_id;
