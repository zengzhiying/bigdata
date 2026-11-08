-- 在配置了外部 Iceberg Catalog 的 Spark 3.5 Session 中执行此文件。
-- Catalog 名称为占位值；请根据外部环境配置将 `ice` 替换为实际名称。

CREATE NAMESPACE IF NOT EXISTS ice.learning;

SHOW NAMESPACES IN ice;

-- 默认情况下数据采用 Parquet 格式存储
-- 配置 TBLPROPERTIES ('write.format.default' = 'parquet'); 可以省略，除此之外还支持 orc、avro
CREATE TABLE IF NOT EXISTS ice.learning.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time));

SHOW TABLES IN ice.learning;

DESCRIBE TABLE EXTENDED ice.learning.orders;

-- Expected: one table named orders in namespace learning.
