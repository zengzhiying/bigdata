-- 先更新一条订单，再删除一条订单。文件级实现取决于表格式版本和引擎策略。
-- 默认情况下更新或删除都会新增 Parquet 文件
UPDATE ice.learning.orders
SET order_status = 'PAID'
WHERE order_id = 1001;
DELETE FROM ice.learning.orders
WHERE order_id = 1004;

-- MERGE 同时演示匹配更新和未匹配插入。
MERGE INTO ice.learning.orders AS target
USING (
    SELECT * FROM VALUES
        (1002, 502, 'SHIPPED', CAST(29.50 AS DECIMAL(12, 2)), TIMESTAMP '2026-01-10 11:00:00'),
        (1006, 506, 'CREATED', CAST(66.60 AS DECIMAL(12, 2)), TIMESTAMP '2026-01-13 08:00:00')
    AS source(order_id, customer_id, order_status, order_total, event_time)
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id = source.customer_id,
    order_status = source.order_status,
    order_total = source.order_total,
    event_time = source.event_time
WHEN NOT MATCHED THEN INSERT (
    order_id, customer_id, order_status, order_total, event_time
) VALUES (
    source.order_id, source.customer_id, source.order_status,
    source.order_total, source.event_time
);

SELECT order_id, order_status
FROM ice.learning.orders
ORDER BY order_id;

-- Expected final IDs: 1001, 1002, 1003, 1005, 1006.
