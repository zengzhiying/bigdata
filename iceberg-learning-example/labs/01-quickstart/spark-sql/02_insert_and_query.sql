-- 插入基础订单，并验证查询结果。

INSERT INTO ice.learning.orders VALUES
    (1001, 501, 'CREATED', 19.90, TIMESTAMP '2026-01-10 10:00:00'),
    (1002, 502, 'PAID', 29.50, TIMESTAMP '2026-01-10 11:00:00'),
    (1003, 503, 'SHIPPED', 88.00, TIMESTAMP '2026-01-11 09:30:00'),
    (1004, 504, 'CREATED', 12.75, TIMESTAMP '2026-01-11 12:00:00'),
    (1005, 505, 'CANCELLED', 45.00, TIMESTAMP '2026-01-12 15:45:00');

SELECT order_id, customer_id, order_status, order_total, event_time
FROM ice.learning.orders
ORDER BY order_id;

-- 过滤 event_time，用于后续观察分区裁剪和快照变化。
SELECT COUNT(*) AS order_count
FROM ice.learning.orders
WHERE event_time >= TIMESTAMP '2026-01-11 00:00:00';
