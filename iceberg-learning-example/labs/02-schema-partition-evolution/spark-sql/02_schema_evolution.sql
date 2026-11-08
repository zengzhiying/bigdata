-- 每次操作后底层的元数据 JSON 文件都会新增一个版本
ALTER TABLE ice.learning_evolution.customers ADD COLUMNS (loyalty_tier STRING);

ALTER TABLE ice.learning_evolution.customers
RENAME COLUMN customer_name TO full_name;

ALTER TABLE ice.learning_evolution.customers
ALTER COLUMN customer_id TYPE BIGINT;

INSERT INTO ice.learning_evolution.customers
    (customer_id, full_name, event_time, region, loyalty_tier)
VALUES
    (504, 'Dora', TIMESTAMP '2026-02-03 12:00:00', 'south', 'gold');

DESCRIBE TABLE EXTENDED ice.learning_evolution.customers;

SELECT customer_id, full_name, loyalty_tier
FROM ice.learning_evolution.customers
ORDER BY customer_id;
