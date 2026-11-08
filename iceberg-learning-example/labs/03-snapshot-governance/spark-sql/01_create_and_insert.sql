CREATE NAMESPACE IF NOT EXISTS ice.learning_governance;

CREATE TABLE ice.learning_governance.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2'
);

INSERT INTO ice.learning_governance.orders VALUES
    (2001, 601, 'CREATED', CAST(10.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-03-01 09:00:00'),
    (2002, 602, 'CREATED', CAST(20.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-03-01 10:00:00'),
    (2003, 603, 'CREATED', CAST(30.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-03-02 11:00:00');

INSERT INTO ice.learning_governance.orders VALUES
    (2004, 604, 'CREATED', CAST(40.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-03-02 12:00:00');

UPDATE ice.learning_governance.orders
SET order_status = 'PAID'
WHERE order_id = 2001;

SELECT order_id, order_status, order_total
FROM ice.learning_governance.orders
ORDER BY order_id;
