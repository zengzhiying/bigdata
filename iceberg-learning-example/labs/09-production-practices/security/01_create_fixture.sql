CREATE NAMESPACE IF NOT EXISTS ice.learning_security;
DROP TABLE IF EXISTS ice.learning_security.orders_access_fixture;

CREATE TABLE ice.learning_security.orders_access_fixture (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'data-classification' = 'internal',
    'business-owner' = 'data-platform'
);

INSERT INTO ice.learning_security.orders_access_fixture VALUES
    (9301, 701, 'PAID', 30.00, TIMESTAMP '2026-09-01 12:00:00');
