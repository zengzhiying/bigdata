-- Run after 00_catalog.sql in the same Flink SQL Client session.
CREATE DATABASE IF NOT EXISTS learning_flink;

CREATE TABLE IF NOT EXISTS learning_flink.orders_from_flink (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP(3)
)
-- Flink SQL accepts identity partition columns here, not Spark's days(event_time) transform.
PARTITIONED BY (event_time)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

INSERT INTO learning_flink.orders_from_flink VALUES
    (9101, 801, 'CREATED', CAST(15.50 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-01 09:00:00'),
    (9102, 802, 'PAID',    CAST(42.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-01 10:30:00'),
    (9103, 803, 'SHIPPED', CAST(88.25 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-02 11:15:00');
