-- Run in Trino 476 with catalog `ice`. This script resets only its dedicated table.
CREATE SCHEMA IF NOT EXISTS ice.learning_trino;

DROP TABLE IF EXISTS ice.learning_trino.orders_from_trino;

CREATE TABLE ice.learning_trino.orders_from_trino (
    order_id BIGINT,
    customer_id BIGINT,
    order_status VARCHAR,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP(6)
)
WITH (
    format_version = 2,
    partitioning = ARRAY['day(event_time)']
);

INSERT INTO ice.learning_trino.orders_from_trino VALUES
    (9201, 901, 'CREATED', DECIMAL '15.50', TIMESTAMP '2026-04-07 09:00:00'),
    (9202, 902, 'PAID', DECIMAL '42.00', TIMESTAMP '2026-04-07 10:30:00'),
    (9203, 903, 'SHIPPED', DECIMAL '88.25', TIMESTAMP '2026-04-08 11:15:00');

-- Schema evolution is deliberately performed before the row-level operations.
ALTER TABLE ice.learning_trino.orders_from_trino ADD COLUMN source VARCHAR;
ALTER TABLE ice.learning_trino.orders_from_trino RENAME COLUMN order_status TO fulfillment_status;

MERGE INTO ice.learning_trino.orders_from_trino AS target
USING (
    VALUES
        (9201, 901, 'PAID', DECIMAL '17.00', TIMESTAMP '2026-04-07 09:00:00'),
        (9204, 904, 'CREATED', DECIMAL '55.00', TIMESTAMP '2026-04-08 12:00:00')
) AS source (order_id, customer_id, fulfillment_status, order_total, event_time)
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    customer_id = source.customer_id,
    fulfillment_status = source.fulfillment_status,
    order_total = source.order_total,
    event_time = source.event_time
WHEN NOT MATCHED THEN INSERT (
    order_id, customer_id, fulfillment_status, order_total, event_time
) VALUES (
    source.order_id, source.customer_id, source.fulfillment_status, source.order_total, source.event_time
);

UPDATE ice.learning_trino.orders_from_trino
SET fulfillment_status = 'SHIPPED', source = 'trino'
WHERE order_id = 9201;

UPDATE ice.learning_trino.orders_from_trino
SET source = 'trino'
WHERE order_id IN (9202, 9203, 9204);

DELETE FROM ice.learning_trino.orders_from_trino
WHERE order_id = 9202;
