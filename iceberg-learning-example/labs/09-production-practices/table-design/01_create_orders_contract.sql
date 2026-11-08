CREATE NAMESPACE IF NOT EXISTS ice.learning_design;
DROP TABLE IF EXISTS ice.learning_design.orders_contract;

-- This is a business contract, not an enforced database primary key. The write
-- pipeline must still deduplicate order_id and source_event_id before publishing.
CREATE TABLE ice.learning_design.orders_contract (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    order_created_at TIMESTAMP,
    source_event_id STRING,
    source_event_version BIGINT,
    source_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(order_created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.distribution-mode' = 'hash',
    'write.target-file-size-bytes' = '268435456',
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'business-owner' = 'data-platform',
    'data-classification' = 'internal',
    'retention-policy' = 'orders-standard-v1'
);

INSERT INTO ice.learning_design.orders_contract VALUES
    (9201, 601, 'CREATED', 22.00, TIMESTAMP '2026-09-01 09:00:00',
     'design-9201-v1', 1, TIMESTAMP '2026-09-01 09:00:00'),
    (9202, 602, 'PAID', 42.25, TIMESTAMP '2026-09-01 10:00:00',
     'design-9202-v1', 1, TIMESTAMP '2026-09-01 10:00:00'),
    (9203, 601, 'SHIPPED', 15.50, TIMESTAMP '2026-09-02 11:00:00',
     'design-9203-v2', 2, TIMESTAMP '2026-09-02 11:00:00');
