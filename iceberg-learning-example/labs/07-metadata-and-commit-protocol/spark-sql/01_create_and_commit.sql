-- Each DML statement below creates a separate snapshot. Keep the statements separate
-- so the metadata lineage has a small, deterministic shape to inspect.
CREATE NAMESPACE IF NOT EXISTS ice.learning_metadata;

CREATE TABLE ice.learning_metadata.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.distribution-mode' = 'hash'
);

INSERT INTO ice.learning_metadata.orders VALUES
    (7101, 801, 'CREATED', CAST(18.50 AS DECIMAL(12, 2)), TIMESTAMP '2026-07-01 09:00:00'),
    (7102, 802, 'CREATED', CAST(25.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-07-01 09:05:00');

INSERT INTO ice.learning_metadata.orders VALUES
    (7103, 803, 'PAID', CAST(41.25 AS DECIMAL(12, 2)), TIMESTAMP '2026-07-02 10:00:00');

UPDATE ice.learning_metadata.orders
SET order_status = 'PAID', order_total = CAST(26.00 AS DECIMAL(12, 2))
WHERE order_id = 7102;

-- A metadata-only commit shows that Metadata JSON can advance without a new snapshot.
ALTER TABLE ice.learning_metadata.orders
SET TBLPROPERTIES ('learning.lab.stage' = 'metadata-and-commit-protocol');

SELECT order_id, order_status, order_total, event_time
FROM ice.learning_metadata.orders
ORDER BY order_id;

