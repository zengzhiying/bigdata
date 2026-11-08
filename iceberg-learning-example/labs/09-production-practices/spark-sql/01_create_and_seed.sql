CREATE NAMESPACE IF NOT EXISTS ice.learning_production;

-- Nullable fields deliberately let the quality gate detect bad input.
CREATE TABLE ice.learning_production.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    order_date DATE
) USING iceberg
PARTITIONED BY (days(order_date))
TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'copy-on-write',
               'write.update.mode' = 'copy-on-write');

INSERT INTO ice.learning_production.orders VALUES
    (9101, 501, 'CREATED', 20.00, DATE '2026-06-01'),
    (9102, 502, 'PAID', 80.25, DATE '2026-06-01'),
    (9103, 503, 'SHIPPED', 15.50, DATE '2026-06-02');

-- Record the main reference, not MAX(snapshot_id) or the newest retained snapshot.
SELECT r.snapshot_id AS baseline_snapshot_id, s.committed_at, s.operation
FROM ice.learning_production.orders.refs r
JOIN ice.learning_production.orders.snapshots s ON r.snapshot_id = s.snapshot_id
WHERE r.name = 'main';
