-- A filesystem source is append-only. Deduplicate its event log before applying
-- the business current-state contract through one deterministic Iceberg MERGE.
MERGE INTO ice.learning_cdc.orders_current AS target
USING (
    WITH unique_events AS (
        SELECT event_id, operation, order_id, event_version, event_time,
               customer_id, order_status, order_total, event_day,
               ROW_NUMBER() OVER (
                   PARTITION BY event_id
                   ORDER BY event_time DESC, event_version DESC
               ) AS duplicate_rank
        FROM ice.learning_cdc.orders_cdc_raw
    ),
    latest_event_per_order AS (
        SELECT event_id, operation, order_id, event_version, event_time,
               customer_id, order_status, order_total, event_day,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY event_version DESC, event_time DESC, event_id DESC
               ) AS version_rank
        FROM unique_events
        WHERE duplicate_rank = 1
    )
    SELECT event_id, operation, order_id, event_version, event_time,
           customer_id, order_status, order_total, event_day
    FROM latest_event_per_order
    WHERE version_rank = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.operation = 'DELETE'
                  AND source.event_version >= target.event_version THEN DELETE
WHEN MATCHED AND source.operation <> 'DELETE'
                  AND source.event_version > target.event_version THEN UPDATE SET
    customer_id = source.customer_id,
    order_status = source.order_status,
    order_total = source.order_total,
    event_version = source.event_version,
    event_time = source.event_time,
    event_day = source.event_day,
    source_event_id = source.event_id
WHEN NOT MATCHED AND source.operation <> 'DELETE' THEN INSERT (
    order_id, customer_id, order_status, order_total, event_version,
    event_time, event_day, source_event_id
) VALUES (
    source.order_id, source.customer_id, source.order_status, source.order_total,
    source.event_version, source.event_time, source.event_day, source.event_id
);

SELECT order_id, customer_id, order_status, order_total, event_version, source_event_id
FROM ice.learning_cdc.orders_current
ORDER BY order_id;
