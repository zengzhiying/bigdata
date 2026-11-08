-- All tables in this lab are isolated in learning_capstone.
DROP TABLE IF EXISTS ice.learning_capstone.orders_daily_metrics;
DROP TABLE IF EXISTS ice.learning_capstone.orders_current;
DROP TABLE IF EXISTS ice.learning_capstone.orders_cdc_raw;
CREATE NAMESPACE IF NOT EXISTS ice.learning_capstone;

CREATE TABLE ice.learning_capstone.orders_cdc_raw (
    event_id STRING,
    operation STRING,
    order_id BIGINT,
    event_version BIGINT,
    event_time TIMESTAMP,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_day DATE
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.distribution-mode' = 'hash'
);

CREATE TABLE ice.learning_capstone.orders_current (
    order_id BIGINT,
    customer_id BIGINT,
    order_status STRING,
    order_total DECIMAL(12, 2),
    event_version BIGINT,
    event_time TIMESTAMP,
    event_day DATE,
    source_event_id STRING
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

CREATE TABLE ice.learning_capstone.orders_daily_metrics (
    event_day DATE,
    fulfillment_region STRING,
    order_status STRING,
    order_count BIGINT,
    total_amount DECIMAL(14, 2)
)
USING iceberg
PARTITIONED BY (event_day)
TBLPROPERTIES ('format-version' = '2');
