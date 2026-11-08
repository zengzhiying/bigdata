CREATE OR REPLACE TEMP VIEW capstone_current_quality AS
SELECT COUNT(*) AS row_count,
       COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_or_null_keys,
       COALESCE(SUM(CASE WHEN order_id IS NULL OR customer_id IS NULL
                             OR order_status IS NULL OR order_total IS NULL
                             OR event_version IS NULL OR source_event_id IS NULL
                         THEN 1 ELSE 0 END), 0) AS missing_required,
       COALESCE(SUM(CASE WHEN order_total < 0 THEN 1 ELSE 0 END), 0) AS invalid_amounts,
       COALESCE(SUM(CASE WHEN order_status NOT IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')
                         THEN 1 ELSE 0 END), 0) AS invalid_statuses,
       SUM(order_total) AS total_amount
FROM ice.learning_capstone.orders_current;

SELECT * FROM capstone_current_quality;
SELECT assert_true(
    row_count = 2 AND duplicate_or_null_keys = 0 AND missing_required = 0
    AND invalid_amounts = 0 AND invalid_statuses = 0
    AND COALESCE(total_amount = CAST(95.75 AS DECIMAL(12, 2)), false),
    'Capstone current-state quality gate failed'
) FROM capstone_current_quality;
SELECT 'PASS' AS current_state_quality_gate;

-- The new attribute is first added without rewriting historical files, then populated deliberately.
ALTER TABLE ice.learning_capstone.orders_current
ADD COLUMN fulfillment_region STRING COMMENT 'Fulfillment routing region derived by the curated layer';

UPDATE ice.learning_capstone.orders_current
SET fulfillment_region = CASE customer_id
    WHEN 502 THEN 'east'
    WHEN 503 THEN 'west'
    ELSE 'unknown'
END;

INSERT OVERWRITE ice.learning_capstone.orders_daily_metrics
SELECT event_day,
       fulfillment_region,
       order_status,
       COUNT(*) AS order_count,
       CAST(SUM(order_total) AS DECIMAL(14, 2)) AS total_amount
FROM ice.learning_capstone.orders_current
GROUP BY event_day, fulfillment_region, order_status;

SELECT order_id, customer_id, fulfillment_region, order_status, order_total
FROM ice.learning_capstone.orders_current
ORDER BY order_id;

SELECT event_day, fulfillment_region, order_status, order_count, total_amount
FROM ice.learning_capstone.orders_daily_metrics
ORDER BY fulfillment_region, order_status;
