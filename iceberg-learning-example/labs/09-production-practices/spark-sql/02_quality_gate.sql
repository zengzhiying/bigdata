CREATE OR REPLACE TEMP VIEW order_quality AS
SELECT COUNT(*) AS row_count,
       COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_or_null_keys,
       COALESCE(SUM(CASE WHEN order_id IS NULL OR customer_id IS NULL
                             OR order_status IS NULL OR order_total IS NULL
                             OR order_date IS NULL THEN 1 ELSE 0 END), 0) AS missing_required,
       COALESCE(SUM(CASE WHEN order_total < 0 OR order_total > 1000000
                        THEN 1 ELSE 0 END), 0) AS invalid_amounts,
       COALESCE(SUM(CASE WHEN order_status NOT IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')
                        THEN 1 ELSE 0 END), 0) AS invalid_statuses,
       SUM(order_total) AS total_amount
FROM ice.learning_production.orders;

SELECT * FROM order_quality;

-- assert_true raises a SQL error; a printed CHECK alone would still exit successfully.
SELECT assert_true(
    row_count = 3 AND duplicate_or_null_keys = 0 AND missing_required = 0
    AND invalid_amounts = 0 AND invalid_statuses = 0
    AND COALESCE(total_amount = CAST(115.75 AS DECIMAL(12, 2)), false),
    'Order quality gate failed: inspect order_quality metrics'
) FROM order_quality;

SELECT 'PASS' AS quality_gate;
