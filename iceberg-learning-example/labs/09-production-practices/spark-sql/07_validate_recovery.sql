CREATE OR REPLACE TEMP VIEW expected_orders AS
SELECT CAST(order_id AS BIGINT) AS order_id, CAST(customer_id AS BIGINT) AS customer_id,
       order_status, CAST(order_total AS DECIMAL(12, 2)) AS order_total, order_date
FROM VALUES
    (9101, 501, 'CREATED', 20.00, DATE '2026-06-01'),
    (9102, 502, 'PAID', 80.25, DATE '2026-06-01'),
    (9103, 503, 'SHIPPED', 15.50, DATE '2026-06-02')
AS fixture(order_id, customer_id, order_status, order_total, order_date);

-- Compare both directions, preserving duplicates; totals alone cannot prove recovery.
CREATE OR REPLACE TEMP VIEW unexpected_orders AS
SELECT * FROM ice.learning_production.orders EXCEPT ALL SELECT * FROM expected_orders;
CREATE OR REPLACE TEMP VIEW missing_orders AS
SELECT * FROM expected_orders EXCEPT ALL SELECT * FROM ice.learning_production.orders;

SELECT * FROM unexpected_orders;
SELECT * FROM missing_orders;
SELECT assert_true(COUNT(*) = 0, 'Recovery contains unexpected rows') FROM unexpected_orders;
SELECT assert_true(COUNT(*) = 0, 'Recovery is missing expected rows') FROM missing_orders;
SELECT assert_true(COUNT(*) = 1, 'Current main reference does not match baseline')
FROM ice.learning_production.orders.refs
WHERE name = 'main' AND snapshot_id = ${hivevar:baseline_snapshot_id};
SELECT 'PASS' AS recovery_check;
