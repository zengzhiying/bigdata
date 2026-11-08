-- Run only AFTER recovering from the bad-input exercise.
-- This remains structurally valid, but violates the business reconciliation.
UPDATE ice.learning_production.orders SET order_total = order_total + 10.00;
SELECT order_id, order_total FROM ice.learning_production.orders ORDER BY order_id;
