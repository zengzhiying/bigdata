-- Separate commit for an intentionally invalid input batch.
INSERT INTO ice.learning_production.orders VALUES
    (9101, 501, 'UNKNOWN', -5.00, DATE '2026-06-01'),
    (9104, NULL, 'CREATED', 10.00, DATE '2026-06-02');

SELECT * FROM ice.learning_production.orders ORDER BY order_id, order_total;
