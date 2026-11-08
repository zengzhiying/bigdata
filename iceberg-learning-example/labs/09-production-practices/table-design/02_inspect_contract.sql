DESCRIBE TABLE EXTENDED ice.learning_design.orders_contract;
SHOW TBLPROPERTIES ice.learning_design.orders_contract;

SELECT order_id, customer_id, order_status, order_total, order_created_at,
       source_event_id, source_event_version, source_updated_at
FROM ice.learning_design.orders_contract
ORDER BY order_id;

SELECT partition, record_count, file_size_in_bytes
FROM ice.learning_design.orders_contract.files
WHERE content = 0
ORDER BY partition, file_path;

SELECT assert_true(
    COUNT(*) = 3 AND COUNT(DISTINCT order_id) = 3
    AND COUNT(DISTINCT source_event_id) = 3
    AND SUM(order_total) = CAST(79.75 AS DECIMAL(12, 2)),
    'Order design fixture violates its business contract'
) FROM ice.learning_design.orders_contract;
SELECT 'PASS' AS design_contract_check;
