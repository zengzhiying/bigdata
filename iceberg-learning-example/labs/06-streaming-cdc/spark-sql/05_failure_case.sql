-- Expected to fail: the source has two rows for order_id 1002, so a MERGE cannot
-- choose one update for the same matched target row. Re-run 03_validate.sql after it.
MERGE INTO ice.learning_cdc.orders_current AS target
USING (
    SELECT * FROM VALUES
        (1002, 'PROCESSING'),
        (1002, 'SHIPPED')
    AS source(order_id, order_status)
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET order_status = source.order_status;
