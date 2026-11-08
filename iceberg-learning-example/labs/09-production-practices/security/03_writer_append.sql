-- Run with the writer identity after the reader test. This is the writer's only
-- allowed mutation in this exercise; it does not test DDL or maintenance rights.
INSERT INTO ice.learning_security.orders_access_fixture VALUES
    (9302, 702, 'CREATED', 12.00, TIMESTAMP '2026-09-02 12:00:00');

SELECT assert_true(COUNT(*) = 2, 'Writer append did not produce two rows')
FROM ice.learning_security.orders_access_fixture;
SELECT 'PASS' AS writer_append_check;
