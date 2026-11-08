SELECT
    CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'CHECK' END AS row_count_check,
    COUNT(*) AS row_count
FROM ice.learning_evolution.customers;

SELECT
    CASE WHEN SUM(CASE WHEN full_name = 'Alice' AND loyalty_tier IS NULL THEN 1 ELSE 0 END) = 1
         AND SUM(CASE WHEN full_name = 'Dora' AND loyalty_tier = 'gold' THEN 1 ELSE 0 END) = 1
         THEN 'PASS' ELSE 'CHECK'
    END AS schema_evolution_check
FROM ice.learning_evolution.customers;

SELECT
    CASE WHEN COUNT(DISTINCT spec_id) >= 2 THEN 'PASS' ELSE 'CHECK' END AS partition_spec_check,
    COUNT(DISTINCT spec_id) AS spec_count
FROM ice.learning_evolution.customers.files;
