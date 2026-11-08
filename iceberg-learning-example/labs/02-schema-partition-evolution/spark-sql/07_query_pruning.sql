-- The table now contains old files with spec_id=0 and new files with spec_id=1.
SELECT spec_id, COUNT(*) AS file_count, SUM(record_count) AS records
FROM ice.learning_evolution.customers.files
GROUP BY spec_id
ORDER BY spec_id;

-- Case 1: both old and new specs can use the days(event_time) transform.
EXPLAIN FORMATTED
SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE event_time >= TIMESTAMP '2026-02-04 00:00:00';

SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE event_time >= TIMESTAMP '2026-02-04 00:00:00'
ORDER BY customer_id;

-- Case 2: the bucket transform can help new files, but old files have no bucket.
EXPLAIN FORMATTED
SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE customer_id = 505;

SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE customer_id = 505
ORDER BY customer_id;

-- Case 3: new files can use both transforms when both predicates are present.
EXPLAIN FORMATTED
SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE event_time >= TIMESTAMP '2026-02-04 00:00:00'
  AND customer_id = 505;

SELECT customer_id, full_name
FROM ice.learning_evolution.customers
WHERE event_time >= TIMESTAMP '2026-02-04 00:00:00'
  AND customer_id = 505
ORDER BY customer_id;
