SELECT customer_id, full_name, loyalty_tier, event_time, region
FROM ice.learning_evolution.customers
ORDER BY customer_id;

SELECT *
FROM ice.learning_evolution."customers$partitions";

SELECT spec_id, file_path, record_count
FROM ice.learning_evolution."customers$files"
ORDER BY file_path;
