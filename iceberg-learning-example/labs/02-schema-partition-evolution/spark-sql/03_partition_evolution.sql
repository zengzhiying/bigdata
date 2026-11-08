-- Keep the original days transform and add a new partition field.
ALTER TABLE ice.learning_evolution.customers
ADD PARTITION FIELD bucket(8, customer_id);

-- 新写入的数据会按照新的结构生成分区
-- 但是查询历史数据可能会扫描更多的文件
INSERT INTO ice.learning_evolution.customers
    (customer_id, full_name, event_time, region, loyalty_tier)
VALUES
    (505, 'Evan', TIMESTAMP '2026-02-04 13:00:00', 'north', 'silver');

SELECT *
FROM ice.learning_evolution.customers.partitions;

SELECT spec_id, content, file_path, record_count
FROM ice.learning_evolution.customers.files
ORDER BY file_path;
