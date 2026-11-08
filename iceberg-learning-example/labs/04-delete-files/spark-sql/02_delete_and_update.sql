-- merge-on-read 下的行级操作可能生成 Delete File；最终 content 类型需查询元数据确认。
-- 经验证会生成 deletes.parquet 文件
DELETE FROM ice.learning_delete_files.events
WHERE event_id = 3002;
-- 会生成 deletes.parquet 和一个新的 parquet 应该是删除 + 新增
UPDATE ice.learning_delete_files.events
SET event_value = 'account'
WHERE event_id = 3004;

MERGE INTO ice.learning_delete_files.events AS target
USING (
    SELECT * FROM VALUES
        (3005, 705, 'click', 'results', TIMESTAMP '2026-03-11 10:05:00'),
        (3007, 707, 'login', 'ok', TIMESTAMP '2026-03-12 08:00:00')
    AS source(event_id, user_id, event_type, event_value, event_time)
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET
    user_id = source.user_id,
    event_type = source.event_type,
    event_value = source.event_value,
    event_time = source.event_time
WHEN NOT MATCHED THEN INSERT (
    event_id, user_id, event_type, event_value, event_time
) VALUES (
    source.event_id, source.user_id, source.event_type,
    source.event_value, source.event_time
);

SELECT event_id, user_id, event_type, event_value
FROM ice.learning_delete_files.events
ORDER BY event_id;
