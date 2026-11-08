CREATE NAMESPACE IF NOT EXISTS ice.learning_delete_files;

CREATE TABLE ice.learning_delete_files.events (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_value STRING,
    event_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

INSERT INTO ice.learning_delete_files.events VALUES
    (3001, 701, 'login',  'ok', TIMESTAMP '2026-03-10 09:00:00'),
    (3002, 702, 'click',  'home', TIMESTAMP '2026-03-10 09:05:00'),
    (3003, 703, 'logout', 'ok', TIMESTAMP '2026-03-10 09:10:00');

INSERT INTO ice.learning_delete_files.events VALUES
    (3004, 704, 'login',  'ok', TIMESTAMP '2026-03-11 10:00:00'),
    (3005, 705, 'click',  'search', TIMESTAMP '2026-03-11 10:05:00'),
    (3006, 706, 'logout', 'ok', TIMESTAMP '2026-03-11 10:10:00');

SELECT event_id, user_id, event_type, event_value
FROM ice.learning_delete_files.events
ORDER BY event_id;
