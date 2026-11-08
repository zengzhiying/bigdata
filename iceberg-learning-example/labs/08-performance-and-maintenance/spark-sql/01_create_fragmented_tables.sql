CREATE NAMESPACE IF NOT EXISTS ice.learning_maintenance;

-- Each INSERT is deliberately a separate commit. This creates a small, controlled
-- set of small files and manifests without relying on cluster-specific parallelism.
CREATE TABLE ice.learning_maintenance.events_binpack (
    event_id BIGINT,
    customer_id BIGINT,
    event_time TIMESTAMP,
    payload STRING
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '32768',
    'write.distribution-mode' = 'none'
);

CREATE TABLE ice.learning_maintenance.events_sort (
    event_id BIGINT,
    customer_id BIGINT,
    event_time TIMESTAMP,
    payload STRING
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '32768',
    'write.distribution-mode' = 'none'
);

-- The first six commits belong to one day and the remaining six to the next day.
-- Payload padding makes the file-size effect visible even in a small learning run.
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:00:00', concat('batch-01-', id, repeat('x', 512)) FROM range(1001, 1011);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:01:00', concat('batch-02-', id, repeat('x', 512)) FROM range(1011, 1021);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:02:00', concat('batch-03-', id, repeat('x', 512)) FROM range(1021, 1031);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:03:00', concat('batch-04-', id, repeat('x', 512)) FROM range(1031, 1041);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:04:00', concat('batch-05-', id, repeat('x', 512)) FROM range(1041, 1051);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:05:00', concat('batch-06-', id, repeat('x', 512)) FROM range(1051, 1061);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:00:00', concat('batch-07-', id, repeat('x', 512)) FROM range(1061, 1071);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:01:00', concat('batch-08-', id, repeat('x', 512)) FROM range(1071, 1081);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:02:00', concat('batch-09-', id, repeat('x', 512)) FROM range(1081, 1091);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:03:00', concat('batch-10-', id, repeat('x', 512)) FROM range(1091, 1101);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:04:00', concat('batch-11-', id, repeat('x', 512)) FROM range(1101, 1111);
INSERT INTO ice.learning_maintenance.events_binpack SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:05:00', concat('batch-12-', id, repeat('x', 512)) FROM range(1111, 1121);

INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:00:00', concat('batch-01-', id, repeat('x', 512)) FROM range(1001, 1011);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:01:00', concat('batch-02-', id, repeat('x', 512)) FROM range(1011, 1021);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:02:00', concat('batch-03-', id, repeat('x', 512)) FROM range(1021, 1031);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:03:00', concat('batch-04-', id, repeat('x', 512)) FROM range(1031, 1041);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:04:00', concat('batch-05-', id, repeat('x', 512)) FROM range(1041, 1051);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-01 09:05:00', concat('batch-06-', id, repeat('x', 512)) FROM range(1051, 1061);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:00:00', concat('batch-07-', id, repeat('x', 512)) FROM range(1061, 1071);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:01:00', concat('batch-08-', id, repeat('x', 512)) FROM range(1071, 1081);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:02:00', concat('batch-09-', id, repeat('x', 512)) FROM range(1081, 1091);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:03:00', concat('batch-10-', id, repeat('x', 512)) FROM range(1091, 1101);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:04:00', concat('batch-11-', id, repeat('x', 512)) FROM range(1101, 1111);
INSERT INTO ice.learning_maintenance.events_sort SELECT id, pmod(id, 11), TIMESTAMP '2026-08-02 09:05:00', concat('batch-12-', id, repeat('x', 512)) FROM range(1111, 1121);

