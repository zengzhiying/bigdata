CREATE NAMESPACE IF NOT EXISTS ice.learning_evolution;

CREATE TABLE ice.learning_evolution.customers (
    customer_id INT,
    customer_name STRING,
    event_time TIMESTAMP,
    region STRING
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES ('format-version' = '2');

INSERT INTO ice.learning_evolution.customers VALUES
    (501, 'Alice', TIMESTAMP '2026-02-01 09:00:00', 'east'),
    (502, 'Bob',   TIMESTAMP '2026-02-01 10:00:00', 'west'),
    (503, 'Carol', TIMESTAMP '2026-02-02 11:00:00', 'east');

SELECT customer_id, customer_name, event_time, region
FROM ice.learning_evolution.customers
ORDER BY customer_id;
