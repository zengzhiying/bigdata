-- Manual SQL Client template only. workdir/flink-sql.sh generates this catalog from workdir/.env.
-- Replace every <...> placeholder before direct execution. Do not commit real credentials.
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG ice WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = '<CATALOG_URI>',
    'warehouse' = '<CATALOG_WAREHOUSE>',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = '<S3_ENDPOINT>',
    's3.path-style-access' = '<S3_PATH_STYLE_ACCESS>',
    's3.access-key-id' = '<MINIO_ACCESS_KEY_ID>',
    's3.secret-access-key' = '<MINIO_SECRET_ACCESS_KEY>',
    'client.region' = '<AWS_REGION>'
);

USE CATALOG ice;
