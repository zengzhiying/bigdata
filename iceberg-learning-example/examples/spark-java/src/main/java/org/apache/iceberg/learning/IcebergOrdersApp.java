package org.apache.iceberg.learning;

import org.apache.spark.sql.SparkSession;

/** A small, repeatable Spark Java path for an Iceberg REST Catalog. */
public final class IcebergOrdersApp {
  private static final String NAMESPACE = "learning_api";
  private static final String TABLE = "orders_java";

  private IcebergOrdersApp() {
  }

  public static void main(String[] args) {
    String catalog = optionalEnv("CATALOG_NAME", "ice");
    if (!catalog.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new IllegalArgumentException("CATALOG_NAME must be a Spark identifier");
    }

    SparkSession spark = SparkSession.builder()
        .appName("iceberg-learning-spark-java")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog." + catalog, "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog." + catalog + ".catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog." + catalog + ".uri", requiredEnv("CATALOG_URI"))
        .config("spark.sql.catalog." + catalog + ".warehouse", requiredEnv("CATALOG_WAREHOUSE"))
        .config("spark.sql.catalog." + catalog + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog." + catalog + ".s3.endpoint", requiredEnv("S3_ENDPOINT"))
        .config("spark.sql.catalog." + catalog + ".s3.path-style-access", optionalEnv("S3_PATH_STYLE_ACCESS", "true"))
        .config("spark.sql.catalog." + catalog + ".s3.access-key-id", requiredEnv("MINIO_ACCESS_KEY_ID"))
        .config("spark.sql.catalog." + catalog + ".s3.secret-access-key", requiredEnv("MINIO_SECRET_ACCESS_KEY"))
        .config("spark.sql.catalog." + catalog + ".client.region", optionalEnv("AWS_REGION", "us-east-1"))
        .getOrCreate();

    String tableIdentifier = catalog + "." + NAMESPACE + "." + TABLE;
    try {
      if (args.length == 1 && "--cleanup".equals(args[0])) {
        spark.sql("DROP TABLE IF EXISTS " + tableIdentifier);
        return;
      }
      if (args.length != 0) {
        throw new IllegalArgumentException("Only --cleanup is supported");
      }

      spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalog + "." + NAMESPACE);
      spark.sql("CREATE TABLE IF NOT EXISTS " + tableIdentifier + " ("
          + "order_id BIGINT, customer_id BIGINT, order_status STRING, "
          + "order_total DECIMAL(12, 2), event_time TIMESTAMP) USING iceberg "
          + "PARTITIONED BY (days(event_time)) TBLPROPERTIES ('format-version' = '2')");
      spark.sql("MERGE INTO " + tableIdentifier + " target USING (SELECT "
          + "CAST(9301 AS BIGINT) AS order_id, CAST(901 AS BIGINT) AS customer_id, "
          + "'PAID' AS order_status, CAST(73.25 AS DECIMAL(12, 2)) AS order_total, "
          + "TIMESTAMP '2026-04-03 09:00:00' AS event_time) source "
          + "ON target.order_id = source.order_id "
          + "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
      spark.sql("SELECT order_id, customer_id, order_status, order_total, event_time FROM "
          + tableIdentifier + " ORDER BY order_id").show(false);
      spark.sql("SELECT snapshot_id, committed_at, operation FROM " + tableIdentifier
          + ".snapshots ORDER BY committed_at").show(false);
    } finally {
      spark.stop();
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must be set");
    }
    return value;
  }

  private static String optionalEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
