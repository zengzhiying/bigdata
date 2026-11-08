package org.apache.iceberg.learning;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/** Shared REST Catalog setup for the Flink Java examples. */
final class FlinkCatalogSupport {
  private FlinkCatalogSupport() {
  }

  static String catalogName() {
    String catalog = optionalEnv("CATALOG_NAME", "ice");
    if (!catalog.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new IllegalArgumentException("CATALOG_NAME must be a Flink SQL identifier");
    }
    return catalog;
  }

  static void configure(TableEnvironment tableEnv, String catalog) throws Exception {
    String createCatalog = "CREATE CATALOG " + catalog + " WITH ("
        + "'type' = 'iceberg', "
        + "'catalog-type' = 'rest', "
        + "'uri' = " + sqlLiteral(requiredEnv("CATALOG_URI")) + ", "
        + "'warehouse' = " + sqlLiteral(requiredEnv("CATALOG_WAREHOUSE")) + ", "
        + "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', "
        + "'s3.endpoint' = " + sqlLiteral(requiredEnv("S3_ENDPOINT")) + ", "
        + "'s3.path-style-access' = " + sqlLiteral(optionalEnv("S3_PATH_STYLE_ACCESS", "true")) + ", "
        + "'s3.access-key-id' = " + sqlLiteral(requiredEnv("MINIO_ACCESS_KEY_ID")) + ", "
        + "'s3.secret-access-key' = " + sqlLiteral(requiredEnv("MINIO_SECRET_ACCESS_KEY")) + ", "
        + "'client.region' = " + sqlLiteral(optionalEnv("AWS_REGION", "us-east-1")) + ")";
    await(tableEnv.executeSql(createCatalog));
    await(tableEnv.executeSql("USE CATALOG " + catalog));
  }

  static void await(TableResult result) throws Exception {
    result.await();
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

  private static String sqlLiteral(String value) {
    return "'" + value.replace("'", "''").replace("\n", " ").replace("\r", " ") + "'";
  }
}
