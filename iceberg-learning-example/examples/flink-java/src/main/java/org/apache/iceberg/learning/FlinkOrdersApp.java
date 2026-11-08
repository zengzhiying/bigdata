package org.apache.iceberg.learning;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/** A repeatable Flink Table API path for an Iceberg REST Catalog. */
public final class FlinkOrdersApp {
  private static final String NAMESPACE = "learning_flink_java";
  private static final String TABLE = "orders_from_flink_java";

  private FlinkOrdersApp() {
  }

  public static void main(String[] args) throws Exception {
    String catalog = FlinkCatalogSupport.catalogName();
    if (args.length > 1 || (args.length == 1 && !"--cleanup".equals(args[0]))) {
      throw new IllegalArgumentException("Only --cleanup is supported");
    }

    TableEnvironment tableEnv = TableEnvironment.create(
        EnvironmentSettings.newInstance().inBatchMode().build());
    FlinkCatalogSupport.configure(tableEnv, catalog);

    String tableIdentifier = catalog + "." + NAMESPACE + "." + TABLE;
    if (args.length == 1) {
      FlinkCatalogSupport.await(tableEnv.executeSql("DROP TABLE IF EXISTS " + tableIdentifier));
      return;
    }

    FlinkCatalogSupport.await(tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + NAMESPACE));
    FlinkCatalogSupport.await(tableEnv.executeSql("CREATE TABLE IF NOT EXISTS " + tableIdentifier + " ("
        + "order_id BIGINT, customer_id BIGINT, order_status STRING, "
        + "order_total DECIMAL(12, 2), event_time TIMESTAMP(3)) "
        + "PARTITIONED BY (event_time) WITH ("
        + "'format-version' = '2', 'write.format.default' = 'parquet')"));
    FlinkCatalogSupport.await(tableEnv.executeSql("INSERT OVERWRITE " + tableIdentifier + " VALUES "
        + "(9501, 901, 'CREATED', CAST(21.50 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-05 09:00:00'), "
        + "(9502, 902, 'PAID', CAST(44.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-05 10:30:00'), "
        + "(9503, 903, 'SHIPPED', CAST(75.25 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-06 11:15:00')"));

    printRows(tableEnv, "SELECT order_id, customer_id, order_status, order_total, event_time FROM "
        + tableIdentifier + " ORDER BY order_id");
    printRows(tableEnv, "SELECT snapshot_id, committed_at, operation FROM " + catalog + "."
        + NAMESPACE + ".`" + TABLE + "$snapshots` ORDER BY committed_at");
  }

  private static void printRows(TableEnvironment tableEnv, String sql) throws Exception {
    try (CloseableIterator<Row> rows = tableEnv.executeSql(sql).collect()) {
      while (rows.hasNext()) {
        System.out.println(rows.next());
      }
    }
  }

}
