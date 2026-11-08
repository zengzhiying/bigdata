package org.apache.iceberg.learning;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/** Writes a deterministic Flink changelog to an Iceberg upsert table without an external connector. */
public final class FlinkChangelogUpsertApp {
  private static final String NAMESPACE = "learning_cdc";
  private static final String TABLE = "orders_changelog_upsert";

  private FlinkChangelogUpsertApp() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 0) {
      throw new IllegalArgumentException("This fixture does not accept arguments; use Spark SQL to reset or clean up");
    }

    String catalog = FlinkCatalogSupport.catalogName();
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
    FlinkCatalogSupport.configure(tableEnv, catalog);

    DataStream<Row> changelogRows = streamEnv.fromData(fixtureRows());
    Table changelog = tableEnv.fromChangelogStream(changelogRows, fixturePhysicalSchema());
    tableEnv.createTemporaryView("fixed_changelog_events", changelog);

    String tableIdentifier = catalog + "." + NAMESPACE + "." + TABLE;
    FlinkCatalogSupport.await(tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + NAMESPACE));
    FlinkCatalogSupport.await(tableEnv.executeSql("CREATE TABLE " + tableIdentifier + " ("
        + "order_id BIGINT NOT NULL, customer_id BIGINT, order_status STRING, "
        + "order_total DECIMAL(12, 2), event_version BIGINT, event_time TIMESTAMP(3), "
        + "PRIMARY KEY (order_id) NOT ENFORCED) WITH ("
        + "'format-version' = '2', 'write.upsert.enabled' = 'true', "
        + "'write.format.default' = 'parquet')"));
    FlinkCatalogSupport.await(tableEnv.executeSql("INSERT INTO " + tableIdentifier + " "
        + "SELECT f0 AS order_id, f1 AS customer_id, f2 AS order_status, f3 AS order_total, "
        + "f4 AS event_version, f5 AS event_time "
        + "FROM fixed_changelog_events"));

    System.out.println("Flink Changelog Upsert fixture completed for " + tableIdentifier);
  }

  private static List<Row> fixtureRows() {
    return List.of(
        row(RowKind.INSERT, 2001L, 701L, "CREATED", "42.50", 1L, "2026-06-02 09:00:00"),
        row(RowKind.UPDATE_BEFORE, 2001L, 701L, "CREATED", "42.50", 1L, "2026-06-02 09:00:00"),
        row(RowKind.UPDATE_AFTER, 2001L, 701L, "PAID", "42.50", 2L, "2026-06-02 09:10:00"),
        row(RowKind.INSERT, 2002L, 702L, "CREATED", "9.50", 1L, "2026-06-02 09:15:00"),
        row(RowKind.DELETE, 2002L, 702L, "CREATED", "9.50", 1L, "2026-06-02 09:15:00"),
        row(RowKind.INSERT, 2003L, 703L, "SHIPPED", "73.25", 1L, "2026-06-02 09:20:00"));
  }

  private static Row row(
      RowKind kind,
      long orderId,
      long customerId,
      String status,
      String total,
      long version,
      String eventTime) {
    return Row.ofKind(
        kind,
        orderId,
        customerId,
        status,
        new BigDecimal(total),
        version,
        LocalDateTime.parse(eventTime.replace(" ", "T")));
  }

  private static Schema fixturePhysicalSchema() {
    return Schema.newBuilder()
        // Row's default type information exposes positional fields as f0 through f5.
        .column("f0", DataTypes.BIGINT().notNull())
        .column("f1", DataTypes.BIGINT())
        .column("f2", DataTypes.STRING())
        .column("f3", DataTypes.DECIMAL(12, 2))
        .column("f4", DataTypes.BIGINT())
        .column("f5", DataTypes.TIMESTAMP(3))
        .build();
  }
}
