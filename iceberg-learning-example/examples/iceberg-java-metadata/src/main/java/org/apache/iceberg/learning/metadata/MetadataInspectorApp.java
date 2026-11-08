package org.apache.iceberg.learning.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

/** Inspects the Metadata JSON and manifest tree for a table in the configured REST Catalog. */
public final class MetadataInspectorApp {
  private static final String DEFAULT_TABLE = "learning_metadata.orders";
  private MetadataInspectorApp() {
  }

  public static void main(String[] args) throws Exception {
    boolean printMetadataJson = false;
    boolean demonstrateConflict = false;
    for (String arg : args) {
      if ("--metadata-json".equals(arg)) {
        printMetadataJson = true;
      } else if ("--demonstrate-conflict".equals(arg)) {
        demonstrateConflict = true;
      } else {
        throw new IllegalArgumentException("Supported options: --metadata-json, --demonstrate-conflict");
      }
    }

    String catalogName = optionalEnv("CATALOG_NAME", "ice");
    String tableName = optionalEnv("METADATA_TABLE", DEFAULT_TABLE);
    TableIdentifier tableIdentifier = TableIdentifier.parse(tableName);
    if (!tableIdentifier.hasNamespace()) {
      throw new IllegalArgumentException("METADATA_TABLE must include a namespace, for example "
          + DEFAULT_TABLE);
    }

    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize(catalogName, catalogProperties());
      Table table = catalog.loadTable(tableIdentifier);
      TableOperations operations = tableOperations(table);
      if (demonstrateConflict) {
        try (RESTCatalog staleCatalog = new RESTCatalog()) {
          staleCatalog.initialize(catalogName, catalogProperties());
          Table staleTable = staleCatalog.loadTable(tableIdentifier);
          demonstrateStaleMetadataConflict(operations, tableOperations(staleTable));
        }
        table.refresh();
      }

      printMetadataTree(table, operations.current());
      if (printMetadataJson) {
        System.out.println("--- BEGIN METADATA JSON ---");
        System.out.println(TableMetadataParser.toJson(operations.current()));
        System.out.println("--- END METADATA JSON ---");
      }
    }
  }

  private static void printMetadataTree(Table table, TableMetadata metadata) {
    System.out.println("table=" + table.name());
    System.out.println("metadata_file=" + metadata.metadataFileLocation());
    System.out.println("format_version=" + metadata.formatVersion());
    System.out.println("current_snapshot_id=" + snapshotId(metadata.currentSnapshot()));
    System.out.println("metadata_log_entries=" + metadata.previousFiles().size());

    for (TableMetadata.MetadataLogEntry entry : metadata.previousFiles()) {
      System.out.println("metadata_log timestamp_ms=" + entry.timestampMillis() + " file=" + entry.file());
    }

    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      System.out.println("manifest_list=none");
      return;
    }

    System.out.println("manifest_list=" + currentSnapshot.manifestListLocation());
    for (ManifestFile manifest : currentSnapshot.allManifests(table.io())) {
      System.out.println("manifest content=" + manifest.content()
          + " path=" + manifest.path()
          + " added_files=" + manifest.addedFilesCount()
          + " existing_files=" + manifest.existingFilesCount()
          + " deleted_files=" + manifest.deletedFilesCount());
    }
  }

  private static void demonstrateStaleMetadataConflict(
      TableOperations winnerOperations, TableOperations staleOperations) {
    TableMetadata winnerBase = winnerOperations.current();
    TableMetadata staleBase = staleOperations.current();
    if (!winnerBase.metadataFileLocation().equals(staleBase.metadataFileLocation())) {
      throw new IllegalStateException("The two writers did not start from the same Metadata JSON");
    }

    String suffix = Integer.toString(winnerBase.lastColumnId() + 1);
    String winnerColumn = "concurrent_writer_a_" + suffix;
    String staleColumn = "concurrent_writer_b_" + suffix;
    winnerOperations.commit(winnerBase, metadataWithAddedColumn(winnerBase, winnerColumn));
    winnerOperations.refresh();

    try {
      staleOperations.commit(staleBase, metadataWithAddedColumn(staleBase, staleColumn));
      throw new IllegalStateException("Expected a stale-metadata commit to fail");
    } catch (CommitFailedException expected) {
      TableMetadata current = winnerOperations.refresh();
      if (current.schema().findField(winnerColumn) == null
          || current.schema().findField(staleColumn) != null) {
        throw new IllegalStateException("The successful schema update was unexpectedly replaced", expected);
      }
      System.out.println("concurrent_commit_check=PASS expected="
          + expected.getClass().getSimpleName() + " winner_column=" + winnerColumn);
    }
  }

  private static TableMetadata metadataWithAddedColumn(TableMetadata base, String columnName) {
    List<Types.NestedField> columns = new ArrayList<>(base.schema().columns());
    columns.add(Types.NestedField.optional(
        base.lastColumnId() + 1, columnName, Types.StringType.get()));
    return base.updateSchema(new Schema(columns));
  }

  private static TableOperations tableOperations(Table table) {
    if (!(table instanceof HasTableOperations)) {
      throw new IllegalStateException("The loaded table does not expose table operations: "
          + table.getClass().getName());
    }
    return ((HasTableOperations) table).operations();
  }

  private static Map<String, String> catalogProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("uri", requiredEnv("CATALOG_URI"));
    properties.put("warehouse", requiredEnv("CATALOG_WAREHOUSE"));
    properties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put("s3.endpoint", requiredEnv("S3_ENDPOINT"));
    properties.put("s3.path-style-access", optionalEnv("S3_PATH_STYLE_ACCESS", "true"));
    properties.put("s3.access-key-id", requiredEnv("MINIO_ACCESS_KEY_ID"));
    properties.put("s3.secret-access-key", requiredEnv("MINIO_SECRET_ACCESS_KEY"));
    properties.put("client.region", optionalEnv("AWS_REGION", "us-east-1"));
    return properties;
  }

  private static String snapshotId(Snapshot snapshot) {
    return snapshot == null ? "none" : Long.toString(snapshot.snapshotId());
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
