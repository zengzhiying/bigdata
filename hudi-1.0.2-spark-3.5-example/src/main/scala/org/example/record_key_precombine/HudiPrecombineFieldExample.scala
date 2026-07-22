package org.example.record_key_precombine

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiPrecombineFieldExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Precombine Field Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "precombine_field_table"
    val basePath = "file:///tmp/precombine_field_table"

    println("=" * 80)
    println("HUDI PRECOMBINE FIELD - DETAILED EXAMPLE")
    println("=" * 80)
    println("Precombine Field: Determines which record to keep when duplicates exist")
    println("=" * 80)

    println("\n=== 1. Understanding Precombine Field ===")
    println("When multiple records have the same Record Key:")
    println("• Hudi uses precombine field to decide which record wins")
    println("• The record with LARGER precombine value is kept")
    println("• Typically a timestamp, version number, or sequence ID")
    println("• Critical for handling late-arriving data and concurrent updates")

    val columns = Seq("order_id", "product", "quantity", "price", "version", "updated_ts")

    println("\n=== 2. Scenario: Concurrent Order Updates ===")
    println("Multiple systems updating the same order concurrently")

    println("\n--- Wave 1: Initial Orders ---")
    val wave1Data = Seq(
      ("ORD-001", "Laptop", 1, 1200.00, 1, 1701388800000L),
      ("ORD-002", "Mouse", 2, 25.00, 1, 1701388801000L),
      ("ORD-003", "Keyboard", 1, 80.00, 1, 1701388802000L)
    )

    val wave1Df = spark.createDataFrame(wave1Data).toDF(columns: _*)
    
    println("Initial orders:")
    wave1Df.show(truncate = false)

    wave1Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")  // Use timestamp
      .option("hoodie.datasource.write.partitionpath.field", "")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    println("✓ Wave 1 written")

    println("\n--- Wave 2: Competing Updates for ORD-001 ---")
    println("Two updates arrive with DIFFERENT timestamps:")
    println("  Update A: ts=1701388850000 (older)")
    println("  Update B: ts=1701388900000 (newer)")

    val wave2Data = Seq(
      ("ORD-001", "Laptop", 2, 1200.00, 2, 1701388900000L),  // Newer - should WIN
      ("ORD-001", "Laptop", 1, 1150.00, 2, 1701388850000L)   // Older - should LOSE
    )

    val wave2Df = spark.createDataFrame(wave2Data).toDF(columns: _*)
    
    println("\nCompeting updates (written in same batch):")
    wave2Df.show(truncate = false)

    wave2Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot2 = spark.read.format("hudi").load(basePath)
    println("\n✓ Wave 2 written - Result:")
    println("ORD-001 should have quantity=2 (newer timestamp won):")
    snapshot2.filter("order_id = 'ORD-001'")
      .select("order_id", "product", "quantity", "price", "updated_ts")
      .show(truncate = false)

    println("\n=== 3. Late-Arriving Data Scenario ===")
    println("Older update arrives AFTER newer update already written")

    println("\nCurrent state of ORD-002:")
    snapshot2.filter("order_id = 'ORD-002'")
      .select("order_id", "quantity", "price", "updated_ts")
      .show(truncate = false)

    println("Now a late-arriving update with OLDER timestamp arrives:")
    val lateData = Seq(
      ("ORD-002", "Mouse", 5, 22.00, 2, 1701388805000L)  // Older than existing
    )
    val lateDf = spark.createDataFrame(lateData).toDF(columns: _*)

    println("\nLate-arriving data:")
    lateDf.show(truncate = false)

    lateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot3 = spark.read.format("hudi").load(basePath)
    println("\n✓ Late data written - Result:")
    println("ORD-002 should be UNCHANGED (newer timestamp already exists):")
    snapshot3.filter("order_id = 'ORD-002'")
      .select("order_id", "quantity", "price", "updated_ts")
      .show(truncate = false)

    println("\n=== 4. Using Version Number as Precombine Field ===")
    val versionTablePath = "file:///tmp/precombine_version_table"

    println("Alternative: Use version number instead of timestamp")
    
    val versionData1 = Seq(
      ("PROD-001", "Widget", 100, 10.00, 1),
      ("PROD-002", "Gadget", 200, 20.00, 1)
    )
    val versionDf1 = spark.createDataFrame(versionData1)
      .toDF("product_id", "name", "stock", "price", "version")

    versionDf1.write.format("hudi")
      .option("hoodie.table.name", "version_table")
      .option("hoodie.datasource.write.recordkey.field", "product_id")
      .option("hoodie.datasource.write.precombine.field", "version")  // Use version
      .option("hoodie.datasource.write.partitionpath.field", "")
      .mode(SaveMode.Overwrite)
      .save(versionTablePath)

    println("\nInitial data (version 1):")
    spark.read.format("hudi").load(versionTablePath)
      .select("product_id", "name", "stock", "price", "version")
      .show(truncate = false)

    println("\nCompeting updates with different versions:")
    val versionUpdates = Seq(
      ("PROD-001", "Widget", 150, 12.00, 3),  // Version 3 - should WIN
      ("PROD-001", "Widget", 120, 11.00, 2)   // Version 2 - should LOSE
    )
    val versionUpdateDf = spark.createDataFrame(versionUpdates)
      .toDF("product_id", "name", "stock", "price", "version")

    versionUpdateDf.show(truncate = false)

    versionUpdateDf.write.format("hudi")
      .option("hoodie.table.name", "version_table")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "product_id")
      .option("hoodie.datasource.write.precombine.field", "version")
      .mode(SaveMode.Append)
      .save(versionTablePath)

    println("\n✓ Result - Version 3 won:")
    spark.read.format("hudi").load(versionTablePath)
      .filter("product_id = 'PROD-001'")
      .select("product_id", "stock", "price", "version")
      .show(truncate = false)

    println("\n=== 5. What Happens Without Precombine Field? ===")
    println("⚠️  Hudi requires a precombine field for deduplication")
    println("⚠️  Without it, behavior is undefined for duplicates in same batch")

    println("\n=== 6. Precombine Field Data Types ===")
    println("Supported data types (must be comparable):")
    println("✓ Long/Timestamp - Most common (e.g., updated_ts)")
    println("✓ Integer - Version numbers")
    println("✓ String - Lexicographically comparable (e.g., ISO timestamps)")
    println("✓ Double/Float - Sequence numbers")
    println("❌ Complex types (arrays, structs) - NOT supported")

    println("\n=== 7. Precombine Logic Details ===")
    println("When Hudi encounters duplicate record keys:")
    println("1. Compare precombine field values")
    println("2. Keep record with LARGER value (a > b)")
    println("3. Discard record with smaller value")
    println("4. If values are equal, behavior is non-deterministic")

    println("\n=== 8. Common Precombine Field Choices ===")
    println("\n┌──────────────────────────┬────────────────┬─────────────────────────┐")
    println("│ Use Case                 │ Precombine     │ Example                 │")
    println("├──────────────────────────┼────────────────┼─────────────────────────┤")
    println("│ Event Streaming          │ event_time     │ Kafka event timestamp   │")
    println("│ Database CDC             │ updated_at     │ Database update time    │")
    println("│ Versioned Records        │ version        │ Integer version counter │")
    println("│ Sequence-based           │ sequence_id    │ Monotonic sequence      │")
    println("│ Processing Time          │ processing_ts  │ Spark processing time   │")
    println("└──────────────────────────┴────────────────┴─────────────────────────┘")

    println("\n=== 9. Best Practices ===")
    println("✓ Use timestamp for event-time semantics")
    println("✓ Use version number for versioned data")
    println("✓ Ensure precombine field is always populated (never null)")
    println("✓ Use monotonically increasing values")
    println("✓ Document precombine field choice in data dictionary")
    println("✓ Consistent precombine logic across all writers")

    println("\n=== 10. Common Mistakes ===")
    println("❌ Using random values (defeats deduplication)")
    println("❌ Using non-monotonic values")
    println("❌ Allowing null values in precombine field")
    println("❌ Changing precombine field between writes")
    println("❌ Using current_timestamp() (all records same value)")

    println("\n=== 11. Debugging Precombine Issues ===")
    println("Check for unexpected updates:")
    
    val finalSnapshot = snapshot3.select("order_id", "quantity", "price", "updated_ts")
      .orderBy("order_id")
    
    println("\nFinal state of all orders:")
    finalSnapshot.show(truncate = false)

    println("\nValidate: All precombine values should be non-null:")
    val nullPrecombine = snapshot3.filter(col("updated_ts").isNull).count()
    if (nullPrecombine > 0) {
      println(s"⚠️  Found $nullPrecombine records with null updated_ts")
    } else {
      println("✓ All records have valid precombine values")
    }

    println("\n=== 12. Configuration Summary ===")
    println("Key parameter:")
    println("  hoodie.datasource.write.precombine.field = <field_name>")
    println("\nExample:")
    println("  .option(\"hoodie.datasource.write.precombine.field\", \"updated_ts\")")

    println("\n✓ Example demonstrated precombine field handling duplicate records")

    spark.stop()
  }
}
