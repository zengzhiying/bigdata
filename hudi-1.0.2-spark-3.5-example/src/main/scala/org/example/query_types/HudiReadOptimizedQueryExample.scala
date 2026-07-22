package org.example.query_types

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiReadOptimizedQueryExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Read Optimized Query Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "read_optimized_table"
    val basePath = "file:///tmp/read_optimized_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

    println("=" * 80)
    println("HUDI READ OPTIMIZED QUERY EXAMPLE")
    println("=" * 80)
    println("Note: Read Optimized Query only applies to MERGE_ON_READ tables")

    println("\n=== 1. Create MOR Table with Initial Data ===")
    val initialData = Seq(
      (1695159649087L, "uuid-001", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1695091554788L, "uuid-002", "rider-B", "driver-L", 27.70, "san_francisco"),
      (1695046462179L, "uuid-003", "rider-C", "driver-M", 33.90, "new_york"),
      (1695516137016L, "uuid-004", "rider-D", "driver-N", 34.15, "new_york"),
      (1695115999911L, "uuid-005", "rider-E", "driver-O", 17.85, "chennai")
    )

    val insertDf = spark.createDataFrame(initialData).toDF(columns: _*)
    
    insertDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    println("✓ MOR table created with 5 records")

    println("\n=== 2. Snapshot Query - Shows all data (base files only at this point) ===")
    val snapshotDf1 = spark.read.format("hudi").load(basePath)
    println(s"Snapshot query count: ${snapshotDf1.count()}")
    snapshotDf1.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    println("\n=== 3. Read Optimized Query - Same as snapshot (no delta logs yet) ===")
    val roDf1 = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    println(s"Read Optimized query count: ${roDf1.count()}")
    roDf1.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    Thread.sleep(2000)

    println("\n=== 4. Update Records (Creates Delta Logs) ===")
    val updateData = Seq(
      (1695159650000L, "uuid-001", "rider-A", "driver-K", 99.99, "san_francisco"),
      (1695159650001L, "uuid-003", "rider-C", "driver-M", 88.88, "new_york")
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)

    println("✓ Updated 2 records (uuid-001: 19.10 → 99.99, uuid-003: 33.90 → 88.88)")
    println("  Updates written to delta logs (not merged into base files)")

    println("\n=== 5. Snapshot Query After Update - Shows latest data ===")
    val snapshotDf2 = spark.read.format("hudi").load(basePath)
    println(s"Snapshot query count: ${snapshotDf2.count()}")
    println("\nNotice: fare updated for uuid-001 and uuid-003")
    snapshotDf2.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    println("\n=== 6. Read Optimized Query After Update - Shows OLD data ===")
    val roDf2 = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    println(s"Read Optimized query count: ${roDf2.count()}")
    println("\nNotice: fare NOT updated (still showing base file data)")
    roDf2.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    println("\n=== 7. Comparison: Snapshot vs Read Optimized ===")
    println("\n┌──────────┬─────────┬──────────────────┬─────────────────────┐")
    println("│ UUID     │ Rider   │ Snapshot (fare)  │ Read Optimized (fare)│")
    println("├──────────┼─────────┼──────────────────┼─────────────────────┤")
    
    val snapshotFares = snapshotDf2.select("uuid", "fare").orderBy("uuid").collect()
    val roFares = roDf2.select("uuid", "fare").orderBy("uuid").collect()
    
    snapshotFares.zip(roFares).foreach { case (s, r) =>
      val uuid = s.getString(0)
      val sFare = f"${s.getDouble(1)}%.2f"
      val rFare = f"${r.getDouble(1)}%.2f"
      val marker = if (sFare != rFare) " ← DIFF" else ""
      println(f"│ $uuid%-8s │ rider-${uuid.takeRight(1)}  │ $$${sFare}%-14s │ $$${rFare}%-18s │$marker")
    }
    println("└──────────┴─────────┴──────────────────┴─────────────────────┘")

    println("\n=== 8. Performance Comparison ===")
    
    val snapshotStart = System.currentTimeMillis()
    val snapshotCount = spark.read.format("hudi").load(basePath).count()
    val snapshotTime = System.currentTimeMillis() - snapshotStart
    
    val roStart = System.currentTimeMillis()
    val roCount = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath).count()
    val roTime = System.currentTimeMillis() - roStart
    
    println(s"Snapshot Query:        ${snapshotCount} records in ${snapshotTime}ms")
    println(s"Read Optimized Query:  ${roCount} records in ${roTime}ms")
    println(f"Performance gain:      ${snapshotTime.toDouble / roTime.toDouble}%.2fx faster")

    Thread.sleep(2000)

    println("\n=== 9. More Updates (More Delta Logs) ===")
    val moreUpdates = Seq(
      (1695159651000L, "uuid-002", "rider-B", "driver-L", 77.77, "san_francisco"),
      (1695159651001L, "uuid-004", "rider-D", "driver-N", 66.66, "new_york")
    )
    val moreUpdatesDf = spark.createDataFrame(moreUpdates).toDF(columns: _*)

    moreUpdatesDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)

    println("✓ Updated 2 more records")

    println("\n=== 10. Final Comparison ===")
    println("\nSnapshot Query (latest data with all updates):")
    val finalSnapshot = spark.read.format("hudi").load(basePath)
    finalSnapshot.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    println("Read Optimized Query (base files only, no updates visible):")
    val finalRO = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    finalRO.select("uuid", "rider", "fare", "city").orderBy("uuid").show(truncate = false)

    println("\n=== 11. Read Optimized Query Characteristics ===")
    println("✓ MOR table ONLY (not applicable to COW tables)")
    println("✓ Reads ONLY base Parquet files, skips delta logs")
    println("✓ Fastest query performance")
    println("✓ May return stale data (missing recent updates)")
    println("✓ Best for analytical queries where slight data lag is acceptable")
    println("✓ Updates become visible after compaction")
    println("✓ Trade-off: Performance vs Data freshness")

    println("\n=== 12. When to Use Read Optimized Query ===")
    println("✅ Recommended for:")
    println("   • Long-running analytical queries")
    println("   • BI dashboards with hourly/daily refresh")
    println("   • Reports that tolerate slight staleness")
    println("   • Maximum query performance is critical")
    println("\n❌ Avoid when:")
    println("   • Real-time data freshness required")
    println("   • Need to see latest updates immediately")
    println("   • Using COW tables (use snapshot query)")

    println("\n=== 13. Configuration Summary ===")
    println("Read Optimized Query configuration:")
    println("  • hoodie.datasource.query.type = read_optimized")
    println("\nSnapshot Query configuration (default):")
    println("  • No configuration needed OR")
    println("  • hoodie.datasource.query.type = snapshot")

    spark.stop()
  }
}
