package org.example.core_concepts

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiTableTypeComparisonExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Table Type Comparison Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
    val data = Seq(
      (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
      (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco"),
      (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
      (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
    )

    val cowTableName = "comparison_cow_table"
    val morTableName = "comparison_mor_table"
    val cowPath = "file:///tmp/comparison_cow_table"
    val morPath = "file:///tmp/comparison_mor_table"

    val insertDf = spark.createDataFrame(data).toDF(columns: _*)

    println("=" * 80)
    println("HUDI TABLE TYPE COMPARISON: COPY-ON-WRITE vs MERGE-ON-READ")
    println("=" * 80)

    println("\n=== 1. Create both COW and MOR tables with same data ===")
    
    insertDf.write.format("hudi")
      .option("hoodie.table.name", cowTableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(cowPath)
    println("✓ COW table created")

    insertDf.write.format("hudi")
      .option("hoodie.table.name", morTableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Overwrite)
      .save(morPath)
    println("✓ MOR table created")

    println("\n=== 2. Perform UPDATE operations on both tables ===")
    val updateData = Seq(
      (1695159650000L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 99.99, "san_francisco"),
      (1695159650001L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 88.88, "san_francisco")
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    val cowUpdateStart = System.currentTimeMillis()
    updateDf.write.format("hudi")
      .option("hoodie.table.name", cowTableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(cowPath)
    val cowUpdateTime = System.currentTimeMillis() - cowUpdateStart
    println(s"✓ COW update completed in ${cowUpdateTime}ms")

    val morUpdateStart = System.currentTimeMillis()
    updateDf.write.format("hudi")
      .option("hoodie.table.name", morTableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(morPath)
    val morUpdateTime = System.currentTimeMillis() - morUpdateStart
    println(s"✓ MOR update completed in ${morUpdateTime}ms")

    println("\n=== 3. Query Performance Comparison ===")
    
    val cowReadStart = System.currentTimeMillis()
    val cowDf = spark.read.format("hudi").load(cowPath)
    val cowCount = cowDf.count()
    val cowReadTime = System.currentTimeMillis() - cowReadStart
    println(s"COW Snapshot Query: ${cowCount} records in ${cowReadTime}ms")

    val morSnapshotStart = System.currentTimeMillis()
    val morSnapshotDf = spark.read.format("hudi").load(morPath)
    val morSnapshotCount = morSnapshotDf.count()
    val morSnapshotTime = System.currentTimeMillis() - morSnapshotStart
    println(s"MOR Snapshot Query: ${morSnapshotCount} records in ${morSnapshotTime}ms")

    val morROStart = System.currentTimeMillis()
    val morRODf = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(morPath)
    val morROCount = morRODf.count()
    val morROTime = System.currentTimeMillis() - morROStart
    println(s"MOR Read Optimized Query: ${morROCount} records in ${morROTime}ms")

    println("\n=== 4. Data Verification ===")
    println("\nCOW Table Data:")
    cowDf.select("rider", "fare", "_hoodie_commit_time").orderBy("rider").show()

    println("MOR Table Data (Snapshot):")
    morSnapshotDf.select("rider", "fare", "_hoodie_commit_time").orderBy("rider").show()

    println("MOR Table Data (Read Optimized):")
    morRODf.select("rider", "fare", "_hoodie_commit_time").orderBy("rider").show()

    println("\n" + "=" * 80)
    println("COMPARISON SUMMARY")
    println("=" * 80)
    
    println("\n┌─────────────────────────────────┬───────────────────┬───────────────────┐")
    println("│ Characteristic                  │ Copy-on-Write     │ Merge-on-Read     │")
    println("├─────────────────────────────────┼───────────────────┼───────────────────┤")
    println("│ Data File Format                │ Parquet           │ Parquet + Avro    │")
    println("│ Update Mechanism                │ Rewrite entire    │ Append delta logs │")
    println("│                                 │ file              │                   │")
    println("│ Write Latency                   │ Higher            │ Lower             │")
    println("│ Read Latency (Snapshot)         │ Lower             │ Higher            │")
    println("│ Read Latency (Read Optimized)   │ N/A               │ Lowest            │")
    println("│ Storage Overhead                │ Lower             │ Higher (before    │")
    println("│                                 │                   │ compaction)       │")
    println("│ Best For                        │ Read-heavy        │ Write-heavy       │")
    println("│ Compaction Required             │ No                │ Yes               │")
    println("└─────────────────────────────────┴───────────────────┴───────────────────┘")

    println("\n=== 5. Use Case Recommendations ===")
    println("\n📊 Copy-on-Write (COW):")
    println("  ✓ Batch processing with infrequent updates")
    println("  ✓ BI/Analytics queries requiring fast reads")
    println("  ✓ Simple operational model (no compaction needed)")
    println("  ✓ Data warehousing scenarios")

    println("\n🚀 Merge-on-Read (MOR):")
    println("  ✓ Streaming ingestion with frequent updates")
    println("  ✓ Near real-time data availability requirements")
    println("  ✓ Write-heavy workloads")
    println("  ✓ CDC (Change Data Capture) pipelines")
    println("  ✓ Scenarios where write latency is critical")

    println("\n=== 6. File Structure Inspection ===")
    println("\nCOW Table Files:")
    cowDf.select("_hoodie_file_name").distinct().show(truncate = false)

    println("MOR Table Files:")
    morSnapshotDf.select("_hoodie_file_name").distinct().show(truncate = false)

    spark.stop()
  }
}
