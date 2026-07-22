package org.example.core_concepts

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiMergeOnReadExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Merge-on-Read Table Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "mor_trips_table"
    val basePath = "file:///tmp/mor_trips_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
    val data = Seq(
      (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
      (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco"),
      (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
      (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
    )

    val insertDf = spark.createDataFrame(data).toDF(columns: _*)

    println("=== 1. Insert data into MOR table ===")
    insertDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val readDf = spark.read.format("hudi").load(basePath)
    println("Initial data count: " + readDf.count())
    readDf.select("uuid", "rider", "driver", "fare", "city").show()

    println("\n=== 2. Update data in MOR table (creates delta log) ===")
    val updateData = Seq(
      (1695159649999L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 99.99, "san_francisco")
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

    println("\n=== 3. Snapshot Query (merges base + delta logs on read) ===")
    val snapshotDf = spark.read.format("hudi").load(basePath)
    println("Snapshot query - rider-A's fare after update:")
    snapshotDf.filter("rider = 'rider-A'").select("uuid", "rider", "fare", "ts").show()

    println("\n=== 4. Read Optimized Query (reads only base files, faster but may not show latest updates) ===")
    val roQueryDf = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    println("Read Optimized query - may show old fare if not compacted:")
    roQueryDf.filter("rider = 'rider-A'").select("uuid", "rider", "fare", "ts").show()

    println("\n=== 5. Insert more updates to demonstrate delta logs ===")
    val moreUpdates = Seq(
      (1695159650000L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 55.50, "san_francisco"),
      (1695159650001L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 77.77, "san_francisco")
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

    println("\n=== 6. View all data after multiple updates ===")
    val finalDf = spark.read.format("hudi").load(basePath)
    finalDf.select("uuid", "rider", "fare", "ts", "_hoodie_commit_time").orderBy("rider").show()

    println("\n=== 7. MOR Table Characteristics ===")
    println("- Write operation: Appends updates to delta log files (Avro format)")
    println("- Snapshot query: Merges base Parquet + delta logs on read (slower read)")
    println("- Read Optimized query: Reads only base Parquet files (faster, but may miss recent updates)")
    println("- Write performance: Faster, only writes deltas")
    println("- Use case: Write-heavy workloads with near real-time requirements")
    println("- Compaction: Periodically merges delta logs into base files")

    println("\n=== 8. File metadata ===")
    finalDf.select("_hoodie_commit_time", "_hoodie_file_name", "rider", "fare")
      .orderBy("rider")
      .show(truncate = false)

    spark.stop()
  }
}
