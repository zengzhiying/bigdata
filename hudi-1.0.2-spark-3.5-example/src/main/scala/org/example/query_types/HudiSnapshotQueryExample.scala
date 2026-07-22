package org.example.query_types

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiSnapshotQueryExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Snapshot Query Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "snapshot_query_table"
    val basePath = "file:///tmp/snapshot_query_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

    println("=" * 80)
    println("HUDI SNAPSHOT QUERY EXAMPLE")
    println("=" * 80)

    println("\n=== 1. Initial Insert - Creating base data ===")
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
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    println("✓ Initial data inserted: 5 records")

    println("\n=== 2. Snapshot Query - Read current state (default query type) ===")
    val snapshotDf1 = spark.read.format("hudi").load(basePath)
    
    println(s"Total records: ${snapshotDf1.count()}")
    println("\nCurrent data:")
    snapshotDf1.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("uuid")
      .show(truncate = false)

    val commit1 = snapshotDf1.select("_hoodie_commit_time").first().getString(0)
    println(s"First commit time: $commit1")

    println("\n=== 3. Update Operation - Modify some records ===")
    Thread.sleep(1000)
    
    val updateData = Seq(
      (1695159650000L, "uuid-001", "rider-A", "driver-K", 99.99, "san_francisco"),
      (1695159650001L, "uuid-003", "rider-C", "driver-M", 88.88, "new_york")
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    println("✓ Updated 2 records (uuid-001 and uuid-003)")

    println("\n=== 4. Snapshot Query After Update - Shows latest data ===")
    val snapshotDf2 = spark.read.format("hudi").load(basePath)
    
    println(s"Total records: ${snapshotDf2.count()}")
    println("\nData after update (notice fare changes for uuid-001 and uuid-003):")
    snapshotDf2.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("uuid")
      .show(truncate = false)

    println("\n=== 5. Insert New Records ===")
    Thread.sleep(1000)
    
    val newData = Seq(
      (1695159651000L, "uuid-006", "rider-F", "driver-P", 45.50, "chennai"),
      (1695159651001L, "uuid-007", "rider-G", "driver-Q", 56.60, "san_francisco")
    )
    val newDf = spark.createDataFrame(newData).toDF(columns: _*)

    newDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    println("✓ Inserted 2 new records")

    println("\n=== 6. Final Snapshot Query - Shows all current data ===")
    val snapshotDf3 = spark.read.format("hudi").load(basePath)
    
    println(s"Total records: ${snapshotDf3.count()}")
    println("\nFinal state (7 records total):")
    snapshotDf3.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("uuid")
      .show(truncate = false)

    println("\n=== 7. Snapshot Query Characteristics ===")
    println("✓ Returns the latest committed snapshot of the table")
    println("✓ Shows the most recent version of each record")
    println("✓ DEFAULT query type for Hudi tables")
    println("✓ Works with both COW and MOR tables")
    println("✓ For MOR tables: merges base files + delta logs")
    println("✓ No additional configuration needed")

    println("\n=== 8. Query with Filters ===")
    println("\nRecords with fare > 50:")
    snapshotDf3.filter("fare > 50")
      .select("uuid", "rider", "fare", "city")
      .show(truncate = false)

    println("\nRecords by city:")
    snapshotDf3.groupBy("city")
      .agg(
        count("*").as("count"),
        avg("fare").as("avg_fare"),
        sum("fare").as("total_fare")
      )
      .show(truncate = false)

    println("\n=== 9. Using Snapshot Query with SQL ===")
    snapshotDf3.createOrReplaceTempView(tableName)
    
    println("\nSQL Query - Top 3 highest fares:")
    spark.sql(s"SELECT uuid, rider, fare, city FROM $tableName ORDER BY fare DESC LIMIT 3")
      .show(truncate = false)

    println("\n=== 10. View Commit History ===")
    val commits = snapshotDf3.select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
    println(s"Number of commits: ${commits.count()}")
    println("\nCommit times:")
    commits.show(truncate = false)

    spark.stop()
  }
}
