package org.example.time_travel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.example.time_travel.HudiTimelineUtils._

object HudiTimeTravelBasicExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Time Travel Basic Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "time_travel_basic_table"
    val basePath = "file:///tmp/time_travel_basic_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

    println("=" * 80)
    println("HUDI TIME TRAVEL - BASIC EXAMPLE")
    println("=" * 80)
    println("Time Travel allows querying historical versions of data")
    println("=" * 80)

    println("\n=== 1. Version 1: Initial Data (2023-12-01) ===")
    val v1Data = Seq(
      (1701388800000L, "uuid-001", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1701388800001L, "uuid-002", "rider-B", "driver-L", 27.70, "san_francisco"),
      (1701388800002L, "uuid-003", "rider-C", "driver-M", 33.90, "new_york")
    )

    val v1Df = spark.createDataFrame(v1Data).toDF(columns: _*)
    
    v1Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot1 = spark.read.format("hudi").load(basePath)
    val commit1Time = latestCommitTime(spark, basePath)
    println(s"✓ Version 1 created")
    printCommitTime("Version 1", commit1Time)
    println(s"  Records: ${snapshot1.count()}")
    snapshot1.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    Thread.sleep(2000)

    println("\n=== 2. Version 2: Price Increase (2023-12-02) ===")
    val v2Data = Seq(
      (1701475200000L, "uuid-001", "rider-A", "driver-K", 25.00, "san_francisco"),
      (1701475200001L, "uuid-002", "rider-B", "driver-L", 35.00, "san_francisco"),
      (1701475200002L, "uuid-003", "rider-C", "driver-M", 40.00, "new_york")
    )
    val v2Df = spark.createDataFrame(v2Data).toDF(columns: _*)

    v2Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot2 = spark.read.format("hudi").load(basePath)
    val commit2Time = latestCommitTime(spark, basePath)
    println(s"✓ Version 2 created (prices increased)")
    printCommitTime("Version 2", commit2Time)
    println(s"  Records: ${snapshot2.count()}")
    snapshot2.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    Thread.sleep(2000)

    println("\n=== 3. Version 3: Add New Riders (2023-12-03) ===")
    val v3Data = Seq(
      (1701561600000L, "uuid-004", "rider-D", "driver-N", 45.50, "chennai"),
      (1701561600001L, "uuid-005", "rider-E", "driver-O", 55.60, "chennai")
    )
    val v3Df = spark.createDataFrame(v3Data).toDF(columns: _*)

    v3Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot3 = spark.read.format("hudi").load(basePath)
    val commit3Time = latestCommitTime(spark, basePath)
    println(s"✓ Version 3 created (new riders added)")
    printCommitTime("Version 3", commit3Time)
    println(s"  Records: ${snapshot3.count()}")
    snapshot3.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    println("\n" + "=" * 80)
    println("TIME TRAVEL QUERIES")
    println("=" * 80)

    println("\n=== 4. Query Current State (Latest Version) ===")
    val current = spark.read.format("hudi").load(basePath)
    println(s"Current version - Total records: ${current.count()}")
    current.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    println("\n=== 5. Time Travel to Version 1 (as.of.instant) ===")
    println(s"Traveling back to requested time: ${commit1Time.requestedTime}")
    val asOfV1 = spark.read.format("hudi")
      .option("as.of.instant", commit1Time.requestedTime)
      .load(basePath)
    
    println(s"Version 1 - Total records: ${asOfV1.count()}")
    println("Notice: Only 3 original records, original prices")
    asOfV1.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    println("\n=== 6. Time Travel to Version 2 (as.of.instant) ===")
    println(s"Traveling back to requested time: ${commit2Time.requestedTime}")
    val asOfV2 = spark.read.format("hudi")
      .option("as.of.instant", commit2Time.requestedTime)
      .load(basePath)
    
    println(s"Version 2 - Total records: ${asOfV2.count()}")
    println("Notice: Still 3 records, but prices increased")
    asOfV2.select("uuid", "rider", "fare", "city").orderBy("uuid").show()

    println("\n=== 7. Compare Price Changes Across Versions ===")
    println("\n┌──────────┬─────────┬─────────────┬─────────────┬─────────────┐")
    println("│ UUID     │ Rider   │ V1 Fare     │ V2 Fare     │ V3 Fare     │")
    println("├──────────┼─────────┼─────────────┼─────────────┼─────────────┤")
    
    val v1Fares = asOfV1.select("uuid", "fare").orderBy("uuid").collect()
    val v2Fares = asOfV2.select("uuid", "fare").orderBy("uuid").collect()
    val v3Fares = current.select("uuid", "fare").orderBy("uuid").collect()
    
    v1Fares.foreach { row =>
      val uuid = row.getString(0)
      val v1Fare = f"$$${row.getDouble(1)}%.2f"
      val v2Fare = f"$$${v2Fares.find(_.getString(0) == uuid).get.getDouble(1)}%.2f"
      val v3Fare = f"$$${v3Fares.find(_.getString(0) == uuid).get.getDouble(1)}%.2f"
      println(f"│ $uuid%-8s │ rider-${uuid.takeRight(1)}  │ $v1Fare%-11s │ $v2Fare%-11s │ $v3Fare%-11s │")
    }
    println("└──────────┴─────────┴─────────────┴─────────────┴─────────────┘")

    println("\n=== 8. View Timeline (All Commits) ===")
    val timeline = getCommitTimes(spark, basePath)
    
    println(s"Total commits: ${timeline.length}")
    timeline.zipWithIndex.foreach { case (commitTime, idx) =>
      val label = if (commitTime.requestedTime == commit1Time.requestedTime) "← Version 1"
                  else if (commitTime.requestedTime == commit2Time.requestedTime) "← Version 2"
                  else if (commitTime.requestedTime == commit3Time.requestedTime) "← Version 3"
                  else ""
      println(s"  ${idx + 1}. requested=${commitTime.requestedTime}, completion=${commitTime.completionTime} $label")
    }

    println("\n=== 9. Time Travel Use Cases ===")
    println("✓ Audit & Compliance: Review historical data states")
    println("✓ Data Recovery: Restore from accidental updates/deletes")
    println("✓ Debugging: Investigate when data changed")
    println("✓ Reporting: Generate reports for specific time points")
    println("✓ A/B Testing: Compare different data versions")

    println("\n=== 10. Key Configuration ===")
    println("Time Travel query configuration:")
    println("  • as.of.instant = <requested_instant>")
    println("  • Commit time should be read from Hudi timeline and sorted by completion time")
    println("  • Works with both COW and MOR tables")
    println("  • Requires commits to be retained (not cleaned)")

    println("\n=== 11. Important Notes ===")
    println("⚠️  Time Travel depends on Hudi's cleaning policy")
    println("⚠️  Old commits may be cleaned if retention period passed")
    println("⚠️  Configure hoodie.cleaner.commits.retained to keep history")
    println("⚠️  Default retention: 10 commits")

    spark.stop()
  }
}
