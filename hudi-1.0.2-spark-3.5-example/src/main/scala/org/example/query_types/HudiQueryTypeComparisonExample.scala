package org.example.query_types

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object HudiQueryTypeComparisonExample {
  case class CommitTime(requestedTime: String, completionTime: String)

  private def getCommitTimes(spark: SparkSession, basePath: String): Seq[CommitTime] = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
      .setBasePath(basePath)
      .build()

    metaClient.getCommitsTimeline
      .filterCompletedInstants()
      .getInstants
      .asScala
      .map(instant => CommitTime(instant.requestedTime(), instant.getCompletionTime))
      .sortBy(_.completionTime)
      .toSeq
  }

  private def latestCommitTime(spark: SparkSession, basePath: String): CommitTime = {
    val commits = getCommitTimes(spark, basePath)
    require(commits.nonEmpty, s"No completed commits found for $basePath")
    commits.last
  }

  private def printCommitTime(label: String, commitTime: CommitTime): Unit = {
    println(s"  $label requested time : ${commitTime.requestedTime}")
    println(s"  $label completion time: ${commitTime.completionTime}")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Query Type Comparison Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "query_comparison_table"
    val basePath = "file:///tmp/query_comparison_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

    println("=" * 90)
    println("HUDI QUERY TYPE COMPARISON: Snapshot vs Incremental vs Read Optimized")
    println("=" * 90)

    println("\n=== 1. Setup: Create MOR Table ===")
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

    val commit1Time = latestCommitTime(spark, basePath)
    println(s"✓ Initial data loaded: 5 records")
    printCommitTime("Commit 1", commit1Time)

    Thread.sleep(2000)

    println("\n=== 2. Batch Update - Wave 1 ===")
    val update1Data = Seq(
      (1695159650000L, "uuid-001", "rider-A", "driver-K", 99.99, "san_francisco"),
      (1695159650001L, "uuid-003", "rider-C", "driver-M", 88.88, "new_york")
    )
    val update1Df = spark.createDataFrame(update1Data).toDF(columns: _*)

    update1Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)

    val commit2Time = latestCommitTime(spark, basePath)
    println(s"✓ Wave 1 updated: 2 records")
    printCommitTime("Commit 2", commit2Time)

    Thread.sleep(2000)

    println("\n=== 3. Batch Insert - Wave 2 ===")
    val insert2Data = Seq(
      (1695159651000L, "uuid-006", "rider-F", "driver-P", 45.50, "chennai"),
      (1695159651001L, "uuid-007", "rider-G", "driver-Q", 56.60, "san_francisco")
    )
    val insert2Df = spark.createDataFrame(insert2Data).toDF(columns: _*)

    insert2Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)

    val commit3Time = latestCommitTime(spark, basePath)
    println(s"✓ Wave 2 inserted: 2 records")
    printCommitTime("Commit 3", commit3Time)

    val allCommitTimes = getCommitTimes(spark, basePath)
    println("\nCompleted commit timeline:")
    allCommitTimes.zipWithIndex.foreach { case (commitTime, idx) =>
      println(s"  Commit ${idx + 1}: requested=${commitTime.requestedTime}, completion=${commitTime.completionTime}")
    }
    println("\nNote: _hoodie_commit_time is the requested time stored on records.")
    println("      Incremental query begin/end instanttime use commit completion time in Hudi 1.0.x timeline layout v2.")

    println("\n" + "=" * 90)
    println("QUERY TYPE COMPARISON")
    println("=" * 90)

    println("\n=== 4. Snapshot Query - Current State ===")
    println("Description: Returns latest committed snapshot, merging base + delta logs")
    
    val snapshotStart = System.currentTimeMillis()
    val snapshotDf = spark.read.format("hudi").load(basePath)
    val snapshotCount = snapshotDf.count()
    val snapshotTime = System.currentTimeMillis() - snapshotStart
    
    println(s"Records: $snapshotCount | Query time: ${snapshotTime}ms")
    println("\nSnapshot data (all current records):")
    snapshotDf.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("uuid")
      .show(truncate = false)

    println("\n=== 5. Incremental Query - Changes Since Commit 1 ===")
    println("Description: Returns only changed records between time ranges (CDC)")
    
    val incrementalStart = System.currentTimeMillis()
    val incrementalDf = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", commit1Time.completionTime)
      .load(basePath)
    val incrementalCount = incrementalDf.count()
    val incrementalTime = System.currentTimeMillis() - incrementalStart
    
    println(s"Records: $incrementalCount | Query time: ${incrementalTime}ms")
    println(s"\nIncremental data (changes after commit completion time: ${commit1Time.completionTime}):")
    incrementalDf.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("_hoodie_commit_time", "uuid")
      .show(truncate = false)

    println("\n=== 6. Read Optimized Query - Base Files Only ===")
    println("Description: Returns data from base Parquet files only (fastest, may be stale)")
    
    val roStart = System.currentTimeMillis()
    val roDf = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    val roCount = roDf.count()
    val roTime = System.currentTimeMillis() - roStart
    
    println(s"Records: $roCount | Query time: ${roTime}ms")
    println("\nRead Optimized data (base files only, updates NOT visible):")
    roDf.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("uuid")
      .show(truncate = false)

    println("\n" + "=" * 90)
    println("DETAILED COMPARISON TABLE")
    println("=" * 90)

    println("\n┌─────────────────────────┬──────────────┬───────────────┬──────────────────┐")
    println("│ Query Type              │ Record Count │ Query Time    │ Data Freshness   │")
    println("├─────────────────────────┼──────────────┼───────────────┼──────────────────┤")
    println(f"│ Snapshot Query          │ ${snapshotCount}%-12d │ ${snapshotTime}%-13dms│ Latest (fresh)   │")
    println(f"│ Incremental Query       │ ${incrementalCount}%-12d │ ${incrementalTime}%-13dms│ Changes only     │")
    println(f"│ Read Optimized Query    │ ${roCount}%-12d │ ${roTime}%-13dms│ Stale (pre-update)│")
    println("└─────────────────────────┴──────────────┴───────────────┴──────────────────┘")

    println("\n=== 7. Feature Comparison Matrix ===")
    println("\n┌────────────────────────────┬──────────┬─────────────┬──────────────────┐")
    println("│ Feature                    │ Snapshot │ Incremental │ Read Optimized   │")
    println("├────────────────────────────┼──────────┼─────────────┼──────────────────┤")
    println("│ Default Query Type         │ YES      │ NO          │ NO               │")
    println("│ Works with COW             │ YES      │ YES         │ NO (COW only)    │")
    println("│ Works with MOR             │ YES      │ YES         │ YES              │")
    println("│ Shows Latest Updates       │ YES      │ YES         │ NO (until compact)│")
    println("│ Returns Full Dataset      │ YES      │ NO          │ YES              │")
    println("│ Returns Only Changes       │ NO       │ YES         │ NO               │")
    println("│ Requires Time Range        │ NO       │ YES         │ NO               │")
    println("│ Merges Delta Logs          │ YES      │ YES         │ NO               │")
    println("│ Query Performance          │ Medium   │ Fast        │ Fastest          │")
    println("│ Data Freshness             │ Fresh    │ Fresh       │ Stale (MOR)      │")
    println("└────────────────────────────┴──────────┴─────────────┴──────────────────┘")

    println("\n=== 8. Use Case Recommendations ===")
    
    println("\n📊 Snapshot Query:")
    println("   ✓ Default query for most use cases")
    println("   ✓ Ad-hoc analytics and exploration")
    println("   ✓ Real-time dashboards")
    println("   ✓ When you need complete current state")
    println("   ✓ Both COW and MOR tables")
    
    println("\n🔄 Incremental Query:")
    println("   ✓ Incremental ETL pipelines")
    println("   ✓ Change Data Capture (CDC)")
    println("   ✓ Event-driven processing")
    println("   ✓ Sync changes to downstream systems")
    println("   ✓ Process only new/updated records")
    
    println("\n⚡ Read Optimized Query:")
    println("   ✓ Long-running analytical queries (MOR only)")
    println("   ✓ BI reports with scheduled refresh")
    println("   ✓ Batch processing where latency is acceptable")
    println("   ✓ Maximum query performance needed")
    println("   ✓ Slight staleness is tolerable")

    println("\n=== 9. Configuration Examples ===")
    
    println("\nSnapshot Query (default):")
    println("  val df = spark.read.format(\"hudi\").load(path)")
    println("  // OR explicitly:")
    println("  val df = spark.read.format(\"hudi\")")
    println("    .option(\"hoodie.datasource.query.type\", \"snapshot\")")
    println("    .load(path)")
    
    println("\nIncremental Query:")
    println("  val df = spark.read.format(\"hudi\")")
    println("    .option(\"hoodie.datasource.query.type\", \"incremental\")")
    println("    .option(\"hoodie.datasource.read.begin.instanttime\", startCompletionTime)")
    println("    .option(\"hoodie.datasource.read.end.instanttime\", endCompletionTime) // optional")
    println("    .load(path)")
    
    println("\nRead Optimized Query (MOR only):")
    println("  val df = spark.read.format(\"hudi\")")
    println("    .option(\"hoodie.datasource.query.type\", \"read_optimized\")")
    println("    .load(path)")

    println("\n=== 10. Performance Analysis ===")
    
    val perfComparison = Seq(
      ("Snapshot", snapshotTime, snapshotCount),
      ("Incremental", incrementalTime, incrementalCount),
      ("Read Optimized", roTime, roCount)
    )
    
    println("\nQuery Performance Ranking (lower is better):")
    perfComparison.sortBy(_._2).zipWithIndex.foreach { case ((name, time, count), idx) =>
      println(f"  ${idx + 1}. $name%-20s: ${time}%4dms (${count} records)")
    }

    println("\n=== 11. Real-World Scenario Example ===")
    println("\nScenario: Daily batch processing pipeline")
    println("  1. Incremental Query: Get yesterday's changes → Process → Write to warehouse")
    println("  2. Snapshot Query: Validate complete current state")
    println("  3. Read Optimized Query: Generate daily reports (for MOR tables)")

    println("\nExample workflow:")
    println("  // Step 1: Get yesterday's changes")
    println(s"""  val changes = incrementalQuery(from = "${commit1Time.completionTime}", to = "${commit3Time.completionTime}")""")
    println("  changes.write.save(\"warehouse/daily_changes\")")
    println("\n  // Step 2: Validate current state")
    println("  val current = snapshotQuery()")
    println("  assert(current.count() == 7)")
    println("\n  // Step 3: Generate reports (fast)")
    println("  val report = readOptimizedQuery()")
    println("  report.write.save(\"reports/daily_summary\")")

    println("\n=== 12. Key Takeaways ===")
    println("✓ Snapshot = Full current state (default)")
    println("✓ Incremental = Only changes (CDC)")
    println("✓ Read Optimized = Fast but stale (MOR only)")
    println("✓ Choose based on: freshness needs, query pattern, table type")
    println("✓ Combine query types for comprehensive data pipelines")

    spark.stop()
  }
}
