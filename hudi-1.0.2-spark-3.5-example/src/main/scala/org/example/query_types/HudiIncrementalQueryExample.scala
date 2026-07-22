package org.example.query_types

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object HudiIncrementalQueryExample {
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
      .sortBy(_.requestedTime)
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
      .appName("Hudi Incremental Query Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "incremental_query_table"
    val basePath = "file:///tmp/incremental_query_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")

    println("=" * 80)
    println("HUDI INCREMENTAL QUERY EXAMPLE")
    println("=" * 80)

    println("\n=== 1. Initial Insert - Batch 1 ===")
    val batch1Data = Seq(
      (1695159649087L, "uuid-001", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1695091554788L, "uuid-002", "rider-B", "driver-L", 27.70, "san_francisco"),
      (1695046462179L, "uuid-003", "rider-C", "driver-M", 33.90, "new_york")
    )

    val batch1Df = spark.createDataFrame(batch1Data).toDF(columns: _*)
    
    batch1Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      // 写入 `_hoodie_operation` 操作类型元数据
      .option("hoodie.allow.operation.metadata.field", "true")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commit1Time = latestCommitTime(spark, basePath)
    println(s"✓ Batch 1 inserted: 3 records")
    printCommitTime("Batch 1", commit1Time)

    Thread.sleep(2000)

    println("\n=== 2. Second Insert - Batch 2 ===")
    val batch2Data = Seq(
      (1695516137016L, "uuid-004", "rider-D", "driver-N", 34.15, "new_york"),
      (1695115999911L, "uuid-005", "rider-E", "driver-O", 17.85, "chennai")
    )
    val batch2Df = spark.createDataFrame(batch2Data).toDF(columns: _*)

    batch2Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.allow.operation.metadata.field", "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val commit2Time = latestCommitTime(spark, basePath)
    println(s"✓ Batch 2 inserted: 2 records")
    printCommitTime("Batch 2", commit2Time)

    Thread.sleep(2000)

    println("\n=== 3. Update Operation - Batch 3 ===")
    val batch3Data = Seq(
      (1695159650000L, "uuid-001", "rider-A", "driver-K", 99.99, "san_francisco"),
      (1695159650001L, "uuid-003", "rider-C", "driver-M", 88.88, "new_york")
    )
    val batch3Df = spark.createDataFrame(batch3Data).toDF(columns: _*)

    batch3Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.allow.operation.metadata.field", "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val commit3Time = latestCommitTime(spark, basePath)
    println(s"✓ Batch 3 updated: 2 records")
    printCommitTime("Batch 3", commit3Time)

    Thread.sleep(2000)

    println("\n=== 4. Another Insert - Batch 4 ===")
    val batch4Data = Seq(
      (1695159651000L, "uuid-006", "rider-F", "driver-P", 45.50, "chennai"),
      (1695159651001L, "uuid-007", "rider-G", "driver-Q", 56.60, "san_francisco")
    )
    val batch4Df = spark.createDataFrame(batch4Data).toDF(columns: _*)

    batch4Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.allow.operation.metadata.field", "true")
      .mode(SaveMode.Append)
      .save(basePath)

    val commit4Time = latestCommitTime(spark, basePath)
    val latestSnapshotDf = spark.read.format("hudi").load(basePath)
    println(s"✓ Batch 4 inserted: 2 records")
    printCommitTime("Batch 4", commit4Time)

    println("\n=== 5. View All Commits ===")
    val allCommits = getCommitTimes(spark, basePath)
    println("All commit times:")
    allCommits.zipWithIndex.foreach { case (commitTime, idx) =>
      println(s"  Commit ${idx + 1}: requested=${commitTime.requestedTime}, completion=${commitTime.completionTime}")
    }
    println("\nNote: _hoodie_commit_time is the requested time stored on records.")
    println("      Incremental query begin/end instanttime use commit completion time in Hudi 1.0.x timeline layout v2.")

    println("\n=== 6. Incremental Query - Changes After Commit 1 ===")
    println(s"Reading changes from Batch 2 completion time: ${commit2Time.completionTime}")
    
    val incrementalDf1 = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", commit2Time.completionTime)
      .load(basePath)

    println(s"Records changed after commit 1: ${incrementalDf1.count()}")
    println("\nIncremental data (Batch 2, 3 and 4):")
    incrementalDf1.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("_hoodie_commit_time", "uuid")
      .show(truncate = false)

    println("\n=== 7. Incremental Query - Changes between Commit 2 and Commit 4 ===")
    println(s"Reading changes from Batch 3 to Batch 4 using completion-time boundaries")
    println(s"  begin: ${commit3Time.completionTime}")
    println(s"  end  : ${commit4Time.completionTime}")
    
    val incrementalDf2 = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", commit3Time.completionTime)
      .option("hoodie.datasource.read.end.instanttime", commit4Time.completionTime)
      .load(basePath)

    println(s"Records changed in Batch 3 and 4: ${incrementalDf2.count()}")
    println("\nIncremental data (Batch 3 and 4):")
    incrementalDf2.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .orderBy("_hoodie_commit_time", "uuid")
      .show(truncate = false)

    println("\n=== 8. Incremental Query - Only Batch 3 (Updates) ===")
    println(s"Reading only Batch 3 completion time: ${commit3Time.completionTime}")
    
    val incrementalDf3 = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", commit3Time.completionTime)
      .option("hoodie.datasource.read.end.instanttime", commit3Time.completionTime)
      .load(basePath)

    println(s"Records in Batch 3: ${incrementalDf3.count()}")
    println("\nIncremental data (only updates):")
    incrementalDf3.select("uuid", "rider", "fare", "city", "_hoodie_commit_time", "_hoodie_operation")
      .show(truncate = false)

    println("\n=== 9. Incremental Query Characteristics ===")
    println("✓ Reads only the changed latest-state records in the selected instant range")
    println("✓ Efficient for processing only new/updated records")
    println("✓ Uses commit completion times to define the range")
    println("✓ Perfect for incremental ETL pipelines")
    println("✓ Supports both insert and update operations")
    println("✓ Can specify begin and end timestamps")

    println("\n=== 10. Use Case Example - Incremental ETL ===")
    println("Scenario: Process only new data since last run")
    println(s"\nLast processed completion time: ${commit2Time.completionTime}")
    val nextBeginCompletionTime = allCommits
      .find(_.completionTime > commit2Time.completionTime)
      .map(_.completionTime)
      .getOrElse(commit2Time.completionTime)
    println(s"Next begin completion time: $nextBeginCompletionTime")
    println("Processing new data...")
    
    val newDataSinceLastRun = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "incremental")
      .option("hoodie.datasource.read.begin.instanttime", nextBeginCompletionTime)
      .load(basePath)

    println(s"\nNew records to process: ${newDataSinceLastRun.count()}")
    
    val summary = newDataSinceLastRun.groupBy("city")
      .agg(
        count("*").as("new_records"),
        sum("fare").as("total_fare")
      )
    
    println("\nSummary by city:")
    summary.show(truncate = false)

    println("\n=== 11. Incremental Query with Filters ===")
    println("Get incremental records with fare > 50:")
    
    val filteredIncremental = incrementalDf1.filter("fare > 50")
    println(s"Matching records: ${filteredIncremental.count()}")
    filteredIncremental.select("uuid", "rider", "fare", "city", "_hoodie_commit_time")
      .show(truncate = false)

    println("\n=== 12. Configuration Summary ===")
    println("Key parameters for Incremental Query:")
    println("  • hoodie.datasource.query.type = incremental")
    println("  • hoodie.datasource.read.begin.instanttime = <start_completion_time>")
    println("  • hoodie.datasource.read.end.instanttime = <end_completion_time> (optional)")
    println("\nNote: If end.instanttime is not specified, reads until latest completed commit")

    spark.stop()
  }
}
