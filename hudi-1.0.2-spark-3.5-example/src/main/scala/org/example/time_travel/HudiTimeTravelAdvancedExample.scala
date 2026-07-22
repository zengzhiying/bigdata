package org.example.time_travel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.example.time_travel.HudiTimelineUtils._

object HudiTimeTravelAdvancedExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Time Travel Advanced Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "time_travel_advanced_table"
    val basePath = "file:///tmp/time_travel_advanced_table"

    val columns = Seq("ts", "order_id", "product", "price", "quantity", "status", "region")

    println("=" * 80)
    println("HUDI TIME TRAVEL - ADVANCED FEATURES")
    println("=" * 80)

    println("\n=== Scenario: E-commerce Order Tracking System ===")
    println("Track order status changes over time")

    println("\n=== 1. Day 1: Orders Placed ===")
    val day1Data = Seq(
      (1701388800000L, "order-001", "Laptop", 1200.00, 1, "placed", "US-West"),
      (1701388800001L, "order-002", "Mouse", 25.00, 2, "placed", "US-East"),
      (1701388800002L, "order-003", "Keyboard", 80.00, 1, "placed", "EU"),
      (1701388800003L, "order-004", "Monitor", 300.00, 2, "placed", "US-West"),
      (1701388800004L, "order-005", "Headset", 150.00, 1, "placed", "APAC")
    )

    val day1Df = spark.createDataFrame(day1Data).toDF(columns: _*)
    
    day1Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .option("hoodie.cleaner.commits.retained", "20")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot1 = spark.read.format("hudi").load(basePath)
    val commit1 = latestCommitTime(spark, basePath)
    println("Day 1 Commit:")
    printCommitTime("Day 1", commit1)
    println(s"Orders placed: ${snapshot1.count()}")
    snapshot1.select("order_id", "product", "price", "quantity", "status").show()

    Thread.sleep(2000)

    println("\n=== 2. Day 2: Some Orders Shipped ===")
    val day2Data = Seq(
      (1701475200000L, "order-001", "Laptop", 1200.00, 1, "shipped", "US-West"),
      (1701475200001L, "order-003", "Keyboard", 80.00, 1, "shipped", "EU")
    )
    val day2Df = spark.createDataFrame(day2Data).toDF(columns: _*)

    day2Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot2 = spark.read.format("hudi").load(basePath)
    val commit2 = latestCommitTime(spark, basePath)
    println("Day 2 Commit:")
    printCommitTime("Day 2", commit2)
    snapshot2.select("order_id", "product", "status", "_hoodie_commit_time").orderBy("order_id").show()

    Thread.sleep(2000)

    println("\n=== 3. Day 3: Price Adjustments & More Shipments ===")
    val day3Data = Seq(
      (1701561600000L, "order-002", "Mouse", 22.00, 2, "shipped", "US-East"),
      (1701561600001L, "order-004", "Monitor", 280.00, 2, "shipped", "US-West"),
      (1701561600002L, "order-005", "Headset", 140.00, 1, "processing", "APAC")
    )
    val day3Df = spark.createDataFrame(day3Data).toDF(columns: _*)

    day3Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot3 = spark.read.format("hudi").load(basePath)
    val commit3 = latestCommitTime(spark, basePath)
    println("Day 3 Commit:")
    printCommitTime("Day 3", commit3)
    snapshot3.select("order_id", "product", "price", "status", "_hoodie_commit_time").orderBy("order_id").show()

    Thread.sleep(2000)

    println("\n=== 4. Day 4: All Orders Delivered ===")
    val day4Data = Seq(
      (1701648000000L, "order-001", "Laptop", 1200.00, 1, "delivered", "US-West"),
      (1701648000001L, "order-002", "Mouse", 22.00, 2, "delivered", "US-East"),
      (1701648000002L, "order-003", "Keyboard", 80.00, 1, "delivered", "EU"),
      (1701648000003L, "order-004", "Monitor", 280.00, 2, "delivered", "US-West"),
      (1701648000004L, "order-005", "Headset", 140.00, 1, "delivered", "APAC")
    )
    val day4Df = spark.createDataFrame(day4Data).toDF(columns: _*)

    day4Df.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot4 = spark.read.format("hudi").load(basePath)
    val commit4 = latestCommitTime(spark, basePath)
    println("Day 4 Commit:")
    printCommitTime("Day 4", commit4)
    snapshot4.select("order_id", "product", "price", "status", "_hoodie_commit_time").orderBy("order_id").show()

    println("\n" + "=" * 80)
    println("ADVANCED TIME TRAVEL OPERATIONS")
    println("=" * 80)

    println("\n=== 5. Query: What were the order statuses on Day 2? ===")
    val day2Status = spark.read.format("hudi")
      .option("as.of.instant", commit2.requestedTime)
      .load(basePath)
    
    println("Order statuses on Day 2:")
    day2Status.groupBy("status")
      .agg(count("*").as("count"))
      .show()
    
    day2Status.select("order_id", "product", "status").orderBy("order_id").show()

    println("\n=== 6. Query: Find Orders That Changed Price ===")
    val day1Prices = spark.read.format("hudi")
      .option("as.of.instant", commit1.requestedTime)
      .load(basePath)
      .select(col("order_id").as("order_id_d1"), col("price").as("price_d1"))
    
    val currentPrices = snapshot4
      .select(col("order_id").as("order_id_current"), col("price").as("price_current"), col("product"))
    
    val priceChanges = day1Prices.join(currentPrices, 
      day1Prices("order_id_d1") === currentPrices("order_id_current"), "inner")
      .filter(col("price_d1") =!= col("price_current"))
      .select(
        col("order_id_current").as("order_id"),
        col("product"),
        col("price_d1").as("original_price"),
        col("price_current").as("final_price"),
        (col("price_current") - col("price_d1")).as("price_change")
      )
    
    println("Orders with price changes:")
    priceChanges.show()

    println("\n=== 7. Audit Trail: Track Order Status Changes ===")
    println("\nOrder-001 Status History:")
    
    val commits = Seq(
      ("Day 1", commit1.requestedTime),
      ("Day 2", commit2.requestedTime),
      ("Day 3", commit3.requestedTime),
      ("Day 4", commit4.requestedTime)
    )
    
    commits.foreach { case (day, commit) =>
      val snapshot = spark.read.format("hudi")
        .option("as.of.instant", commit)
        .load(basePath)
      
      val order = snapshot.filter("order_id = 'order-001'")
        .select("status", "price")
        .first()
      
      println(f"  $day%-8s: ${order.getString(0)}%-12s (Price: $$${order.getDouble(1)}%.2f)")
    }

    println("\n=== 8. Point-in-Time Revenue Analysis ===")
    commits.foreach { case (day, commit) =>
      val snapshot = spark.read.format("hudi")
        .option("as.of.instant", commit)
        .load(basePath)
      
      val totalRevenue = snapshot
        .withColumn("revenue", col("price") * col("quantity"))
        .agg(sum("revenue").as("total"))
        .first()
        .getDouble(0)
      
      val orderCount = snapshot.count()
      
      println(f"$day%-8s: ${orderCount} orders, Total Revenue: $$${totalRevenue}%.2f")
    }

    println("\n=== 9. Compare Regional Performance Over Time ===")
    println("\nRegional order counts across versions:")
    
    println("\n┌─────────────┬─────────┬─────────┬─────────┬─────────┐")
    println("│ Region      │ Day 1   │ Day 2   │ Day 3   │ Day 4   │")
    println("├─────────────┼─────────┼─────────┼─────────┼─────────┤")
    
    val regions = List("US-West", "US-East", "EU", "APAC")
    
    regions.foreach { region =>
      val counts = commits.map { case (_, commit) =>
        spark.read.format("hudi")
          .option("as.of.instant", commit)
          .load(basePath)
          .filter(s"region = '$region'")
          .count()
      }
      println(f"│ $region%-11s │ ${counts(0)}%-7d │ ${counts(1)}%-7d │ ${counts(2)}%-7d │ ${counts(3)}%-7d │")
    }
    println("└─────────────┴─────────┴─────────┴─────────┴─────────┘")

    println("\n=== 10. Rollback Simulation: Restore to Day 2 ===")
    println("Scenario: Found data quality issue, need to rollback to Day 2")
    
    val rollbackData = spark.read.format("hudi")
      .option("as.of.instant", commit2.requestedTime)
      .load(basePath)
    
    println(s"\nData from Day 2 (requested time: ${commit2.requestedTime}):")
    rollbackData.select("order_id", "product", "status", "price").orderBy("order_id").show()
    
    println("Note: In production, you would write this back to restore the state")
    println("  rollbackData.write.format('hudi').mode(SaveMode.Overwrite).save(basePath)")

    println("\n=== 11. Time Travel with Partitions ===")
    println("Query specific partition at specific time:")
    
    val usWestDay2 = spark.read.format("hudi")
      .option("as.of.instant", commit2.requestedTime)
      .load(basePath)
      .filter("region = 'US-West'")
    
    println("US-West region on Day 2:")
    usWestDay2.select("order_id", "product", "status").show()

    println("\n=== 12. Timeline Analysis ===")
    val timeline = getCommitTimes(spark, basePath)
    
    println(s"Total commits in timeline: ${timeline.length}")
    timeline.zipWithIndex.foreach { case (commitTime, idx) =>
      val recordCount = spark.read.format("hudi")
        .option("as.of.instant", commitTime.requestedTime)
        .load(basePath)
        .count()
      println(s"  Commit ${idx + 1}: requested=${commitTime.requestedTime}, completion=${commitTime.completionTime} - $recordCount records")
    }

    println("\n=== 13. Advanced Use Cases ===")
    println("✓ Compliance Auditing: Track all changes for regulatory requirements")
    println("✓ Data Quality Validation: Compare current vs historical data")
    println("✓ Rollback Operations: Restore to known good state")
    println("✓ Historical Reporting: Generate reports for specific dates")
    println("✓ A/B Testing Analysis: Compare different data versions")
    println("✓ Debugging: Identify when incorrect data was introduced")
    println("✓ SLA Monitoring: Track metrics over time")

    println("\n=== 14. Best Practices ===")
    println("✓ Configure hoodie.cleaner.commits.retained to keep enough history")
    println("✓ Use meaningful commit timestamps for easier time travel")
    println("✓ Document critical timestamps for important business events")
    println("✓ Regular cleanup of very old commits to save storage")
    println("✓ Test rollback procedures before production incidents")

    spark.stop()
  }
}
