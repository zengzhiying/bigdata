package org.example.time_travel

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.example.time_travel.HudiTimelineUtils._

object HudiTimeTravelPracticalExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Time Travel Practical Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "customer_balance_table"
    val basePath = "file:///tmp/customer_balance_table"

    val columns = Seq("ts", "customer_id", "name", "balance", "account_type", "region")

    println("=" * 90)
    println("HUDI TIME TRAVEL - PRACTICAL EXAMPLE: Banking Transaction System")
    println("=" * 90)
    println("Scenario: Track customer account balances and restore from errors")
    println("=" * 90)

    def printSection(title: String): Unit = {
      println("\n" + "=" * 90)
      println(title)
      println("=" * 90)
    }

    printSection("STEP 1: Initial Account Setup (Monday Morning)")
    val mondayMorning = Seq(
      (1701662400000L, "C001", "Alice Johnson", 10000.00, "savings", "north"),
      (1701662400001L, "C002", "Bob Smith", 5000.00, "checking", "south"),
      (1701662400002L, "C003", "Carol White", 15000.00, "savings", "east"),
      (1701662400003L, "C004", "David Brown", 8000.00, "checking", "west"),
      (1701662400004L, "C005", "Eve Davis", 12000.00, "savings", "north")
    )

    val mondayDf = spark.createDataFrame(mondayMorning).toDF(columns: _*)
    
    mondayDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "customer_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .option("hoodie.cleaner.commits.retained", "30")
      .option("hoodie.keep.min.commits", "35")
      .option("hoodie.keep.max.commits", "40")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val mondaySnapshot = spark.read.format("hudi").load(basePath)
    val mondayCommit = latestCommitTime(spark, basePath)
    
    println(s"✓ Accounts initialized on Monday")
    printCommitTime("Monday Morning", mondayCommit)
    println(s"  Total accounts: ${mondaySnapshot.count()}")
    println(s"  Total balance: $$${mondaySnapshot.agg(sum("balance")).first().getDouble(0)}")
    
    mondaySnapshot.select("customer_id", "name", "balance", "account_type").orderBy("customer_id").show()

    Thread.sleep(2000)

    printSection("STEP 2: Normal Transactions (Monday Afternoon)")
    val mondayAfternoon = Seq(
      (1701684000000L, "C001", "Alice Johnson", 9500.00, "savings", "north"),
      (1701684000001L, "C002", "Bob Smith", 5200.00, "checking", "south"),
      (1701684000002L, "C003", "Carol White", 14800.00, "savings", "east")
    )
    val mondayAfternoonDf = spark.createDataFrame(mondayAfternoon).toDF(columns: _*)

    mondayAfternoonDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "customer_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val mondayPMSnapshot = spark.read.format("hudi").load(basePath)
    val mondayPMCommit = latestCommitTime(spark, basePath)
    
    println(s"✓ Afternoon transactions processed")
    printCommitTime("Monday PM", mondayPMCommit)
    mondayPMSnapshot.select("customer_id", "name", "balance").orderBy("customer_id").show()

    Thread.sleep(2000)

    printSection("STEP 3: ERROR - Incorrect Batch Update (Tuesday Morning)")
    println("⚠️  A buggy script accidentally multiplied all balances by 10!")
    
    val tuesdayError = mondayPMSnapshot
      .select(columns.map(col): _*)
      .collect()
      .map { row =>
      (
        System.currentTimeMillis(),
        row.getString(1),
        row.getString(2),
        row.getDouble(3) * 10,
        row.getString(4),
        row.getString(5)
      )
    }
    val tuesdayErrorDf = spark.createDataFrame(tuesdayError).toDF(columns: _*)

    tuesdayErrorDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "customer_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val tuesdaySnapshot = spark.read.format("hudi").load(basePath)
    val tuesdayCommit = latestCommitTime(spark, basePath)
    
    println(s"✗ ERROR: Balances corrupted!")
    printCommitTime("Tuesday Error", tuesdayCommit)
    println(s"  Total balance (WRONG): $$${tuesdaySnapshot.agg(sum("balance")).first().getDouble(0)}")
    
    tuesdaySnapshot.select("customer_id", "name", "balance").orderBy("customer_id").show()

    printSection("STEP 4: Detection & Investigation")
    println("Data quality check detected anomaly!")
    println("\nComparing Monday PM vs Tuesday (corrupt) balances:")
    
    val mondayPMData = spark.read.format("hudi")
      .option("as.of.instant", mondayPMCommit.requestedTime)
      .load(basePath)
      .select(col("customer_id").as("cid_mon"), col("balance").as("balance_mon"))
    
    val tuesdayData = tuesdaySnapshot
      .select(col("customer_id").as("cid_tue"), col("balance").as("balance_tue"), col("name"))
    
    val comparison = mondayPMData.join(tuesdayData, 
      mondayPMData("cid_mon") === tuesdayData("cid_tue"), "inner")
      .withColumn("multiplier", col("balance_tue") / col("balance_mon"))
      .select(
        col("cid_tue").as("customer_id"),
        col("name"),
        col("balance_mon").as("correct_balance"),
        col("balance_tue").as("corrupt_balance"),
        col("multiplier")
      )
    
    comparison.show()
    
    println("⚠️  All balances multiplied by 10x - DATA CORRUPTION CONFIRMED!")

    printSection("STEP 5: Recovery Using Time Travel")
    println("Strategy: Restore data from Monday PM (last known good state)")
    
    println(s"\n1. Read correct data from Monday PM requested time: ${mondayPMCommit.requestedTime}")
    val correctData = spark.read.format("hudi")
      .option("as.of.instant", mondayPMCommit.requestedTime)
      .load(basePath)
    
    println("\nCorrect data from Monday PM:")
    correctData.select("customer_id", "name", "balance").orderBy("customer_id").show()
    
    println("\n2. Overwrite with correct data (recovery operation)")
    val recoveryData = correctData.select(columns.map(col): _*)
      .withColumn("ts", lit(System.currentTimeMillis()))
    
    recoveryData.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "customer_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "region")
      .mode(SaveMode.Append)
      .save(basePath)

    val recoveredSnapshot = spark.read.format("hudi").load(basePath)
    val recoveryCommit = latestCommitTime(spark, basePath)
    
    println(s"\n✓ Recovery complete!")
    printCommitTime("Recovery", recoveryCommit)
    println(s"  Total balance (restored): $$${recoveredSnapshot.agg(sum("balance")).first().getDouble(0)}")
    
    recoveredSnapshot.select("customer_id", "name", "balance").orderBy("customer_id").show()

    printSection("STEP 6: Audit Trail - Complete History")
    
    val allCommits = getCommitTimes(spark, basePath)
    
    println(s"Total commits in history: ${allCommits.length}")
    println("\nFull audit trail:")
    
    val commitLabels = Map(
      mondayCommit.requestedTime -> "Monday Morning - Initial Setup",
      mondayPMCommit.requestedTime -> "Monday PM - Normal Transactions",
      tuesdayCommit.requestedTime -> "Tuesday - ERROR (10x multiplication)",
      recoveryCommit.requestedTime -> "Recovery - Restored from Monday PM"
    )
    
    allCommits.zipWithIndex.foreach { case (commit, idx) =>
      val snapshot = spark.read.format("hudi")
        .option("as.of.instant", commit.requestedTime)
        .load(basePath)
      
      val totalBalance = snapshot.agg(sum("balance")).first().getDouble(0)
      val recordCount = snapshot.count()
      val label = commitLabels.getOrElse(commit.requestedTime, "Unknown")
      
      println(s"\n${idx + 1}. Commit requested=${commit.requestedTime}, completion=${commit.completionTime}")
      println(f"   Label: $label")
      println(f"   Records: $recordCount, Total Balance: $$${totalBalance}%.2f")
    }

    printSection("STEP 7: Point-in-Time Balance Report")
    println("\nGenerate balance report for Monday PM (before error):")
    
    val mondayReport = spark.read.format("hudi")
      .option("as.of.instant", mondayPMCommit.requestedTime)
      .load(basePath)
    
    println("\nBalance Summary by Account Type (Monday PM):")
    mondayReport.groupBy("account_type")
      .agg(
        count("*").as("customers"),
        sum("balance").as("total_balance"),
        avg("balance").as("avg_balance")
      )
      .show()
    
    println("Balance Summary by Region (Monday PM):")
    mondayReport.groupBy("region")
      .agg(
        count("*").as("customers"),
        sum("balance").as("total_balance")
      )
      .show()

    printSection("STEP 8: Customer-Specific Audit")
    println("Track balance changes for Alice Johnson (C001):")
    
    println("\n┌────────────────────────┬──────────────────┬─────────────┐")
    println("│ Timestamp              │ Event            │ Balance     │")
    println("├────────────────────────┼──────────────────┼─────────────┤")
    
    allCommits.foreach { commit =>
      val snapshot = spark.read.format("hudi")
        .option("as.of.instant", commit.requestedTime)
        .load(basePath)
      
      val aliceData = snapshot.filter("customer_id = 'C001'")
        .select("balance")
        .first()
      val balance = aliceData.getDouble(0)
      val label = commitLabels.getOrElse(commit.requestedTime, "Event")
      
      println(f"│ ${commit.requestedTime} │ ${label.take(16)}%-16s │ $$${balance}%-10.2f │")
    }
    println("└────────────────────────┴──────────────────┴─────────────┘")

    printSection("STEP 9: Best Practices Demonstrated")
    println("✓ Configure adequate retention: hoodie.cleaner.commits.retained = 30")
    println("✓ Regular data quality checks to detect anomalies early")
    println("✓ Document important commit timestamps for quick recovery")
    println("✓ Test recovery procedures in non-production environment")
    println("✓ Maintain audit logs of all time travel operations")
    println("✓ Set up alerts for unusual data changes")
    println("✓ Use precombine field (ts) to ensure correct ordering")

    printSection("STEP 10: Key Learnings")
    println("\n1. Time Travel Configuration:")
    println("   • hoodie.cleaner.commits.retained: How many commits to keep")
    println("   • hoodie.keep.min.commits: Minimum commits to retain")
    println("   • hoodie.keep.max.commits: Maximum commits before archiving")
    
    println("\n2. Recovery Process:")
    println("   • Identify the error and its timestamp")
    println("   • Find the last known good commit")
    println("   • Use as.of.instant to read correct data")
    println("   • Upsert to restore the correct state")
    
    println("\n3. Prevention Strategies:")
    println("   • Implement data validation before writes")
    println("   • Use staging tables for large updates")
    println("   • Test scripts on sample data first")
    println("   • Monitor balance totals and other invariants")
    
    println("\n4. Production Considerations:")
    println("   • Balance retention vs storage costs")
    println("   • Archive old commits to cheaper storage")
    println("   • Document recovery procedures")
    println("   • Regular disaster recovery drills")

    printSection("STEP 11: Verification")
    println("Verify that current state matches Monday PM (pre-error) state:")
    
    val currentState = spark.read.format("hudi").load(basePath)
    val mondayPMState = spark.read.format("hudi")
      .option("as.of.instant", mondayPMCommit.requestedTime)
      .load(basePath)
    
    val currentBalances = currentState.select("customer_id", "balance")
      .orderBy("customer_id")
      .collect()
      .map(r => (r.getString(0), r.getDouble(1)))
    
    val mondayBalances = mondayPMState.select("customer_id", "balance")
      .orderBy("customer_id")
      .collect()
      .map(r => (r.getString(0), r.getDouble(1)))
    
    val verified = currentBalances.zip(mondayBalances).forall { 
      case ((id1, bal1), (id2, bal2)) => id1 == id2 && bal1 == bal2
    }
    
    if (verified) {
      println("✓ SUCCESS: Current state matches Monday PM state perfectly!")
      println("✓ Recovery operation completed successfully!")
    } else {
      println("✗ WARNING: States do not match - review recovery process")
    }

    println("\n" + "=" * 90)
    println("SUMMARY: Time Travel enabled complete recovery from data corruption")
    println("=" * 90)

    spark.stop()
  }
}
