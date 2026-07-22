package org.example.record_key_precombine

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiCompositeKeyAdvancedExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Composite Key & Advanced Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    println("=" * 90)
    println("HUDI COMPOSITE KEY & ADVANCED SCENARIOS")
    println("=" * 90)

    println("\n=== PART 1: COMPOSITE RECORD KEY ===")
    println("When a single field is not unique, use multiple fields as composite key")

    val tableName1 = "composite_key_table"
    val basePath1 = "file:///tmp/composite_key_table"

    println("\n--- Scenario: Multi-Tenant IoT Sensor Data ---")
    println("Each sensor sends data, but sensor_id alone is not unique")
    println("Need: tenant_id + sensor_id to uniquely identify records")

    val columns1 = Seq("tenant_id", "sensor_id", "temperature", "humidity", "timestamp")

    val sensorData1 = Seq(
      ("tenant-A", "sensor-001", 22.5, 45.0, 1701388800000L),
      ("tenant-A", "sensor-002", 23.1, 47.5, 1701388800000L),
      ("tenant-B", "sensor-001", 21.8, 43.2, 1701388800000L),  // Same sensor_id, different tenant
      ("tenant-B", "sensor-002", 24.2, 50.1, 1701388800000L)
    )

    val sensorDf1 = spark.createDataFrame(sensorData1).toDF(columns1: _*)
    
    println("\nInitial sensor data:")
    sensorDf1.show(truncate = false)

    println("\nWriting with composite key: tenant_id + sensor_id")
    sensorDf1.write.format("hudi")
      .option("hoodie.table.name", tableName1)
      .option("hoodie.datasource.write.recordkey.field", "tenant_id,sensor_id")  // Composite
      .option("hoodie.datasource.write.precombine.field", "timestamp")
      .option("hoodie.datasource.write.partitionpath.field", "tenant_id")
      .mode(SaveMode.Overwrite)
      .save(basePath1)

    val snapshot1 = spark.read.format("hudi").load(basePath1)
    println("\n✓ Initial write complete")
    println("Notice _hoodie_record_key is composite of both fields:")
    snapshot1.select("tenant_id", "sensor_id", "_hoodie_record_key", "temperature")
      .orderBy("tenant_id", "sensor_id")
      .show(truncate = false)

    println("\n--- Update: Same tenant_id + sensor_id combination ---")
    val sensorUpdate = Seq(
      ("tenant-A", "sensor-001", 25.0, 48.0, 1701475200000L)  // Update tenant-A sensor-001
    )
    val updateDf = spark.createDataFrame(sensorUpdate).toDF(columns1: _*)

    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName1)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "tenant_id,sensor_id")
      .option("hoodie.datasource.write.precombine.field", "timestamp")
      .option("hoodie.datasource.write.partitionpath.field", "tenant_id")
      .mode(SaveMode.Append)
      .save(basePath1)

    val snapshot2 = spark.read.format("hudi").load(basePath1)
    println("\n✓ Update complete - tenant-A sensor-001 updated:")
    snapshot2.select("tenant_id", "sensor_id", "temperature", "timestamp")
      .orderBy("tenant_id", "sensor_id")
      .show(truncate = false)

    println("\n=== PART 2: HANDLING NULL VALUES ===")
    
    val tableName2 = "null_handling_table"
    val basePath2 = "file:///tmp/null_handling_table"

    println("\n--- Problem: Null values in record key ---")
    println("⚠️  Record key fields should NEVER be null")

    val columnsNull = Seq("order_id", "customer_id", "amount", "ts")
    
    val dataWithNull = Seq(
      ("ORD-001", "CUST-001", 100.0, 1701388800000L),
      ("ORD-002", null, 200.0, 1701388800000L),  // NULL customer_id
      ("ORD-003", "CUST-003", 300.0, 1701388800000L)
    )

    val nullDf = spark.createDataFrame(dataWithNull).toDF(columnsNull: _*)
    
    println("\nData with null customer_id:")
    nullDf.show(truncate = false)

    println("\nSolution: Filter out nulls BEFORE writing to Hudi")
    val cleanDf = nullDf.filter(col("customer_id").isNotNull)
    
    println("Cleaned data:")
    cleanDf.show(truncate = false)

    cleanDf.write.format("hudi")
      .option("hoodie.table.name", tableName2)
      .option("hoodie.datasource.write.recordkey.field", "order_id,customer_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .mode(SaveMode.Overwrite)
      .save(basePath2)

    println("\n✓ Only valid records written (2 out of 3)")

    println("\n=== PART 3: HANDLING CONCURRENT UPDATES WITH PRECOMBINE ===")
    
    val tableName3 = "concurrent_updates_table"
    val basePath3 = "file:///tmp/concurrent_updates_table"

    println("\n--- Scenario: CDC from Multiple Database Replicas ---")
    println("Same record updated on different replicas with different timestamps")

    val columnsConc = Seq("user_id", "name", "email", "balance", "db_replica", "update_ts")

    println("\nReplica 1 update (ts=1000):")
    val replica1 = Seq(
      ("U001", "Alice", "alice@example.com", 1000.0, "replica-1", 1000L)
    )
    val replica1Df = spark.createDataFrame(replica1).toDF(columnsConc: _*)
    replica1Df.show(truncate = false)

    println("Replica 2 update (ts=2000) - NEWER, should WIN:")
    val replica2 = Seq(
      ("U001", "Alice Updated", "alice.new@example.com", 1500.0, "replica-2", 2000L)
    )
    val replica2Df = spark.createDataFrame(replica2).toDF(columnsConc: _*)
    replica2Df.show(truncate = false)

    println("Replica 3 update (ts=1500) - MIDDLE, should LOSE:")
    val replica3 = Seq(
      ("U001", "Alice Middle", "alice.mid@example.com", 1200.0, "replica-3", 1500L)
    )
    val replica3Df = spark.createDataFrame(replica3).toDF(columnsConc: _*)
    replica3Df.show(truncate = false)

    println("\nWriting all updates in RANDOM order (simulating concurrent arrival):")
    
    replica1Df.write.format("hudi")
      .option("hoodie.table.name", tableName3)
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "update_ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .mode(SaveMode.Overwrite)
      .save(basePath3)

    replica3Df.write.format("hudi")
      .option("hoodie.table.name", tableName3)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "update_ts")
      .mode(SaveMode.Append)
      .save(basePath3)

    replica2Df.write.format("hudi")
      .option("hoodie.table.name", tableName3)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "update_ts")
      .mode(SaveMode.Append)
      .save(basePath3)

    val concSnapshot = spark.read.format("hudi").load(basePath3)
    println("\n✓ Result: Replica 2 (ts=2000) won, regardless of write order:")
    concSnapshot.select("user_id", "name", "email", "balance", "db_replica", "update_ts")
      .show(truncate = false)

    println("\n=== PART 4: CUSTOM RECORD KEY GENERATION ===")
    
    val tableName4 = "custom_key_table"
    val basePath4 = "file:///tmp/custom_key_table"

    println("\n--- Scenario: Generate composite key from multiple fields ---")
    println("Create a custom UUID-like key from business fields")

    val columnsCustom = Seq("country", "city", "store_id", "product_id", "sales", "date")
    
    val salesData = Seq(
      ("US", "NYC", "S001", "P001", 100, "2023-12-01"),
      ("US", "NYC", "S001", "P002", 200, "2023-12-01"),
      ("UK", "London", "S001", "P001", 150, "2023-12-01")
    )
    val salesDf = spark.createDataFrame(salesData).toDF(columnsCustom: _*)

    println("\nOriginal data:")
    salesDf.show(truncate = false)

    println("\nGenerating composite key from country + city + store_id + product_id:")
    val salesWithKey = salesDf.withColumn("record_key", 
      concat_ws(":", col("country"), col("city"), col("store_id"), col("product_id")))

    println("\nData with generated key:")
    salesWithKey.select("record_key", "country", "city", "store_id", "product_id", "sales")
      .show(truncate = false)

    salesWithKey.write.format("hudi")
      .option("hoodie.table.name", tableName4)
      .option("hoodie.datasource.write.recordkey.field", "record_key")
      .option("hoodie.datasource.write.precombine.field", "date")
      .option("hoodie.datasource.write.partitionpath.field", "country")
      .mode(SaveMode.Overwrite)
      .save(basePath4)

    val customSnapshot = spark.read.format("hudi").load(basePath4)
    println("\n✓ Custom keys generated:")
    customSnapshot.select("record_key", "_hoodie_record_key", "sales")
      .show(truncate = false)

    println("\n=== PART 5: BEST PRACTICES SUMMARY ===")
    
    println("\n1. Record Key Selection:")
    println("   ✓ Use natural business keys when available")
    println("   ✓ Use UUIDs for surrogate keys")
    println("   ✓ Use composite keys when single field isn't unique")
    println("   ✓ Ensure keys are immutable (never change)")
    println("   ✓ Never allow null values in key fields")

    println("\n2. Precombine Field Selection:")
    println("   ✓ Use event timestamp for event-time processing")
    println("   ✓ Use database update timestamp for CDC")
    println("   ✓ Use version numbers for versioned data")
    println("   ✓ Ensure monotonically increasing values")
    println("   ✓ Never allow null values")

    println("\n3. Composite Key Guidelines:")
    println("   ✓ Order matters: tenant_id,sensor_id ≠ sensor_id,tenant_id")
    println("   ✓ Use comma separator: \"field1,field2,field3\"")
    println("   ✓ All component fields must be non-null")
    println("   ✓ Document key structure clearly")
    println("   ✓ Consider cardinality and distribution")

    println("\n4. Data Quality Checks:")
    println("   ✓ Validate no nulls in record key fields")
    println("   ✓ Validate no nulls in precombine field")
    println("   ✓ Check for duplicate keys with same precombine value")
    println("   ✓ Monitor key distribution for skew")

    println("\n=== PART 6: VALIDATION EXAMPLE ===")
    
    println("\nValidating data quality before Hudi write:")
    
    def validateHudiData(df: org.apache.spark.sql.DataFrame, 
                         recordKeyFields: Seq[String], 
                         precombineField: String): Boolean = {
      
      println(s"\n  Validating record keys: ${recordKeyFields.mkString(", ")}")
      println(s"  Validating precombine field: $precombineField")
      
      // Check for nulls in record key fields
      val nullKeyCount = recordKeyFields.map { field =>
        df.filter(col(field).isNull).count()
      }.sum
      
      if (nullKeyCount > 0) {
        println(s"  ❌ Found $nullKeyCount null values in record key fields")
        return false
      }
      println("  ✓ No nulls in record key fields")
      
      // Check for nulls in precombine field
      val nullPrecombine = df.filter(col(precombineField).isNull).count()
      if (nullPrecombine > 0) {
        println(s"  ❌ Found $nullPrecombine null values in precombine field")
        return false
      }
      println("  ✓ No nulls in precombine field")
      
      // Check for duplicate keys with same precombine value
      val keyColumns = recordKeyFields.map(col) :+ col(precombineField)
      val duplicates = df.groupBy(keyColumns: _*).count().filter("count > 1")
      
      if (duplicates.count() > 0) {
        println("  ⚠️  Found duplicate keys with same precombine value:")
        duplicates.show()
      } else {
        println("  ✓ No duplicate keys with same precombine value")
      }
      
      println("  ✓ Validation passed")
      true
    }

    // Example validation
    validateHudiData(
      sensorDf1, 
      Seq("tenant_id", "sensor_id"), 
      "timestamp"
    )

    println("\n=== PART 7: TROUBLESHOOTING ===")
    println("\nCommon issues and solutions:")
    println("\n1. Unexpected duplicates:")
    println("   → Check if record key is truly unique")
    println("   → Verify composite key includes all necessary fields")
    println("   → Check for nulls in key fields")

    println("\n2. Updates not working:")
    println("   → Verify record key matches exactly")
    println("   → Check if precombine value is actually newer")
    println("   → Confirm same key fields used across writes")

    println("\n3. Late data not handled correctly:")
    println("   → Verify precombine field represents event time")
    println("   → Check precombine values are comparable")
    println("   → Ensure precombine field never null")

    println("\n" + "=" * 90)
    println("✓ Examples demonstrated: Composite keys, null handling, concurrent updates")
    println("=" * 90)

    spark.stop()
  }
}
