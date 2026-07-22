package org.example.record_key_precombine

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiRecordKeyBasicExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Record Key Basic Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "record_key_basic_table"
    val basePath = "file:///tmp/record_key_basic_table"

    println("=" * 80)
    println("HUDI RECORD KEY - BASIC EXAMPLE")
    println("=" * 80)
    println("Record Key: The unique identifier for each record in Hudi table")
    println("=" * 80)

    println("\n=== 1. Understanding Record Key ===")
    println("Record Key is Hudi's primary key - it uniquely identifies each record")
    println("• Must be unique across the table")
    println("• Used for upsert operations (insert or update)")
    println("• Can be a single field or composite of multiple fields")
    println("• Critical for deduplication")

    val columns = Seq("user_id", "name", "email", "age", "city", "updated_at")

    println("\n=== 2. Initial Data Insert ===")
    val initialData = Seq(
      ("U001", "Alice Johnson", "alice@example.com", 28, "San Francisco", 1701388800000L),
      ("U002", "Bob Smith", "bob@example.com", 35, "New York", 1701388800000L),
      ("U003", "Carol White", "carol@example.com", 42, "Chicago", 1701388800000L),
      ("U004", "David Brown", "david@example.com", 31, "Boston", 1701388800000L)
    )

    val initialDf = spark.createDataFrame(initialData).toDF(columns: _*)

    println("\nInitial dataset:")
    initialDf.show()

    println("\nWriting to Hudi with user_id as Record Key...")
    initialDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "user_id")  // Set Record Key
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val snapshot1 = spark.read.format("hudi").load(basePath)
    println(s"✓ Initial insert complete: ${snapshot1.count()} records")
    snapshot1.select("user_id", "name", "email", "city", "_hoodie_record_key").show(truncate = false)

    println("\n=== 3. Upsert with Existing Record Key (Update) ===")
    println("Attempting to insert record with existing user_id = U001")
    println("Expected: Record will be UPDATED (not duplicated)")

    val updateData = Seq(
      ("U001", "Alice Johnson-Smith", "alice.new@example.com", 29, "San Francisco", 1701475200000L)
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    println("\nData to upsert:")
    updateDf.show()

    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot2 = spark.read.format("hudi").load(basePath)
    println(s"\n✓ After upsert: ${snapshot2.count()} records (still 4, not 5)")
    println("\nNotice U001 was UPDATED, not duplicated:")
    snapshot2.select("user_id", "name", "email", "age").orderBy("user_id").show(truncate = false)

    println("\n=== 4. Insert with New Record Key (Insert) ===")
    println("Inserting record with new user_id = U005")
    println("Expected: New record will be INSERTED")

    val insertData = Seq(
      ("U005", "Eve Davis", "eve@example.com", 27, "Seattle", 1701475200000L)
    )
    val insertDf = spark.createDataFrame(insertData).toDF(columns: _*)

    insertDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    val snapshot3 = spark.read.format("hudi").load(basePath)
    println(s"\n✓ After insert: ${snapshot3.count()} records (now 5)")
    snapshot3.select("user_id", "name", "email", "city").orderBy("user_id").show(truncate = false)

    println("\n=== 5. Hudi Internal Record Key ===")
    println("Hudi stores record key in _hoodie_record_key metadata column")
    
    snapshot3.select("user_id", "_hoodie_record_key", "name")
      .orderBy("user_id")
      .show(truncate = false)

    println("Notice: _hoodie_record_key matches user_id value")

    println("\n=== 6. What Happens Without Record Key? ===")
    println("⚠️  If you don't specify a record key, Hudi will generate one automatically")
    println("⚠️  This prevents proper upsert behavior - all operations become inserts!")

    val noKeyTablePath = "file:///tmp/no_record_key_table"
    
    println("\nWriting WITHOUT explicit record key:")
    val testData = Seq(
      ("T001", "Test User 1", 1701388800000L),
      ("T001", "Test User 1 Updated", 1701475200000L)  // Same ID, should update
    )
    val testDf = spark.createDataFrame(testData).toDF("id", "name", "ts")

    testDf.write.format("hudi")
      .option("hoodie.table.name", "no_key_table")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .mode(SaveMode.Overwrite)
      .save(noKeyTablePath)

    val noKeySnapshot = spark.read.format("hudi").load(noKeyTablePath)
    println(s"\nResult: ${noKeySnapshot.count()} records (duplicates created!)")
    noKeySnapshot.select("id", "name", "_hoodie_record_key").show(truncate = false)
    println("⚠️  Notice: Auto-generated record keys are different, causing duplicates")

    println("\n=== 7. Record Key Best Practices ===")
    println("✓ Always explicitly set hoodie.datasource.write.recordkey.field")
    println("✓ Choose a field that is truly unique (e.g., UUID, user_id)")
    println("✓ Ensure record key values are never null")
    println("✓ Record key should be immutable (doesn't change over time)")
    println("✓ For natural keys, use business identifiers")
    println("✓ For surrogate keys, use UUIDs or auto-generated IDs")

    println("\n=== 8. Validating Record Key Uniqueness ===")
    val recordKeyCounts = snapshot3.groupBy("_hoodie_record_key")
      .agg(count("*").as("count"))
      .filter("count > 1")

    if (recordKeyCounts.count() == 0) {
      println("✓ All record keys are unique")
    } else {
      println("⚠️  Found duplicate record keys:")
      recordKeyCounts.show()
    }

    println("\n=== 9. Record Key Configuration Summary ===")
    println("Key parameter:")
    println("  hoodie.datasource.write.recordkey.field = <field_name>")
    println("\nExample:")
    println("  .option(\"hoodie.datasource.write.recordkey.field\", \"user_id\")")
    println("\nMultiple fields (composite key) - covered in advanced examples:")
    println("  .option(\"hoodie.datasource.write.recordkey.field\", \"field1,field2\")")

    println("\n=== 10. Common Mistakes ===")
    println("❌ Using non-unique fields (e.g., city, age)")
    println("❌ Using fields that can be null")
    println("❌ Using fields that change over time")
    println("❌ Not specifying record key at all")
    println("❌ Changing record key field between writes")

    println("\n✓ Example demonstrated proper record key usage with user_id")

    spark.stop()
  }
}
