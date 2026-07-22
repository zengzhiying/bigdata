package org.example.indexing

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiIndexBasicExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Index Basic Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    println("=" * 90)
    println("HUDI INDEX - BASIC CONCEPTS")
    println("=" * 90)
    println("Index: Speeds up record lookup by mapping Record Key to file location")
    println("=" * 90)

    println("\n=== 1. Understanding Hudi Index ===")
    println("Hudi uses an index to quickly find which file contains a record with a given key")
    println("Without index: Must scan ALL files to find a record → Slow")
    println("With index: Direct lookup to specific file → Fast")
    println()
    println("Index Workflow:")
    println("  1. Upsert operation arrives with Record Key = 'U001'")
    println("  2. Index lookup: Which file contains 'U001'?")
    println("  3. Read only that file, update record, write back")
    println("  4. Update index: 'U001' now in new file location")

    val tableName = "index_basic_table"
    val basePath = "file:///tmp/index_basic_table"
    val columns = Seq("user_id", "name", "email", "city", "balance", "updated_at")

    println("\n=== 2. Default Index: SIMPLE (Spark-based) ===")
    println("SIMPLE index uses Spark join to locate records")
    println("• Good for small to medium datasets")
    println("• No external dependencies")
    println("• Index stored in memory during operation")

    val initialData = Seq(
      ("U001", "Alice Johnson", "alice@example.com", "San Francisco", 1000.0, 1701388800000L),
      ("U002", "Bob Smith", "bob@example.com", "New York", 1500.0, 1701388800000L),
      ("U003", "Carol White", "carol@example.com", "Chicago", 2000.0, 1701388800000L),
      ("U004", "David Brown", "david@example.com", "Boston", 2500.0, 1701388800000L),
      ("U005", "Eve Davis", "eve@example.com", "Seattle", 3000.0, 1701388800000L)
    )

    val initialDf = spark.createDataFrame(initialData).toDF(columns: _*)

    println("\nInitial data (5 users):")
    initialDf.show(truncate = false)

    println("\nWriting with default SIMPLE index...")
    val writeStart = System.currentTimeMillis()
    
    initialDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      // SIMPLE index is default, but we specify it explicitly for clarity
      .option("hoodie.index.type", "SIMPLE")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val writeTime = System.currentTimeMillis() - writeStart
    println(s"✓ Initial write completed in ${writeTime}ms")

    println("\n=== 3. Index in Action: Upsert Operation ===")
    println("Updating user U002 - Index will locate the file quickly")

    val updateData = Seq(
      ("U002", "Bob Smith Updated", "bob.new@example.com", "New York", 1800.0, 1701475200000L)
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    println("\nUpdate data:")
    updateDf.show(truncate = false)

    println("\nUpsert process with index:")
    println("  1. Index lookup: user_id='U002' → found in partition city=New York, file XYZ")
    println("  2. Read only that specific file")
    println("  3. Update the record")
    println("  4. Write updated file")
    
    val upsertStart = System.currentTimeMillis()
    
    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "SIMPLE")
      .mode(SaveMode.Append)
      .save(basePath)

    val upsertTime = System.currentTimeMillis() - upsertStart
    println(s"\n✓ Upsert completed in ${upsertTime}ms (fast due to index lookup)")

    val snapshot = spark.read.format("hudi").load(basePath)
    println("\nVerifying update:")
    snapshot.filter("user_id = 'U002'")
      .select("user_id", "name", "email", "balance")
      .show(truncate = false)

    println("\n=== 4. Available Index Types ===")
    println("\n┌─────────────────┬────────────────────┬───────────────┬──────────────────┐")
    println("│ Index Type      │ Storage            │ Performance   │ Use Case         │")
    println("├─────────────────┼────────────────────┼───────────────┼──────────────────┤")
    println("│ SIMPLE          │ In-memory (Spark)  │ Good          │ Small datasets   │")
    println("│ BLOOM           │ File footer        │ Very Good     │ Large datasets   │")
    println("│ GLOBAL_SIMPLE   │ In-memory (Spark)  │ Good          │ Non-partitioned  │")
    println("│ GLOBAL_BLOOM    │ File footer        │ Very Good     │ Non-partitioned  │")
    println("│ HBASE           │ External HBase     │ Excellent     │ Very large data  │")
    println("│ INMEMORY        │ In-memory          │ Excellent     │ Testing only     │")
    println("└─────────────────┴────────────────────┴───────────────┴──────────────────┘")

    println("\n=== 5. Index Configuration Parameters ===")
    println("\nKey parameters:")
    println("  hoodie.index.type = SIMPLE | BLOOM | GLOBAL_SIMPLE | GLOBAL_BLOOM | HBASE")
    println("  hoodie.bloom.index.parallelism = <number>  (for BLOOM index)")
    println("  hoodie.simple.index.parallelism = <number>  (for SIMPLE index)")

    println("\n=== 6. SIMPLE vs GLOBAL_SIMPLE ===")
    println("\nSIMPLE Index (partition-aware):")
    println("  • Assumes record stays in same partition")
    println("  • Faster for partition-bound data")
    println("  • Example: Records partitioned by country never change country")
    
    println("\nGLOBAL_SIMPLE Index (partition-agnostic):")
    println("  • Can handle partition changes")
    println("  • Slower, scans all partitions")
    println("  • Example: User changes city → moves partitions")

    println("\n=== 7. Testing Partition Change ===")
    println("User U003 moves from Chicago to Los Angeles")

    val partitionChangeData = Seq(
      ("U003", "Carol White", "carol@example.com", "Los Angeles", 2200.0, 1701561600000L)
    )
    val partitionChangeDf = spark.createDataFrame(partitionChangeData).toDF(columns: _*)

    println("\nWith SIMPLE index, old record in Chicago won't be found")
    println("Result: Both records exist (duplicate)")
    
    partitionChangeDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "SIMPLE")  // Partition-aware
      .mode(SaveMode.Append)
      .save(basePath)

    val afterChange = spark.read.format("hudi").load(basePath)
    println("\nRecords for U003 (may have duplicates with SIMPLE index):")
    afterChange.filter("user_id = 'U003'")
      .select("user_id", "name", "city", "balance")
      .show(truncate = false)

    val countU003 = afterChange.filter("user_id = 'U003'").count()
    if (countU003 > 1) {
      println(s"⚠️  Found $countU003 records for U003 (duplicate created)")
      println("Solution: Use GLOBAL_SIMPLE or GLOBAL_BLOOM index for partition changes")
    } else {
      println("✓ No duplicates (partition may not have changed)")
    }

    println("\n=== 8. Index Performance Impact ===")
    println("\nWithout Index (hypothetical):")
    println("  • Must scan ALL files to find records")
    println("  • Upsert time: O(n) where n = total files")
    println("  • Slow for large datasets")

    println("\nWith Index:")
    println("  • Direct lookup to specific file")
    println("  • Upsert time: O(1) or O(log n) depending on index type")
    println("  • Fast even for very large datasets")

    println("\n=== 9. When to Use Which Index ===")
    println("\n📊 SIMPLE / GLOBAL_SIMPLE:")
    println("  ✓ Small to medium datasets (< 100GB)")
    println("  ✓ No external dependencies needed")
    println("  ✓ Good for testing and development")

    println("\n🌸 BLOOM / GLOBAL_BLOOM (Recommended for Production):")
    println("  ✓ Large datasets (> 100GB)")
    println("  ✓ Better performance than SIMPLE")
    println("  ✓ Bloom filter stored in file footer")
    println("  ✓ No external dependencies")

    println("\n🗄️  HBASE:")
    println("  ✓ Very large datasets (> 1TB)")
    println("  ✓ Need consistent upsert performance")
    println("  ✓ Can afford HBase cluster")
    println("  ✓ Multiple writers scenario")

    println("\n=== 10. Index Best Practices ===")
    println("✓ Use BLOOM index for production workloads")
    println("✓ Use GLOBAL_* variants if records can change partitions")
    println("✓ Tune parallelism based on cluster size")
    println("✓ Monitor index lookup performance")
    println("✓ Consider HBASE for very large scale")

    println("\n=== 11. Viewing Index Metadata ===")
    val snapshot2 = spark.read.format("hudi").load(basePath)
    
    println("\nHudi metadata columns (index-related):")
    snapshot2.select("_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name", "user_id")
      .limit(3)
      .show(truncate = false)

    println("\nExplanation:")
    println("  _hoodie_record_key: The indexed key")
    println("  _hoodie_partition_path: Where the record is located")
    println("  _hoodie_file_name: Specific file containing the record")

    println("\n=== 12. Common Index Issues ===")
    println("\n❌ Issue 1: Duplicates after partition change")
    println("   Solution: Use GLOBAL_SIMPLE or GLOBAL_BLOOM index")

    println("\n❌ Issue 2: Slow upsert performance")
    println("   Solution: Switch from SIMPLE to BLOOM index")
    println("   Solution: Increase index parallelism")

    println("\n❌ Issue 3: High memory usage")
    println("   Solution: Use BLOOM index (less memory) or HBASE index")

    println("\n=== 13. Index Configuration Example ===")
    println("\nExample configuration for production:")
    println("""
      |df.write.format("hudi")
      |  .option("hoodie.table.name", "my_table")
      |  .option("hoodie.index.type", "BLOOM")              // Bloom filter index
      |  .option("hoodie.bloom.index.parallelism", "100")   // Parallelism
      |  .option("hoodie.datasource.write.recordkey.field", "id")
      |  .option("hoodie.datasource.write.precombine.field", "ts")
      |  .save(path)
    """.stripMargin)

    println("\n" + "=" * 90)
    println("✓ Basic index concepts demonstrated")
    println("=" * 90)

    spark.stop()
  }
}
