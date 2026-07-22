package org.example.indexing

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiGlobalVsLocalIndexExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Global vs Local Index Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    println("=" * 90)
    println("HUDI INDEX: GLOBAL vs LOCAL COMPARISON")
    println("=" * 90)
    println("Understanding partition-aware (local) vs partition-agnostic (global) indexes")
    println("=" * 90)

    val columns = Seq("user_id", "name", "email", "city", "status", "updated_at")

    println("\n=== 1. Understanding Index Scopes ===")
    println("\n📍 LOCAL Index (SIMPLE, BLOOM):")
    println("  • Partition-AWARE: Assumes records stay in same partition")
    println("  • Searches only within the target partition")
    println("  • Faster performance")
    println("  • Cannot handle partition changes")
    
    println("\n🌍 GLOBAL Index (GLOBAL_SIMPLE, GLOBAL_BLOOM):")
    println("  • Partition-AGNOSTIC: Records can move between partitions")
    println("  • Searches across ALL partitions")
    println("  • Slower performance")
    println("  • Handles partition changes correctly")

    println("\n=== 2. Scenario Setup: User Management System ===")
    println("Users partitioned by 'city'")
    println("Problem: What if a user moves to a different city?")

    val initialData = Seq(
      ("U001", "Alice Johnson", "alice@example.com", "San Francisco", "active", 1701388800000L),
      ("U002", "Bob Smith", "bob@example.com", "New York", "active", 1701388800000L),
      ("U003", "Carol White", "carol@example.com", "Chicago", "active", 1701388800000L),
      ("U004", "David Brown", "david@example.com", "Boston", "active", 1701388800000L),
      ("U005", "Eve Davis", "eve@example.com", "Seattle", "active", 1701388800000L)
    )
    val initialDf = spark.createDataFrame(initialData).toDF(columns: _*)

    println("\nInitial users:")
    initialDf.show(truncate = false)

    println("\n=== 3. Test Case 1: LOCAL Index (SIMPLE) ===")
    val localTablePath = "file:///tmp/local_index_table"
    
    println("\nCreating table with LOCAL index (partition-aware)...")
    initialDf.write.format("hudi")
      .option("hoodie.table.name", "local_index_table")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "SIMPLE")  // LOCAL (partition-aware)
      .mode(SaveMode.Overwrite)
      .save(localTablePath)

    println("✓ Table created with LOCAL index")

    println("\n--- Attempting to move user U003 from Chicago to Los Angeles ---")
    val moveUserLocal = Seq(
      ("U003", "Carol White", "carol@example.com", "Los Angeles", "active", 1701475200000L)
    )
    val moveUserLocalDf = spark.createDataFrame(moveUserLocal).toDF(columns: _*)

    println("\nUpdate data (city changed):")
    moveUserLocalDf.show(truncate = false)

    println("\nWith LOCAL index:")
    println("  1. Index looks in partition city='Los Angeles' (new partition)")
    println("  2. Doesn't find U003 (it's in Chicago partition)")
    println("  3. Treats as INSERT → Creates new record")
    println("  4. Old record in Chicago partition remains")
    println("  5. Result: DUPLICATE records!")

    moveUserLocalDf.write.format("hudi")
      .option("hoodie.table.name", "local_index_table")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "SIMPLE")
      .mode(SaveMode.Append)
      .save(localTablePath)

    val localResult = spark.read.format("hudi").load(localTablePath)
    val u003Count = localResult.filter("user_id = 'U003'").count()
    
    println(s"\n❌ Result with LOCAL index:")
    println(s"   Records for U003: $u003Count")
    localResult.filter("user_id = 'U003'")
      .select("user_id", "name", "city", "updated_at")
      .orderBy("city")
      .show(truncate = false)

    if (u003Count > 1) {
      println("⚠️  DUPLICATE detected! User exists in both Chicago and Los Angeles")
    }

    println("\n=== 4. Test Case 2: GLOBAL Index (GLOBAL_SIMPLE) ===")
    val globalTablePath = "file:///tmp/global_index_table"
    
    println("\nCreating table with GLOBAL index (partition-agnostic)...")
    initialDf.write.format("hudi")
      .option("hoodie.table.name", "global_index_table")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "GLOBAL_SIMPLE")  // GLOBAL (partition-agnostic)
      .mode(SaveMode.Overwrite)
      .save(globalTablePath)

    println("✓ Table created with GLOBAL index")

    println("\n--- Attempting to move user U003 from Chicago to Los Angeles ---")
    val moveUserGlobal = Seq(
      ("U003", "Carol White", "carol@example.com", "Los Angeles", "active", 1701475200000L)
    )
    val moveUserGlobalDf = spark.createDataFrame(moveUserGlobal).toDF(columns: _*)

    println("\nWith GLOBAL index:")
    println("  1. Index searches ALL partitions for U003")
    println("  2. Finds U003 in Chicago partition")
    println("  3. Deletes old record from Chicago")
    println("  4. Inserts updated record in Los Angeles")
    println("  5. Result: NO duplicates, clean move")

    moveUserGlobalDf.write.format("hudi")
      .option("hoodie.table.name", "global_index_table")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.index.type", "GLOBAL_SIMPLE")
      .mode(SaveMode.Append)
      .save(globalTablePath)

    val globalResult = spark.read.format("hudi").load(globalTablePath)
    val u003GlobalCount = globalResult.filter("user_id = 'U003'").count()
    
    println(s"\n✅ Result with GLOBAL index:")
    println(s"   Records for U003: $u003GlobalCount")
    globalResult.filter("user_id = 'U003'")
      .select("user_id", "name", "city", "updated_at")
      .show(truncate = false)

    if (u003GlobalCount == 1) {
      println("✓ No duplicates! User correctly moved to Los Angeles")
    }

    println("\n=== 5. Complete Comparison ===")
    println("\nAll users in LOCAL index table:")
    localResult.select("user_id", "name", "city")
      .orderBy("user_id", "city")
      .show(truncate = false)
    println(s"Total records: ${localResult.count()}")

    println("\nAll users in GLOBAL index table:")
    globalResult.select("user_id", "name", "city")
      .orderBy("user_id", "city")
      .show(truncate = false)
    println(s"Total records: ${globalResult.count()}")

    println("\n=== 6. Index Types Comparison ===")
    println("\n┌──────────────────┬───────────────────┬───────────────────────────────────┐")
    println("│ Index Type       │ Partition Scope   │ Use When                          │")
    println("├──────────────────┼───────────────────┼───────────────────────────────────┤")
    println("│ SIMPLE           │ Local (aware)     │ Records never change partition    │")
    println("│ BLOOM            │ Local (aware)     │ Large data, stable partitions     │")
    println("│ GLOBAL_SIMPLE    │ Global (agnostic) │ Records can change partition      │")
    println("│ GLOBAL_BLOOM     │ Global (agnostic) │ Large data + partition changes    │")
    println("│ HBASE            │ Global (agnostic) │ Very large scale, external index  │")
    println("└──────────────────┴───────────────────┴───────────────────────────────────┘")

    println("\n=== 7. Performance Impact ===")
    println("\nLOCAL Index Performance:")
    println("  ✓ Faster upsert (searches only target partition)")
    println("  ✓ Lower resource usage")
    println("  ✓ Better scalability for large partition counts")

    println("\nGLOBAL Index Performance:")
    println("  ✗ Slower upsert (searches all partitions)")
    println("  ✗ Higher resource usage")
    println("  ✓ Correct behavior for partition changes")
    println("  ✓ No duplicates")

    println("\n=== 8. Real-World Scenarios ===")
    
    println("\n📍 Use LOCAL Index when:")
    println("  ✓ Partition by immutable field (e.g., signup_date, country_code)")
    println("  ✓ Partition field never changes")
    println("  ✓ Records are logically bound to partition")
    println("  ✓ Performance is critical")
    
    println("\nExamples:")
    println("  • Events partitioned by event_date (date never changes)")
    println("  • Logs partitioned by log_date")
    println("  • Transactions partitioned by transaction_date")

    println("\n🌍 Use GLOBAL Index when:")
    println("  ✓ Partition by mutable field (e.g., status, city, category)")
    println("  ✓ Records can move between partitions")
    println("  ✓ Correctness more important than performance")
    println("  ✓ Cannot tolerate duplicates")
    
    println("\nExamples:")
    println("  • Users partitioned by city (user can move)")
    println("  • Orders partitioned by status (status changes)")
    println("  • Products partitioned by category (category can change)")

    println("\n=== 9. Handling Partition Changes with LOCAL Index ===")
    println("\nIf you MUST use LOCAL index but need to handle partition changes:")
    
    println("\nOption 1: Manual delete + insert")
    println("""
      |// Delete from old partition
      |val deleteOld = Seq(("U003", ..., "Chicago", ...))
      |deleteOld.write.format("hudi")
      |  .option("hoodie.datasource.write.operation", "delete")
      |  .save(path)
      |
      |// Insert to new partition
      |val insertNew = Seq(("U003", ..., "Los Angeles", ...))
      |insertNew.write.format("hudi")
      |  .option("hoodie.datasource.write.operation", "insert")
      |  .save(path)
    """.stripMargin)

    println("\nOption 2: Use non-partitioned table")
    println("  .option(\"hoodie.datasource.write.partitionpath.field\", \"\")")

    println("\nOption 3: Switch to GLOBAL index (recommended)")

    println("\n=== 10. BLOOM vs GLOBAL_BLOOM ===")
    val bloomTablePath = "file:///tmp/bloom_local_table"
    val globalBloomTablePath = "file:///tmp/bloom_global_table"

    println("\nCreating BLOOM (local) table...")
    initialDf.write.format("hudi")
      .option("hoodie.table.name", "bloom_local_table")
      .option("hoodie.index.type", "BLOOM")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(bloomTablePath)

    println("\nCreating GLOBAL_BLOOM table...")
    initialDf.write.format("hudi")
      .option("hoodie.table.name", "bloom_global_table")
      .option("hoodie.index.type", "GLOBAL_BLOOM")
      .option("hoodie.datasource.write.recordkey.field", "user_id")
      .option("hoodie.datasource.write.precombine.field", "updated_at")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(globalBloomTablePath)

    println("\n✓ Both BLOOM and GLOBAL_BLOOM tables created")
    println("\nBLOOM (local):")
    println("  • Bloom filter in each partition")
    println("  • Fast lookup within partition")
    println("  • Cannot handle partition changes")

    println("\nGLOBAL_BLOOM:")
    println("  • Bloom filters across all partitions")
    println("  • Slower lookup (checks all partitions)")
    println("  • Handles partition changes correctly")

    println("\n=== 11. Decision Matrix ===")
    println("\n                    Can partition change?")
    println("                           |")
    println("           NO -------------+------------- YES")
    println("            |                               |")
    println("    Small dataset?                  Small dataset?")
    println("    YES → SIMPLE                    YES → GLOBAL_SIMPLE")
    println("    NO  → BLOOM                     NO  → GLOBAL_BLOOM")

    println("\n=== 12. Configuration Examples ===")
    println("\nLOCAL index (partition-aware):")
    println("""
      |.option("hoodie.index.type", "BLOOM")
      |.option("hoodie.datasource.write.partitionpath.field", "date")
    """.stripMargin)

    println("\nGLOBAL index (partition-agnostic):")
    println("""
      |.option("hoodie.index.type", "GLOBAL_BLOOM")
      |.option("hoodie.datasource.write.partitionpath.field", "city")
    """.stripMargin)

    println("\n=== 13. Best Practices ===")
    println("✓ Use LOCAL index for immutable partition fields")
    println("✓ Use GLOBAL index for mutable partition fields")
    println("✓ Prefer BLOOM over SIMPLE for better performance")
    println("✓ Monitor for duplicates if using LOCAL index")
    println("✓ Document partition change expectations")
    println("✓ Test partition change scenarios before production")

    println("\n" + "=" * 90)
    println("✓ GLOBAL vs LOCAL index comparison completed")
    println("=" * 90)

    spark.stop()
  }
}
