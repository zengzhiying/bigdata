package org.example.indexing

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiBloomFilterIndexExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Bloom Filter Index Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    println("=" * 90)
    println("HUDI BLOOM FILTER INDEX - ADVANCED EXAMPLE")
    println("=" * 90)
    println("Bloom Filter: Space-efficient probabilistic data structure for membership testing")
    println("=" * 90)

    println("\n=== 1. What is a Bloom Filter? ===")
    println("A Bloom filter is a probabilistic data structure that can:")
    println("  • Definitively say: 'This key is NOT in the file' (100% accurate)")
    println("  • Probably say: 'This key MIGHT be in the file' (with false positives)")
    println()
    println("Advantages:")
    println("  ✓ Very space-efficient (few KB per file)")
    println("  ✓ Fast lookup (O(1))")
    println("  ✓ Stored in file footer (no external storage)")
    println()
    println("Trade-off:")
    println("  ✗ May have false positives (reads unnecessary files)")
    println("  ✓ Never false negatives (won't miss the right file)")

    val tableName = "bloom_index_table"
    val basePath = "file:///tmp/bloom_index_table"
    val columns = Seq("order_id", "customer_id", "product", "quantity", "price", "order_date", "updated_ts")

    println("\n=== 2. Creating Table with Bloom Filter Index ===")
    
    val initialData = Seq(
      ("ORD-00001", "CUST-001", "Laptop", 1, 1200.00, "2023-12-01", 1701388800000L),
      ("ORD-00002", "CUST-002", "Mouse", 2, 25.00, "2023-12-01", 1701388800000L),
      ("ORD-00003", "CUST-003", "Keyboard", 1, 80.00, "2023-12-01", 1701388800000L),
      ("ORD-00004", "CUST-001", "Monitor", 2, 300.00, "2023-12-02", 1701475200000L),
      ("ORD-00005", "CUST-004", "Headset", 1, 150.00, "2023-12-02", 1701475200000L),
      ("ORD-00006", "CUST-002", "Webcam", 1, 100.00, "2023-12-03", 1701561600000L),
      ("ORD-00007", "CUST-005", "Desk", 1, 400.00, "2023-12-03", 1701561600000L),
      ("ORD-00008", "CUST-003", "Chair", 1, 250.00, "2023-12-04", 1701648000000L)
    )

    val initialDf = spark.createDataFrame(initialData).toDF(columns: _*)

    println("\nInitial dataset (8 orders):")
    initialDf.show(truncate = false)

    println("\nConfiguring Bloom Filter Index:")
    println("  • hoodie.index.type = BLOOM")
    println("  • hoodie.bloom.index.parallelism = 4")
    println("  • hoodie.bloom.index.filter.type = DYNAMIC_V0 (default)")

    val writeStart = System.currentTimeMillis()
    
    initialDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      
      // Bloom Filter Index Configuration
      .option("hoodie.index.type", "BLOOM")
      .option("hoodie.bloom.index.parallelism", "4")
      .option("hoodie.bloom.index.filter.type", "DYNAMIC_V0")
      
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val writeTime = System.currentTimeMillis() - writeStart
    println(s"\n✓ Initial write with Bloom index completed in ${writeTime}ms")

    println("\n=== 3. Bloom Filter in Action: Upsert ===")
    println("Updating order ORD-00003 - Bloom filter helps locate file quickly")

    val updateData = Seq(
      ("ORD-00003", "CUST-003", "Keyboard", 2, 80.00, "2023-12-01", 1701648100000L)
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    println("\nUpdate process with Bloom Filter:")
    println("  1. Check partition: order_date = '2023-12-01'")
    println("  2. Read Bloom filters from all files in that partition")
    println("  3. Bloom filter test: Does file X contain 'ORD-00003'?")
    println("     • If NO → Skip this file (100% certain)")
    println("     • If MAYBE → Read and check this file")
    println("  4. Update record in identified file")

    val upsertStart = System.currentTimeMillis()
    
    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      .option("hoodie.index.type", "BLOOM")
      .mode(SaveMode.Append)
      .save(basePath)

    val upsertTime = System.currentTimeMillis() - upsertStart
    println(s"\n✓ Upsert with Bloom filter completed in ${upsertTime}ms")

    val snapshot = spark.read.format("hudi").load(basePath)
    println("\nVerifying update:")
    snapshot.filter("order_id = 'ORD-00003'")
      .select("order_id", "product", "quantity", "updated_ts")
      .show(truncate = false)

    println("\n=== 4. Bloom Filter Configuration Options ===")
    println("\n┌───────────────────────────────────┬──────────────────┬─────────────────┐")
    println("│ Parameter                         │ Default          │ Description     │")
    println("├───────────────────────────────────┼──────────────────┼─────────────────┤")
    println("│ hoodie.index.type                 │ SIMPLE           │ Use BLOOM       │")
    println("│ hoodie.bloom.index.parallelism    │ 0 (auto)         │ Lookup parallel │")
    println("│ hoodie.bloom.index.filter.type    │ DYNAMIC_V0       │ Filter type     │")
    println("│ hoodie.bloom.index.prune.by.ranges│ true             │ Range pruning   │")
    println("│ hoodie.bloom.index.use.caching    │ true             │ Cache filters   │")
    println("└───────────────────────────────────┴──────────────────┴─────────────────┘")

    println("\n=== 5. Bloom Filter Types ===")
    println("\n1. SIMPLE Bloom Filter:")
    println("   • Fixed size")
    println("   • May have higher false positive rate")
    
    println("\n2. DYNAMIC_V0 Bloom Filter (Recommended):")
    println("   • Dynamically sized based on record count")
    println("   • Lower false positive rate")
    println("   • Better for varying file sizes")

    println("\n=== 6. False Positive Rate ===")
    println("Configuration for false positive rate:")
    println("  .option(\"hoodie.bloom.index.filter.type\", \"DYNAMIC_V0\")")
    println("  .option(\"hoodie.index.bloom.fpp\", \"0.000001\")  // 0.0001% FPP")
    println()
    println("Lower FPP = Larger bloom filter = More storage but fewer false reads")
    println("Higher FPP = Smaller bloom filter = Less storage but more false reads")
    println()
    println("Default: 0.000001 (very low false positive rate)")

    println("\n=== 7. Performance Comparison: SIMPLE vs BLOOM ===")
    
    val simpleTablePath = "file:///tmp/simple_index_perf_table"
    val bloomTablePath = "file:///tmp/bloom_index_perf_table"

    println("\nCreating comparison tables with 1000 records...")
    val perfData = (1 to 1000).map { i =>
      (f"ORD-$i%05d", f"CUST-${i % 100}%03d", s"Product-$i", 1, i * 10.0, s"2023-12-${(i % 30) + 1}", System.currentTimeMillis())
    }
    val perfDf = spark.createDataFrame(perfData).toDF(columns: _*)

    // SIMPLE Index
    val simpleWriteStart = System.currentTimeMillis()
    perfDf.write.format("hudi")
      .option("hoodie.table.name", "simple_perf_table")
      .option("hoodie.index.type", "SIMPLE")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      .mode(SaveMode.Overwrite)
      .save(simpleTablePath)
    val simpleWriteTime = System.currentTimeMillis() - simpleWriteStart

    // BLOOM Index
    val bloomWriteStart = System.currentTimeMillis()
    perfDf.write.format("hudi")
      .option("hoodie.table.name", "bloom_perf_table")
      .option("hoodie.index.type", "BLOOM")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      .mode(SaveMode.Overwrite)
      .save(bloomTablePath)
    val bloomWriteTime = System.currentTimeMillis() - bloomWriteStart

    println(s"\nWrite Performance:")
    println(s"  SIMPLE index: ${simpleWriteTime}ms")
    println(s"  BLOOM index:  ${bloomWriteTime}ms")

    // Upsert Performance
    val upsertTestData = spark.createDataFrame(Seq(
      ("ORD-00500", "CUST-050", "Product-500-Updated", 2, 10000.0, "2023-12-17", System.currentTimeMillis())
    )).toDF(columns: _*)

    val simpleUpsertStart = System.currentTimeMillis()
    upsertTestData.write.format("hudi")
      .option("hoodie.table.name", "simple_perf_table")
      .option("hoodie.index.type", "SIMPLE")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      .mode(SaveMode.Append)
      .save(simpleTablePath)
    val simpleUpsertTime = System.currentTimeMillis() - simpleUpsertStart

    val bloomUpsertStart = System.currentTimeMillis()
    upsertTestData.write.format("hudi")
      .option("hoodie.table.name", "bloom_perf_table")
      .option("hoodie.index.type", "BLOOM")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "order_id")
      .option("hoodie.datasource.write.precombine.field", "updated_ts")
      .option("hoodie.datasource.write.partitionpath.field", "order_date")
      .mode(SaveMode.Append)
      .save(bloomTablePath)
    val bloomUpsertTime = System.currentTimeMillis() - bloomUpsertStart

    println(s"\nUpsert Performance:")
    println(s"  SIMPLE index: ${simpleUpsertTime}ms")
    println(s"  BLOOM index:  ${bloomUpsertTime}ms")
    println(s"  Performance gain: ${(simpleUpsertTime.toDouble / bloomUpsertTime.toDouble)}x faster")

    println("\n=== 8. When to Use Bloom Filter Index ===")
    println("\n✅ Use BLOOM Index when:")
    println("  • Dataset > 100GB")
    println("  • Frequent upsert operations")
    println("  • Many files per partition")
    println("  • Need predictable performance")
    println("  • Production workloads")

    println("\n⚠️  Consider alternatives when:")
    println("  • Very small datasets (< 10GB) → SIMPLE is fine")
    println("  • Need cross-partition updates → GLOBAL_BLOOM")
    println("  • Extremely large scale (> 10TB) → Consider HBASE index")

    println("\n=== 9. Bloom Filter Best Practices ===")
    println("✓ Use DYNAMIC_V0 filter type (default)")
    println("✓ Keep false positive rate low (default 0.000001)")
    println("✓ Set appropriate parallelism based on cluster")
    println("✓ Enable range pruning for better performance")
    println("✓ Monitor false positive rates in production")
    println("✓ Combine with appropriate file sizing")

    println("\n=== 10. Advanced Configuration ===")
    println("\nOptimal configuration for large-scale production:")
    println("""
      |df.write.format("hudi")
      |  .option("hoodie.index.type", "BLOOM")
      |  .option("hoodie.bloom.index.filter.type", "DYNAMIC_V0")
      |  .option("hoodie.index.bloom.fpp", "0.000001")
      |  .option("hoodie.bloom.index.parallelism", "200")
      |  .option("hoodie.bloom.index.prune.by.ranges", "true")
      |  .option("hoodie.bloom.index.use.caching", "true")
      |  .save(path)
    """.stripMargin)

    println("\n=== 11. Troubleshooting Bloom Index ===")
    println("\n❌ Issue: Upserts still slow with Bloom index")
    println("   → Check: Are files too large? (target 128-512MB)")
    println("   → Check: Is parallelism too low?")
    println("   → Check: Monitor false positive rate")

    println("\n❌ Issue: High memory usage")
    println("   → Reduce bloom.index.parallelism")
    println("   → Disable use.caching if memory-constrained")

    println("\n❌ Issue: Too many false positives")
    println("   → Lower hoodie.index.bloom.fpp value")
    println("   → Use DYNAMIC_V0 filter type")

    println("\n=== 12. Viewing Bloom Filter Metadata ===")
    println("\nBloom filters are stored in file footers")
    println("Each Parquet file contains:")
    println("  • Bloom filter for record keys in that file")
    println("  • Min/max values for range pruning")
    println("  • File-level statistics")

    println("\n" + "=" * 90)
    println("✓ Bloom Filter Index demonstrated with performance comparison")
    println("=" * 90)

    spark.stop()
  }
}
