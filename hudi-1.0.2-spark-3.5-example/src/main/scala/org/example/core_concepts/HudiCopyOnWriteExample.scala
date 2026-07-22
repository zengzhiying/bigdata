package org.example.core_concepts

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object HudiCopyOnWriteExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Hudi Copy-on-Write Table Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val tableName = "cow_trips_table"
    val basePath = "file:///tmp/cow_trips_table"

    val columns = Seq("ts", "uuid", "rider", "driver", "fare", "city")
    val data = Seq(
      (1695159649087L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 19.10, "san_francisco"),
      (1695091554788L, "e96c4396-3fad-413a-a942-4cb36106d721", "rider-C", "driver-M", 27.70, "san_francisco"),
      (1695046462179L, "9909a8b1-2d15-4d3d-8ec9-efc48c536a00", "rider-D", "driver-L", 33.90, "san_francisco"),
      (1695516137016L, "e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-F", "driver-P", 34.15, "sao_paulo"),
      (1695115999911L, "c8abbe79-8d89-47ea-b4ce-4d224bae5bfa", "rider-J", "driver-T", 17.85, "chennai")
    )

    val insertDf = spark.createDataFrame(data).toDF(columns: _*)

    println("=== 1. Insert data into COW table ===")
    insertDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val readDf = spark.read.format("hudi").load(basePath)
    println("Initial data count: " + readDf.count())
    readDf.select("uuid", "rider", "driver", "fare", "city").show()

    println("\n=== 2. Update data in COW table ===")
    val updateData = Seq(
      (1695159649999L, "334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "driver-K", 99.99, "san_francisco")
    )
    val updateDf = spark.createDataFrame(updateData).toDF(columns: _*)

    updateDf.write.format("hudi")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .mode(SaveMode.Append)
      .save(basePath)

    val afterUpdateDf = spark.read.format("hudi").load(basePath)
    println("After update - rider-A's fare:")
    afterUpdateDf.filter("rider = 'rider-A'").select("uuid", "rider", "fare", "ts").show()

    println("\n=== 3. COW Table Characteristics ===")
    println("- Write operation: Rewrites entire data file on update")
    println("- Read performance: Fast, reads columnar Parquet directly")
    println("- Write performance: Slower due to full file rewrite")
    println("- Use case: Read-heavy workloads with less frequent updates")
    println("- File size: Larger due to duplicate data during updates")

    println("\n=== 4. Check file structure ===")
    spark.read.format("hudi").load(basePath)
      .select("_hoodie_commit_time", "_hoodie_file_name", "rider", "fare")
      .show(truncate = false)

    spark.stop()
  }
}
