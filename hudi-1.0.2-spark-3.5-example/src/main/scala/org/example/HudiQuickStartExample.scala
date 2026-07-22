package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 当前示例参考自官网：https://hudi.apache.org/docs/quick-start-guide/
 */
object HudiQuickStartExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Hudi QuickStart Example")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .getOrCreate()

    val columns = Seq("ts","uuid","rider","driver","fare","city")
    val data = Seq((1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
        (1695091554788L,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
        (1695046462179L,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
        (1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
        (1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai"))

    val tableName = "trips_table"
    val localPath = "file:///tmp/trips_table"

    val insertDf = spark.createDataFrame(data).toDF(columns: _*)

    insertDf.write.format("hudi")
      // 设置分区
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.table.name", tableName)
      .mode(SaveMode.Overwrite)
      .save(localPath)

    // 查询结果
    val tripsDf = spark.read.format("hudi")
      .load(localPath)

    tripsDf.createOrReplaceTempView(tableName)

    spark.sql("SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0").show()
    spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM trips_table").show()
    spark.sql("SELECT * FROM trips_table limit 1").show()

    val updateDf = spark.sql("SELECT * FROM trips_table WHERE rider = 'rider-D'")
      .withColumn("fare", col("fare") * 10)

    // 基于查询的结果更新表 更新后底层会追加一个新的 Parquet 文件
    updateDf.write.format("hudi")
      .option("hoodie.datasource.write.operation", "upsert")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.table.name", tableName)
      .mode(SaveMode.Append)
      .save(localPath)

    // 更新后需要刷新元数据后才可以查询，否则结果仍然是旧的
    //spark.sql("SELECT * FROM trips_table").show()
    // 重新加载 Hudi 数据查询更新
    val tripsUpdateDf = spark.read.format("hudi")
      .load(localPath)
    tripsUpdateDf.createOrReplaceTempView(tableName)
    spark.sql("SELECT * FROM trips_table").show()

    // 删除数据，删除后同样底层会追加一个新的 Parquet 文件
    val deleteDf = spark.sql("SELECT * FROM trips_table WHERE rider = 'rider-F'")
    deleteDf.write.format("hudi")
      .option("hoodie.datasource.write.operation", "delete")
      .option("hoodie.datasource.write.partitionpath.field", "city")
      .option("hoodie.table.name", tableName)
      .mode(SaveMode.Append)
      .save(localPath)

    // 重新查询
    val tripsDeleteDf = spark.read.format("hudi")
      .load(localPath)
    tripsDeleteDf.show()

    spark.stop()
  }
}
