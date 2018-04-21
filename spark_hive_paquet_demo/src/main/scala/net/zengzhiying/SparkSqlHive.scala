package net.zengzhiying

import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * ./bin/spark-submit --class net.zengzhiying.SparkSqlHive --master spark://master_host:7077 --executor-memory 2G --total-executor-cores 4
 * --deploy-mode cluster ./spark_demo.jar parameter 
 */
object SparkSqlHive {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
    val spark = SparkSession
                .builder()
                .appName("spark sql hive")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate()
    import spark.implicits._
    import spark.sql
    
    println("select.")
    sql("USE test_db")
    
    sql("SELECT * FROM test_db.zzy").show();
    
    val queryDate = 20180412
    
    val sqlDF = sql("SELECT * FROM test_db.par where dateid=" + queryDate)
    sqlDF.foreach(f => {
      println(f)
//      println(f.toSeq)
      println(f.toString())
    })
    
    
    // 将dataframe映射至hive
    val records = List(Record(4, "zzy", 20180319), Record(5, "lsl", 20180319), Record(6, "zzylsl", 20180319))
    val recordsDF = spark.createDataFrame(records)
    recordsDF.createOrReplaceTempView("records")
    // 内部数据查询
    println("query records.")
    sql("select id, username from records where dateid=20180319").show()
    
    println("insert hive_records.")
    // 通过创建hive临时表并写入(一般只创建一次即可)
    // sql("CREATE TABLE IF NOT EXISTS temp_records (id int, username string) PARTITIONED BY (dateid int) STORED AS PARQUET")
    // 每次覆盖表, 一般用于实时数据写入临时表, 然后再同步至其他表
    // 注意不能直接以Overwrite的方式写入正式表, Overwrite默认会清除掉之前分区结构, 直接按照当前实体类的方式建表
    // recordsDF.write.mode(SaveMode.Overwrite).saveAsTable("temp_records")
    // sql("INSERT INTO TABLE hive_records SELECT * FROM temp_records")
    // 直接通过 内部表将数据写入hive表(一般也是将实时的dataframe批量数据写入hive)
    sql("INSERT INTO TABLE hive_records SELECT * FROM records")
    
    // 直接追加表的方式不被允许会直接报错.
//    recordsDF.write.mode(SaveMode.Append).saveAsTable("hive_records1")
//    val recordsDF1 = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i", 20180402)))
//    recordsDF1.write.mode(SaveMode.Append).saveAsTable("hive_records1")
    
    spark.stop()
    
  }
}