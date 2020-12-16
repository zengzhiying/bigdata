package com.monchickey

import com.mongodb.spark.MongoSpark
import net.zengzhiying.Util
import org.apache.spark.sql.SparkSession

object MongoReadExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("spark://bigdata1:7077")
//      .master("local")
      .appName("MongoReadExample")
      .config("spark.mongodb.input.uri", "mongodb://192.168.1.29:27018/db1.tb1")
//      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.characters")
      .getOrCreate()

    println("start.")
    val df = MongoSpark.load(sparkSession)
    df.printSchema()
    df.show()


//    val n = df.count()
//    println("total count: " + n)
    val page = df.select("_id", "col1", "col_count", "num", "row_count").limit(10)
    println("count: " + page.count())
    page.show()


    val row = page.first()
    println(row.prettyJson)
    val objId = row.getString(0)
    println("_id: " + objId)
    val col1 = row.getAs[Array[Byte]]("col1")
//    println(col1)
    Util.byteToNum(col1)
    val count = row.getAs[Int]("row_count")
    val num = row.getInt(3)
    println("count: " + count + " num: " + num)


    df.foreachPartition(partition => {
      var partitionCount: Int = 0
      partition.foreach(row => {
        partitionCount += 1
      })
      println("partition count: " + partitionCount)
    })
    println("end.")
  }

}
