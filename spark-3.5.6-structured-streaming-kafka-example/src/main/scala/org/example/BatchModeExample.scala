package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchModeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark structured streaming batch read")
      .master("local")
      .getOrCreate()
    // 每次运行都会从 earliest 读取到 latest，不会产生消费者组
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("includeHeaders", "true")
      // 这种模式下设置消费者组并没有作用
      // .option("kafka.group.id", "spark-batch-read-group")
      .load()
    df.show()
    val selectDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic")
    selectDf.show()

    // 指定起始偏移运行
    val startOffset =
      """{"testMessage":
        |{"0": 2}}""".stripMargin
    val df1 = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", startOffset)
      .option("endingOffsets", "latest")
      .option("includeHeaders", "true")
      .load()
    df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "topic").show()

    // 拿到目前最新的偏移量
    df1.groupBy("topic", "partition").agg(max("offset").as("max_offset")).show()

    // 指定结束时间或偏移
    val df2 = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", "earliest")
      // 如果指定的时间查询不到偏移，那么会自动读取到最新的
      // .option("endingTimestamp", String.valueOf(System.currentTimeMillis()))
      // 如果指定分区偏移，当前的最新偏移达不到会一直阻塞直到读到最新的偏移为止
      // 例如指定 21，则读取到 20 停止
      .option("endingOffsets", """{"testMessage": {"0": 21}}""")
      .option("includeHeaders", "true")
      .load()
    df2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "topic").show()
  }
}