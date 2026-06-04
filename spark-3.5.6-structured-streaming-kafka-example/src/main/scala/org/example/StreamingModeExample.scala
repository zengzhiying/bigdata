package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, window}

object StreamingModeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark structured streaming streaming read")
      .master("local")
      .getOrCreate()
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "10")
      // 同一个触发间隔内超过 2 条会触发一次计算
      .option("minOffsetsPerTrigger", "2")
      .option("maxTriggerDelay", "1m")
      .option("includeHeaders", "true")
      .load()
    // 每触发一次就会执行一次聚合
//    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "topic")
//      .groupBy("topic", "partition").agg(max("offset").as("max_offset"))
    // 窗口聚合，同样是每触发一次都会生成一次聚合，聚合结果中会带有窗口，每次聚合都会在之前所有聚合的基础上合并结果，也就是会保留之前的聚合状态
    import spark.implicits._
    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "topic", "timestamp")
      .groupBy(window($"timestamp", "1 minutes"), $"topic", $"partition").agg(max("offset").as("max_offset"))

    val query = df1.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    val df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset", "topic", "timestamp")
    // 非聚合输出情况下是增量执行，即每触发一次只执行增量的数据，不需要状态存储
    val query2 = df2.writeStream
      .outputMode("append")
      .format("console")
      // 分布式环境要设置 checkpoint，默认位置在 /tmp 下，保存消费、聚合状态等信息
      .option("checkpointLocation", "s3a://path/to/checkpoint/dir")
      .start()
    query.awaitTermination()
    query2.awaitTermination()
  }
}
