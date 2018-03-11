package net.zengzhiying

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

/**
 * spark streaming 使用 direct API直接操作底层元数据
 * 这样丢数据的可能性比较小
 */
object KafkaDirectDemo {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
                    .setAppName("KafkaDirectDemo")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val topics = "streamingTest"
    val groupId = "streaming_test"
    val brokers = "bigdata1:9092,bigdata2:9092,bigdata3:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> brokers,
        "group.id" -> groupId//,
        // "auto.offset.reset" -> "largest"  默认largest:最新/smallest:最早
        )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet)
    messages.map(_._2).foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        println("rdd长度: " + rdd.count())
        rdd.foreach(msg => {
          println("msg: " + msg)
          // 对msg做各种分布式处理
        })
      }
    })
    
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()
    // 开始流计算
    ssc.start()
    ssc.awaitTermination()
  }
}