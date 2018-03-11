package net.zengzhiying

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.InetAddress
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Minutes
import net.zengzhiying.util.MessageProcessUtil

/**
 * spark streaming 使用默认的 DStream方式处理数据流 属于高级API实现
 * 偏移量保存在zookeeper中
 */
object KafkaStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("KafkaStreamingDemo")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("streaming_checkpoint")
    val topics = "streamingTest"
    val groupId = "streaming_test"
    val zkHosts = "com1:2181,com2:2181,com3:2181,com4:2181,com5:2181/kafka"
    val topicMap = topics.split(",").map((_, 4)).toMap
//    println(topicMap)
    val messages = KafkaUtils.createStream(ssc, zkHosts, groupId, topicMap).map(_._2)
    messages.foreachRDD(rdd => {
      if(!rdd.isEmpty()) {
        println("rdd: " + rdd)
        println("rdd数据长度: " + rdd.count())
        rdd.foreach(info => {
          // foreach里面的消息是分布在多台机器的多个实例处理
          println("msg: " + info)
          val pInfo = MessageProcessUtil.messageProcess(info)
          println(pInfo)
        })
//        rdd.foreachPartition(infos => {
//          // infos都落在同一个进程处理
//          println("消息条数: " + infos.length)
//          infos.foreach(info => {
//            println("msg: " + info)
//          })
//        })
      }
    })
    val words = messages.flatMap(_.split(" "))
    val counts = words.map(x => (x, 1L))
                 .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}