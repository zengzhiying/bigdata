package org.example

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer

/**
 * 实现按照批量和超时时间两个参数读取 Kafka，即满足批量时返回读取到的结果，如果在指定时间内未满足批量直接将当前已有的结果返回
 */
object BatchAndTimeoutReaderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark structured streaming batch read")
      .getOrCreate()
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .load()

    val dfs = ListBuffer[DataFrame]()
    val maxSize = 100
    var currentSize: Long = 0

    val latch = new CountDownLatch(1)
    val startTime = System.currentTimeMillis()
    val timeoutMs = 60000

    val query = df.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => {
        println(s"batch id: $batchId")
        val cacheDf = batchDf.cache()
        cacheDf.show()
        val dfCount = cacheDf.count()
        currentSize += dfCount
        dfs += cacheDf
        if(currentSize >= maxSize) {
          latch.countDown()
        } else {
          val elapsed = System.currentTimeMillis() - startTime
          if (elapsed >= timeoutMs) {
            latch.countDown()
          }
        }
      })
      // 10s 触发一次
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
    // query.awaitTermination()
    new Thread(() => {
      Thread.sleep(timeoutMs)
      println(s"count down ${latch.getCount}")
      if(latch.getCount == 1) {
        latch.countDown()
      }
    }).start()
    latch.await()
    query.stop()

    if(dfs.isEmpty) {
      println("dfs is empty")
    } else {
      val df = dfs.tail.foldLeft(dfs.head) { (df, df1) =>
        df.union(df1)
      }
      println(s"total count: ${df.count()}")
      dfs.foreach(_.unpersist())
    }

    // 使用 Kafka 读取时本身的参数触发器实现
    // 第一次只要有数据，上来就会触发一次，不管几条
    val df2 = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.81:9092")
      .option("subscribe", "testMessage")
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .option("maxOffsetsPerTrigger", "1000")
      .option("minOffsetsPerTrigger", "100")
      .option("maxTriggerDelay", "1m")
      .load()

    val dataFrameBuffer = ListBuffer[DataFrame]()

    val queryRef = new AtomicReference[StreamingQuery]()
    val query2 = df2.writeStream.foreachBatch((batchDf: DataFrame, batchId: Long) => {
      println(s"batchId $batchId")
      val cacheDf = batchDf.cache()
      cacheDf.show()
      val curCount = cacheDf.count()
      println(s"df count: $curCount")
      dataFrameBuffer += cacheDf
      currentSize += curCount
      if(currentSize >= 50) {
        // 超过条数直接结束 query 直接通过 AtomicReference 控制，不需要再开启额外的 CountDown 参数
        queryRef.get().stop()
      }
    }).start()

    queryRef.set(query2)
    // 使用参数阻塞等待，不需要另外开启线程
    println(query2.awaitTermination(65000))
    query2.stop()
    println(query2.awaitTermination())

    if(dataFrameBuffer.nonEmpty) {
      val df = dataFrameBuffer.tail.foldLeft(dataFrameBuffer.head) { (df, df1) =>
        df.union(df1)
      }
      println(s"total count: ${df.count()}")
      dataFrameBuffer.foreach(_.unpersist())
    }

  }
}
