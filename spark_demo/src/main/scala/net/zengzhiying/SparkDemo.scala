package net.zengzhiying

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.InetAddress

object SparkDemo {
  def main(args: Array[String]): Unit = {
    println("driver")
    val data = Array(1, 2, 3, 4, 5)
    val conf = new SparkConf()
               .setAppName("spark_demo")
               // .setMaster("local")
    val sc = new SparkContext(conf)
    println("executer.")
    println("driver data: " + data(2))
    val fileData = sc.textFile("hdfs://index:8020/data.txt")
    println("file content: ")
    println(fileData)
    fileData.foreach(f => {
      println("f: " + f)
    })
//    val f1 = fileData.map(f => {
//      f + "a"
//    })
//    f1.foreach(f => {
//      println("f1: " + f)
//    })
    val count = fileData.count()
    val addr = InetAddress.getLocalHost
    println("ip addr: " + addr.getHostAddress + "--->hostname: " + addr.getHostName + "--->number: " + count)
  }
}