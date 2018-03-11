package net.zengzhiying

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.InetAddress

object DataSortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("data_sort_demo")
//               .setMaster("local")
    val sc = new SparkContext(conf)
    // hdfs://index:8020/
    val numberDataFile = sc.textFile("hdfs://index:8020/number_data.txt")
//    numberDataFile.foreach(f => {
//      println("f: " + f)
//    })
    val count = numberDataFile.count()
    val addr = InetAddress.getLocalHost
    println("ip addr: " + addr.getHostAddress + "--->hostname: " + addr.getHostName + "--->number: " + count)
    // int转换处理
    var numbers = numberDataFile.map(_.toInt)
    // sortBy对list排序
    // spark排序不改变以前的numbers rdd本身也是不可写的
    // sortBy 第二个参数true:升序 false:降序 默认是升序
    // 第三个参数是排序后的partition数量 默认是2 设置为1表示排序到1个分区
    var sortNumbers = numbers.sortBy(x => x, true).collect()
    sortNumbers.foreach(println)
    // sortByKey 对字典排序
    val k = sc.parallelize(List(3, 6, 2, 1, 9))
    // 第二个参数指定分片个数
    val v = sc.parallelize(List("php","python", "java", "machine learning", "bigdata"))
    val dic = k.zip(v)
//    dic.foreach(f=>{
//      println(f._1 + "->" + f._2)
//    })
    val sortDict = dic.sortByKey().collect()
    sortDict.filter(_._1 > 0).foreach(f => {
      println(f._1 + "->" + f._2)
    })
    sc.stop()
  }
}