package test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.InetAddress

object SortTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
               .setAppName("sort_test")
               .setMaster("local")
    val sc = new SparkContext(conf)
    // hdfs://index:8020/
    val numberDataFile = sc.textFile("number_data.txt")
//    fileData.foreach(f => {
//      println("f: " + f)
//    })
    val count = numberDataFile.count()
    val addr = InetAddress.getLocalHost
    println("ip addr: " + addr.getHostAddress + "--->hostname: " + addr.getHostName + "--->number: " + count)
    // int转换处理
//    var numbers = numberDataFile.map(_.toInt)
    var numbers = new Array[Int](6)
    var i = 0
    numberDataFile.foreach(f => {
      // foreach可以看成一个单独的进程分布在多个core中
      numbers(i) = f.toInt
//      println(numbers(i))
      i += 1
      if(i == count) {
        println(addr.getHostName + " 开始排序: " + numbers.length)
        bubbleSort(numbers)
        numbers.foreach(println)
      }
    })
    // 出来foreach之后i和numbers以及其他顶部变量的值全部没有改变
    println(i)
    println(numbers(2))
  }
  
  /**
   * 数组冒泡排序
   */
  def bubbleSort(numbers: Array[Int]): Array[Int] = {
    val n = numbers.length
    println("元素个数: " + n + " 排序")
    var i = 0
    var j = 0
    for(i <- 0 until n -1) {
      for(j <- 0 until n - i - 1) {
        if(numbers(j) > numbers(j + 1)) {
          val temp = numbers(j)
          numbers(j) = numbers(j + 1)
          numbers(j + 1) = temp
        }
      }
    }
    return numbers
  }
}