package net.zengzhiying

import org.apache.spark.sql.SparkSession

object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                .builder()
                .appName("word_count_demo")
//                .master("local")
                .getOrCreate()
    val texts = spark.read.textFile("hdfs://index:8020/wordcount")
    val counts = texts.rdd.flatMap(line => lineSplit(line)).map((_, 1)).reduceByKey(add(_, _))
    counts.collect().foreach(println)
    counts.saveAsTextFile("hdfs://index:8020/wordcount_output")
    spark.close()
  }
  
  def lineSplit(line: String): Array[String]= {
    return line.split(" ")
  }
  
  def add(a: Int, b: Int): Int = {
    return a + b
  }
}