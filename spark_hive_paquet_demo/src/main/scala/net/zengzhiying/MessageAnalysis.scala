package net.zengzhiying

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.relaxng.datatype.Datatype

object MessageAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                     .setAppName("csv_analysis")
                     .setMaster("local")
    
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    
    val df = sqlContext.read.format("csv")
                            .option("sep", ",")
                            .option("inferSchema", "true")
                            .option("header", "true")
                            .load("message.csv")
    df.show()
    
    df.printSchema()
    
    df.select("id").foreach(f => {
      println(f)
    })
    sparkContext.stop()
  }
}