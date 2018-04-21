package net.zengzhiying

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ToParquet {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
                     .setAppName("to_parquet")
//                     .setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    var users: List[User] = List(User(1, "zhangsan", 23), User(2, "lisi", 25), User(3, "wanger", 28))
    
//    users = users :+ User(1, "ss", 25)
    users = users.:+(User(1, "ss", 26))
    println(users.toString())
    
    val df = sqlContext.createDataFrame(users)
    
    df.show()
    
//    df.write.mode(SaveMode.Append).csv("xx.csv")
    
    if(df.select("id").count() > 0) {
      df.write.mode(SaveMode.Overwrite).parquet("/xx.parquet")
    }
    
    sparkContext.stop()
  }
}