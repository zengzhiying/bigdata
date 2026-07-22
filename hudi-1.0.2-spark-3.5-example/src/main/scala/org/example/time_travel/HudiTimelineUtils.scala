package org.example.time_travel

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object HudiTimelineUtils {
  case class CommitTime(requestedTime: String, completionTime: String)

  def getCommitTimes(spark: SparkSession, basePath: String): Seq[CommitTime] = {
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConf(spark.sparkContext.hadoopConfiguration))
      .setBasePath(basePath)
      .build()

    metaClient.getCommitsTimeline
      .filterCompletedInstants()
      .getInstants
      .asScala
      .map(instant => CommitTime(instant.requestedTime(), instant.getCompletionTime))
      .sortBy(_.completionTime)
      .toSeq
  }

  def latestCommitTime(spark: SparkSession, basePath: String): CommitTime = {
    val commits = getCommitTimes(spark, basePath)
    require(commits.nonEmpty, s"No completed commits found for $basePath")
    commits.last
  }

  def printCommitTime(label: String, commitTime: CommitTime): Unit = {
    println(s"  $label requested time : ${commitTime.requestedTime}")
    println(s"  $label completion time: ${commitTime.completionTime}")
  }
}
