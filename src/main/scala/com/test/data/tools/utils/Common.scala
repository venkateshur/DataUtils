package com.test.data.tools.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

object Common {

  def initSparkSession(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).enableHiveSupport.getOrCreate()
  }

  def readTable(tableName:String)(implicit spark: SparkSession): DataFrame = spark.read.table(tableName)

  def readFile(inputPath:String, format: String)(implicit spark: SparkSession): DataFrame =
    spark.read.format(format).load(inputPath)

  def writeFile(inputDf: DataFrame, outputPath: String, format: String, numPartitions: Option[Int]): Unit = {
    numPartitions.fold(inputDf)(partitions => inputDf.coalesce(partitions))
      .write.mode("overwrite").format(format).save(outputPath)
  }

  def writeCSVFile(inputDf: DataFrame, outputPath: String): Unit = {
    inputDf.write.option("header", "true").format("csv").mode("overwrite")
      .save(outputPath)
  }

  def loadConfig(nameSpace: String = "data-validation") = {
    ConfigFactory.defaultApplication().getConfig(nameSpace)
  }

}
