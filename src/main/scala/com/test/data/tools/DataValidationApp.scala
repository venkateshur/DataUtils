package com.test.data.tools

import com.test.data.tools.utils.Common
import com.test.data.tools.utils.Common.writeCSVFile
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}


case class TableData(tableName: String, count: Long, query: String, schema: String)
case class TableResult(tableName: String, sourceCount: Long, targetCount: Long, countMatched: String,
                       dataMatched: String, schemaMatched: String, differencePath: String)

object DataValidation extends App {

  private val logger = LoggerFactory.getLogger("DataValidationApp")

  implicit val spark: SparkSession = Common.initSparkSession("Data Validation App")

  val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  import spark.implicits._

  Try {
    val metaDataPath = args(0)
    val tablesResultBaseDir = args(1)
    val outputPath = args(2)
    val numOfPartitions = args(3).toInt

    val tableData = spark.read.csv(metaDataPath + "/" + "*.csv").as[TableData].collect().toSeq
    val results = tableData.map(tableData => {
      val sourceTableResultDf = spark.read.csv(tablesResultBaseDir + "/" + "table_name=" + tableData.tableName)
      val targetTableDf = spark.read.table(tableData.tableName).persist(StorageLevel.MEMORY_AND_DISK)
      targetTableDf.createOrReplaceTempView(s"source_temp")

      val targetTableResultDf = spark.sql(tableData.query.stripMargin.replace(tableData.tableName, "source" + "_temp"))
      val sourceToTargetDifference = castToString(sourceTableResultDf).except(castToString(targetTableResultDf))
      val orderedSourceToTargetDifference = sourceToTargetDifference.orderBy(sourceToTargetDifference.columns.map(col):_*)
      val targetTableCount = targetTableDf.count()

      val schemaMatches = if(tableData.schema.equalsIgnoreCase(targetTableDf.schema.simpleString)) "MATCHED" else "NOT MATCHED"
      val countMatches = if (tableData.count == targetTableDf.count()) "MATCHED" else "NOT MATCHED"
      val dataMatches = if (sourceToTargetDifference.take(1).isEmpty) "MATCHED" else "NOT MATCHED"
      val outputResultPath = outputPath + "/" + tableData.tableName

      writeCSVFile(orderedSourceToTargetDifference, outputResultPath)
      targetTableDf.unpersist()
      TableResult(tableData.tableName, tableData.count, targetTableCount, countMatches, dataMatches, schemaMatches,
          outputResultPath)
    })
    writeCSVFile(results.toDF(), outputPath + "/" + "comparison_report")
  } match {
    case Success(_) =>
      logger.info("Data Validation Application Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Validation Application Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def castToString(df: DataFrame) = {
    val columns = df.columns
    columns.foldLeft(df)((df, columnName) => df.withColumn(columnName, col(columnName).cast("string")) )
  }
}

