package com.test.data.tools

import com.test.data.tools.utils.Common
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
    val tablesResultBaseDir = args(1)
    val outputPath = args(2)



    val tableData = spark.read.csv(args(0)).as[TableData].collect().toSeq
    val results = tableData.map(tableData => {
      val sourceTableResultDf = spark.read.csv(tablesResultBaseDir + "/" + "table_name=" + tableData.tableName)
      val targetTableDf = spark.read.table(tableData.tableName).persist(StorageLevel.MEMORY_AND_DISK)
      targetTableDf.createOrReplaceTempView(s"${tableData.tableName}_temp")
      val targetTableResultDf = spark.sql(tableData.query.stripMargin.replace(tableData.tableName, tableData.tableName + "_temp"))

      val sourceToTargetDifference = castToString(sourceTableResultDf).except(castToString(targetTableResultDf)).persist(StorageLevel.MEMORY_AND_DISK)

      val targetTableCount = targetTableDf.count()
      val schemaMatches = if(tableData.schema.equalsIgnoreCase(targetTableDf.schema.simpleString)) "MATCHED" else "NOT MATCHED"
      val countMatches = if (tableData.count == targetTableDf.count()) "MATCHED" else "NOT MATCHED"
      val dataMatches = if (sourceToTargetDifference.take(1).isEmpty) "MATCHED" else "NOT MATCHED"
      val differencePath = if (dataMatches == "MATCHED") "" else outputPath
      Map(sourceToTargetDifference.withColumn("table_name", lit(tableData.tableName)) ->
        TableResult(tableData.tableName, tableData.count, targetTableCount, countMatches, dataMatches,
        differencePath + "/table_name=" + tableData.tableName, schemaMatches))

    }).reduceLeft(_ ++ _)
    val resultsDf = results.values.toSeq.toDS()
    resultsDf.write.option("header", "true").mode("overwrite").csv(outputPath + "/" + "comparison_results")

   results.keys.foreach(df =>
       df.orderBy(df.columns.map(col):_*).coalesce(1)
         .write.partitionBy("table_name").option("header", "true").mode("overwrite").csv(outputPath))
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

