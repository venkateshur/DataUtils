package com.test.data.tools


import com.test.data.tools.utils.Common
import com.test.data.tools.utils.Common.{loadConfig, writeCSVFile}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}


case class TableMeta(tableName: String, query: String)
case class TableSummary(tableName: String, count: Long, query: String, schema: String)

object DataTransferApp extends App {

  private val logger = LoggerFactory.getLogger("DataTransfer")

  implicit val spark: SparkSession = Common.initSparkSession("Data Transfer")

  val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  Try {
    val config = loadConfig("data-transfer")
    val tables = buildTables(config)
    val outputBasePath = s"${config.getString("output-path")}/processed_time=$currentTimestamp/"
    val tablesMetaData = tables.map{tableMeta => {
      val inputTable = Common.readTable(tableMeta.tableName).persist(StorageLevel.MEMORY_AND_DISK)
      inputTable.createOrReplaceTempView(tableMeta.tableName + "_temp")
      val queryResult = spark.sql(tableMeta.query.replaceAll(tableMeta.tableName, tableMeta.tableName + "_temp"))
      val schema = getSchema(inputTable)
      val tableCount = inputTable.count()
      val tableSummary = TableSummary(tableMeta.tableName, tableCount, tableMeta.query, schema)
      writeCSVFile(queryResult, s"$outputBasePath/query_results/${tableMeta.tableName}")
      inputTable.unpersist()
      tableSummary
    }}
    import spark.implicits._
    val processedTablesMetaDf = tablesMetaData.toDF()
    writeCSVFile(processedTablesMetaDf, s"$outputBasePath/tables-meta")

  } match {
    case Success(_) =>
      logger.info("Data Transfer Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Transfer Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def getSchema(source: DataFrame)= {
    source.schema.simpleString
  }

  private def buildTables(config: Config) = {
    import collection.JavaConverters._

    config.getConfigList("tables").asScala.toList.map(conf => {
      val tableName = conf.getString("name")
      val query = conf.getString("query")
      TableMeta(tableName, query)
    })
  }
}

