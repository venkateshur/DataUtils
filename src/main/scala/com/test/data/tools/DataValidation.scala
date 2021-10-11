package com.test.data.tools


import com.test.data.tools.utils.Common
import com.test.data.tools.utils.Common.{loadConfig, writeCSVFile}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}


case class TableMeta(tableName: String, statusColumns: Seq[String], query: Option[String], schemaValidation: Boolean)
case class TableSummary(tableName: String, count: Long, schema: String)

object DataValidation extends App {

  private val logger = LoggerFactory.getLogger("DataValidation")

  implicit val spark: SparkSession = Common.initSparkSession("Data Validation")

  val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  Try {
    val config = loadConfig()
    val tables = buildTables(config)
    val outputBasePath = s"${config.getString("output-path")}/processed_time=$currentTimestamp/"
    val tablesMetaData = tables.map{tableMeta => {
      //This is just for testing
      //val inputTable = spark.read.option("header", "true").option("delimiter", ";").csv("src/main/resources/in/username.csv")
      //  .persist(StorageLevel.MEMORY_AND_DISK)

      val inputTable = Common.readTable(tableMeta.tableName).persist(StorageLevel.MEMORY_AND_DISK)
      val statsResult = getStats(inputTable, tableMeta.statusColumns)
      val queryResult = tableMeta.query.fold[Option[DataFrame]](None)(query => Some(getResult(inputTable, query)))
      val schema = if (tableMeta.schemaValidation) getSchema(inputTable) else ""

      val tableOutputBasePath = s"$outputBasePath/${tableMeta.tableName}"
      val outputStatsPath = s"$tableOutputBasePath/stats"
      val queryResultsPath = s"$tableOutputBasePath/query_results"
      writeCSVFile(statsResult, outputStatsPath)
      queryResult.fold[Unit](Unit)(resultDf => writeCSVFile(resultDf, queryResultsPath))
      val tableSummary = TableSummary(tableMeta.tableName, inputTable.count(), schema)
      inputTable.unpersist()
      tableSummary
    }}
    import spark.implicits._
    val processedTablesMetaDf = tablesMetaData.toDF()
    writeCSVFile(processedTablesMetaDf, s"$outputBasePath/tables-meta")

  } match {
    case Success(_) =>
      logger.info("Data Comparison Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Comparison Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def getSchema(source: DataFrame)= {
    source.schema.simpleString
  }

  private def getStats(source: DataFrame, columns: Seq[String] = Seq())= {
    if (columns.nonEmpty) source.describe(columns:_*) else source.describe()
  }

  private def getResult(source: DataFrame, query: String)= {
    source.createOrReplaceTempView("_source")
    spark.sql(query)
  }

  private def buildTables(config: Config) = {
    import collection.JavaConverters._

    config.getConfigList("tables").asScala.toList.map(conf => {
      val tableName = conf.getString("name")
      val statsColumns = if (conf.hasPath("stats-columns"))
        conf.getString("stats-columns").split(";").toSeq
      else Seq()
      val query = if (conf.hasPath("query")) Some(conf.getString("query")) else None
      val schemaValidation = if (conf.hasPath("schema-validation"))
        conf.getBoolean("schema-validation") else false
      TableMeta(tableName, statsColumns, query, schemaValidation)
    })
  }
}

