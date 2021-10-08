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
case class TableSummary(tableName: String, count: Long, columnsCount: Int, schema: String)

object DataValidation extends App {

  private val logger = LoggerFactory.getLogger("DataValidation")

  implicit val spark: SparkSession = Common.initSparkSession("Data Validation")

  val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

  Try {
    val config = loadConfig()
    val tables = buildTables(config)
    val outputBasePath = s"${config.getString("output-path")}/processed_time=$currentTimestamp"

    val tablesMetaData = tables.map{tableMeta => {

      val inputTable = Common.readTable(tableMeta.tableName).persist(StorageLevel.MEMORY_AND_DISK)

      val statsResult = getStats(inputTable, tableMeta.statusColumns).persist(StorageLevel.MEMORY_AND_DISK)

      val queryResult = tableMeta.query.fold[DataFrame](spark.emptyDataFrame)(query => getResult(inputTable, query))
        .persist(StorageLevel.MEMORY_AND_DISK)

      val recordsCount = inputTable.count()

      inputTable.unpersist()

      val (columnsCount, schema) = if (tableMeta.schemaValidation) getSchema(inputTable) else (0, "")

      val tableOutputBasePath = s"$outputBasePath/${tableMeta.tableName}"
      val outputStatsPath = s"$tableOutputBasePath/stats"
      val queryResultsPath = s"$tableOutputBasePath/query_results"

      writeCSVFile(statsResult, outputStatsPath)

      if (queryResult.take(1).nonEmpty) writeCSVFile(queryResult, queryResultsPath)

      val tableSummary = TableSummary(tableMeta.tableName, recordsCount, columnsCount, schema)

      statsResult.unpersist()
      queryResult.unpersist()
      tableSummary
    }}
      import spark.implicits._
      val processedTablesMetaDf = tablesMetaData.toDF()
      writeCSVFile(processedTablesMetaDf, s"$outputBasePath/tables-meta")

  } match {
    case Success(_) =>
      logger.info("Data Validation Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Data Validation Execution with error: " + exception.getLocalizedMessage)
      throw exception
  }

  private def getSchema(source: DataFrame)= {
    (source.columns.length, source.schema.treeString)
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
      val tableName = conf.getString("names")
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

