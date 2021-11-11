package com.test.data.tools

import com.test.data.tools.utils.Common
import com.test.data.tools.utils.Common.{loadConfig, writeCSVFile}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}


object HiveToCsv extends App {

  private val logger = LoggerFactory.getLogger("HiveToCsv")

  implicit val spark: SparkSession = Common.initSparkSession("HiveToCsv")

  val processedDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

  Try {
    val processType = args(0)
    val config = loadConfig("hive-csv").getConfig(processType)
    val tablesToProcess = config.getString("tables").split(";")
    val outputBasePath = s"${config.getString("output-path")}/processed_date=$processedDate/"
    tablesToProcess.foreach{table => {
      val inputTable = Common.readTable(table)
      writeCSVFile(inputTable, outputBasePath + table, inputTable.rdd.getNumPartitions)
    }}

  } match {
    case Success(_) =>
      logger.info("Hive to csv transfer done Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
      logger.error("Hive to csv transfer stopped with error: " + exception.getLocalizedMessage)
      throw exception
  }
}

