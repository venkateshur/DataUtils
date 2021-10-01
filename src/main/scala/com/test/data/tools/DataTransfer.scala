package com.test.data.tools

import com.test.data.tools.utils.Common
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object  DataTransfer extends App {

private val logger = LoggerFactory.getLogger("DataTransfer")

  implicit val spark: SparkSession = Common.initSparkSession("Data Transfer")
  val tableName = args(0)
  val outputPath = args(1)
  val outputFileFormat = args(2)
  val numPartitions = if (args.length == 4) Some(args(3).toInt) else None
  Try {
    val inputTable = Common.readTable(tableName)
    Common.writeFile(inputTable, outputPath, outputFileFormat, numPartitions)
  } match {
    case Success(_) =>
      logger.info("Data Transfer Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
    logger.error("Data transfer failed with error: " + exception.getLocalizedMessage)
    throw exception
  }
}
