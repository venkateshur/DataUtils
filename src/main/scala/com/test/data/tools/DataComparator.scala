package com.test.data.tools

import com.test.data.tools.utils.Common
import org.apache.spark.sql.functions.{col, md5}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

object  DataComparator extends App {

private val logger = LoggerFactory.getLogger("DataComparator")

  implicit val spark: SparkSession = Common.initSparkSession("Data Comparator")
  val tableName = args(0)
  val inputPath = args(1)
  val fileFormat = args(2)
  val numPartitions = if (args.length == 4) Some(args(4).toInt) else None
  val numOfRecords = if (args.length == 5) Some(args(5).toInt) else None

  Try {
    val inputTable = Common.readTable(tableName)
    val inputFile = Common.readFile(inputPath, fileFormat)
    val dataToCompare = numOfRecords.fold(inputFile)(records => inputFile.limit(records))
    val dataToCompareResult = dataCompare(dataToCompare, inputTable)

    if (dataToCompareResult.nonEmpty && dataToCompareResult.get.count() == 0){
      logger.info("Data comparison successful")
    } else {
      logger.info("Data comparison not successful")
    }

    //AS we are hasing the data, writing hashed data no useful
    //Common.writeFile(inputTable, outputPath, fileFormat, numPartitions)
  } match {
    case Success(_) =>
      logger.info("Data Comparison Execution Successful")
      spark.stop()
    case Failure(exception) =>
      spark.stop()
    logger.error("Data Comparison Execution with error: " + exception.getLocalizedMessage)
    throw exception
  }

  private def dataCompare(source: DataFrame, target: DataFrame, columnsToCompare: Seq[String] = Seq())= {
    val (sourceDf, targetDf) = if (columnsToCompare.nonEmpty){
      (source.select(columnsToCompare.map(col):_*) ,target.select(columnsToCompare.map(col):_*))
    } else {
      (source, target)
    }

    val selectedSourceDf = sourceDf.persist(StorageLevel.MEMORY_AND_DISK)
    val selectedTargetDf = targetDf.persist(StorageLevel.MEMORY_AND_DISK)


    val resultDf = if (selectedSourceDf.columns.map(col => col.trim.toLowerCase)
      .diff(selectedTargetDf.columns.map(col => col.trim.toLowerCase)).length == 0) {

      logger.info("Columns Names matched in source and target: " + source.columns.mkString("[", ";", "]"))

      val sourceDataCount = selectedSourceDf.count()
      val targetDataCount = selectedTargetDf.count()

      if(sourceDataCount == targetDataCount){
        logger.info(s"Records count matched in source($sourceDataCount) and target($targetDataCount)")
        Some(hashAllTheColumns(selectedSourceDf).except(hashAllTheColumns(selectedTargetDf))
          .persist(StorageLevel.MEMORY_AND_DISK))
      } else {
        logger.warn(s"Records count does not match in source($sourceDataCount) and target($targetDataCount)")
        None
      }
    } else {
      logger.warn(s"Columns Names does not match in source - ${source.columns.mkString("[", ";", "]")} \n target - ${target.columns.mkString("[", ";", "]")}")
      None
    }
    selectedSourceDf.unpersist()
    selectedTargetDf.unpersist()
    resultDf
  }

  private def hashAllTheColumns(inDataFrame: DataFrame) = {
    val columns = inDataFrame.columns
    columns.foldLeft[DataFrame](inDataFrame){(df, column) =>
      df.withColumn(column, md5(col(column).cast("string")))}
  }
}
