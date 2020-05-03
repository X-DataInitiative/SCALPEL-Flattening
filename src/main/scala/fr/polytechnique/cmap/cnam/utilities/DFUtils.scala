// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import fr.polytechnique.cmap.cnam.flattening.Flattening.logger

object DFUtils {

  def readCSV(
    sqlContext: SQLContext,
    inputPath: Seq[String]): DataFrame = {
    sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath: _*)
  }

  def readParquetAndORC(
    sqlContext: SQLContext,
    inputPath: String,
    fileFormat: String = "parquet"): DataFrame = {
    logger.info("reading files in format "+ fileFormat)

    fileFormat match {
      case "orc" => sqlContext.read.option("mergeSchema", "true").orc(inputPath)
      case _ => sqlContext.read.option("mergeSchema", "true").parquet(inputPath)
    }
  }

  def applySchema(df: DataFrame, columnsType: Map[String, String], dateFormat: String): DataFrame = {

    val inputColumns = df.columns

    val typedColumns = inputColumns.map {
      columnName =>
        val columnType = columnsType(columnName)

        if (columnType == "Date") {
          UDFs.parseDate(dateFormat).apply(col(columnName)).as(columnName)
        } else {
          col(columnName).cast(columnType).as(columnName)
        }
    }

    df.select(typedColumns: _*)
  }

  implicit class CSVDataFrame(dataFrame: DataFrame) {


    override def toString: String = dataFrame.toString


    /**
      * This method reorders the dataframe with the alphabetical order of its columns
      */
    def reorder: DataFrame = {
      val columns: Array[String] = dataFrame.columns
      val reorderedColumnNames: Array[String] = columns.sorted
      dataFrame.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
    }

    /**
      * This method compares the equality of two data frames. To qualify equality, the rows
      * can be in different order but the columns should be in the right order.
      */
    //TODO: This implementation may not be efficient, we should use Karau method from this link:
    // https://github.com/holdenk/spark-testing-base/blob/master/src/main/pre-2.0/scala/com/holdenkarau/spark/testing/DataFrameSuiteBase.scala
    def sameAs(other: DataFrame, weakComparaison: Boolean = false): Boolean = {

      def checkDuplicateRows: Boolean = {

        val dataFrameOrdered = if (weakComparaison) dataFrame.reorder else dataFrame
        val otherOrdered = if (weakComparaison) other.reorder else other

        val dataFrameGroupedByRows = dataFrameOrdered.groupBy(
          dataFrameOrdered.columns.head,
          dataFrameOrdered.columns.tail: _*
        ).count()
        val otherGroupedByRows = otherOrdered.groupBy(
          otherOrdered.columns.head,
          otherOrdered.columns.tail: _*
        ).count()

        dataFrameGroupedByRows.except(otherGroupedByRows).count() == 0 &&
          otherGroupedByRows.except(dataFrameGroupedByRows).count == 0
      }

      def columnNameType(schema: StructType): Seq[(String, DataType)] = {
        if (weakComparaison) {
          schema.fields.sortBy(_.name).map((field: StructField) => (field.name, field.dataType))
        } else {
          schema.fields.map((field: StructField) => (field.name, field.dataType))
        }
      }

      columnNameType(dataFrame.schema) == columnNameType(other.schema) &&
        checkDuplicateRows
    }

    private def saveMode(mode: String): SaveMode = mode match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "errorIfExists" => SaveMode.ErrorIfExists
      case "withTimestamp" => SaveMode.Overwrite
    }

    @scala.annotation.varargs
    def writeCSV(path: String, partitionColumns: String*)(mode: String): Unit = {
      val writer = dataFrame.coalesce(1)
        .write
        .mode(saveMode(mode))
        .option("delimiter", ",")
        .option("header", "true")
      if (partitionColumns.nonEmpty) {
        writer.partitionBy(partitionColumns: _*).csv(path)
      } else {
        writer.csv(path)
      }
    }

    @scala.annotation.varargs
    def writeParquetAndORC(path: String, partitionColumns: String*)
      (mode: String, fileFormat: String = "parquet"): Unit = {
      logger.info("writing files in format "+ fileFormat)

      val writer = dataFrame.write
        .mode(saveMode(mode))
      if (partitionColumns.nonEmpty) {
        fileFormat match {
          case "orc" => writer.partitionBy(partitionColumns: _*).orc(path)
          case _ => writer.partitionBy(partitionColumns: _*).parquet(path)
        }
      } else {
        fileFormat match {
          case "orc" => writer.orc(path)
          case _ => writer.parquet(path)
        }
      }
    }

  }

  implicit class StringPath(path: String) {

    def withTimestampSuffix(
      date: Date = new Date(),
      format: String = "/yyyy_MM_dd",
      oldSuffix: String = "/"): String = {
      path
        .stripSuffix(oldSuffix)
        .concat(new SimpleDateFormat(format).format(date))
    }

  }

}
