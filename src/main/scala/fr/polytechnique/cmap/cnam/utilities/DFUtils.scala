package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._

import com.typesafe.config.Config

import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig

/**
  * Created by sathiya on 27/02/17.
  */
object DFUtils {

  def readCSV(sqlContext: SQLContext,
              inputPath: Seq[String],
              dateFormat: String = "dd/MM/yyyy"): DataFrame = {
    sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath: _*)
  }


  def applySchema(df: DataFrame, columnsType: Map[String,String], dateFormat: String): DataFrame = {

    val inputColumns = df.columns

    val typedColumns = inputColumns.map {
      columnName =>
        val columnType = columnsType(columnName)

        if(columnType == "Date") {
          to_date(
            unix_timestamp(col(columnName), dateFormat)
              .cast(TimestampType)
          ).as(columnName)
        } else {
          col(columnName)
            .cast(columnType)
            .as(columnName)
        }
    }

    df.select(typedColumns: _*)
  }

  implicit class CSVDataFrame(dataFrame: DataFrame) {


    override def toString: String = dataFrame.toString

    /**
      * This method compares the equality of two data frames. To qualify equality, the rows
      * can be in different order but the columns should be in the right order.
      */
    //TODO: This implementation may not be efficient, we should use Karau method from this link:
    // https://github.com/holdenk/spark-testing-base/blob/master/src/main/pre-2.0/scala/com/holdenkarau/spark/testing/DataFrameSuiteBase.scala
    def sameAs(other: DataFrame): Boolean = {

      def checkDuplicateRows: Boolean = {
        val dataFrameGroupedByRows = dataFrame.groupBy(
          dataFrame.columns.head,
          dataFrame.columns.tail: _*).count()
        val otherGroupedByRows = other.groupBy(
          other.columns.head,
          other.columns.tail: _*).count()

        dataFrameGroupedByRows.except(otherGroupedByRows).count() == 0 &&
          otherGroupedByRows.except(dataFrameGroupedByRows).count == 0
      }

      def columnNameType(schema: StructType): Seq[(String, DataType)] = {
        schema.fields.map((field: StructField) => (field.name, field.dataType))
      }

      columnNameType(dataFrame.schema) == columnNameType(other.schema) &&
        checkDuplicateRows
    }
  }
}