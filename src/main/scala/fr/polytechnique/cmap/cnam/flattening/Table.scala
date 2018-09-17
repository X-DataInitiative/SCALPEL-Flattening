package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import fr.polytechnique.cmap.cnam.utilities.DFUtils

class Table(val name: String, val df: DataFrame) {

  implicit class TableHelper(df: DataFrame) {

    def addPrefix(prefix: String, except: List[String]): DataFrame = {
      val renamedColumns = df.columns.map {
        case colName if !except.contains(colName) => prefix + "__" + colName
        case keyCol => keyCol
      }

      df.toDF(renamedColumns: _*)
    }
  }

  def getYears: Array[Int] = df.select(col("year")).distinct.collect().map(_.getInt(0))

  def filterByYear(year: Int): DataFrame = df.filter(col("year") === year)

  def filterByYearAndMonth(year: Int, month: Int, colDate: String): DataFrame = df.filter(col("year") === year).filter(functions.month(col(colDate)) === month)

  def filterByYearAndAnnotate(year: Int, ignoreAnnotateColumns: List[String]): DataFrame = {
    filterByYear(year)
      .drop("year")
      .addPrefix(name, ignoreAnnotateColumns)
  }

  def filterByYearMonthAndAnnotate(year: Int, month: Int, ignoreAnnotateColumns: List[String], dateCol: String): DataFrame = {
    //logger.info("count table : " + df.count())
    val df1 = filterByYear(year)
      .drop("year")
    //logger.info("df1: " + df1.count())
    df1.withColumn("month", functions.month(col(dateCol)))
      .filter(col("month") === month)
      .drop("month")
      .addPrefix(name, ignoreAnnotateColumns)

  }
}

object Table {
  def build(sqlContext: SQLContext,
              inputBasePath: String,
              name: String): Table = {
    val df = DFUtils.readParquet(sqlContext, inputBasePath + "/" + name)
    new Table(name, df)
  }
}