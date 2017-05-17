package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.utilities.DFUtils

class Table(val name: String, df: DataFrame) {

  def this(sqlContext: SQLContext,
            inputBasePath: String,
            name: String) =
    this(name, DFUtils.readParquet(sqlContext, inputBasePath + "/" + name))



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

  def getMonths: Array[String] = df.select(col("year"), col("month")).distinct().collect().map(row => row.getInt(1).toString + "-" + row.getInt(0).toString)

  def filterByYear(year: Int): DataFrame = df.filter(col("year") === year)

  def filterByYearAndAnnotate(year: Int, ignoreAnnotateColumns: List[String]): DataFrame = {
    filterByYear(year)
      .drop("year")
      .addPrefix(name, ignoreAnnotateColumns)
  }

  def filterByMonth(year: Int, month: Int): DataFrame = df.filter(col("year") === year && col("month") === month)

  def filterByMonthAndAnnotate(year: Int, month: Int, ignoreAnnotateColumns: List[String]): DataFrame = {
    filterByMonth(year, month)
      .drop("year")
      .drop("month")
      .addPrefix(name, ignoreAnnotateColumns)
  }

  def write(path: String): Unit = {
    this.df
      .write
      .parquet(path + "/" + this.name)
  }

  private[flattening] def getDF: DataFrame = this.df
}

object Table {
  def build(sqlContext: SQLContext,
              inputBasePath: String,
              name: String): Table = {
    val df = DFUtils.readParquet(sqlContext, inputBasePath + "/" + name)
    new Table(name, df)
  }
}