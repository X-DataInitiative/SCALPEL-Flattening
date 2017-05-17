package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.typesafe.config.Config

import scala.util.Try

class FlatTable(sqlContext: SQLContext, config: Config) {

  import FlatteningConfig.JoinConfig

  val inputBasePath: String = config.inputPath
  val mainTable: Table = new Table(sqlContext, inputBasePath, config.mainTableName)
  val tablesToJoin: List[Table] = config.tablesToJoin.map(
    tableName =>
      new Table(sqlContext, inputBasePath, tableName)
  )
  val outputBasePath: String = config.outputJoinPath
  val foreignKeys: List[String] = config.foreignKeys
  val tableName: String = config.nameFlatTable

  def flatTablePerYear: Array[Table] = mainTable.getYears.map(joinByYear)

  def flatTablePerMonth: Array[Table] = mainTable.getMonths.map(joinByMonth)

  def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .foldLeft(mainTable.filterByYear(year))(joinFunction)

    new Table(name, joinedDF)
  }

  def joinByMonth(date: String): Table = {
    val month = date.split("-")(0).toInt
    val year = date.split("-")(1).toInt
    val name = s"$tableName/year=$year/month=$month"
    val joinedDF = tablesToJoin
      .map(table => table.filterByMonthAndAnnotate(year, month, foreignKeys))
      .foldLeft(mainTable.filterByMonth(year, month))(joinFunction)

    new Table(name, joinedDF)
  }

  val joinFunction: (DataFrame, DataFrame) => DataFrame = (accumulator, tableToJoin) => {
      accumulator.join(tableToJoin, foreignKeys, "left_outer")
  }

  def writeAsParquet: Unit = {
    if (Try(mainTable.getDF("month"))isSuccess) {
      flatTablePerMonth
        .foreach(_.write(outputBasePath))
    } else {
      flatTablePerYear
        .foreach(_.write(outputBasePath))
    }
  }
}
