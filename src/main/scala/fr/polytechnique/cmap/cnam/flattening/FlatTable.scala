package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.typesafe.config.Config

class FlatTable(sqlContext: SQLContext, config: Config) {

  import FlatteningConfig.JoinConfig

  val inputBasePath: String = config.inputPath
  val mainTable: Table = Table.build(sqlContext, inputBasePath, config.mainTableName)
  val tablesToJoin: List[Table] = config.tablesToJoin.map(
    tableName =>
      Table.build(sqlContext, inputBasePath, tableName)
  )
  val outputBasePath: String = config.outputJoinPath
  val foreignKeys: List[String] = config.foreignKeys
  val tableName: String = config.nameFlatTable

  def flatTablePerYear: Array[Table] = mainTable.getYears.map(joinByYear)

  def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .foldLeft(mainTable.filterByYear(year))(joinFunction)

    new Table(name, joinedDF)
  }

  val joinFunction: (DataFrame, DataFrame) => DataFrame = (accumulator, tableToJoin) => {
      accumulator.join(tableToJoin, foreignKeys, "left_outer")
  }

  def writeAsParquet: Unit = flatTablePerYear
    .foreach(
      joinedTable =>
        joinedTable.df
          .write
          .mode(SaveMode.Overwrite)
          .parquet(outputBasePath + "/" + joinedTable.name))
}
