// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening.join

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
import fr.polytechnique.cmap.cnam.flattening.tables.{FlatTable, SingleTable, Table}

class FlatTableJoiner(sqlContext: SQLContext, config: JoinTableConfig, format: String = "parquet") extends Joiner[SingleTable, FlatTable](sqlContext, config, format) {

  val foreignKeys: List[String] = config.joinKeys

  val joinFunction: (DataFrame, DataFrame) => DataFrame =
    (accumulator, tableToJoin) => accumulator.join(tableToJoin, foreignKeys, "left_outer")

  val mainTable: Table[SingleTable] = SingleTable(sqlContext, config.inputPath.get, config.mainTableName, format)
  val tablesToJoin: List[Table[SingleTable]] = config.tablesToJoin.map(
    tableName =>
      SingleTable(sqlContext, config.inputPath.get, tableName, format)
  )
  val referencesToJoin: List[(Table[SingleTable], FlatteningConfig.Reference)] = config.refsToJoin.map {
    refConfig => (SingleTable(sqlContext, refConfig.inputPath.get, refConfig.name, format), refConfig)
  }

  override def joinRefs(table: Table[FlatTable]): Table[FlatTable] = {
    referencesToJoin.foldLeft(table) {
      case (flatTable, (refTable, refConfig)) =>
        val refDF = refTable.annotate(List.empty[String])
        val cols = refConfig.joinKeysTuples.map {
          case (ftKey, refKey) => flatTable.df.col(ftKey) === refDF.col(refKey)
        }.reduce(_ && _)
        FlatTable(flatTable.name, flatTable.df.join(refDF, cols, "left"))
    }
  }

  override def joinByYear(name: String, year: Int): Table[FlatTable] = {
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .foldLeft(mainTable.filterByYear(year))(joinFunction)

    FlatTable(name, joinedDF)
  }

  override def joinByYearAndDate(name: String, year: Int, month: Int, monthCol: String): Table[FlatTable] = {
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .foldLeft(mainTable.filterByYearAndMonth(year, month, monthCol))(joinFunction)

    FlatTable(name, joinedDF)
  }

}
