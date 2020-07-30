// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening.join

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
import fr.polytechnique.cmap.cnam.flattening.tables.{FlatTable, Table}

class SSRRIPFlatTableJoiner(sqlContext: SQLContext, config: JoinTableConfig, format: String = "parquet")
  extends PMSIFlatTableJoiner(sqlContext, config, format) {

  val patientKeys: List[String] = config.joinKeysPatient.get

  override def joinByYear(name: String, year: Int): Table[FlatTable] = {
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYearAndAnnotate(year, foreignKeys),
      pmsiPatientTable.filterByYear(year).drop("year"))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .map(df => joinFunction(centralTableDF, df)).reduce(unionWithDifferentSchemas)
    FlatTable(name, joinedDF)
  }

  override def joinByYearAndDate(name: String, year: Int, month: Int, monthCol: String): Table[FlatTable] = {
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol).drop("year"),
      pmsiPatientTable.filterByYearAndMonth(year, month, monthCol))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .map(table => joinFunction(centralTableDF, table))
      .reduce(unionWithDifferentSchemas)
    FlatTable(name, joinedDF)
  }

  val joinFunctionPatientKeys: (DataFrame, DataFrame) => DataFrame =
    (accumulator, tableToJoin) => accumulator.join(tableToJoin, patientKeys, "left_outer")

}
