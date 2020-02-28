// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig

class SSRRIPFlatTable(sqlContext: SQLContext, config: JoinTableConfig)
  extends PMSIFlatTable(sqlContext: SQLContext, config: JoinTableConfig) {

  val patientKeys: List[String] = config.joinKeysPatient.get

  override def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYearAndAnnotate(year, foreignKeys),
      pmsiPatientTable.filterByYear(year).drop("year")).cache()
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .map(df => joinFunction(centralTableDF, df)).reduce(unionWithDifferentSchemas)
    new Table(name, joinedDF)
  }

  override def joinByYearAndDate(year: Int, month: Int, monthCol: String): Table = {
    val name = s"$tableName/year=$year/month=$month"
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYearMonthAndAnnotate(year, month, foreignKeys,  monthCol).drop("year"),
      pmsiPatientTable.filterByYearAndMonth(year, month, monthCol))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .map(table => joinFunction(centralTableDF, table))
      .reduce(unionWithDifferentSchemas)
    new Table(name, joinedDF)
  }

  val joinFunctionPatientKeys: (DataFrame, DataFrame) => DataFrame =
    (accumulator, tableToJoin) => accumulator.join(tableToJoin, patientKeys, "left_outer")

}
