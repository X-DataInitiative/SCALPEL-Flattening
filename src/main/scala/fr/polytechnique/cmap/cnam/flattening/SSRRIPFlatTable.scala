// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
/*import fr.polytechnique.cmap.cnam.flattening.PMSIFlatTable
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit}*/
import org.apache.spark.sql.{DataFrame, SQLContext}

class SSRRIPFlatTable(sqlContext: SQLContext, config: JoinTableConfig)
  extends PMSIFlatTable(sqlContext: SQLContext, config: JoinTableConfig) {

  val patientKeys: List[String] = config.joinKeysPatient.get

  override def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYear(year).drop("year"),
      pmsiPatientTable.filterByYearAndAnnotate(year, patientKeys)).cache()
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .map(df => joinFunction(centralTableDF, df)).reduce(unionWithDifferentSchemas)
    new Table(name, joinedDF)
  }

  override def joinByYearAndDate(year: Int, month: Int, monthCol: String): Table = {
    val name = s"$tableName/year=$year/month=$month"
    val centralTableDF: DataFrame = joinFunctionPatientKeys(mainTable.filterByYearAndMonth(year, month, monthCol).drop("year"),
      pmsiPatientTable.filterByYearMonthAndAnnotate(year, month, patientKeys, monthCol))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .map(table => joinFunction(centralTableDF, table))
      .reduce(unionWithDifferentSchemas)
    new Table(name, joinedDF)
  }

  val joinFunctionPatientKeys: (DataFrame, DataFrame) => DataFrame =
    (accumulator, tableToJoin) => accumulator.join(tableToJoin, patientKeys, "left_outer")

}
