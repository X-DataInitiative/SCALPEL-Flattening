// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening.join

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
import fr.polytechnique.cmap.cnam.flattening.tables.{FlatTable, SingleTable, Table}

class PMSIFlatTableJoiner(sqlContext: SQLContext, config: JoinTableConfig, format: String = "parquet")
  extends FlatTableJoiner(sqlContext, config, format) {

  val pmsiPatientTable: Table[SingleTable] = SingleTable(sqlContext, config.inputPath.get, config.pmsiPatientTableName.get, format)

  /**
   * This method merge two schemas, if a column is not in the schema of the
   * DataFrame, it created the column as empty
   */
  def mergeSchemas(myCols: Set[String], allCols: Set[String]): List[Column] = {
    allCols.toList.map(colName => if (myCols.contains(colName)) col(colName) else lit(null).alias(colName))
  }

  /**
   * This method is an amelioration of the union method : if a column exists
   * in one DataFrame and not in the other, it is created as emptys
   */

  def unionWithDifferentSchemas(DF1: DataFrame, DF2: DataFrame): DataFrame = {
    val cols1 = DF1.columns.toSet
    val cols2 = DF2.columns.toSet
    val total = cols1 ++ cols2
    DF1.select(mergeSchemas(cols1, total): _*).union(DF2.select(mergeSchemas(cols2, total): _*))
  }

  override def joinByYear(name: String, year: Int): Table[FlatTable] = {
    val centralTableDF: DataFrame = joinFunction(mainTable.filterByYearAndAnnotate(year, foreignKeys),
      pmsiPatientTable.filterByYear(year).drop("year"))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .map(df => joinFunction(centralTableDF, df)).reduce(unionWithDifferentSchemas)
    FlatTable(name, joinedDF)
  }

  override def joinByYearAndDate(name: String, year: Int, month: Int, monthCol: String): Table[FlatTable] = {
    val centralTableDF: DataFrame = joinFunction(mainTable.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol).drop("year"),
      pmsiPatientTable.filterByYearAndMonth(year, month, monthCol))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .map(table => joinFunction(centralTableDF, table))
      .reduce(unionWithDifferentSchemas)
    FlatTable(name, joinedDF)
  }

}
