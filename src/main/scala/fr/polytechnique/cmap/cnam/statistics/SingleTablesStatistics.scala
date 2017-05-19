package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._

object SingleTablesStatistics {

  // Todo: This method works only when there is no unseen keys (key colNames) in the individual DFs
  // that don't exists in the DF that contains the key (eq, PRS for DCIR, C for PMSI MCO ).
  // We should waive this restriction in the future versions.

  def describeSingleTables(singleTables: List[DataFrame]): DataFrame = {

    // Selects all the data of a column from all tables containing this column
    def selectColumnFromTables(colName: String): DataFrame = {
      singleTables
        .collect {
          case table if table.columns.contains(colName) => table.select(col(colName))
        }
        .reduce(_.union(_))
    }

    val columnsToDescribe: Set[String] = singleTables.map(_.columns).reduce(_ ++ _).toSet

    val describedColumns: List[DataFrame] = columnsToDescribe.map {
      colName => selectColumnFromTables(colName).customDescribe(distinctOnly = true)
    }.toList

    describedColumns.reduce(_.union(_))
  }
}