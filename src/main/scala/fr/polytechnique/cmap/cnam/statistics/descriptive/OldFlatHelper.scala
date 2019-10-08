// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.utilities.DFUtils

object OldFlatHelper {

  implicit class ImplicitDF(df: DataFrame) {

    final val OldDelimiter: String = "\\."
    final val NewDelimiter: String = "__"

    def changeColumnNameDelimiter: DataFrame = {
      val renamedColumns = df.columns.map(columnName => {
        val splittedColName = columnName.split(OldDelimiter)
        if (splittedColName.size == 2) {
          col("`" + columnName + "`").as(splittedColName(0) + NewDelimiter + splittedColName(1))
        } else {
          col(columnName)
        }
      })

      df.select(renamedColumns: _*)
    }

    def changeSchema(
        schema: List[TableSchema],
        mainTableName: String,
        dateFormat: String = "dd/MM/yyyy"): DataFrame = {

      val unknownColumnNameType = Map("HOS_NNE_MAM" -> "String")

      val flatSchema: Map[String, String] = schema.map(tableSchema =>
        annotateJoiningTablesColumns(tableSchema, mainTableName)
      ).reduce(_ ++ _) ++ unknownColumnNameType

      DFUtils.applySchema(df, flatSchema, dateFormat)
    }

    def annotateJoiningTablesColumns(
        tableSchema: TableSchema,
        mainTableName: String): Map[String, String] = {

      val tableName: String = tableSchema.tableName
      val columnTypeMap: Map[String, String] = tableSchema.columnTypes

      tableName match {
        case `mainTableName` => columnTypeMap
        case _ => columnTypeMap.map {
          case (colName, colType) => (prefixColName(tableName, colName), colType)
        }
      }
    }

    def prefixColName(tableName: String, columnName: String): String = {
      tableName + NewDelimiter + columnName
    }

    def prefixColumnNames(prefix: String, separator: String = "__"): DataFrame = {
      df.columns.foldLeft(df) {
        (currentDF, colName) => currentDF.withColumnRenamed(colName, prefix + separator + colName)
      }
    }
  }
}
