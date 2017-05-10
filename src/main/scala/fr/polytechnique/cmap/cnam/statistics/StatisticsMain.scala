package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.DFUtils
import fr.polytechnique.cmap.cnam.{Main, statistics, utilities}

object StatisticsMain extends Main {

  override def appName = "Statistics"

  implicit class OldFlatHelper(data: DataFrame) {

    final val OldDelimiter: String = "\\."
    final val NewDelimiter: String = "__"

    def changeColumnNameDelimiter: DataFrame  = {
      val renamedColumns = data.columns
        .map(
          columnName => {
            val splittedColName = columnName.split(OldDelimiter)
            if (splittedColName.size == 2)
              col("`" + columnName + "`").as(splittedColName(0) + NewDelimiter + splittedColName(1))
            else
              col(columnName)
          })

      data.select(renamedColumns: _*)
    }

    def changeSchema(schema: Map[String, List[(String,String)]],
                     mainTableName: String,
                     dateFormat: String = "dd/MM/yyyy"): DataFrame = {

      val unknownColumnNameType = Map("HOS_NNE_MAM" -> "String")
      val flatSchema: Map[String, String] = schema
        .map(tableColumns => annotateJoiningTablesColumns(tableColumns, mainTableName))
        .reduce(_ ++ _) ++ unknownColumnNameType

      DFUtils.applySchema(data, flatSchema, dateFormat)
    }

    def annotateJoiningTablesColumns(tableSchema: (String, List[(String,String)]),
                                     mainTableName: String): Map[String, String] = {
      val tableName: String = tableSchema._1
      val columnTypeList: List[(String, String)] = tableSchema._2

      tableName match {
        case `mainTableName` => columnTypeList.toMap
        case _ => columnTypeList.map(x => (prefixColName(tableName, x._1), x._2)).toMap
      }
    }

    def prefixColName(tableName: String, columnName: String): String = {
      tableName + NewDelimiter + columnName
    }

    import statistics.CustomStatistics._
    def computeStatistics: DataFrame = data.customDescribe(data.columns)

    def writeStatistics(outputPath: String): Unit = {
      data
        .computeStatistics
        .write
        .parquet(outputPath)
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    import utilities.DFUtils._
    val tablesSchema: Map[String, List[(String,String)]] = FlatteningConfig.columnTypes

    import StatisticsConfig.StatConfig

    StatisticsConfig.oldFlatConfig.foreach {conf =>
      logger.info(s"Computing Statistics on the old Flat: ${conf.flatTableName}")
      println(conf.prettyPrint)

      readParquet(sqlContext, conf.inputPath)
        .drop("key")
        .changeColumnNameDelimiter
        .changeSchema(tablesSchema, conf.mainTableName, conf.dateFormat)
        .writeStatistics(conf.statOutputPath)
    }

    StatisticsConfig.newFlatConfig.foreach {conf =>
      logger.info(s"Computing Statistics on the new Flat: ${conf.flatTableName}")
      println(conf.prettyPrint)

      readParquet(sqlContext, conf.inputPath)
        .drop("year")
        .writeStatistics(conf.statOutputPath)
    }

    None
  }
}
