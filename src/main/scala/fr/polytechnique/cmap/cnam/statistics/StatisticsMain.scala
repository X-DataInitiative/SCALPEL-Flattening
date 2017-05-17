package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.{Main, utilities}

case class TableSchema(tableName: String, columnTypes: Map[String, String])

object StatisticsMain extends Main {

  override def appName = "Statistics"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    import utilities.DFUtils._
    val tablesSchema: List[TableSchema] = FlatteningConfig.columnTypes.map(x =>
      TableSchema(x._1, x._2.toMap)).toList

    import OldFlatHelper.ImplicitDF
    import StatisticsConfig.StatConfig

    StatisticsConfig.oldFlatConfig.foreach { conf =>
      logger.info(s"Computing Statistics on the old Flat: ${conf.flatTableName}")
      println(conf.prettyPrint)

      readParquet(sqlContext, conf.inputPath)
        .drop("key")
        .changeColumnNameDelimiter
        .changeSchema(tablesSchema, conf.mainTableName, conf.dateFormat)
        .writeStatistics(conf.statOutputPath)
    }

    StatisticsConfig.newFlatConfig.foreach { conf =>
      logger.info(s"Computing Statistics on the new Flat: ${conf.flatTableName}")
      println(conf.prettyPrint)

      readParquet(sqlContext, conf.inputPath)
        .drop("year")
        .writeStatistics(conf.statOutputPath)
    }

    None
  }
}
